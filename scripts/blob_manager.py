#!/usr/bin/env python3
"""Azure Blob Storage 交互式文件管理器

用法:
    python tools/blob_manager.py                    # 交互模式
    python tools/blob_manager.py ls [path]           # 单次列目录
    python tools/blob_manager.py tree [path]         # 树形显示
    python tools/blob_manager.py du [path]           # 磁盘用量
    python tools/blob_manager.py find <pattern>      # 搜索文件名
    python tools/blob_manager.py download <remote> <local>
    python tools/blob_manager.py upload <local> <remote>
    python tools/blob_manager.py rm <path>
    python tools/blob_manager.py token              # 更新 SAS Token

配置保存在 .blob_config.json，token 过期时会自动提示更新。

环境变量 (可选覆盖):
    BLOB_SAS_URL   - Blob 容器 URL
    BLOB_SAS_TOKEN - SAS Token

依赖: pip install rich prompt_toolkit
"""

import os
import sys
import re
import json
import subprocess
import shlex
import threading
import time
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import defaultdict

from rich.console import Console
from rich.table import Table
from rich.tree import Tree as RichTree
from rich.panel import Panel
from rich.text import Text
from rich.columns import Columns
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.style import Style
from rich.markup import escape

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.history import FileHistory
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.styles import Style as PTStyle

console = Console()

# ── 配置 ──────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_ROOT / ".blob_config.json"
HISTORY_FILE = PROJECT_ROOT / ".blob_history"
BACKUP_PID_FILE = PROJECT_ROOT / ".blob_backup.pid"
BACKUP_LOG_FILE = PROJECT_ROOT / ".blob_backup.log"
BACKUP_SCRIPT_FILE = PROJECT_ROOT / ".blob_backup.sh"
BACKUP_CONFIG_FILE = PROJECT_ROOT / ".blob_backup.json"

# 默认配置（迁移时只需改这里）
DEFAULT_SAS_URL = "https://yifanyang.blob.core.windows.net/yifanyang"
DEFAULT_SAS_TOKEN = "se=2026-04-05&sp=racwdl&sv=2026-02-06&sr=c&skoid=8a589725-7622-41da-8182-8da81127dd72&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2026-03-29T06%3A52%3A33Z&ske=2026-04-05T00%3A00%3A00Z&sks=b&skv=2026-02-06&sig=6pAC3ZJjcWSmflVx1F3pG9e50Sew1rxFxVIhKy0eaoU%3D"
DEFAULT_BASE_PREFIX = "output/liyan"

# 文件类型图标映射
ICONS = {
    # 模型/权重
    ".safetensors": "🧠", ".bin": "🧠", ".pt": "🧠", ".pth": "🧠", ".ckpt": "🧠",
    # 数据
    ".json": "📋", ".jsonl": "📋", ".csv": "📊", ".parquet": "📊", ".tsv": "📊",
    # 图片
    ".png": "🖼️ ", ".jpg": "🖼️ ", ".jpeg": "🖼️ ", ".tif": "🖼️ ", ".tiff": "🖼️ ", ".bmp": "🖼️ ",
    # 文档
    ".md": "📝", ".txt": "📝", ".pdf": "📄", ".log": "📝",
    # 代码
    ".py": "🐍", ".sh": "⚙️ ", ".yaml": "⚙️ ", ".yml": "⚙️ ",
    # 压缩
    ".tar": "📦", ".gz": "📦", ".zip": "📦", ".tar.gz": "📦",
}

def _get_icon(filename):
    name = filename.lower()
    if name.endswith(".tar.gz"):
        return "📦"
    ext = os.path.splitext(name)[1]
    return ICONS.get(ext, "  ")

def _load_config():
    """从 .blob_config.json 加载配置"""
    if CONFIG_FILE.exists():
        try:
            data = json.loads(CONFIG_FILE.read_text())
            return data.get("sas_url"), data.get("sas_token"), data.get("base_prefix", "")
        except (json.JSONDecodeError, KeyError):
            pass
    return None, None, ""

def _save_config(url, token, base_prefix=""):
    """保存配置到 .blob_config.json"""
    data = {"sas_url": url, "sas_token": token}
    if base_prefix:
        data["base_prefix"] = base_prefix
    CONFIG_FILE.write_text(json.dumps(data, indent=2) + "\n")

def get_config():
    """加载配置。优先级: 环境变量 > config 文件 > 代码默认值"""
    url = os.environ.get("BLOB_SAS_URL")
    token = os.environ.get("BLOB_SAS_TOKEN")
    base_prefix = os.environ.get("BLOB_BASE_PREFIX", "")
    if not url or not token:
        u, t, bp = _load_config()
        url = url or u
        token = token or t
        base_prefix = base_prefix or bp
    # 兜底使用代码内默认值
    url = url or DEFAULT_SAS_URL
    token = token or DEFAULT_SAS_TOKEN
    base_prefix = base_prefix or DEFAULT_BASE_PREFIX
    return url, token, base_prefix.strip("/")

def _ensure_config():
    """延迟加载配置，首次调用时初始化全局变量"""
    global SAS_URL, SAS_TOKEN, BASE_PREFIX
    if SAS_URL is None:
        SAS_URL, SAS_TOKEN, BASE_PREFIX = get_config()

SAS_URL, SAS_TOKEN, BASE_PREFIX = None, None, ""

def _update_token(new_token):
    """更新 token 并保存到配置文件"""
    global SAS_TOKEN
    SAS_TOKEN = new_token
    _save_config(SAS_URL, new_token, BASE_PREFIX)
    console.print(f"  [green]✓ Token 已保存到 .blob_config.json[/]")

def _prompt_new_token():
    """交互式询问新 token 并保存"""
    _ensure_config()
    console.print("\n  [bold yellow]请输入新的 SAS Token:[/]")
    console.print("  [dim](从 Azure Portal 生成，粘贴后回车)[/]")
    try:
        new_token = input("  Token: ").strip()
    except (EOFError, KeyboardInterrupt):
        console.print("\n  [dim]已取消[/]")
        return False
    if not new_token:
        console.print("  [red]Token 不能为空[/]")
        return False
    new_token = new_token.strip('"').strip("'")
    # 清理终端转义字符（粘贴时可能混入方向键等 ANSI 序列）
    new_token = re.sub(r'\x1b\[[0-9;]*[A-Za-z]', '', new_token).strip()
    _update_token(new_token)
    _cache.invalidate()
    return True

# ── 工具函数 ──────────────────────────────────────────────────────────

def blob_url(path=""):
    """构造带 SAS token 的完整 URL，自动拼接 BASE_PREFIX（首次调用时加载配置）"""
    _ensure_config()
    path = path.strip("/")
    if BASE_PREFIX:
        full = f"{BASE_PREFIX}/{path}" if path else BASE_PREFIX
    else:
        full = path
    if full:
        return f"{SAS_URL}/{full}?{SAS_TOKEN}"
    return f"{SAS_URL}?{SAS_TOKEN}"

def run_azcopy(args, capture=True):
    cmd = ["azcopy"] + args
    if capture:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return r.stdout, r.stderr, r.returncode
    else:
        return subprocess.run(cmd, timeout=600)

def human_size(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes/1024**2:.1f} MB"
    else:
        return f"{size_bytes/1024**3:.2f} GB"

# ── Blob 列表 + 缓存 ────────────────────────────────────────────────

class BlobCache:
    """带 TTL 的简易缓存，避免重复调 azcopy list"""
    def __init__(self, ttl=60):
        self._cache = {}
        self._ttl = ttl

    def get(self, prefix):
        if prefix in self._cache:
            data, ts = self._cache[prefix]
            if time.time() - ts < self._ttl:
                return data
        return None

    def set(self, prefix, data):
        self._cache[prefix] = (data, time.time())

    def invalidate(self, prefix=None):
        if prefix is None:
            self._cache.clear()
        else:
            self._cache.pop(prefix, None)

_cache = BlobCache(ttl=90)

def list_blobs(prefix="", use_cache=True):
    """列出指定前缀下的所有 blob，返回 [(name, size_bytes, last_modified)] 列表"""
    if use_cache:
        cached = _cache.get(prefix)
        if cached is not None:
            return cached

    url = blob_url(prefix)
    with console.status("[bold cyan]正在获取文件列表...", spinner="dots"):
        stdout, stderr, rc = run_azcopy(["list", url, "--machine-readable",
                                          "--properties", "LastModifiedTime"])

    if rc != 0:
        full_err = (stdout + stderr).strip()
        is_auth_err = "AuthenticationFailed" in full_err or "403" in full_err
        # 也检测 URL 格式错误（脏 token 导致的无效 URL）
        is_parse_err = "failed to parse" in full_err.lower()
        if is_auth_err or is_parse_err:
            if is_parse_err:
                console.print(f"  [bold red]SAS Token 格式无效![/] (可能包含转义字符)")
            else:
                expiry_m = re.search(r'Key expiry \[(.+?)\]', full_err)
                if expiry_m:
                    console.print(f"  [bold red]SAS Token 已过期![/] (到期: {expiry_m.group(1)})")
                else:
                    console.print(f"  [bold red]SAS Token 认证失败![/]")
            # 交互式更新 token 并重试（最多 3 次）
            for _attempt in range(3):
                if _prompt_new_token():
                    console.print("  [dim]正在用新 token 重试...[/]")
                    return list_blobs(prefix, use_cache=False)
                else:
                    break  # 用户取消
        elif "404" in full_err or "BlobNotFound" in full_err:
            console.print(f"  [yellow]路径不存在[/]")
        else:
            err_lines = full_err.split("\n") if full_err else ["unknown error"]
            err_msg = next((l for l in err_lines if "RESPONSE" in l or "Error" in l or "error" in l),
                           err_lines[-1])
            console.print(f"  [red]azcopy list 失败:[/] {err_msg}")
        return []

    results = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("INFO:"):
            continue
        # 格式: "name; LastModifiedTime: ...; Content Length: N"
        # 或:   "name; Content Length: N; Last Modified Time: ..."
        # 先提取 Content Length
        cl_m = re.search(r'Content Length:\s*(\d+)', line)
        if not cl_m:
            continue
        size = int(cl_m.group(1))
        # 文件名是第一个 ";" 之前的部分
        name = line.split(";")[0].strip()
        if name.startswith("INFO:"):
            name = name[5:].strip()
        # 提取修改时间
        mt_m = re.search(r'(?:Last ?Modified ?Time|LastModifiedTime):\s*(.+?)(?:;|$)', line)
        mtime = mt_m.group(1).strip() if mt_m else ""
        results.append((name, size, mtime))

    _cache.set(prefix, results)
    return results

def list_dir_shallow(prefix=""):
    """用 Blob REST API + delimiter=/ 只列当前层级，不递归，速度快"""
    _ensure_config()
    full_prefix = f"{BASE_PREFIX}/{prefix}" if BASE_PREFIX else prefix
    if full_prefix and not full_prefix.endswith("/"):
        full_prefix += "/"

    dirs = {}   # dir_name -> {} (无 count/size，浅列举拿不到)
    files = []  # [(name, size, mtime)]

    marker = ""
    while True:
        # 构造 REST API URL
        params = {
            "restype": "container",
            "comp": "list",
            "delimiter": "/",
            "prefix": full_prefix,
            "maxresults": "5000",
        }
        if marker:
            params["marker"] = marker

        query = urllib.parse.urlencode(params)
        url = f"{SAS_URL}?{query}&{SAS_TOKEN}"

        try:
            with console.status("[bold cyan]正在获取文件列表...", spinner="dots"):
                req = urllib.request.Request(url)
                req.add_header("x-ms-version", "2023-01-03")
                resp = urllib.request.urlopen(req, timeout=30)
                xml_data = resp.read().decode("utf-8")
        except Exception as e:
            err_str = str(e)
            if "403" in err_str or "AuthenticationFailed" in err_str:
                console.print(f"  [bold red]SAS Token 认证失败![/]")
                for _attempt in range(3):
                    if _prompt_new_token():
                        console.print("  [dim]正在用新 token 重试...[/]")
                        return list_dir_shallow(prefix)
                    else:
                        break
            else:
                console.print(f"  [red]列目录失败:[/] {e}")
            return {}, []

        root = ET.fromstring(xml_data)
        # 命名空间可能存在也可能不存在
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"

        # 解析虚拟目录 (BlobPrefix)
        for bp in root.iter(f"{ns}BlobPrefix"):
            name_el = bp.find(f"{ns}Name")
            if name_el is not None and name_el.text:
                # 去掉 full_prefix 前缀，得到目录名
                dir_name = name_el.text[len(full_prefix):].strip("/")
                if dir_name:
                    dirs[dir_name] = {"count": "", "size": 0}

        # 解析文件 (Blob)
        for blob in root.iter(f"{ns}Blob"):
            name_el = blob.find(f"{ns}Name")
            if name_el is None or not name_el.text:
                continue
            name = name_el.text[len(full_prefix):]
            if not name or "/" in name:
                continue

            props = blob.find(f"{ns}Properties")
            size = 0
            mtime = ""
            if props is not None:
                cl = props.find(f"{ns}Content-Length")
                if cl is not None and cl.text:
                    size = int(cl.text)
                lm = props.find(f"{ns}Last-Modified")
                if lm is not None and lm.text:
                    mtime = lm.text

            files.append((name, size, mtime))

        # 翻页
        next_marker = root.find(f"{ns}NextMarker")
        if next_marker is not None and next_marker.text:
            marker = next_marker.text
        else:
            break

    return dirs, files

def list_dir(prefix=""):
    """列出当前"目录"下的文件和子目录 (递归方式，用于 du/tree/find 等需要完整数据的场景)"""
    blobs = list_blobs(prefix)

    dirs = {}   # dir_name -> {"count": N, "size": total}
    files = []

    for name, size, mtime in blobs:
        if "/" in name:
            top = name.split("/")[0]
            if top not in dirs:
                dirs[top] = {"count": 0, "size": 0}
            dirs[top]["count"] += 1
            dirs[top]["size"] += size
        else:
            files.append((name, size, mtime))

    return dirs, files

# ── 自动补全 ──────────────────────────────────────────────────────────

COMMANDS = {
    "ls": "列出目录内容",
    "cd": "切换目录",
    "tree": "树形显示",
    "du": "磁盘用量",
    "find": "搜索文件名",
    "download": "下载到本地",
    "upload": "上传到 Blob",
    "cp": "Blob 内复制",
    "mv": "Blob 内移动",
    "rm": "删除 (需确认)",
    "cat": "查看文件内容",
    "pwd": "显示当前路径",
    "refresh": "刷新缓存",
    "token": "更新 SAS Token",
    "backup": "自动备份 (start/stop/status)",
    "help": "显示帮助",
    "exit": "退出",
}

# 简写
ALIASES = {
    "dl": "download",
    "ul": "upload",
    "ll": "ls",
    "q": "exit",
    "quit": "exit",
    "..": "cd ..",
}

class BlobCompleter(Completer):
    """命令 + 远程路径自动补全"""
    def __init__(self, manager):
        self.manager = manager

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        words = text.split()

        if len(words) == 0 or (len(words) == 1 and not text.endswith(" ")):
            # 补全命令
            prefix = words[0] if words else ""
            for cmd, desc in COMMANDS.items():
                if cmd.startswith(prefix):
                    yield Completion(cmd, start_position=-len(prefix),
                                     display_meta=desc)
            for alias, target in ALIASES.items():
                if alias.startswith(prefix) and alias not in COMMANDS:
                    yield Completion(alias, start_position=-len(prefix),
                                     display_meta=f"→ {target}")
        else:
            # 补全路径参数
            cmd = words[0]
            partial = words[-1] if not text.endswith(" ") else ""

            # upload 的第一个参数是本地路径，不补全远程
            if cmd in ("upload", "ul") and len(words) <= 2 and not text.endswith(" "):
                return

            # 解析部分路径
            if "/" in partial:
                parent = partial.rsplit("/", 1)[0]
                frag = partial.rsplit("/", 1)[1]
            else:
                parent = ""
                frag = partial

            abs_parent = self.manager._abs_path(parent) if parent else self.manager.cwd
            prefix = (abs_parent + "/") if abs_parent else ""

            try:
                dirs, files = list_dir_shallow(prefix)
            except Exception:
                return

            # 补全目录
            for d in dirs:
                if d.lower().startswith(frag.lower()):
                    suffix = (parent + "/" + d + "/") if parent else (d + "/")
                    yield Completion(suffix, start_position=-len(partial),
                                     display=d + "/",
                                     display_meta="dir")
            # 补全文件
            for name, size, _ in files:
                if name.lower().startswith(frag.lower()):
                    suffix = (parent + "/" + name) if parent else name
                    yield Completion(suffix, start_position=-len(partial),
                                     display=name,
                                     display_meta=human_size(size))

# ── 命令实现 ──────────────────────────────────────────────────────────

class BlobManager:
    def __init__(self):
        self.cwd = ""

    def _abs_path(self, path):
        if not path or path == ".":
            return self.cwd
        if path == "/":
            return ""
        if path.startswith("/"):
            parts = path.strip("/").split("/")
        else:
            parts = (self.cwd.rstrip("/") + "/" + path).strip("/").split("/")
        resolved = []
        for p in parts:
            if p == "..":
                if resolved:
                    resolved.pop()
            elif p and p != ".":
                resolved.append(p)
        return "/".join(resolved)

    def _display_path(self, path=None):
        p = path if path is not None else self.cwd
        return "/" + p if p else "/"

    # ── ls ──

    def cmd_ls(self, args):
        path = self._abs_path(args[0] if args else "")
        prefix = (path + "/") if path else ""

        dirs, files = list_dir_shallow(prefix)

        if not dirs and not files:
            console.print(f"  [dim]{self._display_path(path)} 为空[/]")
            return

        table = Table(
            title=f" {self._display_path(path)}",
            title_style="bold cyan",
            show_header=True,
            header_style="bold",
            border_style="dim",
            padding=(0, 1),
            show_edge=False,
        )
        table.add_column("", width=3, justify="center")  # icon
        table.add_column("名称", style="none", no_wrap=True)
        table.add_column("大小", justify="right", style="green")
        table.add_column("详情", style="dim")

        # 目录在前
        for d in sorted(dirs):
            table.add_row(
                "📁",
                f"[bold blue]{escape(d)}/[/]",
                "",
                "",
            )

        # 文件
        for name, size, mtime in sorted(files):
            icon = _get_icon(name)
            date_str = ""
            if mtime:
                try:
                    # REST API 返回的格式: "Sun, 06 Apr 2026 12:00:00 GMT"
                    date_str = mtime.split("T")[0] if "T" in mtime else mtime
                    # 尝试截短
                    if len(date_str) > 20:
                        date_str = date_str[:22]
                except Exception:
                    pass
            table.add_row(icon, escape(name), human_size(size), date_str)

        console.print(table)

        total_size = sum(s for _, s, _ in files)
        console.print(
            f"  [dim]{len(dirs)} 目录, {len(files)} 文件"
            + (f" | 文件合计 {human_size(total_size)}" if total_size else "")
            + "[/]"
        )

    # ── cd ──

    def cmd_cd(self, args):
        if not args:
            self.cwd = ""
            console.print(f"  [cyan]→ /[/]")
            return
        target = args[0]
        if target == "..":
            # 返回上级目录
            self.cwd = "/".join(self.cwd.split("/")[:-1]) if "/" in self.cwd else ""
            console.print(f"  [cyan]→ {self._display_path()}[/]")
            return
        new_path = self._abs_path(target)
        # 验证目录是否存在（浅列举，只查当前层，快）
        prefix = (new_path + "/") if new_path else ""
        dirs, files = list_dir_shallow(prefix)
        if not dirs and not files:
            console.print(f"  [red]目录不存在: /{new_path}[/]")
            return
        self.cwd = new_path
        console.print(f"  [cyan]→ {self._display_path()}[/]")

    # ── tree ──

    def cmd_tree(self, args):
        path = self._abs_path(args[0] if args else "")
        prefix = (path + "/") if path else ""
        max_depth = 3
        if len(args) >= 2:
            try:
                max_depth = int(args[1])
            except ValueError:
                pass

        blobs = list_blobs(prefix)
        if not blobs:
            console.print(f"  [dim]{self._display_path(path)} 为空[/]")
            return

        root_label = self._display_path(path)
        tree = RichTree(f"[bold cyan]{root_label}[/]")
        nodes = {}  # path -> tree node

        for name, size, _ in sorted(blobs, key=lambda x: x[0]):
            parts = name.split("/")
            # 限制深度
            if len(parts) > max_depth + 1:
                # 只显示到 max_depth 级目录
                parts = parts[:max_depth]
                show_file = False
            else:
                show_file = True

            # 构建目录节点
            for i in range(len(parts) - (1 if show_file else 0)):
                node_path = "/".join(parts[:i+1])
                if node_path not in nodes:
                    parent_path = "/".join(parts[:i]) if i > 0 else None
                    parent = nodes[parent_path] if parent_path else tree
                    nodes[node_path] = parent.add(f"[bold blue]📁 {parts[i]}/[/]")

            # 文件叶子
            if show_file:
                parent_path = "/".join(parts[:-1]) if len(parts) > 1 else None
                parent = nodes[parent_path] if parent_path else tree
                icon = _get_icon(parts[-1])
                parent.add(f"{icon} {parts[-1]}  [dim]({human_size(size)})[/]")

        console.print(tree)

        total_size = sum(s for _, s, _ in blobs)
        console.print(f"\n  [dim]{len(blobs)} 文件, 总计 {human_size(total_size)}[/]")

    # ── du ──

    def cmd_du(self, args):
        path = self._abs_path(args[0] if args else "")
        prefix = (path + "/") if path else ""

        blobs = list_blobs(prefix)
        if not blobs:
            console.print(f"  [dim]{self._display_path(path)} 为空[/]")
            return

        summary = defaultdict(lambda: {"count": 0, "size": 0})
        for name, size, _ in blobs:
            parts = name.split("/")
            top = parts[0] + "/" if len(parts) > 1 else parts[0]
            summary[top]["count"] += 1
            summary[top]["size"] += size

        total_size = sum(s for _, s, _ in blobs)
        items = sorted(summary.items(), key=lambda x: x[1]["size"], reverse=True)

        table = Table(
            title=f" 磁盘用量: {self._display_path(path)}",
            title_style="bold cyan",
            show_header=True,
            header_style="bold",
            border_style="dim",
            show_edge=False,
        )
        table.add_column("名称", no_wrap=True)
        table.add_column("大小", justify="right", style="green")
        table.add_column("文件数", justify="right")
        table.add_column("占比", justify="right")
        table.add_column("", width=30)  # bar

        for name, info in items:
            pct = info["size"] / total_size * 100 if total_size > 0 else 0
            bar_len = int(pct / 100 * 25)
            bar = "█" * bar_len + "░" * (25 - bar_len)

            # 颜色根据大小
            if pct > 50:
                bar_style = "bold red"
            elif pct > 20:
                bar_style = "yellow"
            else:
                bar_style = "green"

            is_dir = name.endswith("/")
            display_name = f"[bold blue]{escape(name)}[/]" if is_dir else escape(name)

            table.add_row(
                display_name,
                human_size(info["size"]),
                str(info["count"]),
                f"{pct:.1f}%",
                f"[{bar_style}]{bar}[/]",
            )

        table.add_section()
        table.add_row(
            "[bold]总计[/]",
            f"[bold]{human_size(total_size)}[/]",
            f"[bold]{len(blobs)}[/]",
            "100%",
            "",
        )

        console.print(table)

    # ── find ──

    def cmd_find(self, args):
        if not args:
            console.print("  [yellow]用法: find <pattern> [path][/]")
            return

        pattern = args[0].lower()
        path = self._abs_path(args[1] if len(args) > 1 else "")
        prefix = (path + "/") if path else ""

        blobs = list_blobs(prefix)
        matches = [(n, s, m) for n, s, m in blobs if pattern in n.lower()]

        if not matches:
            console.print(f"  [dim]未找到匹配 '{pattern}' 的文件[/]")
            return

        table = Table(
            title=f" 搜索: '{pattern}'",
            title_style="bold cyan",
            show_header=True, header_style="bold",
            border_style="dim", show_edge=False,
        )
        table.add_column("", width=3)
        table.add_column("路径", no_wrap=False)
        table.add_column("大小", justify="right", style="green")

        for name, size, _ in sorted(matches):
            icon = _get_icon(name.split("/")[-1])
            full = f"/{prefix}{name}" if prefix else f"/{name}"
            # 高亮匹配部分
            highlighted = full.replace(args[0], f"[bold yellow]{args[0]}[/]")
            table.add_row(icon, highlighted, human_size(size))

        console.print(table)
        total = sum(s for _, s, _ in matches)
        console.print(f"  [dim]{len(matches)} 个匹配, 共 {human_size(total)}[/]")

    # ── cat ──

    def cmd_cat(self, args):
        """查看小文件内容 (限 1MB 以内的文本文件)"""
        if not args:
            console.print("  [yellow]用法: cat <文件路径>[/]")
            return

        remote = self._abs_path(args[0])
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=os.path.splitext(remote)[1], delete=False) as tmp:
            tmp_path = tmp.name

        try:
            with console.status("[bold cyan]正在下载..."):
                result = run_azcopy(["cp", blob_url(remote), tmp_path], capture=True)

            if result[2] != 0:
                console.print(f"  [red]下载失败[/]")
                return

            size = os.path.getsize(tmp_path)
            if size > 1024 * 1024:
                console.print(f"  [yellow]文件太大 ({human_size(size)})，请用 download 下载后查看[/]")
                return

            content = open(tmp_path, "r", errors="replace").read()
            from rich.syntax import Syntax
            ext = os.path.splitext(remote)[1].lstrip(".")
            lang = {"py": "python", "sh": "bash", "json": "json", "yaml": "yaml",
                    "yml": "yaml", "md": "markdown", "txt": "text"}.get(ext, "text")
            syntax = Syntax(content, lang, theme="monokai", line_numbers=True)
            console.print(Panel(syntax, title=f"/{remote}", border_style="cyan"))
        finally:
            os.unlink(tmp_path)

    # ── download ──

    def cmd_download(self, args):
        if len(args) < 1:
            console.print("  [yellow]用法: download <远程路径> [本地路径][/]")
            return

        remote = self._abs_path(args[0])
        local = args[1] if len(args) >= 2 else os.path.basename(remote.rstrip("/"))
        local = os.path.abspath(local)

        # 判断远程是目录还是文件
        # 检查: 如果该前缀下有多个 blob，则视为目录
        blobs = list_blobs((remote.rstrip("/") + "/"))
        is_dir = len(blobs) > 0

        src_url = blob_url(remote)
        if is_dir:
            # 目录下载: URL 加 /* 通配符，只下载内容不多套一层
            src_url = src_url.replace("?", "/*?", 1)
            os.makedirs(local, exist_ok=True)

        console.print(Panel(
            f"[bold]从:[/] /{remote}{'/' if is_dir else ''}\n[bold]到:[/] {local}",
            title="下载", border_style="cyan", width=60
        ))

        if not _confirm("开始下载?"):
            return

        result = run_azcopy(["cp", "--recursive", "--overwrite", "ifSourceNewer",
                             src_url, local], capture=False)
        if result.returncode == 0:
            console.print("  [bold green]✓ 下载完成[/]")
        else:
            console.print(f"  [bold red]✗ 下载失败[/] (exit code: {result.returncode})")

    # ── upload ──

    def cmd_upload(self, args):
        if len(args) < 2:
            console.print("  [yellow]用法: upload <本地路径> <远程路径>[/]")
            return

        local = args[0]
        remote = self._abs_path(args[1])

        if not os.path.exists(local):
            console.print(f"  [red]错误: '{local}' 不存在[/]")
            return

        is_dir = os.path.isdir(local)

        # 上传文件到目录时（远程路径以 / 结尾），自动拼接文件名
        if not is_dir and remote.endswith("/"):
            remote = remote + os.path.basename(local)

        # 目录上传: 源路径加 /* 通配符，只上传内容不多套一层
        if is_dir:
            src = os.path.join(os.path.abspath(local), "*")
            if not remote.endswith("/"):
                remote = remote + "/"
        else:
            src = local

        # 显示本地文件/目录大小
        if not is_dir:
            local_info = human_size(os.path.getsize(local))
        else:
            total = sum(f.stat().st_size for f in Path(local).rglob("*") if f.is_file())
            file_count = sum(1 for f in Path(local).rglob("*") if f.is_file())
            local_info = f"{human_size(total)} ({file_count} files)"

        console.print(Panel(
            f"[bold]从:[/] {local} ({local_info})\n[bold]到:[/] /{remote}",
            title="上传", border_style="cyan", width=60
        ))

        if not _confirm("开始上传?"):
            return

        _cache.invalidate()  # 上传后缓存失效
        result = run_azcopy(["cp", "--recursive", "--overwrite", "ifSourceNewer",
                             src, blob_url(remote)], capture=False)
        if result.returncode == 0:
            console.print("  [bold green]✓ 上传完成[/]")
        else:
            console.print(f"  [bold red]✗ 上传失败[/] (exit code: {result.returncode})")

    # ── rm ──

    def cmd_rm(self, args):
        if not args:
            console.print("  [yellow]用法: rm <路径>[/]")
            return

        remote = self._abs_path(args[0])
        console.print(Panel(
            f"[bold red]即将删除: /{remote}[/]\n[dim]此操作不可逆![/]",
            title="⚠️  删除确认", border_style="red", width=60
        ))

        try:
            answer = input("  输入 'yes' 确认删除: ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n  [dim]已取消[/]")
            return

        if answer != "yes":
            console.print("  [dim]已取消[/]")
            return

        _cache.invalidate()
        result = run_azcopy(["rm", "--recursive", blob_url(remote)], capture=False)
        if result.returncode == 0:
            console.print("  [bold green]✓ 删除完成[/]")
        else:
            console.print(f"  [bold red]✗ 删除失败[/] (exit code: {result.returncode})")

    # ── cp ──

    def _is_blob_dir(self, path):
        """检查远程路径是否是目录（有子 blob）"""
        blobs = list_blobs(path.rstrip("/") + "/")
        return len(blobs) > 0

    def _blob_to_blob_cp(self, src, dst):
        """blob-to-blob 复制，正确处理目录（不多套一层）

        azcopy blob-to-blob cp --recursive src dst 会把 src 最后一级作为子目录放到 dst 下。
        例: cp .../A .../B → B/A/...
        要实现 "A 的内容放到 B"，目标应指向 B 的父目录。
        """
        is_dir = self._is_blob_dir(src)

        if is_dir:
            src_name = src.rstrip("/").rsplit("/", 1)[-1]
            dst_stripped = dst.rstrip("/")

            if dst_stripped.endswith("/" + src_name) or dst_stripped == src_name:
                # 用户意图: cp src_dir dst_dir (同名或 dst 以 src 名结尾)
                # azcopy 会自动加一层 src_name → 目标改为 dst 的父目录
                dst_parent = dst_stripped.rsplit("/", 1)[0] if "/" in dst_stripped else ""
                dst_url = blob_url(dst_parent + "/" if dst_parent else "")
            else:
                # dst 是不同名 → azcopy 会创建 dst/src_name/
                # 没有完美方案，改用逐文件复制太慢
                # 最佳折中: 提示用户实际会产生的结构
                dst_url = blob_url(dst + "/")
                console.print(f"  [yellow]注意: blob-to-blob 目录拷贝会在目标下创建 {src_name}/[/]")

            src_url = blob_url(src)
        else:
            src_url = blob_url(src)
            dst_url = blob_url(dst)

        return src_url, dst_url, is_dir

    def cmd_cp(self, args):
        if len(args) < 2:
            console.print("  [yellow]用法: cp <源路径> <目标路径>[/]")
            return

        src = self._abs_path(args[0])
        dst = self._abs_path(args[1])

        src_url, dst_url, is_dir = self._blob_to_blob_cp(src, dst)

        console.print(Panel(
            f"[bold]从:[/] /{src}{'/' if is_dir else ''}\n[bold]到:[/] /{dst}{'/' if is_dir else ''}",
            title="复制", border_style="cyan", width=60
        ))

        if not _confirm("开始复制?"):
            return

        _cache.invalidate()
        result = run_azcopy(["cp", "--recursive", src_url, dst_url], capture=False)
        if result.returncode == 0:
            console.print("  [bold green]✓ 复制完成[/]")
        else:
            console.print(f"  [bold red]✗ 复制失败[/] (exit code: {result.returncode})")

    # ── mv ──

    def cmd_mv(self, args):
        if len(args) < 2:
            console.print("  [yellow]用法: mv <源路径> <目标路径>[/]")
            return

        src = self._abs_path(args[0])
        dst = self._abs_path(args[1])

        src_url, dst_url, is_dir = self._blob_to_blob_cp(src, dst)

        console.print(Panel(
            f"[bold]从:[/] /{src}{'/' if is_dir else ''}\n[bold]到:[/] /{dst}{'/' if is_dir else ''}\n[dim](Blob 不支持原子移动，将执行 复制→删除)[/]",
            title="移动", border_style="yellow", width=60
        ))

        if not _confirm("开始移动?"):
            return

        _cache.invalidate()
        console.print("  [dim]Step 1/2: 复制...[/]")
        r = run_azcopy(["cp", "--recursive", src_url, dst_url], capture=False)
        if r.returncode != 0:
            console.print("  [red]复制失败，移动中止[/]")
            return

        console.print("  [dim]Step 2/2: 删除源...[/]")
        r = run_azcopy(["rm", "--recursive", blob_url(src)], capture=False)
        if r.returncode == 0:
            console.print("  [bold green]✓ 移动完成[/]")
        else:
            console.print("  [yellow]⚠ 复制成功但删除源失败，请手动检查[/]")

    # ── backup ──

    def cmd_backup(self, args):
        if not args:
            _backup_status()
            console.print("\n  [dim]子命令: start | stop | restart | add | remove | list | interval[/]")
            return
        sub = args[0].lower()
        if sub == "start":
            _backup_start()
        elif sub == "stop":
            _backup_stop()
        elif sub == "restart":
            _backup_stop()
            _backup_start()
        elif sub == "add":
            _backup_add_path()
        elif sub in ("remove", "rm", "del"):
            _backup_remove_path()
        elif sub in ("list", "ls"):
            _backup_show_paths()
        elif sub == "interval":
            _backup_set_interval()
        elif sub == "status":
            _backup_status()
        else:
            console.print("  [yellow]用法: backup <start|stop|restart|add|remove|list|interval|status>[/]")

    # ── help ──

    def cmd_help(self, args=None):
        table = Table(
            title=" Blob 文件管理器 - 命令列表",
            title_style="bold cyan",
            show_header=True, header_style="bold",
            border_style="dim", show_edge=False, padding=(0, 2),
        )
        table.add_column("命令", style="bold green", no_wrap=True)
        table.add_column("说明")
        table.add_column("示例", style="dim")

        cmds = [
            ("ls [path]", "列出目录内容", "ls output/liyan"),
            ("cd [path]", "切换目录 (支持 .. /)", "cd output/liyan"),
            ("tree [path] [depth]", "树形显示 (默认深度3)", "tree . 2"),
            ("du [path]", "磁盘用量统计 (带柱状图)", "du"),
            ("find <pattern>", "模糊搜索文件名", "find safetensors"),
            ("cat <file>", "查看小文件内容 (<1MB)", "cat config.json"),
            ("", "", ""),
            ("download <remote> [local]", "下载文件/目录", "dl model/ ./my_model"),
            ("upload <local> <remote>", "上传文件/目录", "ul ./data data/new"),
            ("cp <src> <dst>", "Blob 内复制", "cp old/ new/"),
            ("mv <src> <dst>", "Blob 内移动 (复制+删除)", "mv old/ new/"),
            ("rm <path>", "删除 (输入 yes 确认)", "rm temp/"),
            ("", "", ""),
            ("pwd", "显示当前路径", ""),
            ("refresh", "刷新缓存", ""),
            ("token", "更新 SAS Token", "token"),
            ("backup", "查看备份状态", "backup"),
            ("backup start/stop", "启动/停止自动备份", "backup start"),
            ("backup restart", "重启 (更改配置后生效)", "backup restart"),
            ("backup add", "添加备份路径", "backup add"),
            ("backup remove", "移除备份路径", "backup remove"),
            ("backup list", "列出所有备份路径", "backup list"),
            ("backup interval", "修改同步间隔", "backup interval"),
            ("help", "显示此帮助", ""),
            ("exit / q", "退出", ""),
        ]

        for cmd, desc, example in cmds:
            table.add_row(cmd, desc, example)

        console.print(table)
        console.print("\n  [dim]提示: Tab 自动补全命令和路径 | ↑↓ 历史记录 | dl/ul 是 download/upload 的简写[/]")

    # ── 交互式主循环 ──

    def run_interactive(self):
        _ensure_config()
        # 欢迎信息
        root_info = f"  容器: [bold]{SAS_URL}[/]"
        if BASE_PREFIX:
            root_info += f"\n  根目录: [bold cyan]/{BASE_PREFIX}[/]"
        welcome = Panel(
            root_info + "\n"
            f"  输入 [green]help[/] 查看命令 | [green]Tab[/] 自动补全 | [green]↑↓[/] 历史",
            title="[bold]Azure Blob 文件管理器[/]",
            border_style="cyan",
            width=70,
        )
        console.print(welcome)

        # 启动时询问备份
        _backup_prompt_on_startup()

        # prompt_toolkit session
        session = PromptSession(
            history=FileHistory(str(HISTORY_FILE)),
            completer=BlobCompleter(self),
            complete_while_typing=False,
            style=PTStyle.from_dict({
                "prompt": "#00aaff bold",
                "path": "#00aaff",
            }),
        )

        while True:
            try:
                display = "/" + self.cwd if self.cwd else "/"
                prompt_text = [("class:prompt", "blob"), ("", ":"),
                               ("class:path", display), ("", " > ")]
                line = session.prompt(prompt_text).strip()
            except (EOFError, KeyboardInterrupt):
                console.print("\n  [dim]再见![/]")
                break

            if not line:
                continue

            # 支持引号内的路径
            try:
                parts = shlex.split(line)
            except ValueError:
                parts = line.split()

            cmd = parts[0].lower()
            args = parts[1:]

            # 解析别名
            if cmd in ALIASES:
                if cmd == "..":
                    cmd = "cd"
                    args = [".."]
                else:
                    cmd = ALIASES[cmd]

            handler = {
                "ls": self.cmd_ls, "cd": self.cmd_cd, "tree": self.cmd_tree,
                "du": self.cmd_du, "find": self.cmd_find, "cat": self.cmd_cat,
                "download": self.cmd_download, "upload": self.cmd_upload,
                "rm": self.cmd_rm, "cp": self.cmd_cp, "mv": self.cmd_mv,
                "backup": self.cmd_backup,
                "help": self.cmd_help,
            }.get(cmd)

            if cmd in ("exit", "quit", "q"):
                console.print("  [dim]再见![/]")
                break
            elif cmd == "pwd":
                console.print(f"  [cyan]{self._display_path()}[/]")
            elif cmd == "refresh":
                _cache.invalidate()
                console.print("  [green]✓ 缓存已刷新[/]")
            elif cmd == "token":
                _prompt_new_token()
            elif handler:
                try:
                    handler(args)
                except KeyboardInterrupt:
                    console.print("\n  [dim]已中断[/]")
                except Exception as e:
                    console.print(f"  [red]错误: {e}[/]")
            else:
                console.print(f"  [yellow]未知命令: {cmd}[/]  输入 [green]help[/] 查看帮助")

    def run_oneshot(self, cmd, args):
        if cmd == "token":
            _prompt_new_token()
            return

        handler = {
            "ls": self.cmd_ls, "tree": self.cmd_tree, "du": self.cmd_du,
            "find": self.cmd_find, "cat": self.cmd_cat,
            "download": self.cmd_download, "dl": self.cmd_download,
            "upload": self.cmd_upload, "ul": self.cmd_upload,
            "rm": self.cmd_rm, "cp": self.cmd_cp, "mv": self.cmd_mv,
            "backup": self.cmd_backup,
            "help": self.cmd_help,
        }.get(cmd)

        if handler:
            handler(args)
        else:
            console.print(f"[red]未知命令: {cmd}[/]")
            self.cmd_help()

# ── 备份功能 ──────────────────────────────────────────────────────────

def _backup_load_config():
    """加载备份配置，返回 {"paths": [...], "interval": N}"""
    if BACKUP_CONFIG_FILE.exists():
        try:
            return json.loads(BACKUP_CONFIG_FILE.read_text())
        except (json.JSONDecodeError, KeyError):
            pass
    return {"paths": [], "interval": 30}

def _backup_save_config(cfg):
    """保存备份配置"""
    BACKUP_CONFIG_FILE.write_text(json.dumps(cfg, indent=2, ensure_ascii=False) + "\n")

def _backup_is_running():
    """检查备份进程是否正在运行，返回 PID 或 None"""
    if not BACKUP_PID_FILE.exists():
        return None
    try:
        pid = int(BACKUP_PID_FILE.read_text().strip())
        os.kill(pid, 0)
        return pid
    except (ValueError, ProcessLookupError, PermissionError):
        BACKUP_PID_FILE.unlink(missing_ok=True)
        return None

def _backup_generate_script(cfg):
    """根据配置生成备份脚本"""
    _ensure_config()
    interval = cfg.get("interval", 30)
    interval_sec = interval * 60

    cp_commands = ""
    for i, entry in enumerate(cfg["paths"], 1):
        local = entry["local"]
        remote = entry["remote"]
        if BASE_PREFIX:
            full_remote = f"{BASE_PREFIX}/{remote}"
        else:
            full_remote = remote
        remote_url = f"{SAS_URL}/{full_remote}/?{SAS_TOKEN}"
        cp_commands += f"""
    # ── 路径 {i}: {local} ──
    _start_{i}=$(date +%s)
    _output_{i}=$(azcopy cp --recursive --overwrite ifSourceNewer \\
        "{local}" \\
        "{remote_url}" 2>&1)
    _rc_{i}=$?
    _end_{i}=$(date +%s)
    _elapsed_{i}=$(( _end_{i} - _start_{i} ))

    _done_{i}=$(echo "$_output_{i}" | grep -oP 'Number of File Transfers Completed:\\s*\\K\\d+' || echo "0")
    _failed_{i}=$(echo "$_output_{i}" | grep -oP 'Number of File Transfers Failed:\\s*\\K\\d+' || echo "0")
    _skipped_{i}=$(echo "$_output_{i}" | grep -oP 'Number of File Transfers Skipped:\\s*\\K\\d+' || echo "0")
    _bytes_{i}=$(echo "$_output_{i}" | grep -oP 'Total Number of Bytes Transferred:\\s*\\K\\d+' || echo "0")

    if [ "$_bytes_{i}" -ge 1073741824 ] 2>/dev/null; then
        _size_{i}=$(awk "BEGIN{{printf \\"%.2f GB\\", $_bytes_{i}/1073741824}}")
    elif [ "$_bytes_{i}" -ge 1048576 ] 2>/dev/null; then
        _size_{i}=$(awk "BEGIN{{printf \\"%.1f MB\\", $_bytes_{i}/1048576}}")
    elif [ "$_bytes_{i}" -ge 1024 ] 2>/dev/null; then
        _size_{i}=$(awk "BEGIN{{printf \\"%.1f KB\\", $_bytes_{i}/1024}}")
    else
        _size_{i}="${{_bytes_{i}}} B"
    fi

    if [ "$_rc_{i}" -eq 0 ]; then
        echo "[$(date '+%m-%d %H:%M')] ✓ {local} -> /{full_remote}/ | ${{_done_{i}}} 完成, ${{_skipped_{i}}} 跳过, ${{_failed_{i}}} 失败 | ${{_size_{i}}} | ${{_elapsed_{i}}}s" >> "{BACKUP_LOG_FILE}"
    else
        echo "[$(date '+%m-%d %H:%M')] ✗ {local} -> /{full_remote}/ | 失败 (exit: $_rc_{i})" >> "{BACKUP_LOG_FILE}"
        echo "  错误: $(echo "$_output_{i}" | grep -i 'error\\|failed\\|RESPONSE' | head -1)" >> "{BACKUP_LOG_FILE}"
    fi
"""

    script_content = f"""#!/bin/bash
LOG="{BACKUP_LOG_FILE}"
while true; do
    echo "── [$(date '+%m-%d %H:%M')] 第 $((++_round)) 轮同步 ({len(cfg['paths'])} 条路径) ──" >> "$LOG"
{cp_commands}
    echo "[$(date '+%m-%d %H:%M')] 下次同步: {interval} 分钟后" >> "$LOG"
    echo "" >> "$LOG"
    sleep {interval_sec}
done
"""
    BACKUP_SCRIPT_FILE.write_text(script_content)
    BACKUP_SCRIPT_FILE.chmod(0o755)

def _backup_add_path():
    """交互式添加一条备份路径"""
    _ensure_config()
    cfg = _backup_load_config()

    try:
        console.print("\n  [bold]本地路径[/] (要备份的文件/目录，支持通配符 *):")
        local_path = input("  本地路径: ").strip()
        if not local_path:
            console.print("  [red]路径不能为空[/]")
            return

        console.print(f"\n  [bold]远程目标路径[/] (Blob 上的目标目录):")
        console.print(f"  [dim]当前根目录: /{BASE_PREFIX}[/]")
        target_path = input("  远程路径: ").strip().strip("/")
        if not target_path:
            console.print("  [red]路径不能为空[/]")
            return
    except (EOFError, KeyboardInterrupt):
        console.print("\n  [dim]已取消[/]")
        return

    cfg["paths"].append({"local": local_path, "remote": target_path})
    _backup_save_config(cfg)
    console.print(f"  [green]✓ 已添加备份路径 #{len(cfg['paths'])}:[/] {local_path} → /{target_path}")

    # 如果备份正在运行，提示需要重启
    if _backup_is_running():
        console.print("  [yellow]备份正在运行，请执行 [green]backup restart[/] 使新路径生效[/]")

def _backup_remove_path():
    """交互式移除一条备份路径"""
    cfg = _backup_load_config()
    if not cfg["paths"]:
        console.print("  [dim]没有已配置的备份路径[/]")
        return

    _backup_show_paths(cfg)

    try:
        idx_str = input("\n  输入要删除的编号: ").strip()
        idx = int(idx_str) - 1
        if idx < 0 or idx >= len(cfg["paths"]):
            console.print("  [red]编号无效[/]")
            return
    except (ValueError, EOFError, KeyboardInterrupt):
        console.print("\n  [dim]已取消[/]")
        return

    removed = cfg["paths"].pop(idx)
    _backup_save_config(cfg)
    console.print(f"  [green]✓ 已移除:[/] {removed['local']} → /{removed['remote']}")

    if _backup_is_running():
        console.print("  [yellow]备份正在运行，请执行 [green]backup restart[/] 使更改生效[/]")

def _backup_show_paths(cfg=None):
    """显示已配置的备份路径列表"""
    if cfg is None:
        cfg = _backup_load_config()
    if not cfg["paths"]:
        console.print("  [dim]没有已配置的备份路径[/]")
        return

    table = Table(
        title=" 备份路径列表",
        title_style="bold cyan",
        show_header=True, header_style="bold",
        border_style="dim", show_edge=False,
    )
    table.add_column("#", style="bold", width=4)
    table.add_column("本地路径", no_wrap=False)
    table.add_column("远程路径", no_wrap=False, style="cyan")

    for i, entry in enumerate(cfg["paths"], 1):
        table.add_row(str(i), entry["local"], f"/{entry['remote']}")

    console.print(table)
    console.print(f"  [dim]同步间隔: 每 {cfg.get('interval', 30)} 分钟[/]")

def _backup_set_interval():
    """修改同步间隔"""
    cfg = _backup_load_config()
    console.print(f"  [dim]当前间隔: {cfg.get('interval', 30)} 分钟[/]")
    try:
        val = input("  新间隔 (分钟): ").strip()
        interval = int(val)
        if interval < 1:
            console.print("  [red]间隔不能小于 1 分钟[/]")
            return
    except (ValueError, EOFError, KeyboardInterrupt):
        console.print("\n  [dim]已取消[/]")
        return

    cfg["interval"] = interval
    _backup_save_config(cfg)
    console.print(f"  [green]✓ 间隔已设为 {interval} 分钟[/]")

    if _backup_is_running():
        console.print("  [yellow]备份正在运行，请执行 [green]backup restart[/] 使更改生效[/]")

def _backup_start():
    """启动备份进程"""
    _ensure_config()
    pid = _backup_is_running()
    if pid:
        console.print(f"  [yellow]备份已在运行中 (PID: {pid})[/]")
        return

    cfg = _backup_load_config()
    if not cfg["paths"]:
        console.print("  [yellow]没有配置备份路径，请先用 [green]backup add[/] 添加[/]")
        return

    _backup_show_paths(cfg)
    _backup_generate_script(cfg)
    BACKUP_LOG_FILE.write_text("")

    console.print("\n  [dim]启动中...[/]")
    proc = subprocess.Popen(
        ["nohup", "bash", str(BACKUP_SCRIPT_FILE)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        preexec_fn=os.setpgrp,
    )
    BACKUP_PID_FILE.write_text(str(proc.pid))
    console.print(f"  [bold green]✓ 备份已启动[/] (PID: {proc.pid})")
    console.print(f"  [dim]日志: {BACKUP_LOG_FILE}[/]")

def _backup_stop():
    """停止后台备份进程"""
    pid = _backup_is_running()
    if not pid:
        console.print("  [dim]当前没有运行中的备份[/]")
        return

    try:
        import signal
        os.killpg(os.getpgid(pid), signal.SIGTERM)
        console.print(f"  [bold green]✓ 备份已停止[/] (PID: {pid})")
    except ProcessLookupError:
        console.print(f"  [dim]进程已不存在[/]")
    except PermissionError:
        try:
            import signal
            os.kill(pid, signal.SIGTERM)
            console.print(f"  [bold green]✓ 备份已停止[/] (PID: {pid})")
        except Exception as e:
            console.print(f"  [red]无法停止进程: {e}[/]")
            return

    BACKUP_PID_FILE.unlink(missing_ok=True)
    BACKUP_SCRIPT_FILE.unlink(missing_ok=True)

def _backup_status():
    """显示备份状态"""
    pid = _backup_is_running()
    cfg = _backup_load_config()

    if pid:
        console.print(f"  [bold green]● 备份运行中[/] (PID: {pid})")
    else:
        console.print(f"  [dim]● 备份未运行[/]")

    _backup_show_paths(cfg)

    if BACKUP_LOG_FILE.exists():
        lines = BACKUP_LOG_FILE.read_text().strip().splitlines()
        if lines:
            recent = lines[-15:]
            console.print(f"\n  [bold]最近日志:[/]")
            for line in recent:
                if "✗" in line or "失败" in line:
                    console.print(f"  [red]{line}[/]")
                elif "✓" in line:
                    console.print(f"  [green]{line}[/]")
                elif "──" in line:
                    console.print(f"  [bold cyan]{line}[/]")
                else:
                    console.print(f"  [dim]{line}[/]")

def _backup_start_interactive():
    """首次启动的完整交互流程: 添加路径 → 设间隔 → 启动"""
    _ensure_config()
    cfg = _backup_load_config()

    console.print("\n  [bold cyan]── 配置自动备份 ──[/]")
    console.print("  [dim]可添加多条备份路径，输入空路径结束添加[/]\n")

    count = len(cfg["paths"])
    try:
        while True:
            count += 1
            console.print(f"  [bold]── 路径 #{count} ──[/]")
            console.print("  [bold]本地路径[/] (要备份的文件/目录，支持通配符 *，留空结束):")
            local_path = input("  本地路径: ").strip()
            if not local_path:
                count -= 1
                break

            console.print(f"  [bold]远程目标路径[/] (Blob 上的目标目录):")
            console.print(f"  [dim]当前根目录: /{BASE_PREFIX}[/]")
            target_path = input("  远程路径: ").strip().strip("/")
            if not target_path:
                console.print("  [red]远程路径不能为空，跳过此条[/]")
                count -= 1
                continue

            cfg["paths"].append({"local": local_path, "remote": target_path})
            console.print(f"  [green]✓ 已添加[/]: {local_path} → /{target_path}\n")

    except (EOFError, KeyboardInterrupt):
        console.print("\n  [dim]结束添加[/]")

    if not cfg["paths"]:
        console.print("  [dim]未添加任何路径，已取消[/]")
        return

    try:
        console.print(f"\n  [bold]同步间隔[/] (分钟，默认 {cfg.get('interval', 30)}):")
        interval_str = input("  间隔: ").strip()
        if interval_str:
            interval = int(interval_str)
            if interval < 1:
                console.print("  [red]间隔不能小于 1 分钟，使用默认 30[/]")
                interval = 30
            cfg["interval"] = interval
    except (ValueError, EOFError, KeyboardInterrupt):
        pass

    _backup_save_config(cfg)
    _backup_start()

def _backup_prompt_on_startup():
    """启动时询问用户是否开启备份"""
    pid = _backup_is_running()
    if pid:
        cfg = _backup_load_config()
        console.print(f"  [green]● 自动备份运行中[/] (PID: {pid}, {len(cfg['paths'])} 条路径)")
        return

    try:
        answer = input("\n  是否开启自动备份? (y/n): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return

    if answer in ("y", "yes"):
        _backup_start_interactive()

def _confirm(message="确认?"):
    try:
        answer = input(f"  {message} (y/n): ").strip().lower()
        return answer in ("y", "yes")
    except (EOFError, KeyboardInterrupt):
        console.print("\n  [dim]已取消[/]")
        return False

def main():
    mgr = BlobManager()
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        args = sys.argv[2:]
        mgr.run_oneshot(cmd, args)
    else:
        mgr.run_interactive()

if __name__ == "__main__":
    main()
