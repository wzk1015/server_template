      
#!/bin/bash
# Azure Blob Storage 交互式文件管理器启动脚本
# 用法: bash scripts/blob_manager.sh [子命令和参数]
# 需要在tools下放置脚本：tools/blob_manager.py

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── 环境检查 ──────────────────────────────────────────────────────────

echo "🔍 检查环境..."

# 1. 检查 azcopy（PATH 中找不到则搜索 $HOME）
if ! command -v azcopy &>/dev/null; then
    AZCOPY_PATH="$(find "$HOME" -maxdepth 3 -name 'azcopy' -type f -print -quit 2>/dev/null)"
    if [ -n "$AZCOPY_PATH" ]; then
        export PATH="$(dirname "$AZCOPY_PATH"):$PATH"
        echo "✅ azcopy: 已从 $AZCOPY_PATH 加入 PATH"
    else
        echo "❌ azcopy 未找到"
        echo "   安装方法: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10"
        exit 1
    fi
else
    echo "✅ azcopy: $(azcopy --version 2>&1 | head -1)"
fi

# 2. 检查 Python 依赖（当前环境缺什么装什么）
missing_deps=()
python -c "import rich" 2>/dev/null || missing_deps+=("rich")
python -c "import prompt_toolkit" 2>/dev/null || missing_deps+=("prompt_toolkit")

if [ ${#missing_deps[@]} -gt 0 ]; then
    echo "⚠️  缺少 Python 依赖: ${missing_deps[*]}"
    read -rp "   是否自动安装? [Y/n] " answer
    if [[ "${answer:-Y}" =~ ^[Yy]?$ ]]; then
        pip install "${missing_deps[@]}"
        echo "✅ 依赖已安装"
    else
        echo "   请手动安装: pip install ${missing_deps[*]}"
        exit 1
    fi
fi

echo "✅ 环境检查通过"
echo

# ── 启动 ──────────────────────────────────────────────────────────────

cd "$PROJECT_ROOT"
exec python tools/blob_manager.py "$@"

    