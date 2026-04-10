"""
GPU 占卡脚本 — 尽量占满所有 GPU 显存。
用法: python tools/fzq_thinking.py
"""
import time
import torch

def main():
    num_gpus = torch.cuda.device_count()
    if num_gpus == 0:
        print("No GPU found.")
        return

    tensors = []
    for i in range(num_gpus):
        total = torch.cuda.get_device_properties(i).total_memory
        # 预留 512 MB 给系统
        alloc = total - 512 * 1024 * 1024
        try:
            t = torch.empty(alloc // 4, dtype=torch.float32, device=f"cuda:{i}")
            tensors.append(t)
            used_gb = alloc / 1024**3
            print(f"[GPU {i}] Allocated {used_gb:.1f} GB")
        except RuntimeError as e:
            print(f"[GPU {i}] Failed: {e}")

    print(f"Holding {len(tensors)} GPUs. Ctrl+C to release.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
