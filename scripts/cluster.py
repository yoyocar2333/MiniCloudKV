"""Run a local three-process cluster and clean up on Ctrl-C."""
import argparse
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-port", type=int, default=8901)
    parser.add_argument("--data-root", type=Path)
    args = parser.parse_args()
    temporary = tempfile.TemporaryDirectory() if args.data_root is None else None
    root = args.data_root or Path(temporary.name)
    root.mkdir(parents=True, exist_ok=True)
    processes = []
    try:
        for index in range(3):
            node_id = f"n{index + 1}"
            peers = [f"--peer=n{j + 1}=http://127.0.0.1:{args.base_port + j}"
                     for j in range(3) if j != index]
            processes.append(subprocess.Popen([
                sys.executable, "-m", "minicloudkv.cli", "--id", node_id,
                "--listen", f"127.0.0.1:{args.base_port + index}",
                "--data-dir", str(root / node_id), *peers]))
        print(f"cluster data: {root}", flush=True)
        print("nodes: " + " ".join(f"http://127.0.0.1:{args.base_port + i}" for i in range(3)), flush=True)
        print("pids: " + " ".join(str(p.pid) for p in processes), flush=True)
        while True:
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if temporary:
            temporary.cleanup()


if __name__ == "__main__":
    main()
