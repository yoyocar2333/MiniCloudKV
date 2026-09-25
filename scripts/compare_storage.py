"""Compare full-state snapshots against the append-only WAL on local loopback."""
import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from failover_demo import free_ports, call, wait_for


def run_trial(mode, operations):
    ports = free_ports()
    processes = []
    with tempfile.TemporaryDirectory() as root:
        try:
            for index in range(3):
                peers = [f"--peer=n{j+1}=http://127.0.0.1:{ports[j]}" for j in range(3) if j != index]
                processes.append(subprocess.Popen([
                    sys.executable, "-m", "minicloudkv.cli", "--id", f"n{index+1}",
                    "--listen", f"127.0.0.1:{ports[index]}",
                    "--data-dir", str(Path(root) / f"n{index+1}"), "--storage", mode, *peers],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            def get_leader():
                try:
                    leaders = [port for port in ports if call(port, "/status")["role"] == "leader"]
                except OSError:
                    return None
                return leaders[0] if len(leaders) == 1 else None
            port = wait_for(get_leader)
            result = subprocess.run([sys.executable, "scripts/benchmark.py", "--node",
                                     f"http://127.0.0.1:{port}", "--operations", str(operations)],
                                    capture_output=True, text=True, check=True)
            return json.loads(result.stdout)
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--operations", type=int, default=200)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.operations < 1 or args.repeats < 1:
        parser.error("operations and repeats must be positive")
    results = []
    for trial in range(args.repeats):
        for mode in ("snapshot", "wal") if trial % 2 == 0 else ("wal", "snapshot"):
            result = run_trial(mode, args.operations)
            results.append({"trial": trial + 1, "storage": mode, **result})
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
