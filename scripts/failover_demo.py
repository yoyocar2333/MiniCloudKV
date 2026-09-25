"""Kill the elected leader, measure failover, then restart it from disk."""
import json
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


def free_ports():
    sockets = [socket.socket() for _ in range(3)]
    try:
        for item in sockets:
            item.bind(("127.0.0.1", 0))
        return [item.getsockname()[1] for item in sockets]
    finally:
        for item in sockets:
            item.close()


def call(port, path, method="GET", value=None):
    data = json.dumps({"value": value}).encode() if value is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method,
                                 data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=1) as response:
        return json.load(response)


def wait_for(predicate, seconds=8):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result is not None and result is not False:
                return result
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(.025)
    raise TimeoutError("cluster failed to meet condition")


def main():
    ports = free_ports()
    processes = {}
    with tempfile.TemporaryDirectory() as root:
        def launch(index):
            peers = [f"--peer=n{j+1}=http://127.0.0.1:{ports[j]}" for j in range(3) if j != index]
            processes[index] = subprocess.Popen([
                sys.executable, "-m", "minicloudkv.cli", "--id", f"n{index+1}",
                "--listen", f"127.0.0.1:{ports[index]}",
                "--data-dir", str(Path(root) / f"n{index+1}"), *peers],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def leader(indices):
            leaders = [i for i in indices if call(ports[i], "/status")["role"] == "leader"]
            return leaders[0] if len(leaders) == 1 else None

        try:
            for index in range(3):
                launch(index)
            original = wait_for(lambda: leader(range(3)))
            call(ports[original], "/kv/survives", "PUT", "yes")
            # Abrupt termination mimics a process crash, bypassing graceful shutdown.
            start = time.monotonic()
            processes[original].kill()
            processes[original].wait()
            survivors = [i for i in range(3) if i != original]
            elected = wait_for(lambda: leader(survivors))
            wait_for(lambda: call(ports[elected], "/kv/survives").get("value") == "yes")
            recovery_ms = round((time.monotonic() - start) * 1000, 1)
            launch(original)
            wait_for(lambda: call(ports[original], "/status")["commit"] >= 1)
            print(json.dumps({"old_leader": f"n{original+1}", "new_leader": f"n{elected+1}",
                              "first_successful_read_after_crash_ms": recovery_ms,
                              "restarted_node_caught_up": True}, indent=2))
        finally:
            for process in processes.values():
                if process.poll() is None:
                    process.terminate()
            for process in processes.values():
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
