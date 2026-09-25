"""Repeatable sequential baseline; reports measured throughput and latency."""
import argparse
import json
import statistics
import time
import urllib.error
import urllib.request


def call(url, method, payload=None):
    req = urllib.request.Request(url, method=method,
                                 data=json.dumps(payload).encode() if payload else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", default="http://127.0.0.1:8901")
    parser.add_argument("--operations", type=int, default=100)
    parser.add_argument("--keys", type=int, default=20)
    args = parser.parse_args()
    if args.operations < 1 or args.keys < 1:
        parser.error("operations and keys must be positive")
    durations = []
    start = time.perf_counter()
    for index in range(args.operations):
        operation_start = time.perf_counter()
        call(f"{args.node}/kv/key{index % args.keys}", "PUT", {"value": str(index)})
        durations.append((time.perf_counter() - operation_start) * 1000)
    elapsed = time.perf_counter() - start
    ordered = sorted(durations)
    percentile = lambda p: ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))]
    print(json.dumps({"operations": args.operations, "elapsed_s": round(elapsed, 3),
                      "throughput_ops_s": round(args.operations / elapsed, 2),
                      "p50_ms": round(statistics.median(durations), 3),
                      "p95_ms": round(percentile(.95), 3),
                      "p99_ms": round(percentile(.99), 3)}, indent=2))


if __name__ == "__main__":
    main()
