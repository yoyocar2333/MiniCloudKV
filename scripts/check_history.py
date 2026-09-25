"""Exact linearizability search for small, successful single-key histories.

Input JSON: [{"start": seconds, "end": seconds, "op": "put|get|delete",
              "value": string-or-null, "result": string-or-null}, ...]
Failed or timed-out operations have ambiguous outcomes and must not be fed to
this checker as if they were successful. Up to 16 operations per key.
"""
import argparse
import json
from functools import lru_cache


def check(history):
    if len(history) > 16:
        raise ValueError("maximum 16 operations; split larger histories by key/time")
    for event in history:
        if event["op"] not in ("put", "get", "delete") or event["end"] < event["start"]:
            raise ValueError("invalid event")
    prerequisites = [sum(1 << j for j, other in enumerate(history)
                         if j != i and other["end"] < event["start"])
                     for i, event in enumerate(history)]
    target = (1 << len(history)) - 1

    @lru_cache(None)
    def search(done, value):
        if done == target:
            return True
        for index, event in enumerate(history):
            bit = 1 << index
            if done & bit or prerequisites[index] & ~done:
                continue
            op = event["op"]
            if op == "get" and event["result"] != value:
                continue
            next_value = event["value"] if op == "put" else None if op == "delete" else value
            if search(done | bit, next_value):
                return True
        return False

    return search(0, None)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("history", help="JSON file with successful operations for one initially empty key")
    args = parser.parse_args()
    with open(args.history, encoding="utf-8") as handle:
        valid = check(json.load(handle))
    print("linearizable" if valid else "violation")
    raise SystemExit(0 if valid else 1)


if __name__ == "__main__":
    main()
