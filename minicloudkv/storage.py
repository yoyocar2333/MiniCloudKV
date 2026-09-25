"""Crash-safe replacement of the small Raft state file.

A single state image keeps the term, vote, log, and commit index together. This
is deliberately O(log size) per update; see docs/architecture.md.
"""
import json
import os
from pathlib import Path


def load(path: Path) -> dict:
    if not path.exists():
        return {"term": 0, "voted_for": None, "log": [{"term": 0, "op": "noop"}], "commit": 0}
    state = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(state["term"], int) or not isinstance(state["log"], list):
        raise ValueError("invalid persisted Raft state")
    if not 0 <= state["commit"] < len(state["log"]):
        raise ValueError("invalid committed index")
    if state["log"][0] != {"term": 0, "op": "noop"}:
        raise ValueError("invalid log sentinel")
    return state


def save(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, separators=(",", ":"), sort_keys=True).encode()
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    # Persist the rename as well as the file contents on POSIX file systems.
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
