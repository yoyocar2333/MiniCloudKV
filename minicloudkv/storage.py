"""Durable Raft state: full-state rewrite baseline and append-only WAL.

Both store the term, vote, log, and commit index. WAL checkpoints keep the
whole log and are not state-machine snapshots; see docs/architecture.md.
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

# Append-only variant. Every frame is one atomic transition from the previous
# state: (length, CRC32, JSON delta). Only a complete checksummed prefix replays.
import struct
import zlib

MAX_FRAME = 8 * 1024 * 1024


class WALStore:
    def __init__(self, directory: Path, snapshot_interval=4096):
        self.directory = Path(directory)
        self.snapshot = self.directory / "snapshot.json"
        self.wal = self.directory / "wal.bin"
        self.interval = snapshot_interval
        self.directory.mkdir(parents=True, exist_ok=True)
        checkpoint = load(self.snapshot)
        self.sequence = checkpoint.get("sequence", 0)
        checkpoint_sequence = self.sequence
        state = {key: checkpoint[key] for key in ("term", "voted_for", "log", "commit")}
        count = 0
        skipped_prefix = 0
        if self.wal.exists():
            with self.wal.open("r+b") as handle:
                valid = 0
                while True:
                    header = handle.read(8)
                    if not header:
                        break
                    if len(header) < 8:
                        break
                    length, checksum = struct.unpack(">II", header)
                    if not 0 < length <= MAX_FRAME:
                        raise ValueError("invalid WAL frame length")
                    payload = handle.read(length)
                    if len(payload) < length:
                        break
                    if zlib.crc32(payload) != checksum:
                        raise ValueError("corrupt complete WAL frame")
                    delta = json.loads(payload)
                    if delta["sequence"] <= checkpoint_sequence:
                        if count:
                            raise ValueError("old WAL frame after new frames")
                        skipped_prefix = handle.tell()
                    else:
                        if delta["sequence"] != self.sequence + 1:
                            raise ValueError("WAL sequence gap")
                        state = self._apply_delta(state, delta)
                        self.sequence += 1
                        count += 1
                    valid = handle.tell()
                if valid != handle.tell() or handle.read(1):
                    handle.truncate(valid)
                    handle.flush()
                    os.fsync(handle.fileno())
            if skipped_prefix:
                self._remove_checkpointed_prefix(skipped_prefix, valid)
        if not 0 <= state["commit"] < len(state["log"]):
            raise ValueError("invalid WAL commit index")
        self.state = {**state, "log": list(state["log"])}
        self.since_snapshot = count

    def _remove_checkpointed_prefix(self, skipped, valid):
        if skipped == valid:
            with self.wal.open("wb") as handle:
                handle.flush()
                os.fsync(handle.fileno())
            return
        temporary = self.wal.with_name(self.wal.name + ".tmp")
        with self.wal.open("rb") as source, temporary.open("wb") as target:
            source.seek(skipped)
            remaining = valid - skipped
            while remaining:
                chunk = source.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise ValueError("WAL shortened during recovery")
                target.write(chunk)
                remaining -= len(chunk)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, self.wal)
        directory = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    @staticmethod
    def _apply_delta(state, delta):
        log = state["log"][:delta["prefix"]] + delta["suffix"]
        return {"term": delta["term"], "voted_for": delta["voted_for"],
                "log": log, "commit": delta["commit"]}

    def persist(self, state):
        old = self.state["log"]
        new = state["log"]
        if len(new) >= len(old) and old[-1] is new[len(old) - 1]:
            # Entries are immutable and list copies share their objects. An
            # unchanged last old entry proves the append-only prefix remains.
            prefix = len(old)
        else:
            prefix = 0
            while prefix < min(len(old), len(new)) and old[prefix] == new[prefix]:
                prefix += 1
        delta = {"sequence": self.sequence + 1, "term": state["term"],
                 "voted_for": state["voted_for"], "commit": state["commit"],
                 "prefix": prefix, "suffix": new[prefix:]}
        payload = json.dumps(delta, separators=(",", ":"), sort_keys=True).encode()
        if len(payload) > MAX_FRAME:
            raise ValueError("WAL entry too large")
        first_write = not self.wal.exists()
        with self.wal.open("ab") as handle:
            handle.write(struct.pack(">II", len(payload), zlib.crc32(payload)))
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if first_write:
            directory = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        self.sequence += 1
        self.since_snapshot += 1
        # Entries are immutable after creation; copy only the list so later
        # appends/truncations cannot change the persisted-state comparison.
        self.state = {**state, "log": list(state["log"])}
        if self.since_snapshot >= self.interval:
            save(self.snapshot, {**state, "sequence": self.sequence})
            with self.wal.open("wb") as handle:
                handle.flush()
                os.fsync(handle.fileno())
            self.since_snapshot = 0
