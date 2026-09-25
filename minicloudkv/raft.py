"""Raft election and log replication for a fixed, three-node cluster.

The implementation favors inspectability over write throughput. All durable
state changes are made under the node lock and synchronously flushed.
"""
import json
import random
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from . import storage


class NotLeader(Exception):
    pass


class Unavailable(Exception):
    pass


class Node:
    def __init__(self, node_id: str, peers: dict[str, str], data_dir: Path,
                 election_range=(0.35, 0.65), heartbeat=0.08, rpc_timeout=0.18,
                 storage_mode="wal"):
        if node_id in peers or len(peers) != 2:
            raise ValueError("exactly two other peers are required")
        self.id, self.peers = node_id, peers
        self.storage_mode = storage_mode
        self.path = Path(data_dir) / "raft.json"
        self.wal_store = storage.WALStore(Path(data_dir)) if storage_mode == "wal" else None
        if storage_mode not in ("wal", "snapshot"):
            raise ValueError("storage_mode must be wal or snapshot")
        state = self.wal_store.state if self.wal_store else storage.load(self.path)
        self.term, self.voted_for = state["term"], state["voted_for"]
        self.log, self.commit = state["log"], state["commit"]
        self.kv = {}
        self.applied = 0
        self.lock = threading.RLock()
        self.client_lock = threading.Lock()
        # A single worker per peer coalesces heartbeat and client triggers.
        # A trigger during an in-flight RPC remains set for the next round.
        self.replication_events = {peer: threading.Event() for peer in peers}
        self.condition = threading.Condition(self.lock)
        self.role, self.leader_id = "follower", None
        self.next_index, self.match_index = {}, {}
        self.blocked = set()
        self.election_range, self.heartbeat, self.rpc_timeout = election_range, heartbeat, rpc_timeout
        self.deadline = 0.0
        self._reset_deadline()
        self.stopped = threading.Event()
        self._apply()

    def _persist(self):
        state = {"term": self.term, "voted_for": self.voted_for,
                 "log": self.log, "commit": self.commit}
        if self.wal_store:
            self.wal_store.persist(state)
        else:
            storage.save(self.path, state)

    def _reset_deadline(self):
        self.deadline = time.monotonic() + random.uniform(*self.election_range)

    def _apply(self):
        while self.applied < self.commit:
            self.applied += 1
            entry = self.log[self.applied]
            if entry["op"] == "put":
                self.kv[entry["key"]] = entry["value"]
            elif entry["op"] == "delete":
                self.kv.pop(entry["key"], None)

    def _step_down(self, term, leader=None):
        if term > self.term:
            self.term, self.voted_for = term, None
            self._persist()
        self.role, self.leader_id = "follower", leader
        self._reset_deadline()
        self.condition.notify_all()

    def request_vote(self, request):
        with self.lock:
            term = request["term"]
            if term > self.term:
                self._step_down(term)
            last = self.log[-1]["term"], len(self.log) - 1
            candidate = request["last_term"], request["last_index"]
            granted = (term == self.term and
                       self.voted_for in (None, request["candidate"]) and
                       candidate >= last)
            if granted:
                self.voted_for = request["candidate"]
                self._persist()
                self._reset_deadline()
            return {"term": self.term, "granted": granted}

    def append_entries(self, request):
        with self.lock:
            term = request["term"]
            if term > self.term:
                self._step_down(term, request["leader"])
            if term < self.term:
                return {"term": self.term, "success": False, "match": 0}
            self.role, self.leader_id = "follower", request["leader"]
            self._reset_deadline()
            prev = request["prev_index"]
            if prev >= len(self.log):
                return {"term": self.term, "success": False, "match": 0,
                        "conflict_index": len(self.log)}
            if self.log[prev]["term"] != request["prev_term"]:
                conflict_term = self.log[prev]["term"]
                first = prev
                while first > 0 and self.log[first - 1]["term"] == conflict_term:
                    first -= 1
                return {"term": self.term, "success": False, "match": 0,
                        "conflict_index": first}
            changed = False
            for offset, entry in enumerate(request["entries"]):
                index = prev + offset + 1
                if index < len(self.log) and self.log[index] != entry:
                    if index <= self.commit:
                        raise ValueError("attempted overwrite of committed entry")
                    del self.log[index:]
                    changed = True
                if index == len(self.log):
                    self.log.append(entry)
                    changed = True
            new_commit = min(request["commit"], prev + len(request["entries"]))
            if new_commit > self.commit:
                self.commit = new_commit
                changed = True
            if changed:
                self._persist()
                self._apply()
                self.condition.notify_all()
            return {"term": self.term, "success": True,
                    "match": prev + len(request["entries"])}

    def _rpc(self, peer, route, payload):
        with self.lock:
            if peer in self.blocked:
                raise OSError("peer blocked by fault injector")
            url = self.peers[peer] + route
        data = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=self.rpc_timeout) as response:
            return json.load(response)

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
        for peer in self.peers:
            threading.Thread(target=self._replication_worker, args=(peer,), daemon=True).start()

    def stop(self):
        self.stopped.set()
        for event in self.replication_events.values():
            event.set()

    def _wake_replicators(self):
        for event in self.replication_events.values():
            event.set()

    def _replication_worker(self, peer):
        event = self.replication_events[peer]
        while not self.stopped.is_set():
            event.wait()
            event.clear()
            if not self.stopped.is_set():
                self._replicate(peer)

    def _loop(self):
        next_heartbeat = 0.0
        while not self.stopped.wait(0.02):
            with self.lock:
                role = self.role
                due = time.monotonic() >= self.deadline
            if role == "leader":
                if time.monotonic() >= next_heartbeat:
                    self._wake_replicators()
                    next_heartbeat = time.monotonic() + self.heartbeat
            elif due:
                self._start_election()

    def _start_election(self):
        with self.lock:
            if self.role == "leader" or time.monotonic() < self.deadline:
                return
            self.role, self.leader_id = "candidate", None
            self.term += 1
            self.voted_for = self.id
            self.votes_received = {self.id}
            self._persist()
            self._reset_deadline()
            term = self.term
            request = {"term": term, "candidate": self.id,
                       "last_index": len(self.log) - 1, "last_term": self.log[-1]["term"]}
        for peer in self.peers:
            threading.Thread(target=self._request_vote_from,
                             args=(peer, term, request), daemon=True).start()

    def _request_vote_from(self, peer, term, request):
        try:
            response = self._rpc(peer, "/raft/vote", request)
        except (OSError, ValueError, urllib.error.URLError):
            return
        with self.lock:
            if response["term"] > self.term:
                self._step_down(response["term"])
            if self.role != "candidate" or self.term != term:
                return
            if response["granted"]:
                self.votes_received.add(peer)
            if len(self.votes_received) >= 2:
                self.role, self.leader_id = "leader", self.id
                self.next_index = {p: len(self.log) for p in self.peers}
                self.match_index = {p: 0 for p in self.peers}
                self.condition.notify_all()
                self._wake_replicators()

    def _replicate(self, peer):
        # Called only by this peer's worker: at most one RPC is in flight.
        while not self.stopped.is_set():
            with self.lock:
                if self.role != "leader":
                    return
                term = self.term
                index = self.next_index[peer]
                request = {"term": term, "leader": self.id, "prev_index": index - 1,
                           "prev_term": self.log[index - 1]["term"],
                           "entries": self.log[index:], "commit": self.commit}
            try:
                response = self._rpc(peer, "/raft/append", request)
            except (OSError, ValueError, urllib.error.URLError):
                return
            with self.lock:
                if response["term"] > self.term:
                    self._step_down(response["term"])
                    return
                if self.role != "leader" or self.term != term:
                    return
                if response["success"]:
                    self.match_index[peer] = max(self.match_index[peer], response["match"])
                    self.next_index[peer] = self.match_index[peer] + 1
                    self._advance_commit()
                    if self.next_index[peer] <= len(self.log) - 1:
                        continue
                else:
                    # A short follower or a conflicting term gives the first
                    # index worth retrying, avoiding one heartbeat per entry.
                    hint = response.get("conflict_index", index - 1)
                    self.next_index[peer] = max(1, min(index - 1, hint))
                    if self.next_index[peer] == index:
                        return
                    continue
            event = self.replication_events[peer]
            if event.is_set():
                event.clear()
                continue
            return

    def _advance_commit(self):
        for index in range(len(self.log) - 1, self.commit, -1):
            if (self.log[index]["term"] == self.term and
                    1 + sum(match >= index for match in self.match_index.values()) >= 2):
                self.commit = index
                self._persist()
                self._apply()
                self.condition.notify_all()
                return

    def propose(self, op, key, value=None, timeout=3.0):
        # Serializing clients makes the read entry's position a clear read barrier.
        limit = time.monotonic() + timeout
        remaining = limit - time.monotonic()
        if remaining <= 0 or not self.client_lock.acquire(timeout=remaining):
            raise Unavailable("client deadline expired while waiting for proposal slot")
        try:
            with self.condition:
                if time.monotonic() >= limit:
                    raise Unavailable("client deadline expired before proposal")
                if self.role != "leader":
                    raise NotLeader(self.leader_id)
                term = self.term
                entry = {"term": term, "op": op, "key": key}
                if op == "put":
                    entry["value"] = value
                self.log.append(entry)
                self._persist()
                index = len(self.log) - 1
            self._wake_replicators()
            with self.condition:
                while self.commit < index and self.role == "leader" and self.term == term:
                    remaining = limit - time.monotonic()
                    if remaining <= 0:
                        raise Unavailable("no majority acknowledgement; outcome may be unknown")
                    self.condition.wait(remaining)
                if (self.commit < index or self.role != "leader" or
                        self.term != term or index >= len(self.log) or
                        self.log[index] != entry):
                    raise Unavailable("leadership changed; outcome may be unknown")
                return self.kv.get(key) if op == "get" else None
        finally:
            self.client_lock.release()

    def status(self):
        with self.lock:
            return {"id": self.id, "role": self.role, "term": self.term,
                    "leader": self.leader_id, "commit": self.commit,
                    "last_index": len(self.log) - 1, "blocked": sorted(self.blocked)}
