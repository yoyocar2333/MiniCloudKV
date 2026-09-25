import tempfile
import unittest
from pathlib import Path
from minicloudkv import storage
from minicloudkv.raft import Node


class StorageTest(unittest.TestCase):
    def test_wal_replay_and_torn_tail(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            wal = storage.WALStore(directory, snapshot_interval=100)
            wal.persist({"term": 1, "voted_for": "n1", "commit": 1,
                         "log": [{"term": 0, "op": "noop"},
                                 {"term": 1, "op": "put", "key": "x", "value": "safe"}]})
            clean_size = (directory / "wal.bin").stat().st_size
            with (directory / "wal.bin").open("ab") as handle:
                handle.write(b"\x00\x00\x00")
            recovered = storage.WALStore(directory, snapshot_interval=100)
            self.assertEqual(recovered.state["log"][1]["value"], "safe")
            self.assertEqual(recovered.sequence, 1)
            self.assertEqual((directory / "wal.bin").stat().st_size, clean_size)

    def test_wal_checkpoint_and_replay(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            wal = storage.WALStore(directory, snapshot_interval=2)
            state = {"term": 1, "voted_for": "n1", "commit": 0,
                     "log": [{"term": 0, "op": "noop"}]}
            wal.persist(state)
            state = {**state, "log": state["log"] + [{"term": 1, "op": "put", "key": "a", "value": "b"}], "commit": 1}
            wal.persist(state)
            self.assertTrue((directory / "snapshot.json").exists())
            self.assertEqual((directory / "wal.bin").stat().st_size, 0)
            recovered = storage.WALStore(directory, snapshot_interval=2)
            self.assertEqual(recovered.state, state)
            self.assertEqual(recovered.sequence, 2)

    def test_recovery_removes_checkpointed_frames(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            wal = storage.WALStore(directory, snapshot_interval=2)
            base = {"term": 1, "voted_for": "n1", "commit": 0,
                    "log": [{"term": 0, "op": "noop"}]}
            wal.persist(base)
            old_bytes = (directory / "wal.bin").read_bytes()
            committed = {**base, "commit": 1, "log": base["log"] + [
                {"term": 1, "op": "put", "key": "a", "value": "1"}]}
            wal.persist(committed)
            # Crash after checkpoint rename and before WAL truncation.
            (directory / "wal.bin").write_bytes(old_bytes)
            recovered = storage.WALStore(directory, snapshot_interval=2)
            self.assertEqual(recovered.state, committed)
            self.assertEqual((directory / "wal.bin").stat().st_size, 0)
            newer = {**committed, "commit": 2, "log": committed["log"] + [
                {"term": 1, "op": "put", "key": "b", "value": "2"}]}
            recovered.persist(newer)
            new_bytes = (directory / "wal.bin").read_bytes()
            (directory / "wal.bin").write_bytes(old_bytes + new_bytes)
            again = storage.WALStore(directory, snapshot_interval=2)
            self.assertEqual(again.state, newer)
            self.assertEqual((directory / "wal.bin").read_bytes(), new_bytes)

    def test_replay_only_committed_entries(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "raft.json"
            storage.save(path, {"term": 3, "voted_for": "n2", "commit": 1,
                                "log": [{"term": 0, "op": "noop"},
                                        {"term": 2, "op": "put", "key": "x", "value": "old"},
                                        {"term": 3, "op": "put", "key": "x", "value": "uncommitted"}]})
            node = Node("n1", {"n2": "http://127.0.0.1:1", "n3": "http://127.0.0.1:2"}, Path(root), storage_mode="snapshot")
            self.assertEqual(node.kv["x"], "old")
            self.assertEqual(node.term, 3)
            self.assertEqual(node.voted_for, "n2")

    def test_vote_requires_current_log(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node("n1", {"n2": "http://127.0.0.1:1", "n3": "http://127.0.0.1:2"}, Path(root), storage_mode="snapshot")
            node.append_entries({"term": 2, "leader": "n2", "prev_index": 0,
                                 "prev_term": 0, "entries": [{"term": 2, "op": "put", "key": "a", "value": "b"}], "commit": 0})
            self.assertFalse(node.request_vote({"term": 3, "candidate": "n3", "last_index": 0, "last_term": 0})["granted"])
            self.assertTrue(node.request_vote({"term": 3, "candidate": "n3", "last_index": 1, "last_term": 2})["granted"])

    def test_conflicting_uncommitted_suffix(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node("n1", {"n2": "http://127.0.0.1:1", "n3": "http://127.0.0.1:2"}, Path(root), storage_mode="snapshot")
            node.append_entries({"term": 2, "leader": "n2", "prev_index": 0, "prev_term": 0,
                                 "entries": [{"term": 2, "op": "put", "key": "k", "value": "old"}], "commit": 0})
            node.append_entries({"term": 3, "leader": "n3", "prev_index": 0, "prev_term": 0,
                                 "entries": [{"term": 3, "op": "put", "key": "k", "value": "new"}], "commit": 1})
            self.assertEqual(node.kv["k"], "new")
            self.assertEqual(len(node.log), 2)


if __name__ == "__main__":
    unittest.main()
