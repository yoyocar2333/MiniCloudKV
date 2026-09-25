import tempfile
import unittest
from pathlib import Path
from minicloudkv import storage
from minicloudkv.raft import Node


class StorageTest(unittest.TestCase):
    def test_replay_only_committed_entries(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "raft.json"
            storage.save(path, {"term": 3, "voted_for": "n2", "commit": 1,
                                "log": [{"term": 0, "op": "noop"},
                                        {"term": 2, "op": "put", "key": "x", "value": "old"},
                                        {"term": 3, "op": "put", "key": "x", "value": "uncommitted"}]})
            node = Node("n1", {"n2": "http://127.0.0.1:1", "n3": "http://127.0.0.1:2"}, Path(root))
            self.assertEqual(node.kv["x"], "old")
            self.assertEqual(node.term, 3)
            self.assertEqual(node.voted_for, "n2")

    def test_vote_requires_current_log(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node("n1", {"n2": "http://127.0.0.1:1", "n3": "http://127.0.0.1:2"}, Path(root))
            node.append_entries({"term": 2, "leader": "n2", "prev_index": 0,
                                 "prev_term": 0, "entries": [{"term": 2, "op": "put", "key": "a", "value": "b"}], "commit": 0})
            self.assertFalse(node.request_vote({"term": 3, "candidate": "n3", "last_index": 0, "last_term": 0})["granted"])
            self.assertTrue(node.request_vote({"term": 3, "candidate": "n3", "last_index": 1, "last_term": 2})["granted"])

    def test_conflicting_uncommitted_suffix(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node("n1", {"n2": "http://127.0.0.1:1", "n3": "http://127.0.0.1:2"}, Path(root))
            node.append_entries({"term": 2, "leader": "n2", "prev_index": 0, "prev_term": 0,
                                 "entries": [{"term": 2, "op": "put", "key": "k", "value": "old"}], "commit": 0})
            node.append_entries({"term": 3, "leader": "n3", "prev_index": 0, "prev_term": 0,
                                 "entries": [{"term": 3, "op": "put", "key": "k", "value": "new"}], "commit": 1})
            self.assertEqual(node.kv["k"], "new")
            self.assertEqual(len(node.log), 2)


if __name__ == "__main__":
    unittest.main()
