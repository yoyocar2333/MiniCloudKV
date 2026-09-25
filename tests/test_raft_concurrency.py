import tempfile
import threading
import time
import unittest
from pathlib import Path
from minicloudkv.raft import Node, Unavailable


def node_at(root, name, heartbeat=.08):
    peers = {p: "http://127.0.0.1:1" for p in ("n1", "n2", "n3") if p != name}
    return Node(name, peers, Path(root) / name, heartbeat=heartbeat)


class ConcurrencyTest(unittest.TestCase):
    def test_queue_time_counts_toward_client_deadline(self):
        with tempfile.TemporaryDirectory() as root:
            node = node_at(root, "n1")
            node.role, node.term = "leader", 1
            node.client_lock.acquire()
            durations = []
            errors = []

            def client():
                start = time.monotonic()
                try:
                    node.propose("put", "k", "v", timeout=.15)
                except Unavailable as exc:
                    errors.append(str(exc))
                durations.append(time.monotonic() - start)

            workers = [threading.Thread(target=client) for _ in range(5)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(timeout=1)
            node.client_lock.release()
            self.assertEqual(len(errors), 5)
            self.assertTrue(all(.12 <= d < .4 for d in durations), durations)
            self.assertEqual(len(node.log), 1)

    def test_proposal_during_heartbeat_wakes_peer_immediately(self):
        with tempfile.TemporaryDirectory() as root:
            leader, follower = node_at(root, "n1", heartbeat=1.5), node_at(root, "n2")
            leader.role, leader.term = "leader", 1
            leader.next_index = {"n2": 1, "n3": 1}
            leader.match_index = {"n2": 0, "n3": 0}
            leader._persist()
            in_heartbeat = threading.Event()
            release = threading.Event()
            calls = 0

            def rpc(peer, route, request):
                nonlocal calls
                if peer != "n2":
                    raise OSError("unavailable")
                calls += 1
                if calls == 1:
                    in_heartbeat.set()
                    if not release.wait(2):
                        raise TimeoutError()
                return follower.append_entries(request)

            leader._rpc = rpc
            leader.start()
            try:
                self.assertTrue(in_heartbeat.wait(1))
                done = []
                def propose():
                    try:
                        leader.propose("put", "k", "v", timeout=1.0)
                        done.append("ok")
                    except Unavailable:
                        done.append("timeout")
                worker = threading.Thread(target=propose)
                worker.start()
                end = time.monotonic() + .5
                while len(leader.log) == 1 and time.monotonic() < end:
                    time.sleep(.002)
                self.assertEqual(len(leader.log), 2)
                release.set()
                worker.join(timeout=1.2)
                self.assertEqual(done, ["ok"])
                self.assertGreaterEqual(calls, 2)
                # Appending and committing still produce two durable frames.
                self.assertEqual(leader.wal_store.sequence, 3)
            finally:
                release.set()
                leader.stop()

    def test_catchup_jumps_to_follower_log_end(self):
        with tempfile.TemporaryDirectory() as root:
            leader, follower = node_at(root, "n1"), node_at(root, "n2")
            leader.role, leader.term = "leader", 1
            leader.log.extend({"term": 1, "op": "put", "key": str(i), "value": str(i)}
                              for i in range(100))
            leader._persist()
            leader.next_index = {"n2": len(leader.log), "n3": len(leader.log)}
            leader.match_index = {"n2": 0, "n3": 0}
            calls = []
            def rpc(peer, route, request):
                calls.append(request["prev_index"])
                return follower.append_entries(request)
            leader._rpc = rpc
            leader._replicate("n2")
            self.assertEqual(calls, [100, 0])
            self.assertEqual(follower.log, leader.log)
            self.assertEqual(leader.commit, 100)

    def test_slow_first_voter_does_not_delay_election(self):
        with tempfile.TemporaryDirectory() as root:
            node = node_at(root, "n1")
            node.deadline = 0
            release = threading.Event()
            def rpc(peer, route, request):
                if peer == "n2":
                    release.wait(2)
                return {"term": request["term"], "granted": True}
            node._rpc = rpc
            start = time.monotonic()
            try:
                node._start_election()
                while node.role != "leader" and time.monotonic() - start < .5:
                    time.sleep(.002)
                self.assertEqual(node.role, "leader")
                self.assertLess(time.monotonic() - start, .5)
            finally:
                release.set()
                node.stop()


if __name__ == "__main__":
    unittest.main()
