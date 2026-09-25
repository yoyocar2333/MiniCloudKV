import contextlib
import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from minicloudkv.cli import handler_for
from minicloudkv.raft import Node
from scripts.check_history import check


def request(port, method, path, payload=None, timeout=5):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method,
                                 data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as result:
            return result.status, json.load(result)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def until(predicate, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(.04)
    raise AssertionError("timed out waiting for cluster")


class ClusterTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.nodes, self.servers, self.ports = {}, {}, {}
        # Reserve three distinct ports before starting the nodes.
        for name in ("n1", "n2", "n3"):
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(None))
            self.ports[name] = server.server_address[1]
            self.servers[name] = server
        for server in self.servers.values():
            server.server_close()
        self.servers = {}
        for name in self.ports:
            self.launch(name)

    def launch(self, name):
        peers = {p: f"http://127.0.0.1:{port}" for p, port in self.ports.items() if p != name}
        node = Node(name, peers, Path(self.tmp.name) / name)
        server = ThreadingHTTPServer(("127.0.0.1", self.ports[name]), handler_for(node))
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        node.start()
        self.nodes[name], self.servers[name] = node, server

    def shutdown(self, name):
        self.nodes[name].stop()
        self.servers[name].shutdown()
        self.servers[name].server_close()
        del self.nodes[name], self.servers[name]

    def tearDown(self):
        for name in list(self.nodes):
            self.shutdown(name)
        self.tmp.cleanup()

    def leaders(self, names=None):
        return [name for name in (names or self.nodes) if self.nodes[name].status()["role"] == "leader"]

    def test_failover_and_restart(self):
        leader = until(lambda: self.leaders()[0] if len(self.leaders()) == 1 else None)
        code, _ = request(self.ports[leader], "PUT", "/kv/answer", {"value": "42"})
        self.assertEqual(code, 200)
        self.shutdown(leader)
        survivor = until(lambda: self.leaders()[0] if len(self.leaders()) == 1 else None)
        self.assertNotEqual(leader, survivor)
        self.assertEqual(request(self.ports[survivor], "GET", "/kv/answer"),
                         (200, {"key": "answer", "value": "42"}))
        self.assertEqual(request(self.ports[survivor], "DELETE", "/kv/answer")[0], 200)
        self.launch(leader)
        until(lambda: self.nodes[leader].status()["commit"] >= 3)
        self.assertNotIn("answer", self.nodes[leader].kv)

    def test_isolated_leader_cannot_acknowledge_write(self):
        leader = until(lambda: self.leaders()[0] if len(self.leaders()) == 1 else None)
        for peer in self.nodes:
            if peer != leader:
                request(self.ports[leader], "POST", "/debug/block", {"peer": peer, "enabled": True})
                request(self.ports[peer], "POST", "/debug/block", {"peer": leader, "enabled": True})
        # Direct API call to use a short deadline while checking the majority rule.
        with self.assertRaises(Exception) as caught:
            self.nodes[leader].propose("put", "isolated", "unsafe", timeout=.5)
        self.assertIn("no majority", str(caught.exception))
        others = [n for n in self.nodes if n != leader]
        new_leader = until(lambda: self.leaders(others)[0] if len(self.leaders(others)) == 1 else None)
        self.assertEqual(request(self.ports[new_leader], "PUT", "/kv/safe", {"value": "yes"})[0], 200)
        for peer in others:
            request(self.ports[leader], "POST", "/debug/block", {"peer": peer, "enabled": False})
            request(self.ports[peer], "POST", "/debug/block", {"peer": leader, "enabled": False})
        until(lambda: self.nodes[leader].kv.get("safe") == "yes")
        self.assertNotIn("isolated", self.nodes[leader].kv)

    def test_real_time_read_after_write(self):
        leader = until(lambda: self.leaders()[0] if len(self.leaders()) == 1 else None)
        for number in range(6):
            self.assertEqual(request(self.ports[leader], "PUT", "/kv/counter", {"value": str(number)})[0], 200)
            self.assertEqual(request(self.ports[leader], "GET", "/kv/counter")[1]["value"], str(number))
        self.assertEqual(request(self.ports[leader], "GET", "/kv/absent")[0], 404)

    def test_concurrent_history(self):
        leader = until(lambda: self.leaders()[0] if len(self.leaders()) == 1 else None)
        history = []
        guard = threading.Lock()

        def client(index):
            op = "put" if index % 3 else "get"
            value = str(index) if op == "put" else None
            start = time.monotonic()
            code, result = request(self.ports[leader], "PUT" if op == "put" else "GET",
                                   "/kv/history", {"value": value} if op == "put" else None)
            end = time.monotonic()
            self.assertIn(code, (200, 404))
            with guard:
                history.append({"start": start, "end": end, "op": op, "value": value,
                                "result": result.get("value") if op == "get" else None})

        workers = [threading.Thread(target=client, args=(i,)) for i in range(12)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        self.assertEqual(len(history), 12)
        self.assertTrue(check(history), history)


if __name__ == "__main__":
    unittest.main()
