"""HTTP API and command-line entry point. Run only on a trusted machine."""
import argparse
import json
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit
from .raft import Node, NotLeader, Unavailable

MAX_BODY = 1024 * 1024


def handler_for(node):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def reply(self, status, obj):
            payload = json.dumps(obj, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def body(self):
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= MAX_BODY:
                raise ValueError("body must be between 1 and 1048576 bytes")
            return json.loads(self.rfile.read(length))

        def key(self):
            path = urlsplit(self.path).path
            if not path.startswith("/kv/"):
                return None
            key = unquote(path[4:])
            if not key or "/" in key or len(key.encode()) > 256:
                raise ValueError("key must be 1-256 bytes without a slash")
            return key

        def client(self, op, key, value=None):
            try:
                result = node.propose(op, key, value)
            except NotLeader:
                self.reply(409, {"error": "not leader", "leader": node.status()["leader"]})
            except Unavailable as exc:
                self.reply(503, {"error": str(exc)})
            else:
                if op == "get":
                    self.reply(200 if result is not None else 404,
                               {"key": key, "value": result} if result is not None else {"error": "missing key"})
                else:
                    self.reply(200, {"ok": True})

        def do_GET(self):
            try:
                if urlsplit(self.path).path == "/status":
                    self.reply(200, node.status())
                elif (key := self.key()) is not None:
                    self.client("get", key)
                else:
                    self.reply(404, {"error": "unknown route"})
            except (ValueError, TypeError) as exc:
                self.reply(400, {"error": str(exc)})

        def do_PUT(self):
            try:
                key = self.key()
                if key is None:
                    return self.reply(404, {"error": "unknown route"})
                value = self.body()["value"]
                if not isinstance(value, str):
                    raise ValueError("value must be a string")
                self.client("put", key, value)
            except (ValueError, KeyError, TypeError) as exc:
                self.reply(400, {"error": str(exc)})

        def do_DELETE(self):
            try:
                key = self.key()
                if key is None:
                    return self.reply(404, {"error": "unknown route"})
                self.client("delete", key)
            except (ValueError, TypeError) as exc:
                self.reply(400, {"error": str(exc)})

        def do_POST(self):
            try:
                path = urlsplit(self.path).path
                if path == "/raft/vote":
                    self.reply(200, node.request_vote(self.body()))
                elif path == "/raft/append":
                    self.reply(200, node.append_entries(self.body()))
                elif path == "/debug/block":
                    payload = self.body()
                    peer, enabled = payload["peer"], payload["enabled"]
                    if peer not in node.peers or not isinstance(enabled, bool):
                        raise ValueError("invalid peer or enabled flag")
                    with node.lock:
                        (node.blocked.add if enabled else node.blocked.discard)(peer)
                    self.reply(200, node.status())
                else:
                    self.reply(404, {"error": "unknown route"})
            except (ValueError, KeyError, TypeError) as exc:
                self.reply(400, {"error": str(exc)})
    return Handler


def main():
    parser = argparse.ArgumentParser(description="Fixed three-node Raft key-value store")
    parser.add_argument("--id", required=True)
    parser.add_argument("--listen", required=True, help="127.0.0.1:port")
    parser.add_argument("--peer", action="append", required=True, help="id=http://127.0.0.1:port")
    parser.add_argument("--data-dir", required=True, type=Path)
    args = parser.parse_args()
    peers = dict(peer.split("=", 1) for peer in args.peer)
    host, port = args.listen.rsplit(":", 1)
    if host not in ("127.0.0.1", "localhost", "::1"):
        parser.error("bind to localhost only; the demo RPC/admin API has no authentication")
    node = Node(args.id, peers, args.data_dir)
    server = ThreadingHTTPServer((host, int(port)), handler_for(node))
    node.start()
    signal.signal(signal.SIGTERM,
                  lambda *_: threading.Thread(target=server.shutdown, daemon=True).start())
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        server.server_close()


if __name__ == "__main__":
    main()
