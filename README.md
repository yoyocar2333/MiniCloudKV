# MiniCloudKV

A small, runnable **three-node replicated key-value store** built to study the boundary between a successful write and a durable, recoverable write. It implements leader election, log replication, majority commitment, disk persistence, failover, a fault injector, and a bounded history checker. Python 3.11+; no third-party runtime dependencies.

> This is an educational fixed-membership system, not a production database. It intentionally writes the entire Raft state image on each change; the measured baseline is a starting point for a future WAL/group-commit study.

## Try it

From the repository root, in terminal 1:

```bash
python3 scripts/cluster.py --data-root ./data
```

Wait about a second, then ask which of ports 8901–8903 is leader:

```bash
for port in 8901 8902 8903; do curl -s "http://127.0.0.1:$port/status"; echo; done
```

Use the leader port from that response (replace `8901` below):

```bash
curl -s -X PUT http://127.0.0.1:8901/kv/message -H 'Content-Type: application/json' -d '{"value":"hello"}'
curl -s http://127.0.0.1:8901/kv/message
curl -s -X DELETE http://127.0.0.1:8901/kv/message
```

A nonleader returns HTTP 409 and a leader hint; clients should retry that address. A request without a majority returns HTTP 503; its outcome may be unknown, so read after recovery before retrying a non-idempotent extension. This prototype's PUT and DELETE operations are idempotent with the same input.

## Reproduce the evidence

```bash
python3 -m unittest discover -s tests -v
python3 scripts/failover_demo.py
python3 scripts/benchmark.py --node http://127.0.0.1:LEADER_PORT --operations 100
```

`failover_demo.py` starts three actual processes, acknowledges a PUT, kills the elected leader with SIGKILL, waits for a successful GET on the new leader, restarts the old node from disk, and reports recovery time. `benchmark.py` measures sequential PUT throughput and p50/p95/p99 latency on a running cluster; it is a baseline, not a comparative performance claim. See [experiments](docs/experiments.md) for one recorded run.

To simulate a partition in a running cluster, block each direction of a link (example n1 at 8901 and n2 at 8902):

```bash
curl -s -X POST http://127.0.0.1:8901/debug/block -H 'Content-Type: application/json' -d '{"peer":"n2","enabled":true}'
curl -s -X POST http://127.0.0.1:8902/debug/block -H 'Content-Type: application/json' -d '{"peer":"n1","enabled":true}'
```

Use `"enabled":false` on both ends to heal it. The test suite also isolates a leader from both followers and asserts that its write cannot be acknowledged.

## What is implemented

| Mechanism | Evidence |
|---|---|
| Election | Randomized timeout, persistent term/vote, log freshness check; tested on leader crash |
| Replication | AppendEntries with previous-index/term check and conflicting suffix repair |
| Commitment | Leader acknowledges an operation only after durable majority replication and local commit |
| Reads | GET adds a nonmutating log entry, commits it on a majority, then reads the local state machine |
| Recovery | Atomic fsynced Raft state image, replay of committed entries after restart |
| Verification | Tests for partition, failover/restart, stale reads, conflict repair, concurrent small-history linearizability |
| Measurement | Baseline latency/throughput utility and real process crash-to-read timing |

## Scope and limitations

- Fixed cluster of three nodes and localhost only. No membership changes, TLS, authentication, snapshots, log compaction, or disk fault tolerance beyond one atomic local state file.
- A full state rewrite and directory fsync on each update is expensive and grows with the log. No separate WAL or group commit is claimed.
- The exact history checker handles up to 16 completed, successful operations per initially empty key. It is an experiment aid, not a formal proof or a complete Jepsen-like harness.
- `GET` requires a new majority-committed log entry; reads stop during loss of quorum. This avoids serving stale data but increases disk and network traffic.
- Fault injection blocks outgoing RPC per node. Use the debug route only on the local trusted host.

Implementation and invariants: [architecture](docs/architecture.md). Application-ready, evidence-based summary: [project brief](docs/project-brief-zh.md).
