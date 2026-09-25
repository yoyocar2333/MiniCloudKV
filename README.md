# MiniCloudKV

A small, runnable **three-node replicated key-value store** built to study the boundary between a successful write and a durable, recoverable write. It implements leader election, log replication, majority commitment, a checksummed append-only WAL with periodic snapshots, failover, a fault injector, and a bounded history checker. Python 3.11+; no third-party runtime dependencies.

> This is an educational fixed-membership system, not a production database. The default append-only WAL is measured against a full-state rewrite baseline. Both fsync every state transition; group commit is future work.

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
python3 scripts/compare_storage.py --operations 1000 --repeats 3
python3 scripts/benchmark.py --node http://127.0.0.1:LEADER_PORT --operations 100
```

`failover_demo.py` starts three actual processes, acknowledges a PUT, kills the elected leader with SIGKILL, waits for a successful GET on the new leader, restarts the old node from disk, and reports recovery time. `benchmark.py` measures sequential PUT throughput and p50/p95/p99 latency on a running cluster; it is a baseline, not a comparative performance claim. See [experiments](docs/experiments.md) for recorded runs and the comparison setup.

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
| Recovery | Checksummed append-only WAL, periodic atomic fsynced snapshots, replay of committed entries |
| Storage comparison | Full-state rewrite baseline (`--storage snapshot`) versus WAL (`--storage wal`, default), same sequential workload |
| Verification | 16 tests for partition, failover/restart, stale reads, conflict repair, concurrent small-history linearizability, queue deadlines, concurrent votes, fast catch-up, and checkpoint recovery |
| Measurement | Baseline latency/throughput utility and real process crash-to-read timing |

## Scope and limitations

- Fixed cluster of three nodes and localhost only. No membership changes, TLS, authentication, incremental state-machine snapshots, log compaction, or protection against physical disk loss.
- The WAL fsyncs every transition, including a separate commit-index update, and periodically checkpoints after 4,096 frames; no group commit or background compaction is claimed. A complete corrupt WAL frame stops startup; an incomplete trailing frame is discarded. Checkpointed WAL frames left by a crash are removed on recovery. The comparison baseline rewrites the complete state on each transition. Checkpoints retain the entire log; this system does not implement Raft InstallSnapshot or log compaction, and GET entries also grow the log.
- The exact history checker handles up to 16 completed, successful operations per initially empty key. It is an experiment aid, not a formal proof or a complete Jepsen-like harness.
- `GET` requires a new majority-committed log entry; reads stop during loss of quorum. This avoids serving stale data but increases disk and network traffic.
- Fault injection blocks outgoing RPC per node. Use the debug route only on the local trusted host.

Implementation and invariants: [architecture](docs/architecture.md). Application-ready, evidence-based summary: [project brief](docs/project-brief-zh.md).
