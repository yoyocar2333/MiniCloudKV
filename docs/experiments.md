# Reproducible baseline (2026-09-25)

Environment: local container, Python 3.12.14, three processes on loopback, each with its own temporary data directory. These are **single-run observations**, not statistical estimates or comparisons to Redis/Raft libraries. Ports and file system behavior affect the results.

## Correctness checks

Command: `python3 -m unittest discover -s tests -v`. Nine tests passed. They cover restart replay of committed entries, persistence of votes, stale candidate rejection, conflicting suffix repair, leader crash and recovery, isolated leader write rejection, sequential read after write, and an exact consistency check on a 12-operation concurrent history. A separate checker unit test rejects a stale read and accepts a valid overlapping read.

A bounded exhaustive search enumerates serial orders compatible with completion-before-start precedence for up to 16 successful operations on one initially empty key. A passing finite history is evidence for that history only; timed-out operations are not silently counted as successes.

## Failure recovery

Command: `python3 scripts/failover_demo.py`. The measured interval from SIGKILL of the elected leader to the first successful read of a previously acknowledged value at the newly elected leader was **402.4 ms** in one run; the restarted node caught up. Election time and operation recovery are combined in this figure. Re-run locally for your own measurement.

## Sequential write baseline

Command: `python3 scripts/benchmark.py --node http://127.0.0.1:<leader-port> --operations 100`. A single run of 100 sequential PUTs across 20 keys yielded **351.63 ops/s**, p50 **2.152 ms**, p95 **4.650 ms**, p99 **14.105 ms**. The sample is small and includes local process and filesystem effects. The benchmark does not yet compare configurations or identify the bottleneck experimentally. The full state rewrite likely grows costly as entries accumulate; this is a hypothesis for profiling, not a measured conclusion.

## Suggested next measured comparison

Implement an append-only WAL with snapshots and compare it against the present full-state baseline at 100, 1,000 and 10,000 writes under the same hardware, with repeated runs, fsync settings documented, throughput and latency distributions. Then test group commit under concurrent workloads, reporting both median and tail latency. Do not cite predicted gains before collecting results.
