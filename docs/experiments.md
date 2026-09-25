# Reproducible experiments (2026-09-25–26)

Environment: local container, Python 3.12.14, three processes on loopback, each with its own temporary data directory. The failure and initial baseline are single-run observations; the storage comparison has three runs per configuration. None is a comparison to Redis or another Raft library. Ports and file system behavior affect the results.

## Correctness checks

Command: `python3 -m unittest discover -s tests -v`. Sixteen tests passed after the 2026-09-26 revision. They cover restart replay of committed entries, persistence of votes, stale candidate rejection, conflicting suffix repair, leader crash and recovery, isolated leader write rejection, sequential read after write, and an exact consistency check on a 12-operation concurrent history. WAL tests recover from a torn tail, replay after checkpoint, and remove old checkpointed frames after the simulated checkpoint/truncation crash window. New tests cover client queue deadlines, a proposal arriving during an in-flight heartbeat, fast follower catch-up, and concurrent election RPCs. A separate checker unit test rejects a stale read and accepts a valid overlapping read.

A bounded exhaustive search enumerates serial orders compatible with completion-before-start precedence for up to 16 successful operations on one initially empty key. A passing finite history is evidence for that history only; timed-out operations are not silently counted as successes.

## Failure recovery

Command: `python3 scripts/failover_demo.py`. The measured interval from SIGKILL of the elected leader to the first successful read of a previously acknowledged value at the newly elected leader was **460.6 ms** in one WAL-backed run after the revision; the restarted node caught up. Election time and operation recovery are combined in this figure. Re-run locally for your own measurement.

## Sequential write baseline

Command: `python3 scripts/benchmark.py --node http://127.0.0.1:<leader-port> --operations 100`. A single run of 100 sequential PUTs across 20 keys yielded **351.63 ops/s**, p50 **2.152 ms**, p95 **4.650 ms**, p99 **14.105 ms**. The sample is small and includes local process and filesystem effects. This initial number came from the full-state rewrite mode before the WAL implementation. It is not directly comparable to the later runs, which used 1,000 operations and fresh clusters.

## Storage comparison before the 2026-09-26 revision

Command: `python3 scripts/compare_storage.py --operations 1000 --repeats 3`. Each run starts a fresh three-process cluster, performs 1,000 sequential PUTs across 20 keys, and reports throughput and latency. Trial order alternates between full-state rewrite (`snapshot`) and WAL. Both persist every transition with fsync; WAL checkpoints every 4,096 frames, so this short comparison does not include checkpoint cost. Results, in ops/s (p99 in ms):

| Trial | Snapshot ops/s | WAL ops/s | Snapshot p99 | WAL p99 |
|---|---:|---:|---:|---:|
| 1 | 236.16 | 448.58 | 73.935 | 3.260 |
| 2 | 269.15 | 407.63 | 6.264 | 4.013 |
| 3 | 301.60 | 444.66 | 6.407 | 4.674 |

Median throughput was **269.15 ops/s** for full-state rewrite and **444.66 ops/s** for WAL, about **65.2% higher in this specific short workload**. Snapshot trial 1 had an unusually high p99; these three local trials cannot establish a general tail-latency guarantee. The WAL was also slower in an initial version that deep-copied the entire log during every persistence call; replacing that with a shallow copy of immutable entries removed the hot-path cost. This failed optimization and fix are recorded here because the data structure can defeat the intended I/O benefit.

## Storage comparison after the 2026-09-26 revision

The same command and 1,000 sequential PUT workload was rerun three times per mode. Worker scheduling, concurrent elections, and WAL prefix handling changed, so these measurements should not be pooled with the earlier table.

| Trial | Snapshot ops/s | WAL ops/s | Snapshot p99 ms | WAL p99 ms |
|---|---:|---:|---:|---:|
| 1 | 179.34 | 367.85 | 13.493 | 6.690 |
| 2 | 175.19 | 321.68 | 13.510 | 7.892 |
| 3 | 186.31 | 278.25 | 13.251 | 11.893 |

The medians were **179.34 ops/s** and **321.68 ops/s** (WAL about **79.4% higher** for this short local workload). The figures changed substantially between container runs, so they are evidence for the recorded setup, not a portable speedup claim.

The leader currently persists the append and the commit-index update separately, typically two WAL frames and two fsync calls per successful operation. The 4,096-frame checkpoint occurs after roughly 2,048 successful writes, with variation from elections and other metadata updates. Both modes still retain the complete log and every GET grows it; these 1,000-write runs are below the checkpoint threshold and do not establish long-run behavior.

## Next experiment

Extend the comparison beyond the 4,096-frame checkpoint interval, repeat on controlled hardware, and profile disk fsync versus Python serialization and prefix scanning. Then test group commit under concurrent workloads and report both median and tail latency. Do not cite predicted gains before collecting results.
