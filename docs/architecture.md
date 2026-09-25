# Architecture and invariants

## State and transport

Each process runs an HTTP server with client routes (`GET/PUT/DELETE /kv/{key}`), Raft routes (`POST /raft/vote`, `/raft/append`), and a local fault injector. The key-value state machine is rebuilt from committed log entries at startup. `term`, `voted_for`, the complete log and `commit` are stored together in `raft.json`: serialize to a temporary file, fsync it, atomically rename it, then fsync the directory. A torn or invalid state file causes a startup error instead of silent data loss. No claim is made for a faulty filesystem or broken fsync semantics.

## Election

A follower starts an election after a randomized timeout. It first persists its new term and self vote. A voter grants at most one candidate per term and only if the candidate's `(last term, last index)` is at least as recent as its own. A majority elects a leader. Higher terms cause a node to step down. Membership is hard-coded to three processes.

## Replication and acknowledgement

The leader appends an entry locally and fsyncs the state image before sending AppendEntries. Followers reject a mismatched previous index/term; the leader backs up `next_index` and retries. A matching prefix is preserved; a conflicting **uncommitted** suffix is replaced and persisted. The leader advances `commit` only when a majority has persisted an entry from its **current term**, then applies all earlier entries in index order. It persists the commit index before acknowledging the client. Followers learn the commit index in later AppendEntries messages.

The client operation mutex serializes proposals on one leader. A successful read is also a Raft log entry, so its committed index is a majority barrier for earlier writes; it reads from the local state machine after commit. On election or timeout, the API returns an error rather than reporting uncertain work as committed. A timed-out operation might later commit, hence the explicit "outcome may be unknown" response.

## Crash and partition model

Processes can stop and restart with their own intact data directories. HTTP requests can fail, time out, or be blocked by the test injector. With a majority alive and reachable, a new leader can be elected. An isolated former leader cannot acknowledge new operations. Raft safety depends on persisted votes, terms, and log entries; this implementation is exercised through unit and end-to-end tests, not model checked or formally verified.

## Known limitations and next experiment

Every append/commit rewrites and fsyncs the full log. This deliberately exposes a storage bottleneck; a segmented append-only WAL plus snapshotting and group commit is the next meaningful comparison. There is no leader transfer, dynamic membership, transport authentication, client request deduplication, conflict-index optimization, or bounded log retention. Debug endpoints are only suitable for localhost. For large workloads use a production implementation.
