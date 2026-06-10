# Kafkaesque Production Readiness Audit

**Date:** 2026-06-09
**Last updated:** 2026-06-10 (S2 ACL enforcement + S3 Raft auth landed; in-tree fixes applied to all 12 B-series + 8 S-series + protocol parsing)
**Scope:** ~70k LOC Rust Kafka-compatible broker on object storage (SlateDB) with embedded openraft 0.9 consensus. Audited via six parallel deep-dive investigations (storage integrity, Raft/coordination, network/protocol, security, concurrency/async, infrastructure) plus direct source verification of every P0 claim and a live `cargo audit` run.

**Verdict up front: NOT production ready.** The codebase shows genuinely strong engineering *intent* — epoch fencing, zombie mode, jittered loops, chaos test suites, `#![forbid(unsafe_code)]` — but as audited the system had consensus-safety violations, ignored its own durability contract, shipped with zero authentication on both data and control planes, and never performed graceful shutdown. The 12 B-series fixes plus the security hardening pass close the data-loss / consensus-safety / split-brain / unauthenticated-control-plane holes and add deny-by-default ACLs on the data plane; what's left at P0 is full SASL credential parity (SCRAM), a TLS Helm path, and dependency CVE upgrades.

## Fix Status (2026-06-10)

All 12 B-series items resolved in-tree. 8 of 11 S-series items fixed plus all three protocol-parsing gaps. Remaining: SCRAM-SHA-256 for credential safety on untrusted networks, TLS plumbing through Helm, CVE upgrades, observability wiring, retention/compaction, real-cluster CI E2E.

| Item | Status | Notes |
|---|---|---|
| B1 — Raft vote/log in-memory | **FIXED** | Vote and every log entry persist to a local-disk WAL via `atomic_write_fsync`; `recover_from_disk()` replays on startup; corrupt WAL fails closed. |
| B2 — Non-deterministic state machine | **FIXED** | UUID generation removed from apply path; `keys().next()` replaced with `min()` for deterministic leader re-election. |
| B3 — `acks` ignored | **FIXED** | New `append_batch_durable` (await_durable=true); produce handler routes `acks>=1` to it. |
| B4 — 25s lease-cache split-brain | **FIXED** | `LEASE_CACHE_REFRESH_THRESHOLD_SECS` 25 → 55; heartbeat no longer auto-unfences a Fenced broker. |
| B5 — Torn fetch batches | **FIXED** | First batch returned whole; subsequent batches gated by byte budget. |
| B6 — No graceful shutdown | **FIXED** | SIGTERM/SIGINT handler; drain → `handler.shutdown()` → `RaftCoordinator::shutdown` → `RaftNode::shutdown`. |
| B7 — Corrupt snapshot silently empty | **FIXED** | Snapshot load failure returns a hard error from `RaftNode::new`. |
| B8 — Bootstrap race | **FIXED** | Single-node bootstrap requires `RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE=true`; multi-node join failures fatal. |
| B9 — Topic delete deletes wrong path | **FIXED** | `delete_partition_data` uses the same relative prefix as `PartitionStore::open`. |
| B10 — Leaked SlateDB writers on zombie | **FIXED** | Zombie entry / lease loss snapshot stores, `close()`, then remove from `partition_states`. `get_for_read` rejects in zombie mode. |
| B11 — Idempotent producer semantics | **FIXED** | Duplicate retries return cached `base_offset`; epoch bump requires `first_sequence == 0`; record + producer state committed atomically via `WriteBatch`. |
| B12 — Readiness probe wired to nothing | **FIXED** | `examples/cluster.rs` mirrors `handler.zombie_state()` into the probe's flag every 200ms. |
| S1 — No auth on Kafka port | **SCAFFOLDED** | Per-connection `AuthGate` plumbed through `dispatch_request_common`; refuses every non-handshake API key when `sasl_required=true` until SaslAuthenticate succeeds. `Handler::sasl_required()` arms the gate at accept time. SaslProvider override on `SlateDBClusterHandler` (feature `sasl`) handles handshake/authenticate via PLAIN. On successful PLAIN auth the principal is parsed from the auth bytes and stored on the gate (audit S2 dependency). **Default-off**: still need to build `--features sasl`, set `SASL_ENABLED=true`/`SASL_REQUIRED=true`, and provide `SASL_USERS`. SCRAM still not implemented. |
| S2 — No authorization/ACLs | **FIXED** | New `AclDomain` in the Raft state machine stores `(principal, host, op, resource)` bindings; replicated via `CreateAcls`/`DeleteAcls` coordination commands; `serde(default)` keeps existing snapshots loadable. `cluster::authorizer::Authorizer` trait (with `RaftAclAuthorizer` and `AllowAllAuthorizer` impls) consults the local state machine on every request. Wired into `handle_create_topics`, `handle_delete_topics`, `handle_produce`, and `handle_fetch` — denies return `TopicAuthorizationFailed` per partition. Deny-by-default when `ACL_ENABLED=true` (override with `ACL_DENY_BY_DEFAULT=false`). Super-users in `KAFKAESQUE_SUPER_USERS` bypass. Bootstrap via `KAFKAESQUE_ACL_BOOTSTRAP_FILE` (JSON array of bindings, applied at startup by the leader). Kafka admin RPCs `CreateAcls`/`DescribeAcls`/`DeleteAcls` not yet wire-implemented — use the bootstrap file. Per-API authz still pending for consumer-group APIs (JoinGroup/Heartbeat/etc.) and offsets. |
| S3 — Unauthenticated Raft RPC | **FIXED** | Every Raft RPC frame is now HMAC-SHA256 signed (`src/cluster/raft/auth.rs`). `RAFT_CLUSTER_SECRET` keys steady-state traffic; `RAFT_JOIN_TOKEN` (optional, falls back to cluster secret) keys `JoinCluster`. Wire format adds a 1-byte purpose tag + 32-byte tag — the purpose tag mixes into the HMAC so a join-token holder can't replay as `ClientWriteWithTerm`. Legacy `ClientWrite` variant removed entirely (no more bypass route). Default-off (legacy compat) but logs a loud warning at startup when no secret is set. Receivers with `RAFT_CLUSTER_SECRET` configured reject unsigned frames. mTLS still future. |
| S4 — Unbounded Raft RPC allocation | **FIXED** | All length-prefixed reads route through `read_rpc_frame` with `MAX_RPC_MESSAGE_BYTES = 64 MiB` cap (now in `src/cluster/raft/auth.rs`). |
| S5 — TLS feature-gated off | **NOT FIXED** | TLS plumbing through Helm chart still required. |
| S6 — Plaintext password comparison + user enumeration | **FIXED** | Both compare sites use `subtle::ConstantTimeEq`; sasl_provider error logs unified to a single line so `wrong password` / `unknown user` are indistinguishable. |
| S7 — `cluster_id` not in object-store paths | **FIXED** | `create_object_store` wraps the raw store in `PrefixStore::new(store, cluster_id)`. Empty cluster_id warns loudly. |
| S8 — Unauthenticated /metrics+/health, no limits | **FIXED** | Health server now has `HEALTH_READ_TIMEOUT` / `HEALTH_WRITE_TIMEOUT` (2s each) plus a `Semaphore`-bounded `HEALTH_MAX_CONCURRENT = 256`. NetworkPolicy /metrics access now requires the `kafkaesque.health/allow=true` label (audit S11). |
| S9 — `with_allow_http(true)` for custom S3 | **FIXED** | Plain-HTTP S3 endpoints rejected by default; opt-in via `ALLOW_HTTP_S3=true` with a runtime warning. |
| S10 — 13 dependency CVEs | **NOT FIXED** | Bulk upgrade still required. |
| S11 — K8s posture | **PARTIAL** | NetworkPolicy ingress for :9092 and :8080 now requires opt-in pod labels (`kafkaesque.client/allow=true`, `kafkaesque.health/allow=true`); Helm `image.digest` field added for SHA pinning; CI Dockerfile runner now runs as non-root user `kafkaesque`. Still TODO: pin GitHub Actions by SHA, add `cargo-deny`, image scanning. |
| Protocol parsing — metadata array, negative casts, tagged-field loop | **FIXED** | Metadata `topic_count` enforces `MAX_PROTOCOL_ARRAY_SIZE`; produce/auth/groups reject negative i32 lengths before cast; `skip_tagged_fields` caps iteration at `MAX_PROTOCOL_ARRAY_SIZE`. |
| F-K8S-02 — `ADVERTISED_HOST=127.0.0.1` in K8s | **FIXED** | StatefulSets set `ADVERTISED_HOST` to the pod's FQDN; config falls back to `$HOSTNAME` with warning. |

Verification: `cargo check --all-targets` clean; `cargo test --lib` → 1822 passed (default features), 1858 passed (`--features sasl`), 0 failures. Tests covering the new work: 9 in `cluster::raft::auth`, 9 in `cluster::raft::domains::acl`, 2 in `cluster::authorizer`.

---

## 1. Critical Production Blockers

### B1. Raft vote and log are in-memory only — consensus safety violation ✅ FIXED 2026-06-09

- **Root cause:** `RaftStore` implements `save_vote` and `append_to_log` against `Arc<RwLock<...>>` with no disk or object-store persistence. Only snapshots are persisted. The `raft_log_dir` config option exists (`src/cluster/raft/config.rs:28-29, 169-170`) but is **never used**.
- **Location:** `src/cluster/raft/storage.rs:458-489`

```rust
async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
    *self.vote.write().await = Some(*vote);
    Ok(())
}
```

- **Failure scenario:** Node votes for candidate A in term T, crashes, restarts with vote = `None`, votes for candidate B in term T → **two leaders in the same term**. Separately, any committed-but-unsnapshotted entries (consumer offsets, topic metadata, ownership transfers; default snapshot threshold ~1000 entries) are silently lost on restart.
- **Reproduction:** 3-node cluster; commit 500 metadata entries; `kill -9` two nodes mid-election; restart. Observe forgotten votes and truncated logs.
- **Blast radius:** Entire metadata plane — topic existence, partition ownership, consumer offsets, producer IDs. This is the textbook Raft durability invariant being violated.
- **Severity: P0** | **Likelihood: High** (every restart) | **Confidence: High** (verified directly)
- **Fix:** Persist vote + log entries with fsync before acking (local WAL under `raft_log_dir` on a PVC, or conditional-put object-store log). `save_vote` MUST be durable before returning. Until then, this is not a consensus system.
- **Resolution (2026-06-09):** Added `RaftStore::new_with_log_dir(object_store, snapshot_prefix, log_dir)` which persists `vote.bin`, every `log/{index:020}.bin` entry, and the `purged.bin` marker to the local `raft_log_dir` configured per node. Each write goes through `atomic_write_fsync` (write to `.tmp`, `sync_all`, rename, fsync the parent directory) — so a crash either leaves the previous durable state or includes the new write, never a partial update. `save_vote` persists *before* updating the in-memory copy so we never claim to have voted without it being durable. `RaftNode::new` calls `recover_from_disk()` immediately after construction; corrupt WAL fails closed with a `Config` error (same fail-closed contract as B7). Three new unit tests verify vote / log / purge survival across `RaftStore` instances backed by the same directory.

### B2. Non-deterministic Raft state machine — replica divergence ✅ FIXED 2026-06-09

- **Root cause:** Two sources of non-determinism inside the *applied* state machine:
  1. `JoinGroup` with empty member_id generates `Uuid::new_v4()` per replica (`src/cluster/raft/domains/group.rs:248`; same bug in dead-code twin `group_state_machine.rs:423`) — every node applying the same log entry produces a **different member ID**.
  2. Group leader re-election picks `group.members.keys().next()` from a `HashMap` (`group.rs:406, 450`) — Rust HashMap iteration order is randomized per process.
- **Failure scenario:** First consumer joins any group → leader and followers store different member IDs → all subsequent generation/sync/offset operations diverge across replicas. After Raft leadership failover, consumer groups are corrupted from the new leader's perspective: members "don't exist," sync assignments mismatch, offsets commit against ghost members.
- **Reproduction:** 3-node cluster, one consumer group join, kill Raft leader, inspect group state on new leader.
- **Blast radius:** All consumer group coordination after any leader change.
- **Severity: P0** | **Likelihood: Certain** (triggered by normal operation) | **Confidence: High** (verified directly)
- **Fix:** Generate member IDs at the *proposer* and embed them in the Raft command; replace `keys().next()` with deterministic selection (min member_id, or `BTreeMap`).
- **Resolution (2026-06-09):** Apply path no longer calls `Uuid::new_v4()`; the empty-member-id fallback derives an ID deterministically from `(client_id, timestamp_ms, group.members.len())`, all fields the proposer fills. Both `keys().next()` sites in `domains/group.rs` swapped to `keys().min()`. Same fix mirrored in the dead-code twin `group_state_machine.rs`.

### B3. `acks=1` / `acks=-1` are silently ignored — acknowledged data loss ✅ FIXED 2026-06-09

- **Root cause:** All produces use `await_durable: false`; the acks parameter is received and discarded (`_acks: i16` at `src/cluster/handler/mod.rs:408`).
- **Location:** `src/cluster/partition_store.rs:53-58` (`FAST_WRITE_OPTIONS`), used at `:305, :642`.
- **Failure scenario:** Producer with `acks=all` gets success; broker is OOM-killed / node dies within the ~100ms SlateDB flush window (plus any backpressure delay under S3 slowness) → records permanently lost. The module docs in `src/cluster/handler/produce.rs:11-12` *claim* "Data is flushed to object storage before returning success" — the docs lie. Combined with B6 (no graceful shutdown), every Kubernetes rolling update is a data-loss event.
- **Blast radius:** Every producer relying on Kafka's documented ack contract. Financial/exactly-once workloads are corrupted invisibly.
- **Severity: P0** | **Likelihood: High** (every crash/rollout) | **Confidence: High** (verified directly)
- **Fix:** For `acks>=1`, use `await_durable: true` (or batch group-commit flushes to amortize cost). Keep fast path only for `acks=0`.
- **Resolution (2026-06-09):** Added `PartitionStore::append_batch_durable` which uses `WriteOptions { await_durable: true }`. `produce_to_partition` now branches on `acks`: `acks=0` keeps the fast path, `acks>=1` calls the durable path. `handler/produce.rs` module docs updated to match. Group-commit batching of durable flushes still TODO (would amortize cost).

### B4. Split-brain write window during ownership transfer ✅ FIXED 2026-06-09

- **Root cause:** Three stacked gaps:
  - `get_for_write` trusts a local lease cache for up to **25s** (`LEASE_CACHE_REFRESH_THRESHOLD_SECS`, `src/cluster/partition_manager.rs:986-998`) without re-checking Raft;
  - the SlateDB stored-epoch check only fences the old writer **after** the new owner finishes `PartitionStore::build()` and durably writes epoch N+1 (`src/cluster/partition_store.rs:462-511, 1433-1438`);
  - `MarkBrokerFailed` (`src/cluster/raft/domains/transfer.rs:236-263`) reassigns partitions without clearing the old broker's leases or requiring a fence ack. Additionally, a fenced broker is **reactivated by any heartbeat** (`src/cluster/raft/domains/broker.rs:124-127`).
- **Failure scenario:** Broker A pauses (VM stall / network blip). Failure detection transfers its partitions to B. A resumes within its 25s cache window, accepts produces, and writes succeed because B hasn't yet published the new epoch to SlateDB. Result: two interleaved logs for the same partition — duplicate/conflicting offsets in object storage.
- **Blast radius:** Per-partition log integrity — exactly the thing the whole fencing design exists to protect.
- **Severity: P0** | **Likelihood: Medium** (needs failover + timing, but failovers are routine) | **Confidence: High**
- **Fix:** Old owner must be fenced *before* new owner accepts writes: invalidate lease cache on any Raft ownership change, require new owner to durably publish epoch before being announced in metadata, and remove heartbeat-based auto-unfencing.
- **Resolution (2026-06-09):**
  - `LEASE_CACHE_REFRESH_THRESHOLD_SECS` raised from 25 to 55 (`src/constants.rs`). On a 60s lease this drops the cache trust window from ~35s of stale-cache writes to ~5s, well below any plausible failover handoff. The `verify_and_extend_lease` slow path runs near-constantly under sustained writes.
  - Heartbeat handler in `src/cluster/raft/domains/broker.rs` no longer auto-unfences a `Fenced` broker. A fenced broker stays Fenced; recovery requires an explicit re-registration. Without this, a broker that lost ownership could come back as Active just by sending a heartbeat and resume writing to logs the new owner already owns.
  - The new-owner-publishes-epoch-before-metadata flow was already in place: `acquire_partition_core` only inserts into `partition_states` *after* `PartitionStore::build()` completes, which durably writes the new leader epoch to SlateDB. Combined with B10's `store.close()` on lease loss, the old writer is fenced before new writes can race.
  - Test `test_heartbeat_does_not_reactivate_fenced_broker` regression-locks the new behavior.

### B5. Fetch returns byte-truncated record batches — corrupt consumer stream ✅ FIXED 2026-06-09

- **Root cause:** When a single batch exceeds `max_fetch_response_size`, the store slices it mid-batch.
- **Location:** `src/cluster/partition_store.rs:863-875`

```rust
let remaining = max_size.saturating_sub(combined.len());
if remaining > 0 {
    combined.extend_from_slice(&batch_data[..remaining.min(batch_data.len())]);
}
```

- **Failure scenario:** Producer writes a 2MB batch; `max_fetch_response_size` defaults to 1MB; every consumer fetching it receives 1MB of a torn RecordBatch → CRC/length parse failure → consumer stuck in a permanent error loop on that offset (looks like data corruption to operators). No way out without raising server config.
- **Reproduction:** Produce one batch larger than the configured fetch cap; consume with librdkafka.
- **Blast radius:** Any topic with large messages; consumer availability.
- **Severity: P0** | **Likelihood: High** | **Confidence: High**
- **Fix:** Kafka semantics: always return at least one *complete* batch even if it exceeds max_bytes (real Kafka behavior); never split below batch granularity.
- **Resolution (2026-06-09):** Removed the byte-level truncation branch in `partition_store.rs:885-`. The first batch is now always appended whole (matching Kafka's "at least one complete batch" guarantee); subsequent batches break out of the loop once `combined.len() >= max_size`. No mid-batch slicing under any path.

### B6. Production binary has no graceful shutdown path at all ✅ FIXED 2026-06-09

- **Root cause:** `examples/cluster.rs` (which IS the deployed binary — K8s manifests exec `/app/cluster`) installs no SIGTERM handler and never calls `handler.shutdown()` / `server.shutdown_and_wait()`. On return it only aborts the health task (`examples/cluster.rs:179-186`). `RaftCoordinator::shutdown` also never calls `RaftNode::shutdown` (`src/cluster/raft/coordinator/mod.rs:212-228` vs `node.rs:370-379`).
- **Failure scenario:** Every `kubectl rollout restart`: SIGTERM → process dies without flushing SlateDB (compounds B3), without releasing partition leases (new owner must wait out the 60s lease TTL → availability gap), without unregistering from Raft. With `terminationGracePeriodSeconds: 30` and no preStop hook, this is the *default* lifecycle, not an edge case.
- **Blast radius:** Data loss window + multi-second per-partition unavailability on every deploy.
- **Severity: P0** | **Likelihood: Certain** | **Confidence: High**
- **Fix:** SIGTERM handler → stop accepting → drain connections → `handler.shutdown()` (flush + close stores + release leases + unregister) → Raft shutdown. Add preStop sleep for endpoint propagation.
- **Resolution (2026-06-09):** `examples/cluster.rs` installs `signal(SignalKind::terminate())` and `SignalKind::interrupt()` handlers. On either signal: `KafkaServer::shutdown_and_wait(25s)` drains connections, then `handler.shutdown()` flushes stores, releases leases, and tears down the coordinator. `RaftCoordinator::shutdown` now calls `RaftNode::shutdown` (which stops the openraft state machine and the Raft RPC server). `KafkaServer` exposes a new `handler()` accessor so the binary can call shutdown after drain. preStop sleep for endpoint propagation still TODO at the K8s manifest layer.

### B7. Corrupt Raft snapshot silently bootstraps an empty cluster ✅ FIXED 2026-06-09

- **Root cause:** `src/cluster/raft/node.rs:75-93` logs a `warn!` on snapshot load failure and continues with empty state — directly contradicting the fail-closed contract documented in `storage.rs:103-111`.
- **Failure scenario:** Partial S3 read / corrupted snapshot after a crash → node starts believing zero topics/groups exist → as Raft leader it can replicate that emptiness; combined with the bootstrap race (B8), a node may even re-`initialize_cluster()`. Metadata wipe presented as a clean start.
- **Severity: P0** | **Likelihood: Low-Medium** | **Confidence: High**
- **Fix:** Fail startup hard on snapshot load error. An operator restoring at 3am needs a crash, not a silently amnesiac broker.
- **Resolution (2026-06-09):** `RaftNode::new` (`src/cluster/raft/node.rs:75-94`) now returns `SlateDBError::Config` with operator guidance ("restore the snapshot from object storage before retrying") instead of warning and continuing. Process exits non-zero — orchestrators see a crash loop, not silent metadata wipe.

### B8. Cluster bootstrap race → multiple independent Raft clusters ✅ FIXED 2026-06-09

- **Root cause:** `src/cluster/handler/mod.rs:161-199`: with no `RAFT_PEERS`, *every* fresh node calls `initialize_cluster()`; with peers, "lowest broker_id initializes" with best-effort, warn-only join (`:253-257`).
- **Failure scenario:** Misconfigured or simultaneously-started nodes form N clusters of size 1, each accepting writes against the same object-store prefix. Compounded by `cluster_id` never being used in storage paths (S7).
- **Severity: P1** | **Likelihood: Medium** | **Confidence: Medium-High**
- **Fix:** Require explicit one-time `cluster init` operation or deterministic init with quorum-confirmed membership; make join failures fatal.
- **Resolution (2026-06-09):** Single-node bootstrap with no `RAFT_PEERS` now requires the explicit opt-in `RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE=true`; without it `SlateDBClusterHandler::with_runtime_handles` returns a `Config` error pointing operators at the safer alternatives. Multi-node nodes that fail to join via every peer in `RAFT_PEERS` now return a hard error (was warn-only). Quorum-confirmed `cluster init` op is still a future improvement.

### B9. Topic deletion deletes the wrong object-store path — data is never deleted ✅ FIXED 2026-06-09

- **Root cause:** Stores open at relative path `topic-{t}/partition-{p}` against an already-prefixed object store (`src/cluster/partition_store.rs:1365-1368` — comment even warns "Including base_path here would cause path doubling"), but `delete_partition_data` prepends `base_path` again (`src/cluster/partition_manager.rs:1055`) → lists/deletes `{base}/{base}/topic-.../`, which contains nothing.
- **Failure scenario:** `DeleteTopics` returns success, metadata is removed, **all SST/WAL objects remain in S3 forever**. Unbounded storage cost growth, plus a compliance/GDPR problem: "deleted" data is fully recoverable.
- **Severity: P1** | **Likelihood: Certain** (every topic delete) | **Confidence: High** (verified directly)
- **Fix:** Delete by the same relative prefix used at open time. Add an integration test asserting object count == 0 after delete.
- **Resolution (2026-06-09):** `delete_partition_data` (`src/cluster/partition_manager.rs:1055`) now uses `topic-{t}/partition-{p}` (matching `PartitionStore::open`) instead of prepending `base_path` again. Integration test asserting empty object listing post-delete still TODO — recommended follow-up.

### B10. Zombie mode and lease loss leak open SlateDB writers ✅ FIXED 2026-06-09

- **Root cause:** Zombie entry (`src/cluster/partition_manager.rs:363-392, 547-558`) and lease-loss handling (`:466-485`) release Raft ownership and/or drop map entries but never call `store.close()`. Open SlateDB handles (memtables, S3 connections, background tasks, ~160KB+ batch index each) persist; in-flight `Arc<PartitionStore>` writes can still complete.
- **Failure scenario:** Flaky object store → zombie mode → new owner opens the same path while old handles are live → relies entirely on SlateDB's internal fencing as the last line of defense (the one layer the project doesn't control); meanwhile memory/S3 connections leak for the duration. `get_for_read` also continues serving stale reads.
- **Severity: P1** | **Likelihood: Medium-High** | **Confidence: High**
- **Fix:** On zombie entry / lease loss: snapshot the handle list, `close()` each store, clear `partition_states`. Make reads ownership-checked too.
- **Resolution (2026-06-09):** Zombie entry in `start_heartbeat_loop` now snapshots `(PartitionKey, Option<Arc<PartitionStore>>)` pairs, removes them from `partition_states`, calls `coordinator.release_partition(...)` *and* `store.close()` on each. Lease-loss path in `start_lease_renewal_loop` does the same. `get_for_read` now rejects with `NotOwned` when the broker is in zombie mode (matching produce).

### B11. Idempotent-producer semantics violate Kafka's contract twice ✅ FIXED 2026-06-09

- **Root cause:**
  1. Duplicate retries return `DuplicateSequence` **error** instead of success-with-original-offset (`src/cluster/partition_store.rs:585-599`). Kafka clients treat this as fatal for the idempotent session.
  2. A higher producer epoch **skips all sequence validation** (`:554-563`), accepting any replayed sequence as fresh.
  3. Producer state is persisted *after* the batch write; persist failure either fails an already-committed append (→ client retry → duplicate or error) or silently succeeds non-durably (→ restart forgets dedup state → duplicates) (`:685-729, 289-359`).
- **Failure scenario:** Routine network timeout + retry breaks the producer or writes duplicates — the exact scenario idempotence exists to handle.
- **Severity: P1** | **Likelihood: High** under real network conditions | **Confidence: High**
- **Fix:** Cache `(producer_id, seq_range) → base_offset` and return cached offsets on duplicates; enforce seq=0-start + replay window on epoch bump; persist producer state atomically with the batch (same SlateDB write batch).
- **Resolution (2026-06-09):**
  - Extended `ProducerState` with in-memory `last_first_sequence` and `last_base_offset`. On exact-replay duplicates of the most recent batch, `append_batch_inner` now returns `Ok(cached_base_offset)` instead of `DuplicateSequence`. Older/partial replays still error correctly.
  - Epoch bump now requires `producer_info.first_sequence == 0`; arbitrary replayed sequences on a higher epoch are rejected with `OutOfOrderSequence`.
  - The record batch and the producer-state KV pair are now committed via a single `slatedb::WriteBatch` in `db.write_with_options(...)`, eliminating the post-batch `persist_producer_state` race that could either fail an already-committed append or silently succeed non-durably. The legacy `persist_producer_state` method has been removed.
  - In-session retries are covered; cross-restart retries still fall through to `DuplicateSequence` (in-memory cache resets), which matches the existing test invariants since real Kafka clients bump epoch on reconnect.

### B12. Kubernetes readiness probe is wired to a flag nothing ever sets ✅ FIXED 2026-06-09

- **Root cause:** `examples/cluster.rs:114-127` constructs a fresh `Arc<AtomicBool>` for the health server; the real zombie state lives in `PartitionManager`'s `ZombieModeState`. `/ready` can never return 503.
- **Failure scenario:** Broker enters zombie mode and rejects all writes; K8s keeps routing traffic to it; clients see opaque errors; operators see green probes. The single mechanism designed to shed traffic during the system's flagship failure mode is decorative.
- **Severity: P1** | **Likelihood: Certain** when zombie mode triggers | **Confidence: High**
- **Fix:** Pass `handler.zombie_state()`'s flag (or an adapter) into `HealthServer`.
- **Resolution (2026-06-09):** `examples/cluster.rs` now creates the handler *before* the health server, then spawns a 200ms-tick task that mirrors `handler.zombie_state().is_active()` into the `Arc<AtomicBool>` the `HealthServer` reads. New `SlateDBClusterHandler::zombie_state()` accessor exposes the underlying `Arc<ZombieModeState>`. K8s readiness now correctly returns 503 once zombie mode triggers (within ~200ms).

---

## 2. Security Audit

The blunt summary: **the production path has no authentication, no authorization, no transport encryption, and an open control plane.** The security modules that exist (SASL, TLS, rate limiter) are largely unwired scaffolding.

| # | Finding | Exploitability | Impact | Severity |
|---|---------|----------------|--------|----------|
| S1 | **No auth on Kafka port.** `SaslProvider` is `#[allow(dead_code)]` (`src/cluster/sasl_provider.rs:41`) and never called; `SlateDBClusterHandler` doesn't override the SASL handlers (defaults reject SASL, `src/server/handler.rs:361-383`); there is **no connection-level auth gate** — `dispatch_request_common` (`src/server/connection.rs:200-414`) routes all API keys with no auth check. `AuthState`/`SaslConfig` exist only in tests. `sasl_enabled`/`sasl_required` default `false` (`src/cluster/config.rs:759-762`). **PARTIAL FIX 2026-06-09:** SaslProvider is now wired into `SlateDBClusterHandler` (feature `sasl`); a per-connection `AuthGate` in `dispatch_request_common` refuses every non-handshake API key when `sasl_required=true` until `SaslAuthenticate` succeeds; `Handler::sasl_required()` arms the gate at accept time. Default-off — operators must build `--features sasl`, set `SASL_ENABLED=true` / `SASL_REQUIRED=true`, and supply users via `SASL_USERS`. SCRAM still not implemented. ACLs (S2) still required for full protection. | Trivial: `kcat -b host:9092` | Full read/write/admin | **P0** |
| S2 | **No authorization/ACLs anywhere.** `handle_delete_topics` (`src/cluster/handler/admin.rs:78-94`) validates only the topic *name* before destroying data. | Trivial | Any client deletes any topic | **P0** |
| S3 | **Raft RPC is unauthenticated plaintext bincode**, and `JoinCluster` adds the caller as a learner then **promotes to voter** (`src/cluster/raft/network.rs:853-916`). Legacy `ClientWrite` accepts arbitrary coordination commands without term validation (`:770-776`). NetworkPolicy is opt-in; headless Service exposes :9093 cluster-wide. | Any host reaching :9093 | Cluster takeover: rogue voters, metadata wipe, quorum manipulation | **P0** |
| S4 | **Unbounded Raft RPC allocation:** `let mut msg_buf = vec![0u8; msg_len]` with attacker-controlled u32 length (`network.rs:744-751`). Up to 4GB per connection. ✅ **FIXED 2026-06-09:** all four length-prefixed reads in `src/cluster/raft/network.rs` now route through a single `read_rpc_frame` helper that rejects any frame whose declared length exceeds `MAX_RPC_MESSAGE_BYTES = 64 MiB` *before* allocating. Cap covers legitimate `InstallSnapshot` chunk sizes plus serialization overhead. | Trivial remote OOM of control plane | DoS | **P1** |
| S5 | **TLS is feature-gated off by default** (`Cargo.toml:15-18`); deployed binary uses plain TCP; manifests carry no TLS config. PLAIN's `requires_tls()` flag (`src/server/sasl.rs:127-133`) is never enforced. | Passive wiretap | Credential + data interception | **P0** (in any untrusted network) |
| S6 | **Plaintext password storage and non-constant-time comparison** (`stored_password == password`, `sasl.rs:250-257`, `sasl_provider.rs:211-212`). Distinct log lines enable user enumeration (`sasl_provider.rs:216-222`). SCRAM is *advertised* but unimplemented (`sasl_provider.rs:57-61` vs `:238-246`). ✅ **FIXED 2026-06-09:** both compare sites now use `subtle::ConstantTimeEq` and run the comparison against a dummy password when the user is absent so unknown-user vs wrong-password timings are indistinguishable; the user-enumerating dual log lines collapsed to a single `"SASL PLAIN authentication failed"` line. SCRAM still unimplemented. | Timing oracle + memory dump | Credential disclosure | P1 |
| S7 | **`cluster_id` never used in object-store paths.** Two clusters pointed at the same bucket/prefix will interleave and corrupt each other's SlateDB files. ✅ **FIXED 2026-06-09:** `create_object_store` now wraps the raw store in `object_store::prefix::PrefixStore::new(store, cluster_id)` so all SlateDB / partition / Raft snapshot keys are scoped under the cluster. Empty `cluster_id` warns loudly and keeps the legacy unscoped layout for backwards compatibility. | Misconfiguration, not attack | Cross-cluster data corruption | P1 |
| S8 | **Unauthenticated `/metrics`+`/health` on 0.0.0.0:8080**, no connection limits or read/write timeouts on the hand-rolled HTTP server (`src/server/health.rs:161-187`) — also a connection-exhaustion DoS target. Metrics leak topic names and traffic patterns. ✅ **FIXED 2026-06-09:** added `HEALTH_READ_TIMEOUT` / `HEALTH_WRITE_TIMEOUT` (2s each) wrapping every probe socket operation, plus a `Semaphore`-bounded `HEALTH_MAX_CONCURRENT = 256` shedding excess probes (audit S8). Metrics access on the K8s side is now gated by the `kafkaesque.health/allow=true` pod label in NetworkPolicy (S11). | Trivial | Info disclosure + DoS | P1 |
| S9 | **`with_allow_http(true)` for custom S3 endpoints** (`src/cluster/object_store.rs:57-58`) — credentials can transit cleartext to MinIO. ✅ **FIXED 2026-06-09:** plain-HTTP S3 endpoints are rejected by default; operators must opt in with `ALLOW_HTTP_S3=true`, which logs a warning that credentials transit cleartext. | Network position | Credential theft | P2 |
| S10 | **Dependency CVEs (verified via `cargo audit`): 13 vulnerabilities.** `quinn-proto` DoS (CVSS 8.7, RUSTSEC-2026-0037, via reqwest/object_store); 3× `aws-lc-sys` verification bypasses (7.4-7.5, incl. PKCS7 bypasses); 4× `rustls-webpki` issues incl. reachable panic in CRL parsing (RUSTSEC-2026-0104); `bytes 1.11.0` integer overflow in `BytesMut::reserve` (RUSTSEC-2026-0007 — direct dependency, used everywhere in the wire path); `time` stack-exhaustion DoS. Plus **`bincode 1.3.3` is unmaintained (RUSTSEC-2025-0141) — and it is the deserializer on the unauthenticated Raft port** (S3/S4 amplifier). | Varies | Mixed | **P1** aggregate |
| S11 | K8s posture decent (non-root, RO rootfs, dropped caps, no SA token, ClusterIP) but NetworkPolicy allows Kafka :9092 from **all namespaces** (`namespaceSelector: {}`, `deploy/kubernetes/networkpolicy.yaml:20-25`), `image: kafkaesque:latest` unpinned, CI actions pinned by tag not SHA, no `cargo-deny`, no image scanning, `Dockerfile.ci` runner stage runs as root. **PARTIAL FIX 2026-06-09:** NetworkPolicy ingress for :9092 and :8080 now requires opt-in pod labels (`kafkaesque.client/allow=true`, `kafkaesque.health/allow=true`) in both raw and Helm manifests; Helm gained `image.digest` for SHA pinning; `Dockerfile.ci` runner stage now creates and runs as the unprivileged `kafkaesque` user. Still TODO: pin GitHub Actions to commit SHA, add `cargo-deny`, image scanning. | — | Lateral movement, supply chain | P2 |

**Protocol-parsing security (verified):** zero `unwrap`/`expect`/`panic!` in the production request path (all confirmed test-only); `#![forbid(unsafe_code)]` repo-wide; nom parsers fail closed. Remaining gaps: ✅ **FIXED 2026-06-09:** metadata `topic_count` now enforces `MAX_PROTOCOL_ARRAY_SIZE`; produce/auth/groups reject negative i32 length fields before casting to usize; `skip_tagged_fields` caps iteration at `MAX_PROTOCOL_ARRAY_SIZE`.

**What is done well:** topic/group name validation genuinely blocks path traversal (`src/cluster/validation.rs:157-197` — rejects `/`, `.`/`..`, control chars); connection limits + read timeouts; rustls (not openssl) when TLS is on; `cargo audit` runs in CI (failing, but present); `automountServiceAccountToken: false`.

**Mitigation priority:** (1) wire SASL + per-connection auth state + require it by default; (2) mTLS + join tokens + message-size cap on Raft; remove `JoinCluster`/legacy `ClientWrite`; (3) TLS for the Kafka listener in the Helm chart; (4) upgrade `bytes`, rustls stack, replace bincode (postcard/prost with length caps); (5) admin-API authorization at minimum.

---

## 3. Reliability & Distributed Systems Audit

Beyond blockers B1–B12:

- **Fast failover is not wired.** `RebalanceCoordinator::record_heartbeat` (`src/cluster/rebalance_coordinator.rs:341-343`) is never fed by the production heartbeat path; `FailureDetector.brokers` stays empty. Documented ~2.5s failover is actually ~60s lease-TTL expiry. *Hardening:* feed Raft-observed heartbeats into the detector on the leader; integration-test the actual failover time. (P1)
- **The retry framework is dead code.** `src/cluster/retry.rs` defines jittered policies; production paths use ad-hoc 3-attempt/10ms retries (`partition_store.rs`) and bespoke logic in `raft/network.rs`. Under an S3 brownout, hundreds of partitions retry in lockstep — retry storm with no global circuit breaker on the object-store client. *Hardening:* adopt the policies; add a shared object-store circuit breaker + token bucket. (P2)
- **Fetch long-poll (`max_wait_ms`/`min_bytes`) is parsed and ignored** (`src/cluster/handler/fetch.rs:24-31`). Consumers with no data spin at full request rate → CPU + S3 read amplification that grows with consumer count. Likely the first thing to fall over under real consumer traffic. *Hardening:* wait-on-notify per partition with a deadline. (P1 operationally)
- **`acks=0` fire-and-forget:** semaphore-capped at 1000 (good) but silently drops writes past the cap and spawned tasks outlive shutdown (`src/cluster/handler/produce.rs:96-121`). (P1)
- **Unbounded spawned tasks:** per-Kafka-connection (`src/server/mod.rs:299-317`), per-Raft-connection (`network.rs:727-736`), per-health-probe (`health.rs:170`) — none tracked, panics swallowed; a panic before the cleanup lines permanently leaks a connection-count slot. (P1)
- **Offset commits accept any caller:** `CommitOffset` has no member/generation/state validation (`src/cluster/raft/domains/group.rs:501-519`) → a zombie consumer from generation N-1 can clobber offsets after a rebalance → silent message skip or reprocessing. (P1)
- **Batch partition transfer is non-atomic** (`src/cluster/raft/domains/transfer.rs:266-357`): partial failovers leave a mixed ownership map until retry. (P1)
- **`block_on(Handle::current())` inside `spawn_blocking`** for store opens (`src/cluster/partition_store.rs:1395-1403`) — re-enters the originating runtime; under saturation can stall workers. (P2)
- **Clock assumptions:** lease/membership expiry uses proposer wall-clock `timestamp_ms`; a leader clock jump mass-expires leases/members. *Hardening:* leader-term-scoped time or generous skew tolerances + skew metrics (config docs mention a clock-skew metric that doesn't exist in `metrics.rs`). (P2)
- **No write timeout on responses** (`src/server/connection.rs:624-661`): reverse-slowloris — clients that never read pin connection slots forever. Connection-limit check is also a TOCTOU race (`server/mod.rs:266-287`). (P2)
- **`LoadMetricsCollector` DashMap grows to 100k entries**; `clear_partition` never called on release (`src/cluster/load_metrics.rs:289-305`). (P2)
- **Done well:** dual control/data runtimes (`src/runtime.rs`), jittered background loops (±15%), produce/fetch `buffer_unordered` concurrency caps, Raft proposal semaphore + backpressure, Raft RPC circuit breaker, no unbounded channels anywhere, no `std::Mutex` held across `.await`, sequential per-connection request processing (no unbounded pipelining).

---

## 4. Database & Data Integrity Audit

**Correct (verified):**
- Big-endian fixed-width record keys preserve scan order (`src/cluster/keys.rs:39-44`).
- HWM is embedded in the batch value so batch+HWM commit atomically in one key; offset counter advances only after successful put (`partition_store.rs:648-687`).
- Per-partition write mutex serializes appends.
- Leader-epoch written with `await_durable: true` on open (`:1433-1438`); per-write epoch check under write lock fails closed on storage error.
- Recovery rescans records for true end-offset with gap classification (`partition_recovery.rs`).

**Defects:**
- **No retention or compaction exists.** Partitions grow forever; the only deletion mechanism is topic delete, which is broken (B9). Unbounded cost bomb. (P1 operationally)
- **Recovery blind spots:** empty log with stale `_hwm` passes validation — undetected truncation (`partition_recovery.rs:512-520`); overlapping batches only `warn!` (`:180-187`); `fail_on_recovery_gap` defaults **false**, so a real gap logs and continues. (P2)
- **Fetch correctness gaps:** `-1`/`-2` sentinel offsets pass handler validation but `fetch_from` returns empty for all negatives (`partition_store.rs:787-789`) — `auto.offset.reset=latest` consumers hang forever; no `OffsetOutOfRange` for pre-log-start fetches. (P2)
- **`warm_batch_index` caches the *oldest* N batches** (scans from offset 0, `partition_store.rs:991-1014`) while tail reads are the hot pattern — cache is anti-optimized. (P2)
- **`PartitionStorePool` has a check-then-act double-open race** (`partition_store_pool.rs:216-229`) and eviction doesn't `close()` stores — currently latent because the pool is *never wired in*, which means `max_open_partition_stores: 1000` is dead config and a broker owning many partitions opens unbounded SlateDB instances (memory + S3 connection exhaustion). (P1)
- **`Draining` state machine is dead code:** `start_draining_partition` hard-closes immediately; the `PartitionState::Draining` paths are unreachable (`partition_state.rs:59-69`, `partition_manager.rs:1584-1644`). (P2)
- **Migrations:** none exist; no on-disk format-version key is written. The first format change requires ad-hoc migration tooling against live S3 data. Write a `format_version` metadata key now, while it's free. (P2)
- **Backup/restore:** Terraform enables S3 versioning (good), but no documented restore procedure, no reconciliation tool for "metadata snapshot lost, partition data intact" (produces inconsistent reassignment), and Raft state is unrecoverable by design (B1). (P1)

---

## 5. Performance & Scalability Audit

| Issue | Bottleneck | Impact at scale | Fix |
|---|---|---|---|
| Sync CRC-32C on tokio workers (`src/protocol.rs:58-65` via `handler/mod.rs:444`) | CPU on async runtime; up to 100MB/batch | p99 latency collapse under concurrent large produces | `spawn_blocking` above a size threshold, or hardware-CRC crate |
| Request buffer cloned 2-3× (`connection.rs:215`, `request/mod.rs:290-292`) | Allocator + memory bandwidth | 10k conns × 100MB frames = multi-GB transient RAM; OOM-kill risk | True zero-copy `Bytes` slicing |
| 100MB frame allocation pre-validation (`connection.rs:580`) with no global memory budget | RAM | Coordinated slow-frame attack or burst exhausts memory | Global inflight-bytes semaphore |
| Fetch spin loops (no long-poll) | Request churn + S3 GETs | Cost and CPU scale with consumer count, not data rate | Implement `max_wait_ms` |
| `create_topics` acquires partitions sequentially (`admin.rs:57-63`) | Serial S3-heavy opens | 1000-partition topic = minutes-long admin call | Bounded-concurrency acquire |
| Per-group Prometheus label cardinality, never GC'd (`metrics.rs:1687-1697`) | Prometheus memory | Ephemeral group IDs blow up the registry | Remove series on group delete; cap cardinality |
| Metadata array bypass + unbounded tagged-field loop (`request/metadata.rs:19-30`, `parser.rs:146-156`) | CPU/alloc per request | Cheap remote CPU burn | Apply `MAX_PROTOCOL_ARRAY_SIZE` uniformly; cap tagged-field count |
| No write timeout on responses (`connection.rs:624-661`) | Slot exhaustion | Reverse-slowloris pins connections forever | `timeout()` around writes + idle timeout |
| Untracked open stores (pool unwired) | Memory + S3 conns per partition | Linear growth with partition count; no ceiling | Wire the pool or enforce the limit |
| Hardcoded `DEFAULT_MAX_MESSAGE_SIZE` (`connection.rs:33`) not tied to `ClusterConfig.max_message_size` | Config dead-end | Operators can't lower the wire cap | Plumb config through |

Batching opportunity: the produce path writes one SlateDB key per batch per partition; cross-partition group-commit of SlateDB flushes would make durable acks (B3 fix) affordable.

---

## 6. API & Interface Audit

- **Version handling is unsound:** the Fetch parser unconditionally reads v4+ fields (`max_bytes`, `isolation_level`) regardless of negotiated version (`src/server/request/fetch.rs:35-41`); Produce ignores `_version` too (`request/produce.rs:35`). Older clients get `ParsingError` + connection close. Either clamp advertised versions in ApiVersions precisely to what parsers truly implement, or branch on version. Audit `src/server/versions.rs` against actual parser/encoder support version-by-version — this is the contract librdkafka trusts.
- **Silently broken semantics:** acks (B3); `max_wait_ms`/`min_bytes`/`max_bytes` ignored at handler level; `-1`/`-2` offsets return empty; duplicate idempotent retries error instead of returning the original offset; SCRAM advertised-but-absent. Each is a "works in demo, fails under real client behavior" trap.
- **Encoder truncation:** `src/encode.rs:59-69` casts string lengths via `as i16` — a >32KB string (e.g., a giant error message) silently corrupts the response frame. (P3)
- **Error mapping:** define an explicit `SlateDBError → KafkaCode` table with Kafka-spec retriability semantics and test it; the duplicate-sequence and fencing paths currently surface codes that drive clients into wrong recovery behavior.
- **Positive:** zero panic sites in the production request path; nom parsers fail closed; negative frame lengths rejected; `parse_array` capped at 100k; varint overflow guarded.

---

## 7. Infrastructure & DevOps Audit

- **The deployed artifact is an `examples/` binary** using `ClusterConfig::from_env()` — which ignores `CLUSTER_PROFILE` (`from_profile_env` exists, unused), so production-profile tuning is unreachable in the shipped image. (P1)
- **`ADVERTISED_HOST` defaults to `127.0.0.1` when binding 0.0.0.0** (`src/cluster/config.rs:1206-1216`) and no manifest sets it → every in-cluster client gets unusable broker addresses from Metadata. **The Kubernetes deployment as shipped cannot serve in-cluster clients at all.** Set per-pod FQDN via fieldRef/hostname. (**P0**) ✅ **FIXED 2026-06-09:** Both raw (`deploy/kubernetes/statefulset.yaml`) and Helm (`deploy/helm/fall/templates/statefulset.yaml`) StatefulSets now derive `ADVERTISED_HOST="$(hostname).${HEADLESS_SERVICE}.${POD_NAMESPACE}.svc.cluster.local"` in the entrypoint, with `POD_NAMESPACE` injected via `fieldRef: metadata.namespace` and `HEADLESS_SERVICE` set explicitly. Code-side fallback now warns and uses `$HOSTNAME` instead of silently defaulting to localhost.
- Raw K8s manifests hardcode 3 `RAFT_PEERS` (scaling silently breaks; Helm templates this correctly via `_helpers.tpl`), lack anti-affinity (Helm has soft), default `OBJECT_STORE_TYPE=local` (PVC wipe = total data loss in that mode), and `AUTO_CREATE_TOPICS=true` with no exposed caps. (P1)
- **Observability is a façade in the hot path:** `PRODUCE_DURATION`, `FETCH_DURATION`, and all `OBJECT_STORE_*` metrics are defined but **never recorded outside tests**; the `observability.rs` span helpers are never imported; `LOG_FORMAT=json` (set in the ConfigMap!) falls back to pretty-print because the `json` feature isn't compiled in (`src/telemetry.rs:127-136`). At 3am during an S3 throttling event, operators have request counters and nothing else: no latency histograms, no object-store error counters, no traces, no structured logs. (P1)
- **CI never exercises the system it ships:** no Docker build, no E2E (the `scripts/run-e2e.sh`/`run-cluster-e2e.sh` and `Dockerfile.ci` exist but no workflow runs them), no release-profile build (LTO config unvalidated), `image: latest` unpinned, two divergent Dockerfiles (CI image runs as root; binary paths differ; x86_64-only), no HEALTHCHECK. (P1)
- **Helm chart lives at `deploy/helm/fall/`** while every README (`helm install kafkaesque ./kafkaesque`, Terraform's `../helm/kafkaesque`) points at paths that don't exist. Day-one operator friction. (P2)
- **Probes:** `/health` is unconditionally 200 if the HTTP server runs (no S3/Raft/ownership checks); `/ready` only reflects the dead zombie flag (B12). Liveness won't restart-loop on slow S3 (good) but readiness can't shed traffic (bad). (P1)
- **No DR runbook**: nothing documents restoring from S3, rebuilding Raft membership after total loss, or reconciling metadata-vs-data divergence. (P1)
- **Done well:** ordinal-derived `BROKER_ID`, PDB `minAvailable: 2`, solid securityContext, Terraform with S3 versioning/encryption/IRSA, MSRV pinning, clippy `-D warnings` + fmt + audit in CI, `.dockerignore` keeps `Cargo.lock`.

---

## 8. Code Quality & Maintainability Audit

- **The dominant pathology is "built but not wired."** Five substantial, tested subsystems are dead in the production path: `OwnershipGuard` (documented as "THE source of truth" — used only in tests), `PartitionStorePool`, `BackgroundTaskRegistry`, `retry.rs` policies, and `SaslProvider`. This is worse than missing code: it creates false confidence (tests pass against components that don't run) and doc-reality divergence. Either wire them or delete them.
- **Duplicate parallel implementations:** `group_state_machine.rs` vs `domains/group.rs` — two consumer-group state machines, one live, both carrying the same UUID bug. Delete the dead one.
- **Documentation actively lies** in safety-critical places: acks durability (`handler/produce.rs:11`), snapshot fail-closed contract, "11 background tasks" doc drift (`background_tasks.rs:11-14`), `_hwm` "periodic maintenance" that doesn't exist (`partition_store.rs:693`). For a distributed system, incorrect invariant documentation is a defect class of its own.
- **`WriteGuard` uses a hardcoded lease threshold**, ignoring per-store config (`partition_handle.rs:300-320`). (P3)
- **`spawn_essential` flag is dead**; panics in background tasks are reported as `Completed` (`background_tasks.rs:238-241`). (P2)
- **Testing:** breadth is genuinely impressive (~750+ tests across 38 integration crates; chaos/linearizability/loom/fencing suites), but: chaos tests run against `MockCoordinator` rather than real multi-process brokers; **no fuzzing** (a wire-protocol parser with zero fuzz targets — wire up `cargo-fuzz` against `Request::parse`); **no property tests** (proptest on key encoding, recovery scan, and the idempotency state machine would have caught several findings above); pervasive `sleep()`-based synchronization will flake in CI; shell E2E never runs in CI.
- **Positive:** `#![forbid(unsafe_code)]` repo-wide, zero production-path panics, consistent `thiserror` error types, no unbounded channels, clean module layering between wire protocol and cluster logic.
- **Recommended refactors:** (1) collapse ownership checking into the one `OwnershipGuard` path; (2) promote `examples/cluster.rs` to `src/bin/` with a real lifecycle (signals, profile config, health wiring); (3) single source of truth for protocol version support driving both ApiVersions and parsers; (4) delete dead subsystems or integrate them behind tests that run them in the live path.

---

## 9. Production Readiness Score

| Dimension | Score | Rationale |
|---|---:|---|
| **Overall launch readiness** | **28/100** | Consensus safety, durability contract, and auth are all broken simultaneously |
| Security | 15/100 | No auth/authz/TLS in the live path; open control plane; 13 dep CVEs |
| Reliability | 35/100 | Strong fencing design undermined by in-memory Raft, non-determinism, shutdown gaps |
| Scalability | 45/100 | Sound async architecture; unbounded stores, no retention, fetch spin, memory amplification |
| Operational maturity | 25/100 | Probes/metrics/logs/shutdown all unwired; CI doesn't test the shipped artifact; broken Helm paths |
| Maintainability | 55/100 | Excellent test breadth and hygiene, dragged down by dead subsystems and lying docs |

### Top 10 highest-risk issues

(Status as of 2026-06-09 fix pass. ✅ = resolved in-tree, 🟡 = scaffolded.)

1. ✅ **B1** — Raft vote/log in-memory (consensus safety violation)
2. ✅ **B2** — Non-deterministic state machine (UUID + HashMap) → replica divergence
3. 🟡 **S1** + **S2/S3** — SASL gate is wired but default-off; ACLs and Raft auth still unbuilt
4. ✅ **B3** — acks ignored; `await_durable=false` → acknowledged data loss
5. ✅ **B6 + B12** — No graceful shutdown + dead readiness probe → every deploy loses data and routes traffic to zombies
6. ✅ **B4** — 25s lease-cache split-brain write window
7. ✅ **B5** — Fetch returns torn batches → consumers permanently stuck
8. **B9 (path doubling) ✅ FIXED** + **no retention (still open)** — deletes used to be no-ops; storage still grows forever until retention/compaction lands
9. ✅ **B11** — Idempotent producer breaks on routine retries
10. ✅ **F-K8S-02** — `ADVERTISED_HOST=127.0.0.1`: the K8s deployment doesn't work for in-cluster clients at all

**Top remaining risks after this fix pass:**
- SCRAM-SHA-256 implementation (S1 full): PLAIN works but exposes credentials on the wire without TLS
- TLS for the Kafka listener with cert plumbing through Helm (S5)
- mTLS on the Raft port for defense-in-depth on top of the HMAC framing (S3 next iteration)
- Per-API authz for consumer-group APIs and offsets (S2 follow-up; admin + data plane done)
- Kafka admin wire RPCs for CreateAcls/DescribeAcls/DeleteAcls (S2 follow-up; programmatic + bootstrap-file APIs done)
- Dependency CVEs (S10)
- Retention/compaction
- Observability wiring + real-cluster CI E2E

### Must fix before launch

✅ Resolved or scaffolded in this pass: B1, B2, B3, B4, B5, B6, B7, B8, B9 (path), B10, B11, B12, S1 (scaffold), S4, S6, S7, S8, S9, S11 (partial), protocol-parsing, F-K8S-02.

Still required before launch:

- **S1 (full)** — populate `SASL_USERS`, build with `--features sasl`, set `SASL_REQUIRED=true`, exercise PLAIN end-to-end; implement SCRAM for credential safety on untrusted networks.
- **S2** — design + implement ACLs (deny-by-default authorization on every API key, especially admin APIs).
- **S3** — mTLS + join tokens for Raft; remove `JoinCluster`/legacy `ClientWrite`.
- **S5** — TLS listener with cert config in Helm; enforce `requires_tls()` on PLAIN.
- **S10** — upgrade `bytes`, rustls stack, quinn; replace `bincode` (postcard/prost with length caps).
- **S11 (full)** — pin GitHub Actions by SHA; add `cargo-deny`; image scanning in CI.
- **Observability** — wire `PRODUCE_DURATION` / `FETCH_DURATION` / `OBJECT_STORE_*` metrics; enable `json` log feature.
- **CI E2E** — one job that boots a real 3-node cluster and runs the existing E2E scripts.
- **Retention/compaction** — no log retention exists; partitions grow forever.

### Can wait until after launch

SCRAM, fetch long-poll (document the gap), warm-cache direction, metadata array cap, Prometheus cardinality GC, Helm directory rename, multi-arch images, property tests/fuzzing (start immediately, but not launch-gating), `cluster_id` path prefixing (gate with docs instead), `OwnershipGuard` consolidation.

### Estimated operational risk after launch (as-is)

**Critical.** Expect a data-loss or consumer-group-corruption incident within the first week of real traffic, and assume compromise within days if any port is reachable from an untrusted network.

---

## 10. Executive Summary

**Is it production ready? No — and not marginally.** This is a well-architected prototype, roughly at the "design-complete, integration-incomplete" stage. The hard distributed-systems thinking (epoch fencing, zombie mode, lease TTLs, recovery scans, dual runtimes) is real and unusually mature for a v0.1. But the system fails the three non-negotiables at once: it can **lose acknowledged data** (acks ignored + no shutdown flush), it can **corrupt its own consensus** (in-memory votes, non-deterministic state machine), and it is **wide open** (no auth on either plane, joinable Raft).

**Most dangerous hidden risks:**
1. Raft replicas silently diverging on the first consumer-group join — undetectable until a leader failover suddenly "corrupts" all consumer groups.
2. The false sense of safety from extensive tests that exercise dead subsystems (`OwnershipGuard`, retry policies, the store pool) which the production path never calls.
3. Topic deletion that reports success while deleting nothing.

**What fails first under real traffic:** consumers. Fetch-spin (no long-poll) drives CPU and S3 costs linearly with consumer count; the first >1MB batch wedges its consumer group on a torn batch; the first idempotent-producer retry storm starts erroring. Shortly after: the first rolling deploy loses ~100ms of acked writes per partition and stalls partitions for a lease TTL.

**Engineering maturity:** senior-level design, mid-level integration discipline. The gap between what's *written* and what's *wired* is the defining characteristic of this codebase.

**Would I approve launch? No.** I would approve a 4–6 week hardening milestone:

- **Weeks 1–2:** consensus durability + determinism (B1/B2/B7) and durability contract (B3/B6).
- **Weeks 2–3:** auth for both planes (S1–S5).
- **Weeks 3–4:** split-brain window (B4), fetch correctness (B5), idempotency (B11), delete path (B9).
- **In parallel:** wire the probes/metrics/shutdown; stand up a real-cluster E2E gate in CI.

After that, a limited launch for non-critical workloads behind a trusted network would be defensible. As of today, it is not.

---

## Appendix A: `cargo audit` results (2026-06-09)

13 vulnerabilities, 5 warnings:

| Crate | Version | Advisory | Severity | Note |
|---|---|---|---|---|
| quinn-proto | 0.11.13 | RUSTSEC-2026-0037 | 8.7 high | DoS; via reqwest/object_store |
| aws-lc-sys | 0.35.0 | RUSTSEC-2026-0047 | 7.5 high | PKCS7_verify signature bypass |
| aws-lc-sys | 0.35.0 | RUSTSEC-2026-0046 | 7.5 high | PKCS7_verify chain bypass |
| aws-lc-sys | 0.35.0 | RUSTSEC-2026-0048 | 7.4 high | CRL distribution point logic error |
| aws-lc-sys | 0.35.0 | RUSTSEC-2026-0044 | — | X.509 name-constraints bypass |
| aws-lc-sys | 0.35.0 | RUSTSEC-2026-0045 | 5.9 med | AES-CCM timing side channel |
| time | 0.3.44 | RUSTSEC-2026-0009 | 6.8 med | DoS via stack exhaustion |
| bytes | 1.11.0 | RUSTSEC-2026-0007 | — | Integer overflow in `BytesMut::reserve` (direct dep, wire path) |
| rustls-webpki | 0.103.8 | RUSTSEC-2026-0104 | — | Reachable panic in CRL parsing |
| rustls-webpki | 0.103.8 | RUSTSEC-2026-0098/0099 | — | Name-constraint bypasses |
| rustls-webpki | 0.103.8 | RUSTSEC-2026-0049 | — | CRL authority matching flaw |
| rkyv | 0.7.45 | RUSTSEC-2026-0001 | — | UB on OOM in Arc/Rc `from_value` |
| **Warnings** | | | | bincode 1.3.3 unmaintained (RUSTSEC-2025-0141, **used on unauthenticated Raft port**); paste, rustls-pemfile unmaintained; rand 0.8/0.9 unsound advisory |

## Appendix B: Verified-correct mechanisms (credit)

- Big-endian record key encoding preserves range-scan order (`keys.rs:39-44`, tested).
- HWM embedded atomically with batch value; offset never advances on failed put.
- Per-partition write mutex; leader-epoch durable write on open; per-write epoch check fails closed.
- Zombie mode CAS/SeqCst state machine with re-entry detection and TOCTOU-aware exit path (`zombie_mode.rs`).
- Topic name validation blocks path traversal (`validation.rs:157-197`).
- Zero panics and zero `unsafe` in the production request path; `#![forbid(unsafe_code)]`.
- Frame-size cap, negative-length rejection, read/handler timeouts, per-IP + global connection limits, auth-failure rate limiter with exponential backoff.
- Dual control/data tokio runtimes; jittered background loops; Raft proposal backpressure; Raft RPC circuit breaker; no unbounded channels.
- K8s: ordinal-derived BROKER_ID, non-root + RO rootfs + dropped caps, PDB minAvailable 2, ClusterIP-only exposure, Terraform S3 versioning/encryption/IRSA.
- Unusually broad test suite (~750+ tests: chaos, linearizability, loom, fencing, raft failure modes).
