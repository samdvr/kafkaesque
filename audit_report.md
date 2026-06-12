# Kafkaesque — Staff-Level Codebase Audit

**Scope:** Full codebase at `/Users/sam/Downloads/kafkaesque` (~75k LOC of Rust, Kafka-compatible broker on object storage with embedded Raft).
**Method:** Five parallel audit passes — correctness/bugs, performance, architecture/design, concurrency/reliability, testing/observability — each verifying file:line references against current source.
**Date:** 2026-06-12

---

## Executive Summary

Kafkaesque is an ambitious, well-organized Rust broker that takes correctness seriously: it has CRC validation, leader-epoch fencing, Raft-replicated lease state, a generation-pointer snapshot scheme, fuzz coverage of parsers, and property tests for state-machine determinism. Many of the obvious foot-guns (negative-length parsing, unbounded frame sizes, integer overflow on the wire path) are already defended.

The audit nonetheless surfaced significant production-grade risk in three areas:

1. **Cancellation safety in async hot paths.** Several `await` points hold mutable state across drops in ways that can corrupt invariants — most importantly the cached Raft RPC connection (response-mismatch on cancellation), the partition-store write path (offset duplication on cancel mid-WAL-write), and `install_snapshot` (in-memory state half-applied). These are silent in normal operation but corrupt state under timeouts, shutdown races, or peer disconnects.

2. **Hot-path overhead from per-request allocations and a per-partition write-lock held across storage round-trips.** The fetch response double-copies every payload; produce serializes durable writes per partition behind a single Mutex held across multiple SlateDB awaits; `Arc<str>`/`String`/`UUID` allocations are minted per request. None are bugs in isolation; collectively they cap throughput well below what the architecture is capable of.

3. **Observability and testing gaps that hide regressions.** `record_request` emits `error_code="NONE"` for every request, so the wire-error label has cardinality 1 and SREs are blind to per-error-code rates during incidents. ACL denials log at `debug!` so audit trails are missing at default log level. Several "linearizability" and "chaos" tests run against `MockCoordinator`/in-memory stubs rather than the real Raft coordinator and `PartitionStore`, so the guarantees they advertise are weaker than the file names suggest.

Beneath these, design debt around god-traits (`Handler` 30 methods, `ClusterConfig` ~85 fields), a 3,800-line `metrics.rs` global-singleton kitchen sink, and ~700 lines of TLS/plain server duplication makes maintenance increasingly expensive. The `MockCoordinator` is a parallel re-implementation of business rules that has already drifted (e.g., always returns producer epoch 0).

**Risk verdict:** Suitable for development and small-scale staging today. Not yet ready for high-throughput production with strict durability SLAs without addressing the cancellation-safety findings (#C-1, #C-7, #C-11 below) and tightening observability (#O-12, #O-13). With those fixed, the architecture is sound enough to scale.

---

## Top Critical Issues (Prioritized)

| # | Title | Sev | Location |
|---|-------|-----|----------|
| 1 | Cached Raft RPC connection is cancellation-unsafe; concurrent calls serialize and can desync framing | Critical | `src/cluster/raft/network.rs:744-761,769-777` |
| 2 | Partition-store write lock held across SlateDB write — cancellation can duplicate base offset | High | `src/cluster/partition_store.rs:415,740,784` |
| 3 | `install_snapshot` persist + multi-step in-memory swap is not atomic under cancellation | High | `src/cluster/raft/storage.rs:1080-1122` |
| 4 | Fetch response copies the entire record payload twice before write | Critical (perf) | `src/server/response/fetch.rs:101-107`, `response/mod.rs:111-127` |
| 5 | Per-partition write-Mutex held across 2-3 SlateDB round-trips serializes durable produce | High (perf) | `src/cluster/partition_store.rs:140,415,740` |
| 6 | `record_request` always records `error_code="NONE"` — typed Kafka error codes never reach Prometheus | High (obs) | `src/server/connection.rs:892-895`; `metrics.rs:1376` |
| 7 | Lease validity uses local wall clock instead of replicated `lease_clock_ms` — split-brain risk | High | `src/cluster/raft/coordinator/partition.rs:285,321`; `state_machine.rs:55-68` |
| 8 | Pending-failover set is per-broker in-memory; lost on coordinator-leader change → permanent partition stranding | High | `src/cluster/rebalance_coordinator.rs:337,810-868` |
| 9 | `PartitionStore::close()` takes `&self` and is racy across many call sites | High | `src/cluster/partition_store.rs:1438-1444` |
| 10 | Zombie-mode `try_exit` has TOCTOU between timestamp check and CAS | High | `src/cluster/zombie_mode.rs:152-195` |
| 11 | `ListOffsets` always returns `timestamp = -1` for timestamp queries — `offsetsForTimes()` broken | High | `src/cluster/handler/offsets.rs:159` |
| 12 | `leader_epoch == 0` partitions skip per-write epoch fencing entirely | High | `src/cluster/partition_store.rs:444,1317` |
| 13 | Sequential per-partition coordinator round-trip in `build_topic_metadata` (O(N) sequential Raft) | High (perf) | `src/cluster/handler/mod.rs:602-652`, `handler/metadata.rs:181-191` |
| 14 | Cardinality bombs: many metrics use raw user-controlled `topic`/`group` labels without bounding | High (obs) | `src/cluster/metrics.rs:1764,1830,1842,1852,2123,2305,2320` |
| 15 | ACL denials logged at `debug!` everywhere except produce — no audit trail at default log level | High (obs) | `src/cluster/handler/{fetch,groups,offsets,metadata,admin}.rs` (~12 sites) |
| 16 | Server layer has a hard dependency on `cluster::*` — advertised "embeddable" Handler trait is not embeddable | High | `src/server/{mod.rs:860,871,connection.rs:309,593,594,600,895,health.rs:39,411}` |
| 17 | `/health` `is_ready()` only checks zombie state; ignores object-store health and Raft state | High | `src/server/health.rs:167-178` |
| 18 | Linearizability and chaos tests run against in-process stubs, not real `PartitionStore`/Raft | High | `tests/linearizability_tests.rs:85-150`; `tests/chaos_tests.rs:230-281` |

---

## Detailed Findings

### 1. Correctness & Bugs

#### C-1 — `parse_unsigned_varint` silently truncates 5-byte varint values
- **Severity:** Medium
- **Location:** `src/parser.rs:189-216`
- **Description:** The overflow guard `if shift > 28 { return Err... }` is checked after `shift += 7`, so when `shift == 28` the bits `b & 0x7F` are shifted left by 28 and the high 4 bits of byte 5 are silently dropped. Different on-the-wire varints decode to the same `u32`.
- **Impact:** Breaks canonical-encoding assumptions; parser non-determinism vector for fuzzing oracles and signature equality.
- **Fix:** On the 5th byte, additionally verify `b <= 0x0F` before OR'ing into `result`.

#### C-2 — `ListOffsets` always returns timestamp = -1 even on timestamp-based queries
- **Severity:** High
- **Location:** `src/cluster/handler/offsets.rs:159`
- **Description:** `offset_for_timestamp` returns the offset, but the response's `timestamp` is hardcoded to `-1`. Per Kafka protocol the response should carry the batch timestamp so `KafkaConsumer.offsetsForTimes()` returns real `OffsetAndTimestamp` entries.
- **Impact:** `offsetsForTimes()` consumers see `timestamp: -1` instead of the actual batch timestamp; clients filtering on non-negative timestamps drop the partition silently.
- **Fix:** When `partition.timestamp >= 0` and lookup succeeded, return the batch's `max_timestamp`.

#### C-3 — `leader_epoch == 0` partitions skip per-write epoch fencing
- **Severity:** High
- **Location:** `src/cluster/partition_store.rs:444,1317` (`decode_leader_epoch(...).unwrap_or(0)` + `if self.leader_epoch > 0` gate)
- **Description:** Brand-new partitions opened with `leader_epoch == 0` (the default) skip the epoch-fencing check entirely. Combined with the lease cache fast path, partitions opened pre-fencing have no per-write epoch validation; only SlateDB's internal single-writer fencing protects them.
- **Impact:** Pre-fencing partitions are vulnerable to TOCTOU regardless of code path.
- **Fix:** Either require `leader_epoch > 0` at open, or bump the stored epoch on every open so future opens always have a non-zero baseline.

#### C-4 — CRC validation runs after `get_for_write` (Raft round-trip on cache miss) for `acks>=1`
- **Severity:** Medium
- **Location:** `src/cluster/handler/mod.rs:710-770`
- **Description:** `produce_to_partition_inner` acquires the lease (potentially a Raft read) before validating the batch CRC. The `acks=0` path correctly validates first. Asymmetric — an attacker can drive Raft load with corrupt batches.
- **Fix:** Move CRC validation above `get_for_write` on the `acks>=1` path.

#### C-5 — JoinGroup protocol metadata and SyncGroup assignment have no parse-time bound
- **Severity:** Medium
- **Location:** `src/server/request/groups.rs:99-106,215-222`
- **Description:** Both parsers reject negative lengths but accept up to `i32::MAX`. The `MAX_MEMBER_METADATA_SIZE` validation runs in the handler, after the allocation has already happened.
- **Fix:** Bound at parse time using the constant from `crate::constants`.

#### C-6 — SASL `auth_bytes` length has no per-mechanism upper bound
- **Severity:** Medium
- **Location:** `src/server/request/auth.rs:34-41`
- **Description:** `parse_sasl_authenticate_request` rejects negative lengths but accepts up to `i32::MAX` (frame cap is 100 MB). Pre-auth DoS amplifier — an unauthenticated client allocates ~100 MB per SaslAuthenticate request.
- **Fix:** Cap to a reasonable maximum (e.g., 64 KiB) before `take`.

#### C-7 — Error response on handler timeout drops `flush`/`shutdown`
- **Severity:** Low
- **Location:** `src/server/connection.rs:399`
- **Description:** Timeout-path error response lacks `flush`/`shutdown` calls, racing with TCP RST so clients see a connection reset rather than `RequestTimedOut(7)`.
- **Fix:** Add `let _ = stream.flush().await; let _ = stream.shutdown().await;` after the error write.

#### C-8 — Trailing-bytes rejection closes the connection (not just the request)
- **Severity:** Medium
- **Location:** `src/server/request/mod.rs:421-434`; `src/server/connection.rs:386,639-654`
- **Description:** Extra trailing bytes after a clean parse cause a typed `InvalidRequest` error AND a connection close. Clients don't recover.
- **Fix:** Distinguish `Error::TrailingBytes` from `Error::ParsingError` and treat the former as non-fatal — return the error response, keep the connection open.

#### C-9 — Corrupt batch error mapped to `InvalidTopic` instead of `CorruptMessage`
- **Severity:** Low
- **Location:** `src/protocol.rs:262`; `src/cluster/handler/fetch.rs:334-338`; `partition_store.rs::append_batch_inner`
- **Description:** When `record_count <= 0` rejection fires, the SlateDB `Config` error maps to `KafkaCode::InvalidTopic` (42) instead of `CorruptMessage` (2). Clients don't trigger the "refresh metadata" recovery flow.
- **Fix:** Add a dedicated `SlateDBError::CorruptBatch` variant or pattern-match on the message in `to_kafka_code()`.

#### C-10 — Information leak: HWM returned in `last_stable_offset` on `OffsetOutOfRange`
- **Severity:** Low
- **Location:** `src/cluster/handler/fetch.rs:281-301`
- **Description:** When `effective_offset < log_start`, response sets `last_stable_offset: current_hwm`. Per Kafka contract this should be `-1` for OOR responses; here it leaks the HWM to a client that requested a denied range.
- **Fix:** Return `last_stable_offset: -1` for `OffsetOutOfRange`.

#### C-11 — `set_global_inflight_byte_budget` not wired from config
- **Severity:** Low
- **Location:** `src/server/connection.rs:73-78`
- **Description:** Setter is `#[allow(dead_code)]` and never called. Production runs at the 1 GiB hardcoded default regardless of `ClusterConfig`.
- **Fix:** Wire the setter from `ClusterConfig` at startup; log the active value.

#### C-12 — SASL PLAIN principal extraction missing password presence check
- **Severity:** Low
- **Location:** `src/server/connection.rs:952-961`
- **Description:** `splitn(3, |b| *b == 0)` accepts inputs like `\0user` (no password section); `principal_from_sasl_plain` returns `Some(...)` even when the SASL PLAIN payload has no password byte at all.
- **Fix:** Require `parts.next().is_some()` for the password section before extracting `authcid`.

#### C-13 — Short frames not rejected pre-allocation
- **Severity:** Low
- **Location:** `src/server/connection.rs:190-210`
- **Description:** `read_kafka_frame` accepts size=0 and proceeds to header parse. Wastes resources on clearly malformed frames.
- **Fix:** Reject `size < 8` (minimum: api_key + api_version + correlation_id) early.

#### C-14 — Failed `release_partition` swallowed during zombie-recovery
- **Severity:** Low
- **Location:** `src/cluster/partition_manager.rs:1682`
- **Description:** `let _ = ctx.coordinator.release_partition(...)` discards errors. Combined with `OWNED_PARTITIONS.dec()` immediately after, metrics drift from reality and the partition stays "stuck" until lease expiry.
- **Fix:** Log the error explicitly; consider retry-with-backoff.

---

### 2. Performance

#### P-1 — Fetch response copies the entire record payload twice before write
- **Severity:** Critical
- **Location:** `src/server/response/fetch.rs:101-107`; `response/mod.rs:111-127`; `connection.rs:729-736`
- **Description:** `Bytes` payload from SlateDB is `put_slice`'d into the response body `Vec`, then `extend_from_slice`'d into the framed `Vec`. For a 10 MB fetch, peak resident memory is ~30 MB and 2 full memcpy passes happen before the syscall.
- **Impact:** ~2× memory + 2× memcpy of every fetched byte on the hot path.
- **Fix:** Build the response as `Vec<Bytes>` chunks; use `write_all_vectored` with `IoSlice`, or write structural header followed by each payload `Bytes` separately via `write_all`.

#### P-2 — Per-partition write-Mutex held across 2-3 SlateDB awaits
- **Severity:** High
- **Location:** `src/cluster/partition_store.rs:140,415,740`
- **Description:** `let _guard = self.write_lock.lock().await;` is held across (1) SlateDB `get` for epoch, (2) SlateDB `get` for persisted producer state on cache miss, (3) `write_with_options` (with `await_durable=true` for `acks>=1`). Serializes durable writes per partition at object-store latency.
- **Impact:** A single partition's durable produce throughput is bounded by `1 / object_store_latency`, not `1 / WAL_flush_interval`. Dominant produce-throughput limiter for `acks=1`.
- **Fix:** Pre-fetch epoch + producer state outside the lock; verify under the lock with in-memory comparison. Cache leader epoch as `AtomicI32`. Consider an in-flight queue that allocates offsets inside the lock and dispatches durable waits outside.

#### P-3 — `cached_topic_name` allocates a `String` on every cache hit
- **Severity:** High
- **Location:** `src/cluster/handler/mod.rs:530-533`
- **Description:** `moka::sync::Cache::get_with` takes `K` by value, so `topic.to_string()` runs on every call — defeating the caching. Called multiple times per produce/fetch.
- **Fix:** `cache.get(topic)` first; only `get_with` on miss.

#### P-4 — `partition_manager.get_for_write/read` allocates `Arc<str>` per call
- **Severity:** High
- **Location:** `src/cluster/partition_manager.rs:904-907,911-916,929,1051,1088,1147,1155,1747,1800,1839,1897`
- **Description:** Every call constructs `Arc::from(topic)` to look up `partition_states`; lease-cache lookup (line 1155) does it again. 200 extra allocations on a 100-partition produce.
- **Fix:** Plumb `&Arc<str>` through the API; or make `PartitionKey` borrowable so lookups don't allocate.

#### P-5 — Per-write `RwLock` read on the load-metrics collector
- **Severity:** High
- **Location:** `src/cluster/partition_store.rs:180,342-353,844,1064`
- **Description:** Every successful append/fetch acquires `RwLock<Option<Arc<...>>>::read().await`. The collector is set once at startup and is constant after.
- **Fix:** Replace with `arc_swap::ArcSwapOption<LoadMetricsCollector>` or `OnceLock<Arc<...>>`.

#### P-6 — Sequential coordinator round-trip per partition in `build_topic_metadata`
- **Severity:** High
- **Location:** `src/cluster/handler/mod.rs:602-652`; `handler/metadata.rs:181-191`
- **Description:** N sequential Raft reads per topic; for 1k partitions × 1ms per read, that's 1s per Metadata refresh per topic.
- **Fix:** Add `coordinator.get_topic_owners(topic) -> HashMap<i32, i32>` that returns owners in one read; cache invalidated on rebalance events.

#### P-7 — Fetch handler clones partitions Vec + re-runs ACL/topic-validation per long-poll iteration
- **Severity:** High
- **Location:** `src/cluster/handler/fetch.rs:104-133,179-242`
- **Description:** `topic.partitions.clone()` per pass; `validate_topic_name` + async `authorizer.authorize()` re-run every pass. Wakes ×5 on a long poll = 5× the work.
- **Fix:** Move per-topic validation/ACL outside the `collect_fetch` loop; iterate by reference.

#### P-8 — Topics processed sequentially in produce/fetch; only partitions fan out
- **Severity:** High
- **Location:** `src/cluster/handler/produce.rs:255-364`; `handler/fetch.rs:179-397`
- **Description:** A 5-topic × 20-partition request runs at 20-wide concurrency for 5 sequential rounds (5 × T_max), instead of 100-wide concurrency for one round.
- **Fix:** Flatten to a single stream over `(topic_idx, partition)` and `buffer_unordered(max_concurrent)` across the whole request.

#### P-9 — `validate_batch_crc_async` does a full payload memcpy before `spawn_blocking`
- **Severity:** High
- **Location:** `src/protocol.rs:126-135`
- **Description:** Signature takes `&[u8]`, so it copies via `Bytes::copy_from_slice`. Every caller has `&Bytes`, so a `Bytes::clone()` (refcount only) would suffice.
- **Fix:** Change signature to `&Bytes`; clone instead of copy.

#### P-10 — Missing `TCP_NODELAY` on accepted client connections
- **Severity:** Medium
- **Location:** `src/server/mod.rs:435-531,778`
- **Description:** Raft network sets it; the public Kafka socket does not. Up to 40 ms of Nagle delay on small responses.
- **Fix:** `stream.set_nodelay(true)` after `listener.accept()`.

#### P-11 — Per-request String/Arc/UUID allocations in `RequestContext`
- **Severity:** Medium
- **Location:** `src/server/connection.rs:682-696`
- **Description:** `principal.clone()`, `client_id.clone()`, `client_addr.ip().to_string()`, `Uuid::new_v4()` minted per request.
- **Fix:** Borrow-friendly `RequestContext` (`&str`/`IpAddr`); share `principal` as `Arc<str>` on the connection; lazy-generate `request_id` only when tracing is enabled.

#### P-12 — Lease-cache `Arc::from(topic)` allocation on every `get_for_write`
- **Severity:** Medium
- **Location:** `src/cluster/partition_manager.rs:1154-1168`
- **Description:** Even on lease-cache hit, `Arc<str>` is allocated to construct the lookup key.
- **Fix:** Same as P-4 — make `PartitionKey` borrowable.

#### P-13 — Metrics labels allocate `String` and acquire `Mutex` per request
- **Severity:** Medium
- **Location:** `src/cluster/metrics.rs:1474-1496,1500-1516,1520-1540,1544-1575,1700-1704`
- **Description:** `bounded_topic_label` acquires a Mutex and `to_string()`s 1-3 times. `get_partition_label_sync` allocs `String` per call.
- **Fix:** Cache `(topic, partition) -> &'static GenericCounter` directly in moka. Replace topic-label `Mutex` with `arc_swap::ArcSwap<HashSet<Arc<str>>>` or `DashSet`. Use `itoa` for partition formatting.

#### P-14 — `find_batch_start` issues 2 round-trips per fetch on miss
- **Severity:** Medium
- **Location:** `src/cluster/partition_store.rs:1090-1112`
- **Description:** Strategy 2 (exact `get`) and Strategy 3 (range scan) are independent reads; the range scan covers the exact case for free.
- **Fix:** Drop strategy 2; reuse the iterator from strategy 3.

#### P-15 — Client connections lack `BufReader`/`BufWriter`
- **Severity:** Medium
- **Location:** `src/server/connection.rs:177-237,256-280`
- **Description:** 4-byte size header always triggers a fresh `read_exact` syscall.
- **Fix:** Wrap stream in `tokio::io::BufReader::with_capacity(64 * 1024, ...)` for reads.

#### P-16 — Failover transfer planning is O(N×B); `min_by_key` per partition
- **Severity:** Low
- **Location:** `src/cluster/rebalance_coordinator.rs:469-509`
- **Description:** Cold path; 200k comparisons for 10k partitions × 20 brokers.
- **Fix:** Use a `BinaryHeap<(usize, i32)>`; pop, increment, push — O(N log B).

#### P-17 — Metadata "list-all" is O(topics × partitions) sequential coordinator calls
- **Severity:** Medium
- **Location:** `src/cluster/handler/metadata.rs:181-191`
- **Description:** 200 topics × 50 partitions = 10,000 sequential Raft reads.
- **Fix:** Bulk fetch via `get_all_partition_owners()`; parallelize topic authorization with `buffer_unordered`.

---

### 3. Concurrency & Reliability

#### R-1 — Cached Raft RPC connection serializes RPCs and is cancellation-unsafe
- **Severity:** Critical
- **Location:** `src/cluster/raft/network.rs:744-761,769-777`
- **Description:** `try_send_rpc` holds `cached_conn.lock().await` for the full duration of `do_rpc_with_timeout`. All concurrent calls to a peer serialize on one socket. If the outer future is cancelled mid-RPC (caller timeout, shutdown, openraft drop), the next call finds the same `Some(stream)` with a half-written frame still on the wire — framing desyncs and the next caller may interpret the previous response as its own.
- **Impact:** (a) AppendEntries + InstallSnapshot serialize end-to-end; (b) Cancellation-induced response-mismatch can deserialize a `Vote` reply as `AppendEntries`, producing phantom errors, false leadership transitions, or Raft state corruption.
- **Fix:** Use a drop guard pattern that nulls the slot unless `commit()` is called on success; or drop the cache and use one connection per RPC; or make `do_rpc` fully cancellation-safe by reading the response under timeout in a `select!` arm that drops the stream on cancel.

#### R-2 — Partition append: write lock + SlateDB await — cancellation can duplicate base offset
- **Severity:** High
- **Location:** `src/cluster/partition_store.rs:415,740,784`
- **Description:** HWM is bumped only after `write_with_options` returns. If the caller is cancelled between the SlateDB write submit and the HWM update, SlateDB may have queued the batch (so the data eventually persists) but the broker's view says it didn't. The next caller assigns the same `base_offset`. Two batches end up at the same SlateDB key — the second clobbers the first.
- **Impact:** Producer-acked data lost on the durable path; outright corruption on the fast path.
- **Fix:** Reserve the offset range before the await — bump `high_watermark` atomically before issuing the write. Or wrap the await in a non-cancellable `tokio::spawn` + `JoinHandle::await`.

#### R-3 — `install_snapshot`: persist + multi-step in-memory swap not atomic under cancellation
- **Severity:** High
- **Location:** `src/cluster/raft/storage.rs:1080-1122`
- **Description:** Sequence is persist → SM swap → last_applied_log → membership → cached_snapshot. Cancellation between SM swap and last_applied_log leaves SM with the new state but `last_applied_log` pointing to the old; openraft re-delivers entries that are already applied via the snapshot, double-applying non-idempotent commands (counters, sequence allocations).
- **Fix:** Take all relevant write locks before step 3 and structure so the in-memory commit point is `last_applied_log`'s update; reorder so `last_applied_log` is the last thing updated and acts as the commit point.

#### R-4 — Lease validity uses local wall clock instead of replicated `lease_clock_ms`
- **Severity:** High
- **Location:** `src/cluster/raft/coordinator/partition.rs:285,321`; `state_machine.rs:55-68`
- **Description:** State machine tracks a replicated, monotonic `lease_clock_ms` so all replicas agree on lease expiration. But `get_partition_owner` and `owns_partition_for_read` compare against `current_time_ms()` (local `SystemTime::now()`). A broker with a forward-skewed wall clock declares its own lease expired locally while the cluster still considers it owned.
- **Impact:** Split-brain reads on clock skew.
- **Fix:** Compare against `inner_state.lease_clock_ms` from the same state read.

#### R-5 — Pending failover set is per-broker in-memory; lost on coordinator-leader change
- **Severity:** High
- **Location:** `src/cluster/rebalance_coordinator.rs:337,810-868`
- **Description:** Newly Failed brokers seed `pending_failovers: Mutex<HashSet<i32>>` which is process-local. Only the Raft leader retries; if the leader steps down mid-retry, the new leader has an empty pending set and the failure detector already shows the broker as Failed → no transition → no retry.
- **Impact:** Permanent partition stranding after leader change during a transient failover failure.
- **Fix:** On leader takeover, seed `pending_failovers` from the failure detector. Or replicate the set via Raft. Or drive failover from a state-machine invariant ("any Failed broker with non-empty owner set → reassign").

#### R-6 — Zombie-mode `try_exit` has TOCTOU between timestamp check and CAS
- **Severity:** High
- **Location:** `src/cluster/zombie_mode.rs:152-195`
- **Description:** Timestamp check, then CAS on `active`. Between the two, a `force_exit()` followed by re-`enter()` can flip `active` to a new generation — the CAS still sees `true` and exits a *new* zombie session whose timestamp was never validated.
- **Impact:** After a flap, the recovery thread exits the new zombie session prematurely; broker resumes serving while heartbeat is still failing.
- **Fix:** Pack `(active, generation)` into one `AtomicU64` and CAS the whole word. Or guard transitions with a Mutex.

#### R-7 — `PartitionStore::close()` takes `&self`; concurrent calls race SlateDB
- **Severity:** High
- **Location:** `src/cluster/partition_store.rs:1438-1444`
- **Description:** `close()` is `pub async fn close(&self)` with `self.db.close().await?`. SlateDB's `Db::close` is not safe to call twice concurrently. The codebase calls `store.close()` from many paths (release, zombie-entry, lease-loss, shutdown) that can race.
- **Impact:** Two concurrent close paths → SlateDB internal panic in compaction/WAL.
- **Fix:** Guard with `tokio::sync::OnceCell<()>` or an `AtomicBool` sentinel.

#### R-8 — `apply_command` holds SM write lock across heartbeat hook
- **Severity:** Medium
- **Location:** `src/cluster/raft/state_machine.rs:186,202-208`
- **Description:** Heartbeats are the most frequent command; the SM write lock is held during the hook call, blocking every reader. Future hooks that touch shared state could deadlock.
- **Fix:** Capture the broker_id, drop the guard, then fire the hook.

#### R-9 — `apply_to_state_machine` holds `last_applied_log.write` across SM `apply_command`
- **Severity:** Medium
- **Location:** `src/cluster/raft/storage.rs:1031-1052`
- **Description:** Lock order: `last_applied_log.write` → `sm.read` → `state.write`. No deadlock today (build_snapshot uses the reverse-read order), but any future inversion deadlocks. Also blocks readers during apply.
- **Fix:** Apply first; then briefly take `last_applied_log.write` to bump the index.

#### R-10 — Raft RPC server has no shutdown — listener loop runs forever
- **Severity:** Medium
- **Location:** `src/cluster/raft/network.rs:917-1017`
- **Description:** No cancellation token. Graceful shutdown is impossible; runtime drop forcibly aborts in-flight `append_entries`/`install_snapshot` mid-write.
- **Fix:** Add a `CancellationToken` plumbed into `run()`; `select!` against it; track JoinHandles for spawned per-connection tasks.

#### R-11 — `fire_and_forget_produce` (acks=0) doesn't track JoinHandles
- **Severity:** Medium
- **Location:** `src/cluster/handler/produce.rs:220-237`
- **Description:** Spawned task's handle is never tracked. On shutdown, in-flight SlateDB writes are dropped; combined with `release_partition` → `store.close()`, can trip a SlateDB-internal panic in compaction.
- **Fix:** Track via `JoinSet`; drain (or abort with brief timeout) before `store.close()`.

#### R-12 — Snapshot persist data PUT happens before pointer-lock acquired
- **Severity:** Medium
- **Location:** `src/cluster/raft/storage.rs:755-858`
- **Description:** Two concurrent persists race on data PUT before serializing on the pointer lock. The first to commit wins; the second's data object is orphaned. On pointer-PUT failure, the orphan-cleanup `delete` is best-effort (`let _ =`); transient S3 failures accumulate orphans.
- **Fix:** Acquire `snapshot_pointer.write()` before the data PUT.

#### R-13 — owner_cache invalidation races local readers
- **Severity:** Medium
- **Location:** `src/cluster/raft/coordinator/partition.rs:223-249,259-263`; `state_machine.rs:168-172`
- **Description:** The hook fires inside the SM write lock, but a parallel reader on the same broker can fetch the cached owner just before the hook completes — small window of "I own this" after a release.
- **Fix:** Add a generation counter to cached entries, or invalidate before the Raft write returns.

#### R-14 — Raft RPC server drops state if response write fails after SM apply
- **Severity:** Medium
- **Location:** `src/cluster/raft/network.rs:1066-1071,1129-1140`
- **Description:** `dispatch_rpc_message` mutates state, then `write_rpc_frame` may fail; the leader retries (potentially against a different leader) and may issue a duplicate command. For idempotent commands (AppendEntries) it's harmless; for `InitProducerId` it could allocate two IDs.
- **Fix:** Make all coordination commands idempotent at the SM level via a client-supplied operation ID + dedup.

#### R-15 — Recovery silently skips files with unparseable filenames
- **Severity:** Medium
- **Location:** `src/cluster/raft/storage.rs:285-300`
- **Description:** `let Some(idx) = path.file_stem()...parse::<u64>().ok() else { continue; }` silently skips. A corrupted log filename is invisible while corrupted content errors loudly. Could create a hole that openraft's invariants forbid.
- **Fix:** Distinguish "wrong pattern" (warn loudly, abort startup) from "unrelated file" (ignore).

#### R-16 — Per-connection serve loop has no per-connection in-flight cap
- **Severity:** Medium
- **Location:** `src/server/connection.rs:309-407`
- **Description:** Single client connection is purely sequential. A 30s slow handler blocks the connection's read loop. Hostile IP × 256 connections × 30s × 1MB inflight = 7.5 GB held for 30 s.
- **Fix:** Add a per-connection request semaphore; ensure handler timeout < per-connection read timeout.

---

### 4. Architecture & Design

#### A-1 — Server layer reaches into cluster internals (layering violation)
- **Severity:** High
- **Location:** `src/server/mod.rs:860,871`; `connection.rs:309,593,594,600,895`; `health.rs:39,411`; `src/error.rs:120`
- **Description:** The `server/` module is documented as embeddable but directly references `cluster::metrics::*`, `cluster::buffer_pool::*`, `cluster::zombie_mode::*`, plus `From<SlateDBError> for Error`. Drop the cluster code and the server stops compiling.
- **Fix:** Move metrics/buffer_pool/zombie_mode/observability to a top-level `infra/` or pass `Arc<dyn ServerMetrics>` into `KafkaServer::new`. Remove `From<SlateDBError>` and map at the cluster handler boundary only.

#### A-2 — TLS and plain server / connection are duplicate copies (~700 lines)
- **Severity:** High
- **Location:** `src/server/mod.rs:215-551` vs `554-927`; `connection.rs:964-1075` vs `1076-1200+`
- **Description:** The TLS server and connection are byte-for-byte copies of their plain counterparts apart from stream type and one handshake step. Bug fixes drift between paths.
- **Fix:** Generic `Connection<S: AsyncRead + AsyncWrite + Unpin>` with two acceptor impls (`PlainAcceptor`, `TlsAcceptor`). Net deletion: ~600 lines.

#### A-3 — `metrics.rs` is a 3,800-line global-singleton kitchen sink
- **Severity:** High
- **Location:** `src/cluster/metrics.rs`
- **Description:** ~70 `pub static` `Lazy<...>` Prometheus metrics + ~50 record/set free functions + a circuit-breaker subsystem + an object-store health tracker + a metric-cardinality limiter — all in one file with one global `REGISTRY`. Two `Handler` instances cannot run in the same process.
- **Fix:** Split into `cluster/metrics/{registry,connection,request,produce,fetch,group,raft,partition,failover,storage}.rs`. Move circuit breaker to `cluster/circuit_breaker.rs`. Move object-store health to `cluster/object_store_health.rs`. Pass `Arc<Metrics>` into handlers explicitly.

#### A-4 — `ClusterConfig` is a 60-field god struct with hand-written `from_env`
- **Severity:** High
- **Location:** `src/cluster/config.rs:243-781,1355-1822`
- **Description:** ~85 `pub` fields covering 9 unrelated concerns; `from_env` is ~470 lines of repeated `std::env::var` parsing. Profile overrides silently reset fields after `..base`. `validate_or_panic` is a third error-handling style.
- **Fix:** Split into themed sub-structs (`BrokerConfig`, `NetworkConfig`, `LeaseConfig`, `RaftConfig`, etc.). Replace `from_env` with serde-driven loader (`figment` or `config-rs`) + a tiny `env_or<T>` helper.

#### A-5 — `partition_manager.rs` does six unrelated jobs in one 2,800-line file
- **Severity:** High
- **Location:** `src/cluster/partition_manager.rs:60-1404`
- **Description:** One struct owns: partition-state map, store opening, heartbeat loop, lease renewal loop, ownership reconciliation loop (six concerns alone), retention loop, session-timeout loop, zombie-mode management.
- **Fix:** Extract each background task into `cluster/tasks/{heartbeat,lease_renewal,ownership_reconciliation,retention,session_timeout}.rs` as `pub async fn run(ctx: TaskCtx)`. Keep `PartitionManager` as the synchronous data plane.

#### A-6 — `Handler` trait is a 30-method god trait; `handler_traits.rs` is a dead 1,028-line parallel
- **Severity:** Medium
- **Location:** `src/server/handler.rs:88-501`; `handler_traits.rs:1-1028`
- **Description:** `Handler` defines every API method in one trait with default-impl traps. A parallel `handler_traits.rs` defines seven sub-traits with a blanket impl that the production `SlateDBClusterHandler` never uses. The doc-comment recommends a path no production code takes.
- **Fix:** Pick one and delete the other. `dispatch_request_common` repetitive arms compress with a single `dispatch_versioned!` helper.

#### A-7 — `mock_coordinator.rs` is a 2,400-line parallel re-implementation of business rules
- **Severity:** Medium
- **Location:** `src/cluster/mock_coordinator.rs`
- **Description:** Re-implements rebalance state machines, generation tracking, member eviction, producer-id epoch bumping. Already drifted: `init_producer_id` always returns epoch 0 (line 1017). Tests pass against the mock and miss real-state-machine bugs.
- **Fix:** Delete the mock and replace with a `LocalStateMachineCoordinator` that wraps the real `cluster/raft/domains/*::apply()` functions with a fake-time, single-node, no-network harness (~150 lines).

#### A-8 — `RaftStore` uses 9 independent `Arc<RwLock<_>>` fields ("granular" locks that are actually one logical lock)
- **Severity:** Medium
- **Location:** `src/cluster/raft/storage.rs:105-136`
- **Description:** `vote`, `log`, `last_purged_log_id`, `sm`, `last_applied_log`, `last_membership`, `cached_snapshot`, `snapshot_pointer` are each separately locked; openraft calls them in fixed sequences (callers acquire 2-4 in lockstep). Granularity buys nothing and increases deadlock risk; everything is `async fn` even though most operations don't await.
- **Fix:** Wrap into one `RaftStoreState` struct under one `Arc<RwLock<_>>`. Use `parking_lot::RwLock` for in-memory pieces.

#### A-9 — Coordinator trait family split four ways but always implemented together
- **Severity:** Medium
- **Location:** `src/cluster/traits.rs:57-714`
- **Description:** Four sub-traits + blanket `ClusterCoordinator`; both real impls implement all four. `PartitionCoordinator` itself is a god trait (24 methods) with default-impl shims (`acquire_partition_with_epoch`, `owns_partition_for_write`, `current_leader_id`) that exist only to keep the mock compiling.
- **Fix:** Either collapse to one `Coordinator` trait, or reorganize by call-site scope (`BrokerLifecycle`, `PartitionOwnership`, `TopicAdmin`, `ConsumerGroups`, `Producers`, `Transfers`). Strip speculative defaults.

#### A-10 — Config error path uses three different error styles
- **Severity:** Medium
- **Location:** `src/cluster/config.rs:1355`; `src/cluster/error.rs:117-253`; `src/error.rs:69-129`
- **Description:** `from_env` returns `Box<dyn Error>`; `Error::Config(String)` is unstructured; `validate()` returns `Result<(), Vec<String>>`; `validate_or_panic` panics. Tests resort to `string.contains(...)`.
- **Fix:** Define `ConfigError` (thiserror) with structured variants. `from_env -> Result<ClusterConfig, ConfigError>`. Remove `validate_or_panic`.

#### A-11 — `cluster/raft/mod.rs` mass re-exports internal types
- **Severity:** Medium
- **Location:** `src/cluster/raft/mod.rs:74-91`
- **Description:** Re-exports `RaftStore`, `RaftRpcMessage`, `RaftRpcResponse`, `CoordinationStateMachine`, plus 14 `*Command`/`*Response`/`*DomainState` types. Internal types become semver-public.
- **Fix:** Limit `raft/mod.rs` re-exports to `RaftCoordinator`, `RaftConfig`, `RaftNode`. Make the rest `pub(crate)`.

#### A-12 — SASL state plumbed via `Handler::take_sasl_post_auth` / `on_connection_closed` (leaky abstraction)
- **Severity:** Medium
- **Location:** `src/server/handler.rs:401-433`; `cluster/handler/mod.rs:124-132,940-955`; `connection.rs:808-855`
- **Description:** SCRAM is multi-step but `Handler` is "stateless"; the cluster handler stashes per-connection state in a `DashMap<SocketAddr, _>`. The dispatcher knows about SASL specifics that the trait was supposed to hide. Two impls of `Handler` cannot share auth state correctly.
- **Fix:** Pass `&mut SaslSession` alongside `RequestContext`; dispatcher owns the per-connection `SaslSession`.

#### A-13 — Validation lives in two places via "backwards compatibility" re-export
- **Severity:** Low
- **Location:** `src/cluster/validation.rs:73-117`; `cluster/coordinator/mod.rs:30`
- **Description:** Functions live in `validation.rs` but are imported via `cluster::coordinator::` re-export. Three call sites use one path, two the other.
- **Fix:** Drop the re-export; rename `validation.rs` to `cluster/names.rs` if appropriate.

#### A-14 — Long methods with deep nesting in handler hot paths
- **Severity:** Low (cumulative)
- **Locations:** `partition_manager.rs:360-489` (`start_heartbeat_loop`, 130 lines), `:696-864` (`start_ownership_loop`, 165 lines, 6 responsibilities); `cluster/handler/groups.rs:154-347` (`handle_join_group`, 195 lines); `cluster/raft/domains/group.rs:311-693` (`apply`, 380-line match); `connection.rs:712-890` (180-line dispatch match).
- **Fix:** Extract per-command arms in `apply` into named functions; extract error-response builders; macro-fy the dispatch arms.

#### A-15 — `cluster::handler::mod.rs` mixes a god struct with thin wrappers
- **Severity:** Medium
- **Location:** `src/cluster/handler/mod.rs:60-1109`
- **Description:** `SlateDBClusterHandler` carries the genuinely complex `produce_to_partition_inner` (130 lines) in `mod.rs` next to one-line `handle_produce` wrapper that delegates to `produce.rs`. ACL helpers live in `mod.rs` but are called from every submodule.
- **Fix:** Move `produce_to_partition*` into `handler/produce.rs`; move `build_topic_metadata` into `handler/metadata.rs`; move `authorize_cluster_api`/`topic_authorized` into `handler/acl.rs`.

---

### 5. Testing & Observability

#### O-1 — `record_request` always records `error_code="NONE"` — typed errors lost
- **Severity:** High
- **Location:** `src/server/connection.rs:892-895`; `src/cluster/metrics.rs:1376` (`record_request_with_code` exists but is never called)
- **Description:** Every request is recorded with `error_code="NONE"`. The contract documented at line 1370 (wire-level Kafka error code) is silently broken. Operators cannot graph the rate of `NotLeaderForPartition`, `OffsetOutOfRange`, etc.
- **Fix:** In `dispatch_request_common`, extract the typed `error_code` from each response and call `record_request_with_code(api, status, code.as_str(), duration)`. Add a metrics-quality test.

#### O-2 — ACL denials log at `debug!` everywhere except produce
- **Severity:** High
- **Location:** `src/cluster/handler/{fetch.rs:215, groups.rs:175,178,381,597,658,722,823, offsets.rs:96-707, metadata.rs:64,98, admin.rs:58,222}`
- **Description:** Default `RUST_LOG=info` swallows them. Compliance-grade audit trail missing for every API except produce. Counters fire but per-event detail is unrecoverable.
- **Fix:** Promote to `info!(target: "audit", ...)` consistently. Add a test asserting the audit target line appears for every `Denied`.

#### O-3 — Cardinality bombs: many metrics use raw user-controlled labels
- **Severity:** High
- **Location:** `src/cluster/metrics.rs:1764` (`offset_commits_total{group, topic, status}`), `:1830` (`rebalance_duration_seconds{group}`), `:1842` (`partition_acquisition_duration_seconds{topic, status}`), `:1852` (`topic_partition_count{topic}`), `:2123` (`lease_too_short_total{topic, partition}`), `:2305,2320` (`producer_state_*{topic, partition}`)
- **Description:** `bounded_topic_label` / `bounded_principal_label` / `bounded_partition_label` exist but only some call sites use them. A buggy or hostile client churning random group/topic IDs inflates Prometheus cardinality without bound.
- **Impact:** Prometheus OOM; "metrics scrape times out, broker is fine, no visibility."
- **Fix:** Wrap every user-controlled label site in the existing `bounded_*` helpers. Add a property test asserting total series count stays bounded after 100k random IDs.

#### O-4 — `/health` `is_ready()` only checks zombie state
- **Severity:** High
- **Location:** `src/server/health.rs:167-178`
- **Description:** Doesn't consult `is_object_store_healthy()`, doesn't check Raft state (am I a follower with no leader for >N seconds?). A broker that has lost the object store still answers `/ready 200`, so load balancers route produce traffic to a broker that can't durably persist.
- **Fix:** AND in `metrics::is_object_store_healthy()`, an "in-quorum-with-leader" check, listener health. Add tests covering each new failure mode.

#### O-5 — Linearizability tests run against an in-process stub, not the real partition store
- **Severity:** High
- **Location:** `tests/linearizability_tests.rs:85-150`
- **Description:** The "Jepsen-style" tests assert linearizability of `Arc<RwLock<BTreeMap>>` + `AtomicI64`, not of `cluster/partition_store.rs`. Real HWM / batch-index / SlateDB visibility races are uncovered.
- **Fix:** Replace `TestPartitionStore` with a real `PartitionStore` over `InMemory` object store (the `e2e_slatedb_tests.rs` setup already shows the pattern). Or rename the file honestly.

#### O-6 — Chaos tests are 100% `MockCoordinator`; no real Raft fault injection
- **Severity:** High
- **Location:** `tests/chaos_tests.rs:230-281`
- **Description:** All 84 chaos scenarios use `MockCoordinator` with `Arc<RwLock>` shared state. Real Raft pathologies — leader stickiness during partition, log compaction races, snapshot-install during leader change — are uncovered.
- **Fix:** Add at least one chaos scenario per failure mode against a real 3-node `RaftCoordinator` cluster (the harness in `raft_integration_tests.rs` already exists). Mark the mock chaos tests `mod mock_chaos`.

#### O-7 — ACL bypass tests cover only 3 of ~15 API authz checks
- **Severity:** High
- **Location:** `tests/acl_enforcement_tests.rs`
- **Description:** Covers produce, metadata, list_offsets. Missing: OffsetCommit, OffsetFetch, JoinGroup, SyncGroup, Heartbeat, FindCoordinator, DescribeGroups, DeleteGroups, CreateTopics, DeleteTopics, InitProducerId.
- **Fix:** Add one negative test per API asserting both the wire error code AND that `record_acl_denial` was incremented.

#### O-8 — No test exercises a slow / failing object store (degraded mode)
- **Severity:** High
- **Location:** Production has explicit detection at `src/cluster/metrics.rs:2836` (`OBJECT_STORE_CONSECUTIVE_FAILURES`) but no test injects flaky behavior.
- **Description:** Regression in object-store backoff (infinite retry holding write guards, OOM from queued batches) would pass CI.
- **Fix:** Add a wrapping `ObjectStore` that injects per-call delays/errors; assert produce latency p99 stays bounded, `track_object_store_health(false)` is called, and `is_object_store_healthy()` flips.

#### O-9 — Per-connection background tasks spawn without `.instrument(span)` — trace context lost
- **Severity:** Medium
- **Location:** `src/server/mod.rs:503`; `src/cluster/raft/coordinator/mod.rs:144,183`; `src/cluster/raft/auth.rs:749,769`
- **Description:** `tokio::spawn(async move { ... })` for per-connection or background tasks doesn't `.instrument(span)`. With OTel enabled, traces show fragmented spans with no parent linkage.
- **Fix:** `tokio::spawn(serve.instrument(info_span!("connection", client = %addr)))` everywhere.

#### O-10 — Heavy reliance on `tokio::time::sleep` for synchronization (~90 sites; flake risk)
- **Severity:** Medium
- **Location:** `tests/raft_integration_tests.rs:1164-1809` (17+ `sleep(50ms)`); `tests/distributed_systems_tests.rs:1142,1153` (`sleep(1100ms)`); `tests/integration_tests.rs:355` (`sleep(50ms)` for server start).
- **Description:** Loaded CI runners produce intermittent failures; chaos_tests.rs:711 already documents migrating one of these to a `oneshot`.
- **Fix:** Replace with `wait_for(|| condition, timeout)` polling helper (already exists in `server_lifecycle_tests.rs:29`). Use mockable clocks for lease-expiry tests.

#### O-11 — Successful SASL PLAIN auth logs the username (PII)
- **Severity:** Medium
- **Location:** `src/cluster/sasl_provider.rs:286`
- **Description:** Failed auth correctly redacts to a SHA-256 prefix; success logs raw username. Asymmetric. Some compliance regimes treat usernames as identifiers requiring access controls.
- **Fix:** Symmetrize (also hash on success) or gate behind a config flag.

#### O-12 — No consumer-lag metric updated from production paths
- **Severity:** Medium
- **Location:** `src/cluster/metrics.rs:2237` (`record_consumer_lag` exists; production callers grep returns the metrics module only).
- **Description:** `CONSUMER_LAG{group, topic, partition}` is declared but always empty.
- **Fix:** On every successful `OffsetCommit`, cross-reference `PARTITION_HIGH_WATERMARK` and call `record_consumer_lag` — one-line addition in `handler/offsets.rs`.

#### O-13 — Topic delete leaves dead time series (no `forget_topic_metrics`)
- **Severity:** Medium
- **Location:** `src/cluster/metrics.rs:1900` (`forget_group_metrics` exists; no equivalent for topics)
- **Description:** Per-topic labels stay in the registry forever after `DeleteTopics`.
- **Fix:** Add `forget_topic_metrics(topic)` mirroring `forget_group_metrics`; call from the `DeleteTopics` path.

#### O-14 — Some Raft state metrics are declared but never updated from production
- **Severity:** Medium
- **Location:** `src/cluster/metrics.rs:2428-2490` (`RAFT_STATE`, `RAFT_TERM`, `RAFT_COMMIT_INDEX`, `RAFT_APPLIED_INDEX`, `RAFT_ELECTIONS`, `RAFT_SNAPSHOTS`, `RAFT_LOG_ENTRIES`)
- **Description:** Helpers exist but several are unused in production. "Raft has lost quorum" / "Raft is stuck in candidate state" alerts cannot be authored.
- **Fix:** Hook into the openraft `RaftMetrics` watch in `cluster/raft/node.rs`.

#### O-15 — No fuzz coverage of stateful SCRAM (client_first + client_final transition)
- **Severity:** High
- **Location:** Only `fuzz/fuzz_targets/scram_client_first.rs`; `src/cluster/scram.rs:1-501` has no logging.
- **Description:** Pre-auth SCRAM is the auth boundary. A panic or auth bypass in the second SCRAM step would slip past CI.
- **Fix:** Add a stateful fuzz target driving `client_first → client_final` with attacker bytes for both steps.

#### O-16 — Many "tests" only check Default/Debug/Clone — no contract assertion
- **Severity:** Medium
- **Location:** `tests/admin_response_tests.rs:14-118`; `tests/extended_group_tests.rs:16-300+`; `tests/metrics_tests.rs:60-118`
- **Description:** `metrics_tests.rs:62-66` calls `record_request` three times and asserts nothing about the recorded values. Coverage looks good; behavior coverage is poor.
- **Fix:** For metric tests, assert `REQUEST_COUNT.with_label_values(...).get() == 1`. Drop derive-tests for Debug/Clone.

#### O-17 — No edge-case tests for max-size message, max partitions, max fetch response
- **Severity:** Medium
- **Location:** `tests/edge_case_tests.rs` (691 lines covers small edges only).
- **Description:** No test pushes message size to `max_message_size` boundary, partition count to max, fetch response to `max_bytes` boundary.
- **Fix:** Add boundary tests asserting success at `cap` and `InvalidRequest` at `cap+1`.

#### O-18 — No metric for `REQUEST_DURATION` by status — slow errors poison success p99
- **Severity:** Medium
- **Location:** `src/cluster/metrics.rs:114-138`
- **Description:** `REQUEST_DURATION{api}` lacks a `status` label.
- **Fix:** Add `status` (cardinality ×2). Graph error vs success p99 separately.

#### O-19 — No flexible-encoding fuzz seeded into per-API parsers
- **Severity:** Medium
- **Location:** `fuzz/fuzz_targets/parser_primitives.rs`, `flexible_encoding.rs`; per-API targets pick `version` uniformly but corpora aren't seeded with flexible-encoded payloads.
- **Fix:** Seed per-API fuzz corpora with valid flexible-encoded requests (the differential `tests/wire_differential.rs` knows how to emit them).

---

## Quick Wins (high impact, low effort)

These are 1-line to 1-day fixes with disproportionate impact.

| Fix | Effort | Impact | Reference |
|-----|--------|--------|-----------|
| Wire typed Kafka error codes through `record_request_with_code` | 1 hour | Unblocks per-error-code SRE alerting; fixes broken metric contract | O-1 |
| Promote ACL denials from `debug!` to `info!(target: "audit", ...)` in 12 sites | 30 minutes | Compliance audit trail at default log level | O-2 |
| Add `stream.set_nodelay(true)` after each client `accept()` | 5 minutes | Up to 40 ms tail-latency reduction for small responses | P-10 |
| Change `validate_batch_crc_async` signature from `&[u8]` to `&Bytes` | 30 minutes | Eliminates a full-payload memcpy per >64 KiB produce | P-9 |
| Replace `cached_topic_name` `get_with` → `get`-then-insert | 15 minutes | Eliminates ~100+ String allocs per multi-partition request | P-3 |
| Replace per-call `RwLock<Option<Arc<LoadMetricsCollector>>>` with `ArcSwapOption` | 1 hour | Removes one async lock per append/fetch | P-5 |
| Reject `read_kafka_frame` `size < 8` early | 5 minutes | Cheap pre-allocation rejection | C-13 |
| Add `forget_topic_metrics` and call from DeleteTopics path | 30 minutes | Prevents Prometheus cardinality leak on topic churn | O-13 |
| Wire `record_consumer_lag` from `handle_offset_commit` | 30 minutes | Enables canonical "consumer lag" alert | O-12 |
| Wrap user-controlled topic/group label sites in `bounded_*_label` helpers | 2 hours | Prevents Prometheus OOM from cardinality bombs | O-3 |
| Cap SASL `auth_bytes` to 64 KiB at parse time | 5 minutes | Closes pre-auth DoS amplifier | C-6 |
| Bound JoinGroup metadata at parse time using `MAX_MEMBER_METADATA_SIZE` | 15 minutes | Closes authenticated DoS | C-5 |
| Return real timestamp in `ListOffsets` response when `ts >= 0` | 1 hour | Fixes `KafkaConsumer.offsetsForTimes()` for affected clients | C-2 |
| Add `flush`/`shutdown` to handler-timeout error path | 5 minutes | Clients see typed `RequestTimedOut` instead of TCP RST | C-7 |
| Move CRC validation above `get_for_write` on `acks>=1` path | 30 minutes | Removes Raft-load DoS amplifier on corrupt batches | C-4 |
| Replace `partition.to_string()` in metric labels with stack-buffer formatting (`itoa`) | 1 hour | Eliminates per-request String alloc in metrics path | P-13 |

---

## Long-Term Recommendations

### Reliability hardening (1-3 months)

1. **Cancellation-safety review across all `await`s in mutating code.** The pattern of "lock a partition mutex, then `await` SlateDB" appears in multiple places (R-2, R-3, R-7, R-11). Treat every such await as a potential cancellation point that must leave invariants intact. The simplest mechanical fix is to wrap critical sequences in `tokio::spawn` + `JoinHandle::await` so the inner future is uncancellable.

2. **Replace the cached Raft RPC connection (R-1) with either short-lived connections or a properly committed pool.** This is the highest-leverage Raft correctness fix in the codebase. Until done, all subsequent Raft work is built on a foundation that can desynchronize under load.

3. **Switch lease and ownership checks from `current_time_ms()` to the replicated `lease_clock_ms` (R-4).** The lease_clock is already replicated — using local wall clock for any check is a latent split-brain trigger.

4. **Make all Raft state-machine commands idempotent at the apply layer (R-14, R-3).** Producer-id allocation and any non-monotonic command must carry a client-supplied operation ID and dedup at apply time.

5. **Replicate the rebalance coordinator's pending-failover set (R-5)** — or seed it from the failure detector on leader takeover. As-is, leader transitions during a failover storm strand partitions.

### Architecture & code-quality (3-6 months)

6. **Decouple `server` from `cluster` (A-1).** Introduce a thin `ServerMetrics` trait and an explicit `infra/` for cross-cutting modules. The advertised "embeddable Kafka protocol library" must actually compile without the cluster stack.

7. **Unify TLS and plain server/connection paths (A-2).** Generic over the stream type with two acceptor impls; net deletion ~600 lines and elimination of a bug-drift surface.

8. **Split `ClusterConfig` and `metrics.rs` into themed modules (A-3, A-4).** These two files alone are ~7,000 lines that block every change to a metric or a config knob. Replace `from_env` with a serde-driven loader.

9. **Replace `MockCoordinator` with a real-state-machine harness (A-7).** The 2,400-line parallel implementation has already drifted; the maintenance ratio is permanently negative. The real `apply()` functions in `cluster/raft/domains/*` are ~150 lines away from being usable as a single-node test backend.

10. **Pick one of `Handler` and `handler_traits.rs` and delete the other (A-6).** Both compile, only one is used in production, and the doc-comment recommends the dead path.

11. **Extract background tasks from `partition_manager.rs` into per-task modules (A-5).** The six concurrent loops are the single biggest source of "where does this Arc come from" navigation pain.

### Observability (1 month)

12. **Establish a metrics contract test.** Assert `REQUEST_COUNT` carries non-`NONE` `error_code` values; assert `bounded_*_label` is invoked at every user-controlled label site; assert `forget_topic_metrics` is called from `DeleteTopics`. This single test regression-locks O-1, O-3, O-13.

13. **Extend `/ready` to include object-store and Raft health (O-4).** Health-check coverage is the dominant cause of "load balancer routes to bad node" incidents.

14. **Wire OTel trace propagation through `tokio::spawn` (O-9).** Every long-running task should `.instrument(span)`; otherwise distributed traces fragment at every spawn boundary.

15. **Migrate sleep-based test sync to polling (O-10).** A `wait_for(|| cond, timeout)` helper already exists; mechanical replacement reduces CI flake.

### Testing (2-3 months)

16. **Replace stub-based linearizability/chaos tests with real-component tests (O-5, O-6).** The harness already exists in `raft_integration_tests.rs` and `e2e_slatedb_tests.rs`. The promised guarantees are weaker than file names imply.

17. **Round out ACL coverage to all 15 API authz surfaces (O-7).** One negative test per API. Without these, an authorize-call regression on any single API ships green.

18. **Add a flaky-object-store test harness (O-8).** Wrap `ObjectStore` with a fault injector parameterized by per-call delay and error rate. Use it to verify produce backpressure, `/ready` flipping, and metric updates.

19. **Add stateful SCRAM fuzzing (O-15).** Pre-auth state machines are the highest-priority fuzz targets.

### Performance (2-3 months)

20. **Implement vectored fetch response writes (P-1).** Single largest CPU + memory bandwidth win in the hot path. Touches `Response::encode_with_size` and the connection write-frame.

21. **Refactor partition-store write critical section (P-2).** Pre-fetch validation outside the mutex; cache leader epoch as `AtomicI32`. Required to lift `acks=1` per-partition throughput beyond object-store latency.

22. **Add bulk `coordinator::get_topic_owners` and use it in metadata path (P-6, P-17).** Reduces metadata-refresh latency from O(N partitions) to O(1) Raft reads.

23. **Flatten produce/fetch concurrency over `(topic, partition)` pairs (P-8).** Multi-topic clients (Streams, Connect) see the largest improvement.

24. **Remove per-request `Arc<str>` / `String` / `UUID` allocations (P-3, P-4, P-11, P-12).** Borrow-friendly `RequestContext`; borrowable `PartitionKey`. ~5-10 fewer allocations per request.

---

## Appendix: Audit methodology

Five parallel audit agents ran against the live source tree, each tasked with one category:

| Agent | Files most-read | Findings |
|-------|-----------------|----------|
| Correctness & bugs | `src/parser.rs`, `src/protocol.rs`, `src/server/{request,response}/*`, `src/cluster/handler/*`, `src/cluster/partition_store.rs`, `src/cluster/raft/auth.rs` | C-1 to C-14 |
| Performance | `src/cluster/handler/{produce,fetch}.rs`, `src/cluster/partition_store.rs`, `src/cluster/partition_manager.rs`, `src/server/connection.rs`, `src/server/response/*`, `src/cluster/metrics.rs` | P-1 to P-17 |
| Architecture & quality | `src/lib.rs`, `src/cluster/{traits,config,metrics,mock_coordinator}.rs`, `src/server/{handler,handler_traits}.rs`, `src/cluster/raft/storage.rs` | A-1 to A-15 |
| Concurrency & reliability | `src/cluster/raft/{network,storage,state_machine}.rs`, `src/cluster/{partition_store,partition_manager,zombie_mode,rebalance_coordinator,failure_detector}.rs`, `src/server/connection.rs` | R-1 to R-16 |
| Testing & observability | `tests/*` (51 files), `fuzz/fuzz_targets/*`, `src/cluster/metrics.rs`, `src/cluster/observability.rs`, `src/server/health.rs`, all handler `src/cluster/handler/*` for ACL log-level inspection | O-1 to O-19 |

All file:line references in this report were verified against the source at audit time. Findings withdrawn during agent verification (where the alleged issue was guarded elsewhere) are not included.
