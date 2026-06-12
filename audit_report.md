# kafkaesque — Staff Engineering Audit Report

**Scope:** entire `src/` (~73k LOC), `tests/` (51 files), `fuzz/`, `benches/`.
**Method:** parallel deep-read by eight Staff-level reviewers — server/protocol,
Raft consensus, storage, request handlers, security, concurrency/reliability,
testing/observability, and architecture/quality. Findings are de-duplicated and
ordered by real-world impact.

---

## Executive Summary

kafkaesque is an ambitious, well-tested Rust implementation of a Kafka-compatible
broker with an embedded Raft control plane and SlateDB on object storage.
The codebase shows clear engineering discipline — `#![forbid(unsafe_code)]`,
35 fuzz targets, property tests, an integration suite covering wire-protocol,
chaos, and durability, and explicit fail-closed posture in the security
configuration. Hot paths like fencing detection, lease handling, replay-cache
construction, and HMAC verification are implemented with non-trivial care.

That said, this audit found **systemic weaknesses across three axes**:

1. **Determinism & durability of the Raft state machine.** Multiple non-atomic
   reads/writes in snapshot construction, ordering bugs in `apply_to_state_machine`,
   and pervasive use of `HashMap` (with random `RandomState`) inside replicated
   state lead to a state machine that is *correct in the steady-state happy
   path* but cannot guarantee replicas converge to byte-identical snapshots, and
   has small, real windows where a snapshot can claim to cover an entry that has
   not been applied. Under cancellation or panic these become silent data loss.

2. **Coordination is not actually leader-gated.** The rebalance loop,
   metrics-reset loop, and several admin commands run on every broker
   independently. A coordinator-side proposal failure during failover is not
   retried and strands a broker's partitions until manual intervention.
   Combined with non-supervised background tasks (panics are swallowed),
   reliability under partial failures is materially worse than the codebase
   appears at first glance.

3. **Security posture is correct in the documented path but porous in practice.**
   `User:ANONYMOUS` is matched by wildcard `User:*` ACL bindings; the production
   posture gate accepts `acl_enabled` *or* `sasl_required` instead of requiring
   both; mTLS does not derive a principal; `RaftAuthKeys::default()` is the
   permissive variant; the metrics bearer-token check is not constant-time;
   PBKDF2 iterations are pinned to the historical Kafka floor of 4096. Each is
   small alone; together they describe a broker that an authenticated insider
   or a network-local adversary can pivot inside.

In addition, several handler paths violate Kafka's semantic contract in ways
clients will trip on (FindCoordinator routing, idempotent-producer ACL,
OffsetCommit bounds, SyncGroup non-leader fall-through), and observability has
real gaps (no trace propagation across Raft messages, opaque error labels,
sub-ms latency buckets missing, no shipped dashboards/runbooks).

**Production readiness assessment:** The system is fit for **non-critical
internal workloads** today, with operator vigilance over the security
configuration and known correctness gaps. It is **not yet ready** to back a
multi-tenant Kafka API or workloads with strict acks=all durability claims
without addressing the Critical and High items below.

### Risk dashboard

| Category | Critical | High | Medium | Low |
|---|---:|---:|---:|---:|
| Correctness & Bugs | 9 | 18 | 22 | 14 |
| Performance | 0 | 4 | 9 | 8 |
| Concurrency & Reliability | 6 | 9 | 8 | 4 |
| Security | 0 | 4 | 8 | 8 |
| Architecture & Quality | 0 | 6 | 8 | 11 |
| Testing & Observability | 2 | 6 | 8 | 5 |

---

## Top Critical Issues (fix before next deploy)

These are the items where the consequence is *silent data loss*, *cross-broker
corruption*, *unauthenticated control-plane access*, or *cluster-wide stalls*.

| # | Issue | Location | Why it's critical |
|---|---|---|---|
| C1 | `apply_to_state_machine` updates `last_applied_log` **before** running `apply_command`; not awaited under a write guard | `src/cluster/raft/storage.rs:1022-1044` | A cancellation or panic between the index-update and the apply causes the snapshot to claim an entry was applied that was not. Permanent silent loss of one or more state-machine commands. |
| C2 | `build_snapshot` reads `state` and `last_applied_log` non-atomically | `src/cluster/raft/storage.rs:1130-1158` | Snapshot meta can claim coverage of an apply that is not yet in the captured state. A replica restored from this snapshot skips that command on replay. |
| C3 | `apply_retention` performs no `leader_epoch` check | `src/cluster/partition_store.rs:1288-1378` and `src/cluster/partition_manager.rs:483-558` | Two brokers can both `apply_retention` on the same partition during a leader hand-off; both `await_durable=true` writes advance LSO, deleting records the new owner has acked reads on. Cross-broker corruption. |
| C4 | Zombie-mode entry calls `coordinator.release_partition` **before** `store.close()` | `src/cluster/partition_manager.rs:445-454` | After release the new owner can begin writing while we still hold unflushed `acks=0` writes in our memtable. Data loss for already-acked writes, and on flush failure during shutdown those writes are silently dropped. |
| C5 | Rebalance loop is not leader-gated; every broker proposes simultaneous moves every tick | `src/cluster/rebalance_coordinator.rs:810-820`, `metrics_reset_loop` similarly | N concurrent rebalance proposals fight over the same partitions; followers' proposals fail or redirect — wasted Raft bandwidth and split views of "what is overloaded". Same defect on `metrics_reset_loop`. |
| C6 | Failover is non-idempotent under errors — a single proposal failure strands a broker | `src/cluster/rebalance_coordinator.rs:540-590, 770-807` | If `mark_broker_failed` returns `Err`, `clear_failed_broker` is never called and the next tick sees `state == Failed` (no event), so the broker's partitions remain unowned forever. |
| C7 | `FindCoordinator` returns a broker that does not own the group's state | `src/cluster/handler/groups.rs:91-177` | Group state is fully replicated by Raft (any broker can serve), but the consistent-hash routing tells clients to go to broker B, while ops can be served on A. Two brokers concurrently handle the same group's transitions. |
| C8 | Idempotent producer path has no `IdempotentWrite` cluster-ACL check | `src/cluster/handler/produce.rs:80-110` and `src/cluster/handler/mod.rs:669-816` | An authenticated principal denied `IdempotentWrite` can still send idempotent batches under another producer-id and fence the legitimate producer. |
| C9 | Simple-consumer offset commit creates arbitrary `group_id` entries with no group-existence or fencing check | `src/cluster/handler/offsets.rs:206-207, 382-435` | Authenticated client can spam `OffsetCommit` with arbitrary `group_id` strings, growing the replicated state machine unboundedly; can also clobber an active group's offsets when `generation_id == -1`. |
| C10 | `OffsetCommit` accepts any `committed_offset` including past `i64::MAX` and below `log_start` | `src/cluster/handler/offsets.rs:382-435` | A consumer commits `i64::MAX` and silently skips every record forever; on retention-purged partitions, an old offset commits successfully and the next fetch fails with a delayed `OFFSET_OUT_OF_RANGE`. |
| C11 | `SyncGroup` non-leader assignments are silently dropped — leader-failover-mid-rebalance can lose assignments | `src/cluster/handler/groups.rs:421-510` | When two clients race to be leader, the one that lands second has its assignment overwritten with no diagnostic and no metric — split views of group assignments. |
| C12 | Inflight-byte budget releases the moment the frame is read, not when dispatch finishes | `src/server/connection.rs:198-216` | The documented "bound on inbound bytes allocated but not yet dispatched" is not in fact maintained. Coordinated produce flood blows past 1 GiB easily. |
| C13 | Replay-cache populated *before* HMAC verification of Raft frames | `src/cluster/raft/auth.rs:395-421` | An attacker can inject 200,000 attacker-chosen nonces into the replay cache without authenticating, DoSing it and producing a timing oracle for live nonces. |
| C14 | `User:*` ACL wildcard matches `User:ANONYMOUS`; production posture gate is satisfied with `acl_enabled` *or* `sasl_required` | `src/cluster/raft/domains/acl.rs:144`, `src/cluster/authorizer.rs:85`, `src/cluster/config.rs:1816-1826` | A common `User:*` Allow ACL gives unauthenticated TCP clients the same access. The posture gate doesn't catch this configuration. |
| C15 | `RaftAuthKeys::default()` returns the permissive (unauthenticated-OK) variant | `src/cluster/raft/auth.rs:372-381` | Any code path that builds the broker without going through `from_env` (tests, embedded use, programmatic construction) runs the Raft control plane wide open. A single TCP packet from any host with route to the Raft port wins the cluster (`JoinCluster` auto-promotes). |
| C16 | `init_telemetry` panics if `init_logging` already ran; `shutdown_telemetry()` is never called from main | `src/telemetry.rs:241-245`, `src/bin/kafkaesque.rs:344-353` | Enabling the `otel` feature crashes startup; even if it didn't, the OTLP batch exporter drops pending spans on exit. |
| C17 | Background tasks have no supervisor — panic-and-forget | `src/cluster/rebalance_coordinator.rs:719-757`, `src/bin/kafkaesque.rs:222-224` | Failure-detector or health-server panics are silent. K8s liveness probes still pass at the LB but the metrics endpoint is dead and there's no log. |

---

## Detailed Findings

### 1. Correctness & Bugs

#### 1.1 Raft state machine — non-determinism and atomicity (HIGH–CRITICAL)

* **Snapshot/apply atomicity (Critical).** See **C1, C2** above.
  Additional fix: hold a single `RwLock` write guard around both
  "advance `last_applied_log`" and "run `apply_command`", and have
  `build_snapshot` capture the state under that same lock to fence apply.

* **`HashMap` everywhere in the replicated state machine (Critical).**
  Every domain (`partition.rs`, `group.rs`, `producer.rs`, `transfer.rs`,
  `broker.rs`, `acl.rs`) uses `HashMap` / `HashSet`. Postcard serializes
  these in iteration order, randomized per-process. **Snapshots built on
  different replicas have different bytes for the same logical state.**
  This blocks any future Merkle-tree snapshot diffing, content-addressed
  dedup, or operator-driven byte-level reconciliation. **Fix:** switch all
  domain maps to `BTreeMap`/`BTreeSet`, or implement custom serialize that
  sorts. This is also the root cause of `removed: Vec<AclBinding>` being
  unstable across replicas (`raft/domains/acl.rs:240-273`).

* **`apply_command` increments `state.version` even on no-op / rejected commands**
  (`src/cluster/raft/state_machine.rs:181-269`). Any caller using `version`
  as a cache-invalidation token is misled. Either rename to `apply_count`
  or only bump when state actually mutates.

* **Cross-replica determinism violations in command logic:**
  - `JoinGroup` synthesizes member-id from `members.len()` without checking
    whether the same `(client_id, client_host)` is already present, producing
    duplicate members on rejoin (`src/cluster/raft/domains/group.rs:343-347`).
  - `BrokerCommand::Heartbeat` accepts a backdated `timestamp_ms` and
    monotonicity-violates `last_heartbeat_ms`
    (`src/cluster/raft/domains/broker.rs:119-132`); use
    `broker.last_heartbeat_ms = broker.last_heartbeat_ms.max(timestamp_ms)`.
  - `current_time_ms()` uses `unwrap()` on `SystemTime::duration_since(UNIX_EPOCH)`
    (`src/cluster/raft/coordinator/mod.rs:331-336`). One bad clock read
    panics the coordinator background task.

* **Idempotent retry handling on the join path:** `handle_join_cluster` and
  `handle_promote_member` substring-match `"already"` in the openraft error
  message (`src/cluster/raft/network.rs:1180-1188, 1212-1220`). A minor
  openraft bump silently breaks join idempotency.

* **`MAX_FORWARD_HOPS` mixes `>=` (client) and `>` (server)** — boundary
  condition wrong by 1, can spuriously trigger `ForwardLoopDetected` on
  legitimate 3-broker chains during transient leader instability
  (`src/cluster/raft/network.rs:62-63, 406-411, 1078-1086`).

* **`CompleteRebalance` / `TriggerRebalance` are not leader-gated and carry
  no proposing-broker identity** (`src/cluster/raft/domains/group.rs:630-659`).
  Any broker (or compromised one) can drive rebalance storms.

#### 1.2 Storage layer — durability and consistency (CRITICAL)

* **`apply_retention` lacks epoch and lease check (Critical, C3).** See top
  of report. The single mutating path on `PartitionStore` that bypasses
  fencing.

* **Zombie-mode close ordering loses acked writes (Critical, C4).** See top
  of report. Fix is to call `store.close()` *before* `coordinator.release_partition`
  and to refuse release on flush failure.

* **`recover_hwm_from_records` `scan_floor` masks confirmed gaps (High).**
  Recovery uses `log_start_offset.max(persisted_hwm)`, but the standalone
  `_hwm` key is only updated every 64 batches — gaps in the most recent
  64-batch window (the most likely source of post-crash gaps) are categorized
  as "potential unflushed writes" and the broker comes online silently.
  (`src/cluster/partition_store.rs:1843-1845`,
  `src/cluster/partition_recovery.rs:245`). Use the highest *batch-embedded*
  HWM seen during the scan as the reference.

* **`ensure_partition` accepts negative partition indices (High)** —
  `src/cluster/partition_manager.rs:948-1037`. SlateDB instances created at
  `topic-X/partition--1/`. Test on line 2380 explicitly accepts either
  outcome, masking the bug. Reject `partition < 0` at the top of the function.

* **Record-key encoding broken for negative offsets (Medium, latent).**
  `i64::to_be_bytes` lex-orders negatives *after* positives; range queries
  against negative offsets silently return empty results
  (`src/cluster/keys.rs:41-46`). Fix: flip the sign bit (or assert
  `offset >= 0` at every encode site).

* **`record_count: i32` cast to `i64` and added to `base_offset` without
  checked arithmetic** (`src/cluster/partition_store.rs:667`). On corrupt
  input `new_hwm` overflows negative; combined with the negative-offset
  encoding bug, undefined ordering follows. Use `checked_add`.

* **`delete_partition_data` uses `try_collect()` on a paged `list()` (Medium).**
  Memory-spike on partition deletion of busy partitions
  (`src/cluster/partition_manager.rs:1217-1228`). Stream the listing.

* **`get_for_write` slow path treats any error as ownership loss (Medium).**
  Transient Raft hiccups eject the partition and force re-acquire +
  recovery scan (`src/cluster/partition_manager.rs:1153-1173`). Retry with
  backoff; only close on explicit "no longer own" responses.

* **`release_partition_for_deleted_topic` does not call `coordinator.release_partition`**
  (`src/cluster/partition_manager.rs:1849-1872`). Lease lingers until natural
  expiry; inconsistent with the metric label `("release", "topic_deleted")`.

* **`WriteGuard::estimated_remaining_lease()` uses `held_secs` arithmetic
  on a stale `lease_remaining_secs`** (`src/cluster/partition_handle.rs:303-314`).
  TOCTOU between the cached lease grant and the actual wall-clock deadline.
  Real safeguard is the per-write epoch check in `append_batch_inner` —
  rename `lease_likely_valid` to make this explicit.

#### 1.3 Wire protocol & request handling (HIGH)

* **Inflight budget doesn't bound dispatch memory (Critical, C12).** See top.

* **Fetch parser is not version-aware** — `parse_fetch_request` reads
  `max_bytes` and `isolation_level` unconditionally
  (`src/server/request/fetch.rs:35-54`). Currently safe only because
  `versions.rs:104` clamps Fetch min-version to 4. Time-bomb: any future
  range bump silently corrupts request parsing.

* **`parse_array` accepts `-1` (null) for non-nullable fields** —
  `parse_produce_request` calls it with the topics array and silently
  treats `-1` as "no topics," giving a no-op produce ack instead of an
  error (`src/parser.rs:113-119` + `src/server/request/produce.rs`).
  Provide separate `parse_array` and `parse_nullable_array` helpers.

* **`parse_compact_nullable_string` does not bound length before `take`**
  (`src/parser.rs:187-204`). A varint length of `u32::MAX` asks for a
  4 GiB take; nom fails on insufficient bytes but no explicit upper bound.
  Add a `MAX_STRING_SIZE` cap.

* **CRC validation is performed deep in the produce handler, not at ingress**
  (`src/server/connection.rs:567+`). A flood of corrupt-batch produces
  wastes CPU on partition lookup, leader checks, etc. before per-partition
  rejection.

* **Trailing bytes after request body parse are warned-and-dispatched** —
  silently masks parser drift bugs and allows attacker-smuggled bytes
  appended to a valid request (`src/server/request/mod.rs:413-431`).
  Reject with `InvalidRequest`.

* **`Produce` parser rejects null record-set (`-1`)** — Kafka specifies
  this is `NULLABLE_BYTES`; clients legitimately sending `-1` get a parse
  error (`src/server/request/produce.rs:57-69`).

* **Handler-timeout response uses `KafkaCode::InvalidRequest` instead of
  `RequestTimedOut`** (`src/server/connection.rs:339-352`). Clients log
  this as a bug and may not retry.

* **Many handler errors collapse to a single Kafka error code:**
  - `produce_to_partition` returns `NotLeaderForPartition` for *any*
    `get_for_write` error (`src/cluster/handler/mod.rs:710-726`) —
    triggers metadata-refresh storms on transient I/O.
  - `handle_init_producer_id` returns `KafkaCode::Unknown` for everything
    (`src/cluster/handler/producer_id.rs:78-87`).
  - `handle_offset_fetch` returns `error_code = None` for both "no offset"
    and coordinator failures (`src/cluster/handler/offsets.rs:556-570`),
    causing silent data skip on consumer startup hiccups.

* **`OffsetCommit` accepts any `committed_offset` (Critical, C10).** See top.

* **`handle_metadata` auto-create races with concurrent `DeleteTopics`**
  (`src/cluster/handler/metadata.rs:78-141`) — topic resurrection bug.

* **`handle_fetch` does not enforce upper bound on `max_wait_ms`**
  (`src/cluster/handler/fetch.rs:66-72`). Client can request `i32::MAX`
  (~24.8 days); pin a connection task indefinitely.

* **`handle_fetch` long-poll arm-and-await race** — `Notify::notified()`
  futures created *after* `collect_fetch` returns; a producer wake landing
  in the gap is dropped, causing spurious `max_wait_ms` waits
  (`src/cluster/handler/fetch.rs:108-121`).

* **`handle_list_offsets` does not check zombie mode** —
  `src/cluster/handler/offsets.rs:27-143`.

* **Offset metadata is unbounded in size; bytes go straight into the
  replicated state machine** (`src/cluster/handler/offsets.rs:382-393`).

* **`validate_group_id` is only called in `JoinGroup`** — every other
  group/offset endpoint accepts arbitrary group strings including control
  characters (`src/cluster/handler/groups.rs:553-605` and others).

* **`JoinGroup` does not validate `session_timeout_ms` / `rebalance_timeout_ms`
  bounds**. A negative value casts to `u64::MAX`, permanently holding a
  group slot (`src/cluster/handler/groups.rs:255-265`).

* **`is_simple_consumer_commit` uses `< 0` instead of `== -1`**
  (`src/cluster/handler/offsets.rs:206`). Buggy clients sending
  `generation_id = -2, member_id = ""` bypass fencing entirely.

* **ACL-denied `acks=0` produce drops are not audit-logged**
  (`src/cluster/handler/produce.rs:131-156`). No per-principal counter.

#### 1.4 Connection & resource management (HIGH)

* **Per-IP connection counter cleanup is fire-and-forget on `Drop`**
  (`src/server/mod.rs:187-208`). The async spawn from `Drop` may not run
  on shutdown; `active_connections` decrements synchronously while
  `connections_per_ip[ip]` lags, causing per-IP false rejections under
  churn. Use a synchronous primitive.

* **Plain Kafka listener has no "first-byte" timeout** —
  `read_kafka_frame`'s 30 s read timeout is the only bound. With a default
  `max_total_connections = 1024`, ~34 conn/s pins the broker
  (`src/server/mod.rs:847-872`).

* **TLS server config:** `with_no_client_auth()` by default and even when
  mTLS is configured the validated cert is **not** surfaced as a SASL
  principal (`src/server/tls.rs:75-76, 92-121`). Operators believe mTLS
  pins identity but unauthenticated SASL still produces `User:ANONYMOUS`.
  Combined with C14 the wildcard ACL still applies.

* **No `ensure_crypto_provider()` in Raft TLS** — possible runtime panic
  on Raft TLS startup (`src/cluster/raft/tls.rs:91-126`).

* **Neither TLS module pins TLS 1.3** — TLS 1.2 permitted unnecessarily.

* **Health server reads only 1024 bytes** — long Authorization headers
  (e.g. JWTs from a forwarding gateway) silently truncated, returning 401
  (`src/server/health.rs:215`).

* **`send_error_response` writes then returns Err — race with connection
  close** can cause clients to see TCP RST instead of the Kafka error code
  (`src/server/connection.rs:324-338`). Add `flush + shutdown` before the
  return.

* **`SCRAM` session map keyed by `SocketAddr`, no TTL, no cap**
  (`src/cluster/sasl_provider.rs:64, 322-323`). Half-open SCRAMs (drop
  before client-final) leak entries until OS notifies the connection layer.

#### 1.5 Authentication & authorization (HIGH)

* **Metrics bearer-token compare is non-constant-time (High, fix in 1 line)** —
  `src/server/health.rs:302`. Use `subtle::ConstantTimeEq` exactly as
  `sasl_provider.rs:278` already does.

* **Production posture gate satisfied by `acl_enabled` *or* `sasl_required`** —
  `src/cluster/config.rs:1816-1826`. Should require `sasl_required==true`.

* **`User:ANONYMOUS` matched by `User:*` (C14)** — already covered.

* **PBKDF2 iterations hardcoded at 4096** — OWASP 2023 floor is 600 000.
  Stolen `stored_key`+`salt` from a leaked SlateDB snapshot is cheaply
  crackable. Expose `SASL_SCRAM_ITERATIONS` (`src/cluster/scram.rs:45, 80-84`).

* **Cleartext passwords retained in `UserRecord`** even when only SCRAM is
  used (`src/cluster/sasl_provider.rs:48-51, 172-182`). Wrap in
  `secrecy::SecretString` / `Zeroizing`; recompute PBKDF2 for PLAIN.

* **SCRAM gaps:**
  - GS2 channel-binding consistency (n,, vs y,,) not enforced between
    client-first and client-final (`src/cluster/scram.rs:129-135, 250-256`).
  - `m=` mandatory-extension marker in client-final silently ignored.
  - `authzid` silently dropped instead of either honored or rejected.
  - `random_nonce()` derived from a single UUIDv4 (~122 bits) with replaced
    chars; switch to 32 bytes from `OsRng`.

* **Raft replay window 5 minutes / 60 s clock skew** is generous for a
  control plane; cache is in-memory only. Persist nonce-cache or tighten
  to 60 s / 5 s (`src/cluster/raft/auth.rs:85-87`).

* **ACL host matching is exact-string only** — IPv4-mapped IPv6, dual-stack,
  and zone-id literals all silently miss
  (`src/cluster/raft/domains/acl.rs:147-149`). Normalize via
  `Ipv6Addr::to_canonical()` and compare typed `IpAddr`s. Add CIDR support
  or document the limitation explicitly.

* **`from_strings` for `RaftAuthKeys` accepts any non-empty secret** —
  one-byte `RAFT_CLUSTER_SECRET=x` passes the gate
  (`src/cluster/raft/auth.rs:247-263`). Require ≥32 bytes.

* **PLAIN failure path logs the username**, which is regularly the user's
  password due to typos in interactive clients
  (`src/cluster/sasl_provider.rs:280-286`).

* **Super-user list does not validate `User:` prefix** — operator can set
  `KAFKAESQUE_SUPER_USERS=User:*` and grant cluster-admin to everyone
  (`src/cluster/config.rs:1598-1603`).

#### 1.6 Other correctness items

* **`AuthRateLimiter::record_failure` race with `record_success`** across
  connections from the same IP — sticky lockouts after legitimate logins
  (`src/server/rate_limiter.rs:115-180`). CAS on
  `(failure_count, lockout_until)`.

* **`RaftRpcServer::run` has no per-IP cap or accept semaphore on Raft port**
  (`src/cluster/raft/network.rs:917-955`). Connection-flood DoS;
  authenticated cluster-secret holders aside, an attacker can still
  exhaust file descriptors and CPU on TLS handshake.

* **`read_rpc_frame` cancellation between reads** can leak partial state
  on the server side (`src/cluster/raft/auth.rs:431-501`). Atomic read of
  the rest after a single length prefix.

* **Raft RPC dispatch deserialize errors close the connection without
  responding** (`src/cluster/raft/network.rs:1019-1162`). Clients retry 3×
  with backoff against a closing socket. Send a typed
  `RaftRpcResponse::ErrorV2(InvalidRequest)`.

* **`proposal_semaphore` not held on the leader side for forwarded writes**
  — backpressure is asymmetric; a misconfigured follower can DoS the leader
  (`src/cluster/raft/node.rs:289-374`).

* **Trace-context fields on `RaftRpcMessage` envelope are absent**
  (`src/cluster/raft/network.rs`) — see Observability §6.

---

### 2. Performance

#### 2.1 Hot-path allocations

* **Per-produce/fetch `String` allocations** in `LoadMetricsCollector::record_*`
  (`src/cluster/load_metrics.rs:289-323`) and in
  `cached_topic_name`/`select_coordinator_id`
  (`src/cluster/handler/mod.rs:530-533`, `groups.rs:86-87`). Switch to
  `Arc<str>` or `(Arc<str>, i32)` keys; use `Cache::get` before
  `get_with(topic.to_string(), …)`.

* **`vec![0u8; size]` zero-init for incoming Kafka frames**
  (`src/server/connection.rs:207`). For 100 MB produce frames this
  zeroes 100 MB before `read_exact` overwrites it. Use `BytesMut::with_capacity`
  + `read_buf`.

* **`encode_with_size` triple-allocation per response**
  (`src/server/response/mod.rs:111-126`) — header `Vec`, body `Vec`,
  combined `Vec`. For a 10 MB fetch response this peaks ~30 MB. Encode
  into a single `BytesMut`.

* **`LoadSnapshot::with_raft_index` clones every map every evaluation cycle**
  (`src/cluster/auto_balancer.rs:228-246`). Borrow the maps; the snapshot
  is consumed in place.

#### 2.2 Lock contention

* **`auto_balancer` write lock held across awaits during entire batch
  transfer** (`src/cluster/rebalance_coordinator.rs:614-714`). Readers
  starve for seconds; `metrics_reset_loop`'s cooldown cleanup blocks
  exactly when cooldowns are being created. Take the write lock for
  `should_evaluate()` + `evaluate_snapshot` (sync), drop, then run transfers.

* **`bounded_topic_label` mutex on the produce/fetch hot path**
  (`src/cluster/metrics.rs:1446-1456`). Sharded set or `arc_swap`.

#### 2.3 I/O and scans

* **`recover_hwm_from_records` and `load_producer_states` are unbounded
  scans on partition open** (`src/cluster/partition_recovery.rs:81-179`,
  `366-401`, `partition_store.rs:1865`). Periodically `tokio::yield_now()`,
  add a `recovery_scan_records_processed` counter, and lazy-load producer
  state per producer-id rather than full prefix scan.

* **`find_batch_start` cache-miss path re-scans previously scanned ranges**
  (`src/cluster/partition_store.rs:1107-1170`). Track `prev_scan_start`
  and only scan `[wider_start..prev_scan_start]` on each iteration.

* **`apply_retention` builds one `WriteBatch` of all expired batches**
  (`src/cluster/partition_store.rs:1305, 1356-1362`). Chunk into ~1000
  batches per WriteBatch with intermediate LSO advances.

* **`validate_offset_continuity_from` clones+sorts on every call** even
  though SlateDB scans are sorted (`src/cluster/partition_recovery.rs:225-226`).
  Validate sortedness as you iterate; drop the clone+sort.

#### 2.4 Object-store usage

* **Retention loop has no per-partition lease check (High)** — see C3 in
  the storage section. Beyond correctness, retention also runs `await_durable=true`
  writes, doubling object-store traffic from a non-leader.

#### 2.5 BufferPool

* **Returned buffers retain worst-case capacity**
  (`src/cluster/buffer_pool.rs:107-118`). Per-thread thread-local pool
  accumulates 4 MB per worker. Drop or `shrink_to(DEFAULT_BUFFER_CAPACITY)`
  on `put` when the buffer grew past threshold.

---

### 3. Code Quality & Maintainability

#### 3.1 Module size and responsibility

* **`src/cluster/error.rs` (2541 LOC)** — ~1800 LOC are inline tests; the
  enum + impl are ~250 LOC. Move tests to `error/tests.rs`; extract
  `From<slatedb::Error>` + fencing patterns to `error/fencing.rs`.
  Result: `error.rs` < 300 LOC.

* **`src/cluster/metrics.rs` (3709 LOC)** — 85+ free `record_*`/`set_*`
  functions, 80+ `Lazy<…>` statics, plus the fencing circuit-breaker
  (lines 522-638) and cardinality-limiting subsystem (1363-1470). Split
  into `metrics/{mod, broker, partition, group, request, raft, fencing,
  cardinality, circuit_breaker}.rs` and introduce typed builders so call
  sites get IDE help.

* **`src/cluster/config.rs` (3151 LOC)** — `from_env` is a single ~500-line
  method. Introduce `env_helpers` (`env_or`, `env_opt`, `env_bool`); split
  into `from_env_broker`, `from_env_storage`, `from_env_sasl`, `from_env_failover`.

* **`src/cluster/handler/mod.rs` (1654 LOC)** — `SlateDBClusterHandler::new`
  is ~360 lines and orchestrates the whole cluster bring-up. Builder with
  explicit phases: `build()` → `bootstrap()` → `start()`. Move the
  `RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE` env-var parse out of the constructor.

* **`src/server/connection.rs` (1783 LOC)** — bundles inflight budget,
  frame reader, auth gate, plain client, TLS client, fuzz hook. Split into
  `server/connection/{frame, inflight, auth_gate, plain, tls, mod}.rs`.

* **`src/cluster/raft/domains/transfer.rs` (1498 LOC)** — split into
  `domains/transfer/{types, state, apply, mod}.rs`. Apply systematically
  to `group.rs` (1072 LOC), `partition.rs` (927 LOC).

* **Trivial constructor tests inflate `handler/mod.rs` by ~450 LOC**
  (`src/cluster/handler/mod.rs:1200-1654`). Delete tests that assert
  `BrokerId::new(42).value() == 42`.

#### 3.2 Duplicate trait scaffolding

* **`Handler` trait and `MetadataHandler`/`ProduceHandler`/… sub-traits
  duplicate 600+ LOC of default impls** — sub-traits are unused outside
  their own test (`src/server/handler.rs` and `src/server/handler_traits.rs`).
  Delete `handler_traits.rs`, or commit to it: pick one.

* **`PartitionCoordinator` trait has 23 async methods; only `RaftCoordinator`
  and `MockCoordinator` (2381 LOC) implement it**
  (`src/cluster/traits.rs`). Premature abstraction. Either slim it (split
  along read/write boundary) or delete and use `RaftCoordinator` concretely
  with single-node Raft test cluster.

* **`coordinator/<x>.rs` mirrors `domains/<x>.rs` mechanically** — three
  files in lockstep per domain. Either collapse the trait impl into the
  domain file, or generate it via macro.

#### 3.3 Layering violations

* **Wire-protocol `crate::cluster::raft::AclOperation` types leak through
  every handler** (`src/cluster/handler/{produce, fetch, groups, offsets,
  admin, metadata, mod}.rs`). Move ACL types to `cluster/authorizer.rs`
  (or `cluster/acl/types.rs`) and re-export from `raft/domains/acl.rs`.

* **Two `BrokerInfo` structs with different shapes** —
  `src/cluster/coordinator/mod.rs:34` vs
  `src/cluster/raft/domains/broker.rs:18`. Define once with the richer
  fields; provide `BrokerSummary` view where identity-only is needed.

* **Namespace collision: `cluster::coordinator` (98 LOC utility) vs
  `cluster::raft::coordinator` (372 LOC actual)**. Rename top-level to
  `cluster::types` or fold contents into `cluster::traits`/`cluster::validation`.

#### 3.4 API surface

* **Overly public `pub mod` in `cluster/mod.rs:64-95`** —
  `keys` (on-disk binary key encoder), `buffer_pool`, `load_metrics`,
  `observability` are implementation detail. Demote to `pub(crate) mod`.
  `mock_coordinator` should be `#[doc(hidden)]` even when the
  `test-utilities` feature is enabled.

#### 3.5 Panics on production paths

* **`unwrap()` on `f64::partial_cmp` in auto-balancer**
  (`src/cluster/auto_balancer.rs:467, 500-501, 505-506, 608`) — NaN inputs
  panic the unsupervised auto-balance task.

* **`unwrap()` on poisoned `RwLock`** in `rebalance_coordinator.rs:1120-1199`
  (largely the test mock — gate it tighter and use `parking_lot::RwLock`).

* **`SystemTime::now().duration_since(UNIX_EPOCH).unwrap()`** in
  `coordinator/mod.rs:331-336` — see §1.1.

#### 3.6 Errors and validation

* **`SlateDBError` stringifies underlying errors and uses pattern matching
  on the message text for `is_fenced()`** (`src/cluster/error.rs:531-566`).
  Pattern strings (`"conditionnotmet"`, `"leaseidmismatch"`) drift across
  upstream library version bumps and silently disable fencing detection.
  Wrap typed errors and pattern-match structurally.

* **`SlateDB(_)` is blanket-retriable but covers fencing too**
  (`src/cluster/error.rs:367`). Write path can retry through fencing.

* **`ClusterConfig::validate()` returns `Vec<String>`**
  (`src/cluster/config.rs:1102`). Use a typed enum so callers can
  programmatically distinguish error classes.

* **Trait methods that are infallible and synchronous are `async fn`
  returning `SlateDBResult`** (`traits.rs:227, 230, 695`) — pollutes the
  call graph and hides real-async work.

---

### 4. Architecture & Design

* **Lack of leader-only execution gating across coordinator loops** —
  see §5.

* **Authorization logic is scattered**: ACL checks open-coded in five
  handlers (`groups.rs:45-62`, `fetch.rs:194`, `produce.rs:138, 243`,
  `admin.rs`, `mod.rs:579`). Add `Authorizer::allows(ctx, op,
  resource_type, name) -> bool` extension method.

* **No internal command/result types between server-protocol and cluster
  layer** — `ClusterHandler` consumes `ProduceRequestData` and produces
  `ProduceResponseData` directly. Wire format *is* the internal API. Adds
  friction for batching, internal fan-out, and ACL-driven response shaping.
  Consider `cluster::commands::{ClusterProduceRequest, ClusterProduceResult}`.

* **`SlateDBClusterHandler::new` performs IO, network, and starts background
  tasks in the constructor**. Builder pattern, see §3.1.

* **Mock-vs-real divergence in `MockCoordinator`** — tests using it skip
  postcard-encode + replicate + apply, hiding bugs that only show under
  the serialized-then-applied ordering. Consider replacing with
  `RaftCoordinator` over an in-memory transport.

* **No log compaction implementation; not documented as missing**. Either
  ship `cleanup.policy=compact` semantics (with concurrent-writers test)
  or document the gap in README and the API_VERSIONS surface.

---

### 5. Concurrency & Reliability

* **Rebalance loop, metrics-reset loop not leader-gated (Critical, C5).**
  See top.

* **Failover non-idempotent under errors (Critical, C6).** See top.

* **Background tasks have no supervisor (Critical, C17).** See top. Add
  `match handle.await { Err(panic) => abort/restart-and-log }` wrappers.

* **Failure detector counts missed heartbeats purely by wall time** with
  no "monitor itself was paused" detector (`src/cluster/failure_detector.rs:264-287`).
  A 3-second leader GC pause declares every broker dead with default
  `heartbeat_interval=500ms, threshold=5`. Track the gap between
  successive `check_brokers()` ticks and advance `last_heartbeat` for all
  brokers by the gap.

* **`LoadSnapshot.is_stale` uses `SystemTime`; backwards NTP step
  false-positives "fresh"; forwards step false-positives stale**
  (`src/cluster/auto_balancer.rs:248-265`). Use `Instant` for monotonic age,
  use Raft `index` for cross-process freshness.

* **`record_fencing_detection_with_circuit_breaker` race on counter reset**
  (`src/cluster/metrics.rs:532-624`). Gauge can mismatch counter under
  concurrent fencing. Use compare-exchange or wrap in `Mutex<CircuitBreakerState>`.

* **Zombie-mode `try_exit` race with concurrent `enter`**
  (`src/cluster/zombie_mode.rs:152-195`). Pack `(active: bool, generation: u64)`
  into a single AtomicU64; CAS them together.

* **Auto-balancer always picks `underloaded.first()`** —
  `src/cluster/auto_balancer.rs:586`. All `max_partitions_per_cycle` (5)
  moves go to the same target; thundering herd until cooldown.

* **Rebalance batch chunk failures: no retry path** —
  `src/cluster/rebalance_coordinator.rs:540-555`. Don't `clear_failed_broker`
  on batch error; let the next tick retry.

* **`RebalanceCoordinator::stop` doesn't await tasks** —
  `src/cluster/rebalance_coordinator.rs:760-762`. Use
  `tokio::sync::Notify` / `CancellationToken` so loops wake immediately.

* **`bin/kafkaesque.rs` shutdown drops `BrokerRuntimes` via Drop, blocking
  silently up to 30s**. Call `BrokerRuntimes::shutdown()` explicitly with
  a logged timeout.

* **OTLP exporter has no shutdown hook / timeout (Critical, C16)** — see top.

---

### 6. Testing & Observability

#### 6.1 Critical missing tests

* **No in-flight produce-during-leader-failover test (Critical).** No test
  asserts "every `acks=all` response that returned OK is durable on the
  new leader". This is the most likely place data loss appears in practice.

* **No object-store transient-error retry integration test (Critical).**
  `FailingPutStore` is wired to Raft-snapshot install only — no test
  asserts the data plane retries `slatedb` writes on flapping S3.

* **No log-compaction tests; unclear if compaction is implemented** —
  document the gap or add the feature with concurrent-writer tests.

* **No boot-from-empty vs boot-from-snapshot equivalence property test**
  — adds confidence that no domain is silently excluded from snapshot
  serialization (a real risk given `HashMap` non-determinism).

* **SCRAM second-attempt-with-wrong-password not covered at session/handler
  level** (`src/cluster/sasl_provider.rs:674`). Add: client_first → wrong
  client_final → state cleared → second client_first on same socket
  succeeds with correct creds.

#### 6.2 Property and loom test weakness

* **Loom tests use only `Ordering::SeqCst`** — guarantees the tests pass
  but won't catch bugs production code might have if it ever uses
  `Relaxed`/`Acquire`/`Release`. Mirror production orderings.
  `test_zombie_mode_double_check` has comment-only invariants — promote
  to `assert!`.

* **Property test ranges too small** — `0..32` cmds with 256 cases barely
  visits states with >4 brokers × >5 topics. Bump to `0..256` cmds with
  1024 cases, plus a soft runtime cap.

#### 6.3 Test hygiene

* **~50 `tokio::time::sleep(50ms)` calls drive flaky CI** — replace with
  `assert_eventually(predicate, deadline)` and `wait_for_leader` test
  harness API.

* **Fuzz corpus growth never minimized** — `scripts/fuzz-all.sh` doesn't
  run `cargo fuzz cmin`; api_create_topics has 176 corpus files at 17.9 KB.
  Add `fuzz.log` to `.gitignore`.

* **Fuzz target gaps:** ACL bootstrap-file parser
  (`KAFKAESQUE_ACL_BOOTSTRAP_FILE`), TLS handshake, frame writer,
  ApiVersions parse.

* **Tests use real ports via `PORT_COUNTER`** — port reuse across runs
  is possible.

* **`info!` log on partition-store hot path** at six locations — flood
  log aggregators at scale.

#### 6.4 Observability gaps

* **No trace-context propagation across Raft messages**
  (`src/cluster/raft/network.rs`). One client request that traverses
  leader → followers becomes 3+ disjoint traces in Jaeger. Add
  `trace_context: Option<Vec<u8>>` to `RaftRpcMessage`; inject/extract
  via the W3C TraceContext propagator.

* **`requests_total` only labels `success`/`error`**
  (`src/cluster/metrics.rs:113-118`). Cannot answer "what fraction of
  Produce returns `NOT_LEADER_FOR_PARTITION` vs `OUT_OF_ORDER_SEQUENCE`".
  Add `error_code` label (cardinality bounded by ~80 codes × ~25 APIs).

* **`request_duration_seconds` histogram starts at 1 ms**
  (`src/cluster/metrics.rs:124-126`) — well-tuned p99 produce is in the
  200–800 µs range. Add sub-ms buckets.

* **Cardinality bombs** — `EPOCH_MISMATCH_DETECTIONS`, `HWM_RECOVERY_EVENTS`,
  `BATCH_INDEX_SIZE`, `BATCH_INDEX_EVICTIONS`, `RETENTION_DELETED_BATCHES`
  all use `&["topic", "partition"]` labels with no
  `bounded_topic_label`/`bounded_partition_label` guard
  (`src/cluster/metrics.rs:316, 715, 869, 879, 898`). Topics × partitions
  blow past `MAX_METRIC_CARDINALITY` (10 000).

* **`BROKER_LOAD` and `BROKER_PARTITION_COUNT` gauges leak on broker
  removal** — no `forget_broker_metrics` analog
  (`src/cluster/metrics.rs:961-980, 1056-1067`). Stale series after
  cluster shrink.

* **Errors not recorded on spans** — `record_error!` macro exists at
  `src/cluster/observability.rs:275-291` but call density on error paths
  is low. Auto-deduced trace error rate is wrong.

* **No shipped dashboards/alerts/runbooks** — `metrics.rs` peppers
  `// ALERT when …` comments but no `dashboards/grafana-*.json` or
  `alerts/prometheus-rules.yml`.

* **`SlateDBError` stringification (cf. §3.6)** also breaks
  `is_fenced()` once upstream wording changes — observability of
  fencing then silently drops to zero.

#### 6.5 Repo hygiene

* **`fuzz.log` (47 KB) untracked in repo root** — add to `.gitignore`.

* **Inline `mod tests` in giant production files** masks real LOC and
  slows recompile. Promote to sibling `*_tests.rs` consistently with
  the `partition_store_tests.rs` pattern.

---

## Quick Wins (high impact, low effort)

These are surgical fixes — most are 1–20 lines — with disproportionate value.

1. **Constant-time bearer-token compare** for `/metrics`
   (`src/server/health.rs:302`) — change `==` to `subtle::ConstantTimeEq`. **5 min.**

2. **Tighten production posture gate**
   (`src/cluster/config.rs:1816-1826`) — require both
   `sasl_required==true` *and* `acl_enabled==true` (or just `sasl_required`)
   for non-development profiles. **15 min.**

3. **Fix idempotent-producer ACL gap** in produce path
   (`src/cluster/handler/produce.rs:80-110`) — gate on
   `authorize_cluster_api(InitProducerId)` when
   `parse_producer_info(records).is_idempotent()`. **30 min.**

4. **Validate `committed_offset` against `[log_start, hwm]` in
   `OffsetCommit`** (`src/cluster/handler/offsets.rs:382-435`). **45 min.**

5. **Validate `session_timeout_ms`/`rebalance_timeout_ms` bounds in
   `JoinGroup`** (`src/cluster/handler/groups.rs:255-265`). **15 min.**

6. **Cap `max_wait_ms` in fetch handler**
   (`src/cluster/handler/fetch.rs:66-72`). **10 min.**

7. **Use `e.to_kafka_code()` in `init_producer_id` and `produce_to_partition`
   error paths** instead of collapsing to `Unknown`/`NotLeaderForPartition`
   (`src/cluster/handler/producer_id.rs:78-87`,
   `src/cluster/handler/mod.rs:710-726`). **30 min.**

8. **Use `RequestTimedOut` instead of `InvalidRequest` for handler
   timeouts** (`src/server/connection.rs:339-352`). **5 min.**

9. **`Heartbeat` monotonicity:** `last_heartbeat_ms.max(timestamp_ms)`
   (`src/cluster/raft/domains/broker.rs:119-132`). **5 min.**

10. **Replace `unwrap()` on `partial_cmp` with `unwrap_or(Equal)` or
    `total_cmp`** (`src/cluster/auto_balancer.rs:467, 500-505, 608`). **5 min.**

11. **`current_time_ms()` `.unwrap_or_default()`**
    (`src/cluster/raft/coordinator/mod.rs:331-336`). **5 min.**

12. **Swap `HashMap` → `BTreeMap` in domain state structs** for
    deterministic snapshot bytes. **2–3 hours; high value.**

13. **Verify HMAC before populating replay cache**
    (`src/cluster/raft/auth.rs:395-421`). **30 min.**

14. **Make `RaftAuthKeys::default()` deny rather than permit**, gate
    the unauthenticated branch on an explicit `unauthenticated_ok: bool`
    field set only in dev/tests (`src/cluster/raft/auth.rs:372-381`). **1 hour.**

15. **Leader-gate `rebalance_loop` and `metrics_reset_loop`**
    (`src/cluster/rebalance_coordinator.rs:810-820`) — wrap loop body in
    `if executor.should_initiate_failover().await { … }`. **15 min.**

16. **Add `error_code` label to `requests_total`**
    (`src/cluster/metrics.rs:113-118`). **30 min.**

17. **Sub-ms latency buckets** in `request_duration_seconds`
    (`src/cluster/metrics.rs:124-126`). **5 min.**

18. **`fuzz.log` in `.gitignore`**. **30 seconds.**

19. **`store.close()` before `coordinator.release_partition` in
    zombie-mode entry** (`src/cluster/partition_manager.rs:445-454`).
    **20 min.** *(High blast radius — exercise care.)*

20. **Wrap spawned background tasks in a panic-logging supervisor**
    (`src/cluster/rebalance_coordinator.rs:719-757`,
    `src/bin/kafkaesque.rs:222-224`). **1 hour.**

21. **Reject `partition < 0` at the top of `ensure_partition`**
    (`src/cluster/partition_manager.rs:948-1037`). **5 min.**

22. **Remove the trivial constructor tests at the bottom of
    `cluster/handler/mod.rs`** (~450 LOC). **15 min.**

23. **Delete `handler_traits.rs`** (sub-traits unused outside their own
    test). **30 min.**

24. **Add `handle_list_offsets` zombie-mode short-circuit**
    (`src/cluster/handler/offsets.rs:27-143`). **15 min.**

25. **Strip cleartext password from `UserRecord` when PLAIN is disabled**;
    wrap in `Zeroizing<String>` otherwise
    (`src/cluster/sasl_provider.rs:48-51`). **1 hour.**

---

## Long-Term Recommendations

### LR1. Establish replicated-state-machine hygiene as a hard invariant

* Forbid `HashMap`/`HashSet` in any type that is part of `apply_command`
  output or snapshot — enforce via clippy lint and code review.
* Adopt a snapshot-fingerprinting test: two replicas applying the same
  command sequence must produce byte-identical snapshots. Add to CI.
* Bind `last_applied_log` and `apply_command` under one write guard;
  add a property test that checks "snapshot's `last_log_id` ⇔ all entries
  ≤ that index visible in `state`".

### LR2. Make leader-gating an architectural primitive

* Introduce `LeaderOnlyTask` newtype that takes a closure and an
  `is_leader` callback. Convert `rebalance_loop`, `metrics_reset_loop`,
  retention sweep (currently per-broker), and any future scheduled work
  to use it. Reject reviews that spawn unguarded `loop` tasks doing
  state-changing work.

### LR3. Promote auth posture from configuration discipline to type-safe construction

* Make the unauthenticated `RaftAuthKeys` variant impossible to construct
  outside an explicit `dev_only_unauthenticated()` factory.
* Enforce `User:*` ACL bindings to require an authenticated principal
  — pass an `authenticated: bool` through `AuthorizeRequest` and reject
  the wildcard match against `User:ANONYMOUS`.
* Surface the mTLS subject as the authenticated principal when no SASL
  layer is required; provide an `ssl.principal.mapping.rules`-equivalent
  config knob.
* Rotate to OWASP-floor PBKDF2 iterations and expose the knob; keep the
  4096 default available only behind an explicit "kafka-1.x compatibility"
  flag.

### LR4. Decompose the four "god" files

* Split `cluster/error.rs`, `cluster/metrics.rs`, `cluster/config.rs`,
  `cluster/handler/mod.rs` per the recommendations in §3.1. Most of the
  work is mechanical and isolated; the shrunken files are easier to
  re-architect afterward.
* Decide on the `Handler`/`HandlerTraits` and `PartitionCoordinator`
  abstractions — pick one, delete the other. Both abstractions are paid
  for in lockstep maintenance and not actually used for substitution.

### LR5. Replace string-pattern fencing detection with typed errors

* Wrap `slatedb::Error`, `openraft::error::*`, and `object_store::Error`
  variants directly in `SlateDBError` (use `#[from]`). The existing
  pattern-matching at `error.rs:531-566` becomes structural and survives
  upstream refactors.

### LR6. Observability for SREs, not for developers

* Add `error_code` label to `requests_total`; sub-ms histogram buckets;
  per-broker forget hooks; route every per-partition counter through
  `bounded_partition_label`.
* Ship `dashboards/grafana-*.json`, `alerts/prometheus-rules.yml`, and
  `RUNBOOK.md` mapping each `// ALERT when …` comment in `metrics.rs` to
  an alert and a runbook entry.
* Add W3C trace-context to `RaftRpcMessage` envelope so a client request
  can be followed across the leader and the followers in Jaeger.
* Audit every `instrument`-decorated `Result`-returning function and add
  `record_error!` on `Err` branches.

### LR7. Lift the test bar to durability and failover claims

* Add the in-flight-produce-during-leader-failover test (top item).
* Reuse `FailingPutStore` against `slatedb` writes through `PartitionStore`
  to validate the retry framework.
* Add `boot_from_snapshot ≡ replay_from_log` property test.
* Replace `tokio::time::sleep(_)` with `assert_eventually` helpers.
* Switch `MockCoordinator`-based integration tests to single-node
  `RaftCoordinator` over an in-memory transport — kill the 2381 LOC of
  parallel-but-divergent semantics.

### LR8. Decide on log compaction

* Either ship `cleanup.policy=compact` with a concurrent-writer test, or
  document the gap and reflect it in the advertised API surface.

### LR9. Reorganize tests out of giant files; align with project pattern

* Move inline `#[cfg(test)] mod tests` to sibling `*_tests.rs` for
  `error.rs`, `metrics.rs`, `config.rs`, `handler/mod.rs` — already the
  pattern in `partition_store_tests.rs`.
* Result: real source LOC drops to roughly half of the apparent number,
  CI compile times improve, and review focus stays on production logic.

---

## Closing assessment

The codebase is meaningfully more rigorous than typical for a project of
this scope: fuzz coverage is broad, fail-closed posture is the default,
fencing detection has a circuit-breaker, the lease/epoch design has been
thought through. The Critical findings are mostly "correctness on the
unhappy path" — which is exactly where Kafka-compatible brokers earn or
lose their reputation. With the **Top 17 critical items** addressed plus
the 25 quick wins, kafkaesque is a credible candidate for production
workloads inside an organization. The Long-Term Recommendations are
investments to take it from "credible" to "boring" — the highest praise
for a database/broker system.

