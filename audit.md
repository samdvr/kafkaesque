# Kafkaesque — Staff-Level Technical Audit

**Scope:** ~88 K LoC of production Rust + ~40 K LoC of tests across 90 integration suites and 44 fuzz targets, audited by six parallel investigators across correctness, architecture, performance, refactoring, reliability, and maintainability dimensions. The codebase implements a Kafka-compatible broker backed by object storage (S3/GCS/Azure) with embedded multi-group openraft for metadata.

**Headline:** This is a **disciplined, production-aspirational codebase** — uniform `thiserror`, exhaustive fuzz coverage, a `durability_contract_props` test that actually crashes the LSM, near-zero TODO/FIXME, no `unwrap()` outside one hot-path file. But it is **mid-migration**: ~5,700 LoC of multi-Raft sharding sit behind blanket `#![allow(dead_code)]` markers, and several documented invariants in module headers are **not yet wired through**, including KIP-320 fencing on the Fetch/Produce paths and HMAC purpose-mixing in the Raft mux. Add to that several CRITICAL throughput cliffs in the Raft storage layer that will surface the moment the cluster is load-tested at production scale, and the picture is: solid bones, real data-correctness gaps, and a mid-flight refactor that needs to land.

---

## CRITICAL findings (data-loss, hard outage, or correctness contract violation)

| # | Area | Finding | File:line |
|---|---|---|---|
| C1 | Correctness | **KIP-320 leader-epoch fencing is decoded but never enforced on Fetch/Produce.** Wire structs carry `current_leader_epoch`; only `OffsetForLeaderEpoch` validates it. A consumer with a stale view after a failover silently reads from the new owner without a `FencedLeaderEpoch` reject — silent reprocessing or skipping. README explicitly advertises this guarantee. | `src/cluster/handler/fetch.rs:298-510`, `produce.rs:490-700`, vs. `leader_epoch.rs:138` |
| C2 | Correctness | **Transient SlateDB write error → permanent offset gap that becomes a partition-DOA on restart.** `next_offset` is `disarm()`ed before `db.write_with_options`; a 5xx from S3 leaves the reservation advanced and HWM un-bumped, but the next successful append jumps HWM past the gap. With the *default* `fail_on_recovery_gap=true`, the partition refuses to re-open after restart. | `src/cluster/partition_store.rs:1058-1130` |
| C3 | Performance | **Per-entry fsync inside `apply_to_state_machine` and `append_to_log`.** Each Raft log entry triggers tmp-write+fsync+rename+dir fsync (≥2 fsyncs/entry), serialized through `apply_lock` and `log RwLock`. Every metadata mutation (topic create, lease extend, group join) is throttled to ≈1/(N×fsync_latency) — order ~20 batches/s on NVMe with N=10. | `src/cluster/raft/storage.rs:1232-1262, 1341-1385, 726-737` |
| C4 | Reliability | **`/readyz` only sees ~2 of the object-store call sites.** `MetricsObjectStore` records latency/errors but never calls `track_object_store_health(success)`; the gauge feeds zombie-mode entry off `partition_store.rs:1090,1119,1315,1320` only. A bucket outage in list/delete/head leaves `/readyz` at 200 and the broker keeps taking traffic. | `src/cluster/object_store_metrics.rs:43-50`, `metrics.rs:3246-3270` |
| C5 | Server | **Unbounded `Vec::with_capacity(attacker_i32)` in DescribeConfigs.** `parse_nullable_string_array` rejects `len < 0` then immediately `Vec::with_capacity(len as usize)` — every other parser clamps to `MAX_PROTOCOL_ARRAY_SIZE = 100_000` first. A 24-byte payload (`0x7FFFFFFF` + trash) attempts ~48 GiB allocation. Pre-auth where DescribeConfigs is reachable. | `src/server/request/configs.rs:115-134` |

---

## HIGH findings

### Correctness / contract

- **Lease cache fast-path bypass on coordinator-side revoke.** Cache validates by wall clock only; if the coordinator revokes a lease (force-fence after S3 unhealth), the cache admits writes for `lease_duration − threshold`. Surfaces as `EpochMismatch` instead of clean `NotOwned`. — `src/cluster/partition_manager.rs:1467-1490`
- **Fence-during-rebalance bypasses the "preserve lease on flush failure" guard.** The user-initiated release path keeps the lease if `close()` errors; the rebalance-driven release proceeds anyway. An `acks=0` batch still buffered at the moment of reassignment can be lost — directly contradicts the README's "graceful shutdown flushes every owned store before releasing its lease" claim. — `src/cluster/partition_manager.rs:2400-2411` vs `release_partition_inner:1252-1259`
- **Producer-state pruning can delete a live producer's persisted state during cache eviction.** Moka size-based eviction + `prune_producer_states` together can drop a legitimate idempotent producer's window; next sequence-0 batch is admitted as new. Bounded by `retention_ms < idle_window`, which is a tunable users may configure into the bug. — `partition_store.rs:2802-2826, 1971-1992`
- **Listener brought down by single `accept()` error.** `accept_result.map_err(Error::from)?` propagates every error out of `run()` — an `EMFILE` from one fd-exhaustion spike kills the data plane. Standard pattern is to retry/backoff on `EMFILE`/`ENFILE`/`EINTR`. — `src/server/mod.rs:437,796`
- **Frame response size truncated by `as i32` cast.** `let size: i32 = (body_len + (header_len - 4)) as i32;` — multi-partition fetches can push body past 2 GiB, wrapping the wire size negative; client desync. — `src/server/connection.rs:244`

### Performance / scalability

- **Per-forwarded-write TCP + TLS handshake.** Every leader-forwarded `client_write` opens a fresh connection. With TLS, 5–15 ms of handshake on top of every cross-broker metadata mutation. Should pool through `MuxConnInner`. — `src/cluster/raft/mux_client.rs:655-680, 566, 599`
- **Per-peer `Mutex<Option<Stream>>` head-of-lines all RPCs to a peer.** AppendEntries, snapshot installs, vote, and forwarded writes all queue on one mutex per peer; a slow snapshot install blocks heartbeats and triggers spurious elections. — `src/cluster/raft/mux_client.rs:246-280`
- **Lease cache miss costs a full Raft commit per produce.** Documented at "removes Raft overhead from ~67% of produce." On rolling restart every cache entry expires simultaneously and the remaining 33% becomes 100% — every produce blocks on a Raft write. — `src/cluster/partition_manager.rs:1493-1535`
- **`get(LEADER_EPOCH_KEY)` per durable produce.** SlateDB KV read on the hot path even though the in-memory `self.leader_epoch` is sufficient (SlateDB's own fence detects bad-epoch writes). At 100 batches/s × 1k partitions = 100k cache-hot KV gets/s competing with the actual produce. — `src/cluster/partition_store.rs:671-706`
- **Prometheus `with_label_values(&[topic, partition])` per produce/fetch.** ~400 k label-set probes/s + DashSet lookup at 100 k msg/s. Cache the resolved `IntCounter` Arc on `PartitionStore` once. — `src/cluster/metrics.rs:1807-1838, 1688-1707`
- **3 allocations per response in connection encode loop.** `Vec::new()` body + `Vec::new()` header + `Vec::with_capacity` result, on every API path; existing `buffer_pool.rs` is not wired in. — `src/server/connection.rs:756, 760, 784, 789, 1085-1278`

### Architecture

- **`ClusterCoordinator` trait surface leaks Raft domain types.** `traits.rs:718` imports `super::raft::domains::{PartitionTransfer, TransferReason}` into `PartitionTransferCoordinator`. The "swap the backend" abstraction is hardwired to Raft. ACL types follow the same pattern across 10 handlers (`AclOperation`, `AclResourceType` imported from `cluster::raft`). — `src/cluster/traits.rs:718`, handlers in `src/cluster/handler/{leader_epoch,metadata,groups,partitions,configs,incremental_configs,offsets,admin}.rs`
- **`SlateDBClusterHandler` is concrete on `RaftCoordinator`.** Even though `PartitionManager<C: ClusterCoordinator>` is generic, the handler is `partition_manager: Arc<PartitionManager<RaftCoordinator>>` — `MockCoordinator` cannot drive the real handler in tests. — `src/cluster/handler/mod.rs:129,132`
- **Mid-flight multi-Raft refactor under blanket `#![allow(dead_code)]`.** `mux.rs:34`, `mux_client.rs:60`, `cluster.rs:29`. Doc comments cite a "step 5b/step 10" plan and reference `super::node::RaftNode` / `super::network::RaftRpcServer` types that **no longer exist in the tree**. ~5,700 LoC where contributors cannot tell live-vs-dead from the code. The biggest single maintainability liability.
- **Documented HMAC purpose-mixing not yet enforced.** `mux.rs` documents per-group authentication purpose; `mux_client.rs:518-522` and `mux_server.rs:37-40` openly admit `FramePurpose` is "intentionally unused at this layer for now." The dispatch tag prevents accidental cross-routing, but a forged Shard(N) frame whose payload deserializes as Control would still pass HMAC. Module's own threat model not satisfied.

### Reliability / ops

- **Default `RAFT_LISTEN_ADDR = 127.0.0.1:9093`.** A pod that misses one env var binds Raft on loopback; peers can't reach it; broker silently learner-only. Should be `0.0.0.0:9093` with explicit `advertised`, like Kafka itself. — `src/cluster/config.rs:1026, 1780`
- **`deploy/kubernetes/secret.yaml` ships `CHANGE-ME` placeholder secrets.** A naïve `kubectl apply -k deploy/kubernetes` deploys those into etcd. The Helm path fails closed; the Kustomize path does not. `configmap.yaml:21` also hard-codes `KAFKAESQUE_SUPER_USERS: "User:ANONYMOUS"`, making ACLs a no-op by default. — `deploy/kubernetes/secret.yaml:24-25`, `configmap.yaml:21`
- **Lease-renewal loop closes lost partitions serially.** On a 100-partition reassignment storm, each `store.close().await` flushes SlateDB inline; further lease renewals starve, cascading to more lease losses. Should `tokio::spawn` the close work. — `src/cluster/partition_manager.rs:867-883`
- **`/health` returns 200 even when zombie.** K8s manifest maps `livenessProbe` to `/health`. A broker stuck in zombie mode is never restarted by kubelet — silent failure mode. — `src/server/health.rs:391-414`, `deploy/kubernetes/statefulset.yaml:104-111`
- **Listener accepts before Raft has converged.** Clients connecting in the first ~seconds get `BROKER_NOT_AVAILABLE` storms. — `crates/kafkaesque-bin/src/main.rs:271-340`

---

## MEDIUM findings (selection)

- **`apply_to_state_machine` is not atomic with `persist_last_applied`.** Panic between FSM mutation and last-applied write means openraft re-applies entry N on restart. Producer-id alloc is gated by request-token cache, so likely benign there; epoch bumps are not. — `src/cluster/raft/storage.rs:1341-1385`
- **`acquire_partition_core` cap-check is racy.** "Different partitions may still race; cap can briefly exceed" — under failover storms this is "briefly" + concurrency factor and can OOM the broker. — `src/cluster/partition_manager.rs:2241-2284`
- **Fetch always returns `log_start_offset: -1`.** Forces clients into a separate `ListOffsets` round trip on `OffsetOutOfRange`. May break `auto.offset.reset=earliest` fast paths. — `src/cluster/handler/fetch.rs:439`
- **`patch_base_offset` silently no-ops on short batches.** No `Result<(), TooShort>`. Caller cannot tell the patch happened. — `crates/kafkaesque-protocol/src/protocol.rs:332-338`
- **`encode_string_len` rejects `> i16::MAX`** but no upstream guard — handlers that hand the encoder a 40 KB error_message connection-kill instead of returning a Kafka error code. — `crates/kafkaesque-protocol/src/encode.rs:11-19`
- **`record_error!` per-message logging on hot paths.** During an S3 outage every produce error is logged at error level → log-shipper saturation amplifies the outage. Sampling/aggregation needed. — `src/cluster/observability.rs:275-280`
- **No retry/backoff on `MuxConnInner::send`.** Module doc claims "exponential backoff with jitter" — code has zero. — `src/cluster/raft/mux_client.rs:262-284`
- **Heartbeat loop sleeps `initial_jitter(interval)` *before* the first send.** Rolling-deploy windows can show a healthy broker as missing for up to one heartbeat interval. — `src/cluster/partition_manager.rs:504-510`
- **`partition_recovery::recover_hwm_from_records` has no scan deadline.** Cold-start after a cluster-wide outage can stall the broker startup for minutes per partition with no progress logs. — `src/cluster/partition_recovery.rs:82-193`
- **`ClusterConfig` god-object (95 public fields, 2,345 LoC).** The `config_views` module is the right idea but inverted — views are read-shapes, not the storage shape. Adding a knob touches struct + Default + validate + from_env + profiles + at least one view. — `src/cluster/config.rs`
- **`metrics.rs` 3,489 LoC, 91 globals, no tenant isolation.** `metrics_handle.rs:3-21` documents the migration plan to a per-handler `Metrics`; migration is unfinished. Two brokers in one process today clobber each other's circuit breaker. — `src/cluster/metrics.rs`, `metrics_handle.rs:3-21`
- **CI clippy uses `-D warnings` without `--all-targets`.** Test code can carry warnings that gate locally but pass CI — backwards. — `.github/workflows/ci.yml:181` vs `Makefile:7`
- **`handle_join_group`/`handle_sync_group` exceed 200 lines each, dominated by 4× repeated `JoinGroupResponseData {..}` early returns.** Trivial extraction, prevents copy-paste-bug class. — `src/cluster/handler/groups.rs:195-397, 408-631`
- **Sleep-as-synchronization in `linearizability_tests.rs:494,633`, `distributed_systems_tests.rs:517,543,1125`.** 500 ms–3 s sleeps; flake budget will erode under loaded CI. The `wait_until` polling pattern from `tests/common/raft_helper.rs:44-53` already exists. — flagged tests above

---

## LOW findings / notes

- **Epoch overflow.** `leader_epoch` is `i32`; no overflow check. Practical risk near zero (bumps per reassignment), but a chaotic cluster with constant rebalances can wrap to negative and permanently brick a partition. — `partition_store.rs:671`
- **`as_millis() as i64` truncation in `last_used_at_ms`.** `unwrap_or(0)` masks system-clock issues, including a clock that jumped backward — back-dates the field and causes premature producer-state pruning. — `partition_store.rs:622-625`
- **Buffer pool returns dirty buffers.** `return_buffer` doesn't clear capacity bytes; current consumers don't read past `len`, but a future caller using `Bytes::from(buf)` would expose previous batch contents (potential cross-tenant leak on a multi-tenant broker). — `buffer_pool.rs:117-145`
- **TLS hardcoded to TLS 1.3 only.** Defensible but produces an opaque "TLS handshake failed" with no version-mismatch metric for legacy librdkafka. — `src/server/tls.rs:75,113`
- **mTLS verifier swallows cert-validation cause.** Expired/untrusted-issuer details flattened into `Error::MissingData(...)`; operator tooling that filters by error kind misclassifies the failure. — `src/server/tls.rs:109-111`
- **SCRAM accepts both `n,,` and `y,,` channel-binding bytes.** RFC 5802 says the server MUST verify the cbind flag matches the GS2 header from client-first; current code only string-matches static bytes. CB isn't actually implemented anyway. — `src/cluster/scram.rs:307-316`
- **SCRAM `split_once(",,")` rejects valid GS2 with non-empty `authzid`.** Real-world Kafka clients rarely use authzid, but parser is not RFC-compliant. — `src/cluster/scram.rs:180-186`
- **`PlainAuthenticator` PAD_LEN=128 vs. >128-byte passwords.** Long passwords silently fail to authenticate themselves on the legacy in-memory authenticator. Production `SaslProvider` uses a different path. — `src/server/sasl.rs:272-291`
- **`acquire_locks` DashMap grows unboundedly** with topic churn (auto-create + delete pattern). Slow memory creep on long-running brokers. — `src/cluster/partition_manager.rs:143, 2226-2231`
- **`fire_and_forget_pool` shards by partition_index only.** Partition 0 of every topic lands on shard 0. Acks=0 backpressure drops bias toward small partition indices. — `src/cluster/handler/produce.rs:226-228`
- **`tokio::spawn` per connection close in CloseGuard Drop.** Probe-then-disconnect floods saturate the scheduler with one-shot tasks. — `src/server/connection.rs:497-505`
- **No correlation/request IDs in produce/fetch spans.** Tracing produce → fetch → replication requires reading client IP + offset and guessing. — `src/cluster/observability.rs:187-226`
- **Telemetry shutdown after health-handle abort.** OTLP buffer flushed last; if handler shutdown hangs, the spans documenting the bad shutdown are dropped. — `crates/kafkaesque-bin/src/main.rs:380-391`
- **Stale `super::node::RaftNode` / `super::network::RaftRpcServer` doc references** in `mux*.rs` and `cluster.rs`. Symbols don't exist in the tree. Onboarding readers grep for them and find nothing. — `raft/mux*.rs`, `cluster.rs`
- **`validate_or_panic` lives next to `validate` in config.rs.** Footgun helper for what is otherwise a `Result` API. — `src/cluster/config.rs:1648`
- **`MIN_SESSION_TIMEOUT_MS` / `MAX_SESSION_TIMEOUT_MS`** are function-local; should sit in `kafkaesque-protocol::constants` so the wire bound and handler bound can never drift. — `src/cluster/handler/groups.rs:240-241`
- **CRC-32C table reimplemented in two test files.** `durability_contract_props.rs:31-58` vs `cluster_handler_tests.rs:52-79`. Promote into `tests/common/wire_fixtures.rs`. — flagged tests
- **No dedicated integration suites** for `cluster/handler/{incremental_configs,leader_epoch,partitions,producer_id,configs}.rs` — covered only via cross-cutting tests.

---

## Cross-cutting themes

1. **Throughput cliff in the Raft storage layer.** C3 alone bounds metadata throughput to ~20 commits/s; combined with H1/H2 (handshake + per-peer head-of-line) and H4/H5 (per-produce Raft round-trip on lease miss + per-batch epoch GET) the broker has at least four ~order-of-magnitude levers before it scales to its design target. None of these are speculative — the line numbers are pinned, and #2 has explicit test coverage demonstrating the path.

2. **Observability lag undermines incident response.** Object-store health, raft step-down, lease loss, and fence violations all surface via 1-second polling loops or aggregated counters, not events. Combined with the fact that liveness is wired to `/health` (always 200), an incident that resolves in 5 s leaves no record. C4 makes this a hard outage detector failure, not just a UX issue.

3. **Documented invariants outpace shipped invariants.** KIP-320 fencing (C1), HMAC purpose-mixing (mux module headers), retry-with-backoff on raft RPC (mux_client docs), and the `Metrics` per-handler migration are all *documented as designed* but **not enforced in the code that runs in production**. Doc-vs-code drift on safety-relevant invariants is the most dangerous class of debt this codebase carries — it produces both false confidence and false positives in audit.

4. **Mid-migration ambiguity.** ~5,700 LoC under `#![allow(dead_code)]` with comments referencing types that no longer exist. Either land the migration or cordon it behind a feature flag — today, contributors cannot tell from the source what is live, what is shipped-but-unused, and what is half-wired.

5. **Config + metrics god-objects.** `config.rs` and `metrics.rs` together are ~5,800 LoC and concentrate change risk: every new tunable touches `config.rs` six times, every new metric touches one of 91 globals. The codebase already recognized both (`config_views`, `metrics_handle`) — the work to flip the ownership is incremental and unblocks the multi-tenancy/per-handler-isolation work.

---

## Recommended remediation order

### Tier 1 — production blockers (do before any non-toy load)

1. Enforce `current_leader_epoch` on Fetch + Produce (C1).
2. Fix the gap-then-DOA on transient SlateDB write error (C2).
3. Wire `MetricsObjectStore` into `track_object_store_health` (C4).
4. Cap `Vec::with_capacity` in `parse_nullable_string_array` (C5).
5. Survive `accept()` errors in the listener loop (HIGH — `server/mod.rs:437`).
6. Default `RAFT_LISTEN_ADDR` to `0.0.0.0:9093` (HIGH — `config.rs:1026`).
7. Remove `CHANGE-ME` placeholders + ANONYMOUS super-user from `deploy/kubernetes/`.

### Tier 2 — load-test blockers

8. Group-fsync the Raft `append_to_log` and FSM `last_applied` paths (C3).
9. Pool `MuxConnInner` for forwarded writes; multiplex per-peer (HIGH).
10. Eliminate per-batch `LEADER_EPOCH_KEY` get on the produce path (HIGH).
11. Pre-resolve metric `IntCounter` Arcs per `PartitionStore` (HIGH).
12. Wire `buffer_pool.rs` into `connection.rs` response encoders (HIGH).
13. Fan out `release_partition` close work; cap-check `acquire_partition` properly (HIGH/MEDIUM).

### Tier 3 — close the doc/architecture gap

14. Land or feature-flag the multi-Raft sharding work; remove blanket `#![allow(dead_code)]`; delete or implement the "step N" comments.
15. Make `SlateDBClusterHandler` generic on `C: ClusterCoordinator`; lift `AclOperation`/`AclResourceType`/`PartitionTransfer` out of `cluster::raft` into a domain module.
16. Split `metrics.rs` along its existing section banners; finish the `Metrics` handle migration so two brokers can coexist in one process.
17. Invert `config_views`: substructs become owners, flat `ClusterConfig` becomes a deprecated view. Reduces every-knob change cost.
18. Add `docs/` with three ADRs: object-store-as-truth durability model, partition-acquire/lease/fence state machine, multi-Raft sharding plan.

### Tier 4 — quality of life

19. CI clippy → `--all-targets`. Extract a `JoinGroupResponseData::error()` helper. Promote `MIN/MAX_SESSION_TIMEOUT_MS` into `kafkaesque-protocol::constants`. Replace 500 ms–3 s sleeps in the linearizability/distributed tests with `wait_until`. Add per-handler integration suites for `incremental_configs`, `leader_epoch`, `partitions`, `producer_id`, `configs`.
