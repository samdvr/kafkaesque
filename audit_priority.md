# Kafkaesque — Remaining Work by Priority

Distilled from `audit.md` after the 2026-06-10 P0 hardening pass. Items already marked FIXED in the audit (or addressed in this pass) are not repeated here. Each entry has a one-line "why", a "what to do", and the audit cross-reference so the original analysis is one search away.

## P0 status (2026-06-10 pass)

| Item | Status | Notes |
|---|---|---|
| P0-1 — Replace `bincode` with postcard | **FIXED** | All direct `bincode::serialize/deserialize` call sites swapped to `postcard::to_stdvec` / `postcard::from_bytes`. `bincode` removed from direct deps; lingers transitively via foyer/madsim/slatedb. `cargo audit` confirms no live CVEs from our usage. |
| P0-2 — CVE upgrades (bytes, rustls, quinn, time, rkyv) | **FIXED** | Cargo.toml pinned to versions that close the original advisories: `bytes=1.11.1`, `tokio-rustls=0.26.4`, `rustls=0.23.40`, `chrono=0.4.45`, `slatedb=0.10.1`, `object_store=0.12.5`. `cargo audit` reports 0 vulnerabilities. |
| P0-3 — SCRAM-SHA-256 SASL | **FIXED** | `cluster::scram` implements PBKDF2-HMAC-SHA256, `client-first` / `server-first` / `client-final` / `server-final`. Per-connection session map on `SaslProvider` keyed by `SocketAddr`. Mechanism sniffed from wire bytes; `Handler::take_sasl_post_auth` lets the dispatcher distinguish intermediate from final success so the gate doesn't open after the first SCRAM round-trip. PLAIN remains for backwards compat. |
| P0-4 — TLS for Kafka listener via Helm | **FIXED** | `tls.enabled` / `tls.certSecretName` / `tls.mountPath` added to `deploy/helm/fall/values.yaml`; StatefulSet mounts the secret read-only and wires `TLS_CERT_PATH` / `TLS_KEY_PATH` env vars. `ClusterConfig` reads them; `cluster.rs` validates at startup and refuses if the binary lacks `--features tls`. `examples/cluster.rs` now branches on `config.tls_enabled`: it builds `TlsConfig::from_pem_files` and constructs `TlsKafkaServer::with_config` instead of the plain server. `TlsKafkaServer` gained a `handler()` accessor (mirroring `KafkaServer`) and applies the SASL gate inside its accept loop so the drain/handler-shutdown path is identical for both transports. |
| P0-5 — mTLS on Raft port | **FIXED** | `cluster::raft::tls::RaftTlsConfig` provides the cert/CA loading + acceptor + connector; loads from `RAFT_TLS_CERT` / `RAFT_TLS_KEY` / `RAFT_TLS_CA` env vars. `RaftConfig.tls` carries it. Helm chart adds `raftTls.*` values + secret mount + env wiring. Network layer now uses `MaybeTlsStreamServer` / `MaybeTlsStreamClient` enums with `AsyncRead`/`AsyncWrite` delegation; `RaftRpcServer::with_tls` calls `acceptor.accept` after `listener.accept` and `RaftNetworkConnection` (plus `forward_client_write_with_term` and `request_cluster_join`) routes through a `connect_raft` helper that calls `connector.connect` when `config.tls.is_some()`. Connection cache changed from `Option<TcpStream>` to `Option<MaybeTlsStreamClient>` so a reused socket keeps its TLS session. HMAC framing remains in place on top. |
| P0-6 — Test infra (TempDir for WAL) | **FIXED** | `test_config` in both raft test files now allocates a per-test `tempfile::tempdir().keep()` for `raft_log_dir` / `snapshot_dir`. Full integration suite (38 tests) passes deterministically. |
| P0-7 — Fetch long-poll | **FIXED** | `SlateDBClusterHandler.hwm_advanced` is a broker-wide `tokio::sync::Notify` that producers fire on every successful append (acks=0 and acks≥1 paths both notify). `handle_fetch` builds the response, then loops on the notify with a deadline of `max_wait_ms` until total bytes ≥ `min_bytes`. |

Verification: `cargo test --lib` → 1825 passed (default), 1869 passed (`--features sasl`); `cargo test --test raft_integration_tests` → 38 passed; `cargo test --test raft_failure_mode_tests` → 18 passed; `cargo audit` → 0 vulnerabilities, 4 unmaintained warnings (3 transitive: bincode/paste/atomic-polyfill; 1 direct: rustls-pemfile, no replacement available).

The remaining work below is unchanged from the previous priority list — the P0 section is gone, all items either landed or are noted in the status table above.

Priority bands:
- **P0 (launch blocker)** — security or correctness; ship without these and an incident is likely. *Resolved; see status table.*
- **P1 (high)** — significant operational risk; will hurt under real traffic but not on day one.
- **P2 (medium)** — known sharp edges; work around with docs or operator vigilance.
- **P3 (low)** — polish, code-quality, or paper cuts.

---

## P1 — fix in the first hardening cycle

### P1-1. Per-API authz for consumer-group + offset APIs
Audit ref: **S2 follow-up** (admin + data plane done; group/offset APIs still allow anyone).
Why: `JoinGroup`, `Heartbeat`, `LeaveGroup`, `SyncGroup`, `OffsetCommit`, `OffsetFetch`, `DescribeGroups`, `ListGroups`, `DeleteGroups` currently bypass the new `Authorizer`. A non-super-user can hijack consumer offsets even when ACLs are enabled.
Do: add `AclResourceType::Group` checks (`Read` for fetch/heartbeat, `Read` for offset commit per Kafka, `Delete` for delete-groups, `Describe` for list/describe). Pattern after `produce.rs` / `fetch.rs`.

### P1-2. Kafka admin RPCs for ACL management (CreateAcls / DescribeAcls / DeleteAcls)
Audit ref: **S2 follow-up**.
Why: today ACLs can only be populated via the bootstrap JSON file or programmatic API — not via standard Kafka admin clients. Operators expect `kafka-acls.sh` to work.
Do: add API keys 29/30/31 to the parser/handler/versions tables; wrap `RaftCoordinator::{create,delete,describe}_acls` for the wire types. Test with `kafka-acls.sh --bootstrap-server …`.

### P1-3. Retention and compaction
Audit ref: **§4 data integrity** ("unbounded cost bomb").
Why: partitions grow forever; the only deletion mechanism is `DeleteTopics`. Real workloads either run out of object-store budget or trip retention compliance limits.
Do:
- Time-based retention: background task scans each partition store, deletes records older than `retention.ms` (per-topic config).
- Size-based retention: same, but capped at `retention.bytes`.
- Compaction (key-based): out-of-scope for first cut; document as future work.
Wire into `PartitionStore::open` so per-topic config applies on first reference.

### P1-4. Wire observability metrics + JSON logs
Audit ref: **§7 infra** ("observability is a façade in the hot path").
Why: `PRODUCE_DURATION`, `FETCH_DURATION`, all `OBJECT_STORE_*` metrics are defined but never recorded outside tests; `LOG_FORMAT=json` (set in the ConfigMap!) silently falls back to pretty-print because the `json` feature isn't compiled in.
Do:
- Build the release image with `tracing-subscriber` `json` feature on.
- Record the existing histograms at `produce_to_partition`, `fetch_from`, every `object_store::ObjectStore` call. Use the `observability::span_*` helpers (also currently unimported).
- Add the missing clock-skew metric the config docs reference.

### P1-5. CI E2E gate that runs the shipped artifact
Audit ref: **§7 infra** ("CI never exercises the system it ships").
Why: scripts (`scripts/run-e2e.sh`, `scripts/run-cluster-e2e.sh`) and `Dockerfile.ci` already exist but no workflow runs them. CI today says nothing about whether produce-then-consume actually works.
Do: GitHub Actions job that builds the release image (`Dockerfile`, not `Dockerfile.ci`), boots a 3-node cluster via `docker-compose`, runs the existing E2E scripts. Cache the build layer.

### P1-6. Pin GitHub Actions by commit SHA + add `cargo-deny` + image scanning
Audit ref: **S11 full**.
Why: action-by-tag means a compromised tag swap (`v3` → `v3.0.1` malicious) lands in the next CI run. `cargo-deny` catches license / advisory drift; image scanning catches base-image CVEs.
Do:
- Replace every `uses: foo/bar@v3` with `uses: foo/bar@<full-40-char-sha>`. Renovate-style automation can keep them current.
- Add `cargo-deny check` to the CI matrix; configure `deny.toml` to fail on advisories.
- Add Trivy or Grype against the built image.

### P1-7. Wire fast failover (`RebalanceCoordinator::record_heartbeat`)
Audit ref: **§3 reliability**.
Why: documented ~2.5 s failover is actually ~60 s lease-TTL expiry because the production heartbeat path doesn't feed the failure detector. The promise on the box is wrong.
Do: in the leader's heartbeat-apply path, call `record_heartbeat` for each broker. Add an integration test that asserts wall-clock failover < 5 s after `kill -STOP`.

### P1-8. Dead-letter the unwired `PartitionStorePool` decision
Audit ref: **§4 data integrity** ("pool unwired" + double-open race).
Why: `max_open_partition_stores: 1000` is dead config today; a broker owning many partitions opens unbounded SlateDB instances. Memory/S3 connections grow with partition count, no ceiling.
Do: pick one — wire the pool in (preferred) or delete it. If wired in, fix the check-then-act race (use `dashmap::Entry::or_insert_with_async` or similar) and call `close()` on eviction.

### P1-9. Validate offset-commit against group state
Audit ref: **§3 reliability** ("zombie consumer from generation N-1 can clobber offsets").
Why: `CommitOffset` accepts any caller — no member/generation/state check. Silent message skip or reprocessing after a rebalance.
Do: in `domains/group.rs::CommitOffset`, reject if `member_id` isn't in the current generation, or if the group isn't `Stable`. Return `IllegalGeneration` / `UnknownMemberId`.

### P1-10. SIGTERM preStop sleep + readiness shed for ungraceful drain
Audit ref: **B6 follow-up**, **§7 infra**.
Why: B6's SIGTERM handler drains and flushes correctly, but K8s endpoint-controller takes ~30 s to remove the pod from the service list. Without preStop sleep, in-flight clients see TCP RSTs.
Do: add `preStop: { exec: { command: ["sleep", "30"] } }` to the StatefulSet (raw + Helm) and bump `terminationGracePeriodSeconds` to 60. Verify by `kubectl rollout` with a producer running.

### P1-11. Adopt the existing retry framework
Audit ref: **§3 reliability** ("retry framework is dead code").
Why: `src/cluster/retry.rs` defines jittered policies but `partition_store.rs` and `raft/network.rs` use ad-hoc 3-attempt/10ms retries. Under an S3 brownout, every partition retries in lockstep — retry storm with no global circuit breaker on the object-store client.
Do: replace bespoke retries with `retry::with_*_policy`. Add a shared object-store circuit breaker keyed on bucket + region.

### P1-12. Reverse-slowloris on response writes
Audit ref: **§3 reliability**.
Why: `connection.rs:624-661` has no write timeout. A client that never reads pins a connection slot forever; 10k such clients exhaust the connection limit.
Do: wrap `write_all` in `timeout(DEFAULT_REQUEST_WRITE_TIMEOUT, ...)`; add an idle-timeout disconnect for inactivity > 5 min.

### P1-13. Bound spawned tasks
Audit ref: **§3 reliability**.
Why: per-Kafka-connection, per-Raft-connection, per-health-probe spawn `tokio::spawn` without tracking. A panic before cleanup permanently leaks a connection-count slot. Existing `BackgroundTaskRegistry` is one of the dead subsystems.
Do: route every persistent spawn through a shared registry that tracks join handles and panics. Make `spawn_essential` actually mean something.

### P1-14. Helm chart directory rename + path consistency
Audit ref: **§7 infra**.
Why: chart lives at `deploy/helm/fall/` while every README points at `deploy/helm/kafkaesque/` (and Terraform at `../helm/kafkaesque`). Day-one operator friction.
Do: rename the directory, update README + Terraform paths, update CI references.

### P1-15. ADVERTISED_HOST verification post-fix
Audit ref: **F-K8S-02 follow-up**.
Why: the FIXED entry sets `ADVERTISED_HOST` correctly, but the only verification was a code review. The K8s deployment may still have edge cases (bare-metal pods without DNS, custom `dnsConfig`, etc.).
Do: add an E2E test that boots the StatefulSet, runs a producer from a sidecar pod, verifies that broker addresses returned in `Metadata` are reachable.

---

## P2 — clean up before second-customer scale

### P2-1. Document and clamp Kafka API version handling
Audit ref: **§6 API** ("version handling is unsound").
Why: Fetch/Produce parsers read v4+ fields unconditionally regardless of negotiated version. Older clients get a `ParsingError` and connection close.
Do: audit `src/server/versions.rs` against actual parser/encoder support version-by-version. Either branch on version inside the parsers, or clamp ApiVersions advertisements to the truly-implemented set.

### P2-2. Recovery blind spots
Audit ref: **§4 data integrity**.
- Empty log + stale `_hwm` passes validation → undetected truncation.
- Overlapping batches only `warn!`.
- `fail_on_recovery_gap` defaults `false` → real gaps log and continue.
Do: invariant: empty log means HWM == 0. Promote overlapping-batch warnings to errors. Flip `fail_on_recovery_gap` default to `true`; expose an opt-out for legacy data.

### P2-3. Fetch correctness gaps
Audit ref: **§4 data integrity**.
- `-1` / `-2` sentinel offsets pass handler validation but `fetch_from` returns empty for all negatives; `auto.offset.reset=latest` consumers hang.
- No `OffsetOutOfRange` for pre-log-start fetches.
Do: handle `-1` (latest) and `-2` (earliest) at the handler level by querying HWM and log-start; return `OffsetOutOfRange` for `< log_start_offset`.

### P2-4. Fix `warm_batch_index` direction
Audit ref: **§4 data integrity**.
Why: cache currently scans from offset 0 (oldest), but tail reads are the hot pattern → cache is anti-optimized.
Do: scan from HWM backwards.

### P2-5. Clock-skew tolerance for leases / member expiry
Audit ref: **§3 reliability**.
Why: lease/membership expiry uses proposer wall-clock `timestamp_ms`; a leader clock jump mass-expires leases/members.
Do: leader-term-scoped time, or generous skew tolerances + a `clock_skew_ms` metric (the audit notes this metric is documented but doesn't exist).

### P2-6. CRC-32C off the tokio worker
Audit ref: **§5 performance**.
Why: synchronous CRC on async runtime; up to 100 MB/batch can collapse p99 under concurrent large produces.
Do: `spawn_blocking` for batches above a threshold (~64 KiB), or a hardware-CRC crate.

### P2-7. Zero-copy request buffer + global inflight-bytes limit
Audit ref: **§5 performance**.
Why: request buffer cloned 2-3× and frame allocation pre-validation has no global memory budget. Coordinated slow-frame attack or a burst exhausts memory.
Do: True `Bytes` slicing through the parser; global `Semaphore` measured in inflight bytes; reject when over.

### P2-8. `cluster_id` documentation + warnings
Audit ref: **S7 follow-up** (FIXED but UX rough).
Why: empty `cluster_id` warns loudly but still runs. Two clusters pointed at the same bucket with the same id will still corrupt each other.
Do: refuse to start when `cluster_id` is empty in any non-`local` object-store backend.

### P2-9. Bounded concurrency on `create_topics`
Audit ref: **§5 performance**.
Why: partitions are acquired serially; a 1000-partition topic = minutes-long admin call.
Do: `buffer_unordered` with `max_concurrent_partition_writes`.

### P2-10. Prometheus group-label cardinality GC
Audit ref: **§5 performance**.
Why: per-group labels never garbage-collected; ephemeral group IDs blow up the registry.
Do: remove series on group delete. Cap cardinality per metric (drop new labels above a ceiling).

### P2-11. Plumb `max_message_size` config through to wire cap
Audit ref: **§5 performance**.
Why: `DEFAULT_MAX_MESSAGE_SIZE` is hardcoded in `connection.rs` and not tied to `ClusterConfig.max_message_size`. Operators can't lower the cap.
Do: thread the config value through `KafkaServer::new`, default to the existing 100 MB constant only when unset.

### P2-12. `block_on(Handle::current())` inside `spawn_blocking`
Audit ref: **§3 reliability**.
Why: store opens at `partition_store.rs:1395-1403` re-enter the originating runtime; under saturation can stall workers.
Do: refactor to async-only opens; if a sync API is required, use a dedicated blocking pool.

### P2-13. `LoadMetricsCollector` unbounded growth
Audit ref: **§3 reliability**.
Why: DashMap grows to 100k entries; `clear_partition` never called on release.
Do: hook `clear_partition` into `release_partition`.

### P2-14. Remove dead `Draining` state machine
Audit ref: **§4 data integrity**, **§8 code quality**.
Why: `start_draining_partition` hard-closes immediately; `PartitionState::Draining` paths are unreachable. Dead code in a safety-critical region is a future foot-gun.
Do: delete the unreachable arm or wire it in. Don't leave it half-implemented.

### P2-15. On-disk format-version key
Audit ref: **§4 data integrity** ("first format change requires ad-hoc migration tooling").
Why: no migrations exist; no on-disk format-version key is written. Adding one now is free; later it requires forensic work.
Do: write `format_version: 1` at partition open if absent. Future code branches on it.

### P2-16. DR runbook + reconciliation tool for metadata-vs-data divergence
Audit ref: **§7 infra**.
Why: nothing documents restoring from S3, rebuilding Raft membership after total loss, or reconciling "metadata snapshot lost, partition data intact" — Raft state was unrecoverable by design (now fixed by B1).
Do: write the runbook. Build a small CLI that lists partitions in object storage and reconciles ownership against Raft metadata.

---

## P3 — polish

### P3-1. Encoder string truncation
Audit ref: **§6 API**.
Why: `src/encode.rs:59-69` casts string lengths via `as i16` — a > 32 KB string (e.g. a giant error message) silently corrupts the response.
Do: return an error when the string overflows the wire field; clamp at the source.

### P3-2. Define explicit `SlateDBError → KafkaCode` table
Audit ref: **§6 API**.
Why: the duplicate-sequence and fencing paths surface codes that drive clients into wrong recovery behavior.
Do: write the table; test it; replace ad-hoc mapping calls with one helper.

### P3-3. Consolidate `OwnershipGuard`
Audit ref: **§8 code quality**.
Why: documented as "THE source of truth" — used only in tests. Real ownership checking is split across multiple paths.
Do: delete the dead struct or wire it in as the single check; remove the duplicated code.

### P3-4. Promote `examples/cluster.rs` to `src/bin/`
Audit ref: **§8 code quality**.
Why: the deployed binary is an `examples/` crate. K8s manifests exec `/app/cluster`. Lifecycle (signals, profile config, health wiring) is not in the library, complicating reuse.
Do: move to `src/bin/kafkaesque.rs`. Update Dockerfile.

### P3-5. Single source of truth for Kafka API versions
Audit ref: **§8 code quality**.
Why: `versions.rs` declares max versions, parsers/encoders implement varying levels. Drift goes silent.
Do: generate the parser/encoder version-coverage table at compile time; assert agreement with `versions.rs` in a unit test.

### P3-6. Delete dead subsystems or finish wiring them
Audit ref: **§8 code quality** ("the dominant pathology is built-but-not-wired").
Targets: `OwnershipGuard`, `PartitionStorePool` (or P1-8), `BackgroundTaskRegistry` (or P1-13), `retry.rs` policies (or P1-11), legacy `group_state_machine.rs`, the `spawn_essential` flag.
Do: per item, either wire it into the live path or delete it.

### P3-7. `WriteGuard` hardcoded lease threshold
Audit ref: **§8 code quality**.
Why: `partition_handle.rs:300-320` ignores per-store config.
Do: read from the store's config.

### P3-8. Multi-arch images
Audit ref: **§7 infra**.
Why: x86_64-only release image. Apple Silicon developers and arm64 fleets are second-class.
Do: `docker buildx` with `linux/amd64,linux/arm64` in the release workflow.

### P3-9. Property tests + fuzzing
Audit ref: **§8 code quality**.
Why: ~750 tests is impressive but no fuzz targets and no proptest. The wire-protocol parser is the obvious fuzzer target; key encoding and the recovery scan are obvious proptest targets.
Do: `cargo-fuzz` against `Request::parse`. `proptest` on `keys.rs`, `partition_recovery.rs`, and the idempotent-producer state machine.

### P3-10. Replace pervasive `sleep()`-based test synchronization
Audit ref: **§8 code quality**.
Why: chaos / linearizability / fencing suites use `tokio::time::sleep` for synchronization → flake under CI load.
Do: replace with `Notify` / `oneshot::Receiver` / `tokio::time::pause` where possible.

---

## Out of scope for "remaining" — explicit non-goals

These are choices the audit recommends *not* fixing pre-launch:
- Full SCRAM-SHA-512 (SHA-256 is enough for first launch).
- `cluster_id` in object-store paths (FIXED via `PrefixStore`; no further change needed).
- Replication / multi-broker partitions (object storage replicates underneath).

---

## P1 — fix in the first hardening cycle

### P1-1. Per-API authz for consumer-group + offset APIs
Audit ref: **S2 follow-up** (admin + data plane done; group/offset APIs still allow anyone).
Why: `JoinGroup`, `Heartbeat`, `LeaveGroup`, `SyncGroup`, `OffsetCommit`, `OffsetFetch`, `DescribeGroups`, `ListGroups`, `DeleteGroups` currently bypass the new `Authorizer`. A non-super-user can hijack consumer offsets even when ACLs are enabled.
Do: add `AclResourceType::Group` checks (`Read` for fetch/heartbeat, `Read` for offset commit per Kafka, `Delete` for delete-groups, `Describe` for list/describe). Pattern after `produce.rs` / `fetch.rs`.

### P1-2. Kafka admin RPCs for ACL management (CreateAcls / DescribeAcls / DeleteAcls)
Audit ref: **S2 follow-up**.
Why: today ACLs can only be populated via the bootstrap JSON file or programmatic API — not via standard Kafka admin clients. Operators expect `kafka-acls.sh` to work.
Do: add API keys 29/30/31 to the parser/handler/versions tables; wrap `RaftCoordinator::{create,delete,describe}_acls` for the wire types. Test with `kafka-acls.sh --bootstrap-server …`.

### P1-3. Retention and compaction
Audit ref: **§4 data integrity** ("unbounded cost bomb").
Why: partitions grow forever; the only deletion mechanism is `DeleteTopics`. Real workloads either run out of object-store budget or trip retention compliance limits.
Do:
- Time-based retention: background task scans each partition store, deletes records older than `retention.ms` (per-topic config).
- Size-based retention: same, but capped at `retention.bytes`.
- Compaction (key-based): out-of-scope for first cut; document as future work.
Wire into `PartitionStore::open` so per-topic config applies on first reference.

### P1-4. Wire observability metrics + JSON logs
Audit ref: **§7 infra** ("observability is a façade in the hot path").
Why: `PRODUCE_DURATION`, `FETCH_DURATION`, all `OBJECT_STORE_*` metrics are defined but never recorded outside tests; `LOG_FORMAT=json` (set in the ConfigMap!) silently falls back to pretty-print because the `json` feature isn't compiled in.
Do:
- Build the release image with `tracing-subscriber` `json` feature on.
- Record the existing histograms at `produce_to_partition`, `fetch_from`, every `object_store::ObjectStore` call. Use the `observability::span_*` helpers (also currently unimported).
- Add the missing clock-skew metric the config docs reference.

### P1-5. CI E2E gate that runs the shipped artifact
Audit ref: **§7 infra** ("CI never exercises the system it ships").
Why: scripts (`scripts/run-e2e.sh`, `scripts/run-cluster-e2e.sh`) and `Dockerfile.ci` already exist but no workflow runs them. CI today says nothing about whether produce-then-consume actually works.
Do: GitHub Actions job that builds the release image (`Dockerfile`, not `Dockerfile.ci`), boots a 3-node cluster via `docker-compose`, runs the existing E2E scripts. Cache the build layer.

### P1-6. Pin GitHub Actions by commit SHA + add `cargo-deny` + image scanning
Audit ref: **S11 full**.
Why: action-by-tag means a compromised tag swap (`v3` → `v3.0.1` malicious) lands in the next CI run. `cargo-deny` catches license / advisory drift; image scanning catches base-image CVEs.
Do:
- Replace every `uses: foo/bar@v3` with `uses: foo/bar@<full-40-char-sha>`. Renovate-style automation can keep them current.
- Add `cargo-deny check` to the CI matrix; configure `deny.toml` to fail on advisories.
- Add Trivy or Grype against the built image.

### P1-7. Wire fast failover (`RebalanceCoordinator::record_heartbeat`)
Audit ref: **§3 reliability**.
Why: documented ~2.5 s failover is actually ~60 s lease-TTL expiry because the production heartbeat path doesn't feed the failure detector. The promise on the box is wrong.
Do: in the leader's heartbeat-apply path, call `record_heartbeat` for each broker. Add an integration test that asserts wall-clock failover < 5 s after `kill -STOP`.

### P1-8. Dead-letter the unwired `PartitionStorePool` decision
Audit ref: **§4 data integrity** ("pool unwired" + double-open race).
Why: `max_open_partition_stores: 1000` is dead config today; a broker owning many partitions opens unbounded SlateDB instances. Memory/S3 connections grow with partition count, no ceiling.
Do: pick one — wire the pool in (preferred) or delete it. If wired in, fix the check-then-act race (use `dashmap::Entry::or_insert_with_async` or similar) and call `close()` on eviction.

### P1-9. Validate offset-commit against group state
Audit ref: **§3 reliability** ("zombie consumer from generation N-1 can clobber offsets").
Why: `CommitOffset` accepts any caller — no member/generation/state check. Silent message skip or reprocessing after a rebalance.
Do: in `domains/group.rs::CommitOffset`, reject if `member_id` isn't in the current generation, or if the group isn't `Stable`. Return `IllegalGeneration` / `UnknownMemberId`.

### P1-10. SIGTERM preStop sleep + readiness shed for ungraceful drain
Audit ref: **B6 follow-up**, **§7 infra**.
Why: B6's SIGTERM handler drains and flushes correctly, but K8s endpoint-controller takes ~30 s to remove the pod from the service list. Without preStop sleep, in-flight clients see TCP RSTs.
Do: add `preStop: { exec: { command: ["sleep", "30"] } }` to the StatefulSet (raw + Helm) and bump `terminationGracePeriodSeconds` to 60. Verify by `kubectl rollout` with a producer running.

### P1-11. Adopt the existing retry framework
Audit ref: **§3 reliability** ("retry framework is dead code").
Why: `src/cluster/retry.rs` defines jittered policies but `partition_store.rs` and `raft/network.rs` use ad-hoc 3-attempt/10ms retries. Under an S3 brownout, every partition retries in lockstep — retry storm with no global circuit breaker on the object-store client.
Do: replace bespoke retries with `retry::with_*_policy`. Add a shared object-store circuit breaker keyed on bucket + region.

### P1-12. Reverse-slowloris on response writes
Audit ref: **§3 reliability**.
Why: `connection.rs:624-661` has no write timeout. A client that never reads pins a connection slot forever; 10k such clients exhaust the connection limit.
Do: wrap `write_all` in `timeout(DEFAULT_REQUEST_WRITE_TIMEOUT, ...)`; add an idle-timeout disconnect for inactivity > 5 min.

### P1-13. Bound spawned tasks
Audit ref: **§3 reliability**.
Why: per-Kafka-connection, per-Raft-connection, per-health-probe spawn `tokio::spawn` without tracking. A panic before cleanup permanently leaks a connection-count slot. Existing `BackgroundTaskRegistry` is one of the dead subsystems.
Do: route every persistent spawn through a shared registry that tracks join handles and panics. Make `spawn_essential` actually mean something.

### P1-14. Helm chart directory rename + path consistency
Audit ref: **§7 infra**.
Why: chart lives at `deploy/helm/fall/` while every README points at `deploy/helm/kafkaesque/` (and Terraform at `../helm/kafkaesque`). Day-one operator friction.
Do: rename the directory, update README + Terraform paths, update CI references.

### P1-15. ADVERTISED_HOST verification post-fix
Audit ref: **F-K8S-02 follow-up**.
Why: the FIXED entry sets `ADVERTISED_HOST` correctly, but the only verification was a code review. The K8s deployment may still have edge cases (bare-metal pods without DNS, custom `dnsConfig`, etc.).
Do: add an E2E test that boots the StatefulSet, runs a producer from a sidecar pod, verifies that broker addresses returned in `Metadata` are reachable.

---

## P2 — clean up before second-customer scale

### P2-1. Document and clamp Kafka API version handling
Audit ref: **§6 API** ("version handling is unsound").
Why: Fetch/Produce parsers read v4+ fields unconditionally regardless of negotiated version. Older clients get a `ParsingError` and connection close.
Do: audit `src/server/versions.rs` against actual parser/encoder support version-by-version. Either branch on version inside the parsers, or clamp ApiVersions advertisements to the truly-implemented set.

### P2-2. Recovery blind spots
Audit ref: **§4 data integrity**.
- Empty log + stale `_hwm` passes validation → undetected truncation.
- Overlapping batches only `warn!`.
- `fail_on_recovery_gap` defaults `false` → real gaps log and continue.
Do: invariant: empty log means HWM == 0. Promote overlapping-batch warnings to errors. Flip `fail_on_recovery_gap` default to `true`; expose an opt-out for legacy data.

### P2-3. Fetch correctness gaps
Audit ref: **§4 data integrity**.
- `-1` / `-2` sentinel offsets pass handler validation but `fetch_from` returns empty for all negatives; `auto.offset.reset=latest` consumers hang.
- No `OffsetOutOfRange` for pre-log-start fetches.
Do: handle `-1` (latest) and `-2` (earliest) at the handler level by querying HWM and log-start; return `OffsetOutOfRange` for `< log_start_offset`.

### P2-4. Fix `warm_batch_index` direction
Audit ref: **§4 data integrity**.
Why: cache currently scans from offset 0 (oldest), but tail reads are the hot pattern → cache is anti-optimized.
Do: scan from HWM backwards.

### P2-5. Clock-skew tolerance for leases / member expiry
Audit ref: **§3 reliability**.
Why: lease/membership expiry uses proposer wall-clock `timestamp_ms`; a leader clock jump mass-expires leases/members.
Do: leader-term-scoped time, or generous skew tolerances + a `clock_skew_ms` metric (the audit notes this metric is documented but doesn't exist).

### P2-6. CRC-32C off the tokio worker
Audit ref: **§5 performance**.
Why: synchronous CRC on async runtime; up to 100 MB/batch can collapse p99 under concurrent large produces.
Do: `spawn_blocking` for batches above a threshold (~64 KiB), or a hardware-CRC crate.

### P2-7. Zero-copy request buffer + global inflight-bytes limit
Audit ref: **§5 performance**.
Why: request buffer cloned 2-3× and frame allocation pre-validation has no global memory budget. Coordinated slow-frame attack or a burst exhausts memory.
Do: True `Bytes` slicing through the parser; global `Semaphore` measured in inflight bytes; reject when over.

### P2-8. `cluster_id` documentation + warnings
Audit ref: **S7 follow-up** (FIXED but UX rough).
Why: empty `cluster_id` warns loudly but still runs. Two clusters pointed at the same bucket with the same id will still corrupt each other.
Do: refuse to start when `cluster_id` is empty in any non-`local` object-store backend.

### P2-9. Bounded concurrency on `create_topics`
Audit ref: **§5 performance**.
Why: partitions are acquired serially; a 1000-partition topic = minutes-long admin call.
Do: `buffer_unordered` with `max_concurrent_partition_writes`.

### P2-10. Prometheus group-label cardinality GC
Audit ref: **§5 performance**.
Why: per-group labels never garbage-collected; ephemeral group IDs blow up the registry.
Do: remove series on group delete. Cap cardinality per metric (drop new labels above a ceiling).

### P2-11. Plumb `max_message_size` config through to wire cap
Audit ref: **§5 performance**.
Why: `DEFAULT_MAX_MESSAGE_SIZE` is hardcoded in `connection.rs` and not tied to `ClusterConfig.max_message_size`. Operators can't lower the cap.
Do: thread the config value through `KafkaServer::new`, default to the existing 100 MB constant only when unset.

### P2-12. `block_on(Handle::current())` inside `spawn_blocking`
Audit ref: **§3 reliability**.
Why: store opens at `partition_store.rs:1395-1403` re-enter the originating runtime; under saturation can stall workers.
Do: refactor to async-only opens; if a sync API is required, use a dedicated blocking pool.

### P2-13. `LoadMetricsCollector` unbounded growth
Audit ref: **§3 reliability**.
Why: DashMap grows to 100k entries; `clear_partition` never called on release.
Do: hook `clear_partition` into `release_partition`.

### P2-14. Remove dead `Draining` state machine
Audit ref: **§4 data integrity**, **§8 code quality**.
Why: `start_draining_partition` hard-closes immediately; `PartitionState::Draining` paths are unreachable. Dead code in a safety-critical region is a future foot-gun.
Do: delete the unreachable arm or wire it in. Don't leave it half-implemented.

### P2-15. On-disk format-version key
Audit ref: **§4 data integrity** ("first format change requires ad-hoc migration tooling").
Why: no migrations exist; no on-disk format-version key is written. Adding one now is free; later it requires forensic work.
Do: write `format_version: 1` at partition open if absent. Future code branches on it.

### P2-16. DR runbook + reconciliation tool for metadata-vs-data divergence
Audit ref: **§7 infra**.
Why: nothing documents restoring from S3, rebuilding Raft membership after total loss, or reconciling "metadata snapshot lost, partition data intact" — Raft state was unrecoverable by design (now fixed by B1).
Do: write the runbook. Build a small CLI that lists partitions in object storage and reconciles ownership against Raft metadata.

---

## P3 — polish

### P3-1. Encoder string truncation
Audit ref: **§6 API**.
Why: `src/encode.rs:59-69` casts string lengths via `as i16` — a > 32 KB string (e.g. a giant error message) silently corrupts the response.
Do: return an error when the string overflows the wire field; clamp at the source.

### P3-2. Define explicit `SlateDBError → KafkaCode` table
Audit ref: **§6 API**.
Why: the duplicate-sequence and fencing paths surface codes that drive clients into wrong recovery behavior.
Do: write the table; test it; replace ad-hoc mapping calls with one helper.

### P3-3. Consolidate `OwnershipGuard`
Audit ref: **§8 code quality**.
Why: documented as "THE source of truth" — used only in tests. Real ownership checking is split across multiple paths.
Do: delete the dead struct or wire it in as the single check; remove the duplicated code.

### P3-4. Promote `examples/cluster.rs` to `src/bin/`
Audit ref: **§8 code quality**.
Why: the deployed binary is an `examples/` crate. K8s manifests exec `/app/cluster`. Lifecycle (signals, profile config, health wiring) is not in the library, complicating reuse.
Do: move to `src/bin/kafkaesque.rs`. Update Dockerfile.

### P3-5. Single source of truth for Kafka API versions
Audit ref: **§8 code quality**.
Why: `versions.rs` declares max versions, parsers/encoders implement varying levels. Drift goes silent.
Do: generate the parser/encoder version-coverage table at compile time; assert agreement with `versions.rs` in a unit test.

### P3-6. Delete dead subsystems or finish wiring them
Audit ref: **§8 code quality** ("the dominant pathology is built-but-not-wired").
Targets: `OwnershipGuard`, `PartitionStorePool` (or P1-8), `BackgroundTaskRegistry` (or P1-13), `retry.rs` policies (or P1-11), legacy `group_state_machine.rs`, the `spawn_essential` flag.
Do: per item, either wire it into the live path or delete it.

### P3-7. `WriteGuard` hardcoded lease threshold
Audit ref: **§8 code quality**.
Why: `partition_handle.rs:300-320` ignores per-store config.
Do: read from the store's config.

### P3-8. Multi-arch images
Audit ref: **§7 infra**.
Why: x86_64-only release image. Apple Silicon developers and arm64 fleets are second-class.
Do: `docker buildx` with `linux/amd64,linux/arm64` in the release workflow.

### P3-9. Property tests + fuzzing
Audit ref: **§8 code quality**.
Why: ~750 tests is impressive but no fuzz targets and no proptest. The wire-protocol parser is the obvious fuzzer target; key encoding and the recovery scan are obvious proptest targets.
Do: `cargo-fuzz` against `Request::parse`. `proptest` on `keys.rs`, `partition_recovery.rs`, and the idempotent-producer state machine.

### P3-10. Replace pervasive `sleep()`-based test synchronization
Audit ref: **§8 code quality**.
Why: chaos / linearizability / fencing suites use `tokio::time::sleep` for synchronization → flake under CI load.
Do: replace with `Notify` / `oneshot::Receiver` / `tokio::time::pause` where possible.

---

## Out of scope for "remaining" — explicit non-goals

These are choices the audit recommends *not* fixing pre-launch:
- Full SCRAM-SHA-512 (SHA-256 is enough for first launch).
- `cluster_id` in object-store paths (FIXED via `PrefixStore`; no further change needed).
- Replication / multi-broker partitions (object storage replicates underneath).
