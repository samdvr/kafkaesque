# Kafkaesque — Comprehensive Codebase Audit

**Date:** 2026-06-11
**Scope:** Full repository (~52K lines of Rust source + tests, CI/CD, deployment tooling)
**Subject:** Kafka-compatible broker backed by object storage (SlateDB) with openraft-based cluster coordination

---

## Executive Summary

Kafkaesque is an ambitious and, in many places, carefully engineered system. The fundamentals are stronger than typical for a young broker: big-endian key encodings are property-tested, record batches and producer state commit in a single atomic `WriteBatch`, `acks>=1` correctly awaits SlateDB durability, vote/log persistence happens before Raft ACKs, frame sizes are validated against a global inflight-memory budget, and SCRAM/PLAIN use constant-time comparisons with anti-enumeration dummies. CI is broad (clippy, fmt, audit, MSRV, cargo-deny, Trivy, e2e with real kcat).

That said, the audit found **~130 findings**, of which **8 are Critical** and **~25 are High**. They cluster into five systemic themes:

1. **Security wiring is incomplete.** ACL checks exist for produce/fetch/groups but are entirely absent for Metadata, ListOffsets, FindCoordinator, and InitProducerId — `cluster_operation_for_api()` is dead code. Raft RPC runs unauthenticated by default (`RAFT_CLUSTER_SECRET` optional, join auto-promotes to voter), and HMAC frames have no replay protection.
2. **Failover correctness has real gaps.** The fast failure detector is configured to expect heartbeats every 500 ms while actual heartbeats flow through Raft every 5 s — the production default will flag healthy brokers as failed. `MarkBrokerFailed` does not fence the broker or release its partitions atomically, batch transfers are not atomic, and lease expiry is driven by wall-clock timestamps.
3. **Protocol conformance bugs.** `parse_string` reads the length as *unsigned* 16-bit (Kafka spec: signed INT16); advertised API versions are not enforced before body parsing; OffsetCommit v0/v2 and JoinGroup v2 layouts are wrong; InitProducerId always responds with flexible encoding even to v0–v2 clients; the client's fetch `max_bytes`/`partition_max_bytes` are parsed and then ignored.
4. **Operational wiring gaps.** `ClusterConfig`'s SlateDB tuning (backpressure, flush interval, batch-index size) never reaches `PartitionStore` — operators believe they configured memory limits that are silently ignored. `configure_metrics()` is never called. Partition stores are leaked (no `close()`) on several ownership-loss paths. There is **no log retention** — object-store usage grows forever.
5. **Test fidelity.** Coverage volume is high but much of it asserts struct fields rather than behavior. The "distributed systems" and "linearizability" suites run against `MockCoordinator`, not real multi-node Raft. There are no ACL/SASL enforcement tests, no WAL crash-recovery test, and loom tests are not in CI.

**Risk assessment:** *Not production-ready for multi-broker deployments.* The single-broker data path (produce → SlateDB → fetch) is solid. The cluster coordination layer has split-brain windows, spurious-failover defaults, and missing fencing that will surface under real failure conditions. The security posture is acceptable only on a fully trusted network.

---

## Top Critical Issues (prioritized)

| # | Issue | Severity | Area |
|---|-------|----------|------|
| 1 | Failure detector expects 500 ms heartbeats but Raft delivers them every 5 s → spurious failovers in default production profile (`config.rs:809–811`, `partition_manager.rs:140`) | Critical | Reliability |
| 2 | `MarkBrokerFailed` neither fences the broker nor releases its partitions; failed broker stays `Active` and can keep writing (`raft/domains/transfer.rs:236–263`) | Critical | Consensus |
| 3 | ACL table never enforced for Metadata / ListOffsets / FindCoordinator / InitProducerId; `cluster_operation_for_api()` is dead code (`authorizer.rs:119`, only call site is its definition) | Critical | Security |
| 4 | `parse_string` reads STRING length as unsigned `be_u16` instead of signed INT16 — `0xFFFF` becomes 65 535 instead of null (`parser.rs:37–41`) | Critical | Protocol |
| 5 | Raft RPC accepts unauthenticated connections by default; `JoinCluster` auto-promotes to voter → full cluster takeover from network access (`raft/network.rs:885–891, 1111–1120`) | Critical | Security |
| 6 | No API-version enforcement before body parse — clients sending unadvertised versions get mis-parsed layouts, not `UnsupportedVersion` (`server/request/mod.rs:288–400`) | High | Protocol |
| 7 | `ClusterConfig` SlateDB backpressure/memory settings never wired into `PartitionStore` — OOM risk under object-store latency despite "configured" limits (`partition_manager.rs:1183–1196`) | High | Reliability |
| 8 | No log retention/truncation — object-store usage grows unbounded; `retention.ms` is inert (`partition_store.rs`, no delete API) | High | Architecture |
| 9 | Idempotency broken across restart and cache eviction: retry of last batch after restart returns `DuplicateSequence`; idle-producer eviction (15 min TTL) silently accepts duplicates (`partition_store.rs:1587–1628`) | High | Correctness |
| 10 | Fetch ignores client `max_bytes` / `partition_max_bytes` entirely (`handler/fetch.rs:250`, `partition_store.rs:806`) | High | Protocol |
| 11 | WAL recovery can resurrect purged/conflict-deleted Raft log entries after crash (best-effort disk deletes, no purge-floor filter on reload) (`raft/storage.rs:163–183, 695–713`) | High | Consensus |
| 12 | Partition stores leaked without `close()` on ownership-loss paths — SlateDB tasks/memtables/connections accumulate during rebalances (`partition_manager.rs:1028–1031, 1574–1580`) | High | Reliability |

---

## Detailed Findings

### 1. Correctness & Bugs

#### 1.1 `parse_string` uses unsigned length — **Critical**
`src/parser.rs:37–41`. Kafka `STRING` length is signed INT16; the encoder (`encode.rs:97–101`) writes `i16` but the parser reads `be_u16`. `0xFFFF` (= null for nullable contexts, invalid for non-nullable) is treated as length 65 535, enabling oversized reads, mis-aligned decoding of every name field, and encoder/parser asymmetry.
**Fix:** parse `be_i16`, reject negatives, share one `read_string_len`/`write_string_len` pair with the encoder. Existing unit tests mask this by building fixtures with `u16` lengths (`server/request/mod.rs:634–637`) — fix those too.

#### 1.2 Advertised API versions not enforced — **High**
`src/server/request/mod.rs:288–400`. `versions.rs` carefully constrains advertised ranges (Produce 3–3, Fetch 4–4, …) and a test keeps parsers and advertisements in sync — but nothing calls `is_version_supported()` at runtime. A client claiming `api_version=0` on Produce gets the v3 layout applied to v0 bytes. Parsers also ignore `_version` for layout (`produce.rs:35`, `fetch.rs:35`).
**Fix:** after header parse, reject unsupported versions with `KafkaCode::UnsupportedVersion` before body decode; version-gate fields as defense in depth.

#### 1.3 Per-API wire-format bugs — **High**
- **OffsetCommit v0** mis-parsed: reads `generation_id`/`member_id` unconditionally; v0 has neither (`request/offsets.rs:110–117`). v0 is advertised.
- **OffsetCommit v2** missing `retention_time_ms` (INT64) before topics — off-by-8 for the advertised max version.
- **JoinGroup v2** missing `group_instance_id` (NULLABLE_STRING) between `member_id` and `protocol_type` (`request/groups.rs:60–73`).
- **InitProducerId**: v3+ requests are parsed with non-flexible encoding (no compact strings, no tagged fields) (`request/admin.rs:110–128`), and *all* responses are encoded flexible — including to v0–v2 clients (`connection.rs:510–516`, `response/admin.rs:121–125`). Both directions break for some advertised version.
- **Metadata v1 `allow_auto_topic_creation`** and **CreateTopics v1 `validate_only`** are never parsed; trailing request bytes are silently discarded everywhere (`request/mod.rs:296`).

#### 1.4 `parse_record_count` trusts `last_offset_delta`, defaults to 1 — **High**
`src/protocol.rs:169–181`. Record count is derived from `last_offset_delta + 1`; the header's actual `records_count` field at byte 57 (documented in the same file) is never read or cross-checked, and truncated batches "count" as 1 record. A crafted batch with inconsistent fields corrupts offset accounting, HWM math, and idempotency sequence tracking (`ProducerBatchInfo::last_sequence`).
**Fix:** read `records_count`, validate it equals `last_offset_delta + 1`, reject mismatches and short batches with `CorruptMessage`.

#### 1.5 Idempotent-producer guarantees break across restart and eviction — **High**
`src/cluster/partition_store.rs:1587–1628`.
- The retry-dedup fields (`last_first_sequence`, `last_base_offset`) are not persisted; after a broker restart, an exact network retry of the last acked batch gets `DuplicateSequence` instead of success-with-original-offset (the test suite codifies this gap at `partition_store_tests.rs:880–896`).
- The moka producer-state cache uses `time_to_idle` (default 900 s). A producer idle longer than the TTL is evicted; only an eviction *warning* is logged, and a subsequent duplicate is **accepted as new** — an idempotence violation (duplicate records in the log).
**Fix:** persist the retry-dedup pair in the producer-state value; on cache miss for an idempotent batch, consult the persisted `p<producer_id>` key before accepting.

#### 1.6 Fetch ignores `max_bytes`, `partition_max_bytes`, and `isolation_level` — **High**
`src/cluster/handler/fetch.rs:250–270`, `partition_store.rs:806`. The values are parsed off the wire and dropped; the only cap is the broker-wide `max_fetch_response_size`. Multi-partition fetches can vastly exceed the client's budget (client-side `RecordTooLarge`/memory issues); `read_committed` consumers silently read uncommitted data (`last_stable_offset` always = HWM).
**Fix:** thread the request budgets into `fetch_from` with a shared per-request accounting; reject or document `isolation_level=1`.

#### 1.7 ListOffsets timestamp lookup silently wrong — **High**
`src/cluster/handler/offsets.rs:59–63`. Any non-sentinel timestamp returns the *latest* offset. Time-based consumer resets and Connect-style tooling silently start at the wrong position.
**Fix:** implement timestamp lookup (batch `max_timestamp` is in the header at bytes 35–42) or return an explicit per-partition error.

#### 1.8 Group-protocol semantic bugs — **High**
- **JoinGroup** returns the *requesting member's* metadata blob for every member (`handler/groups.rs:250–262`) — the leader's assignor computes assignments from wrong subscriptions.
- **SyncGroup** returns `error_code: None` even when assignment storage failed or was rejected (`handler/groups.rs:385–430`).
- **OffsetCommit** skips generation fencing entirely when `member_id` is empty (`handler/offsets.rs:138–141`) — stale consumers can clobber committed offsets.
- **LeaveGroup** always returns success (`handler/groups.rs:506–520`).
- Raft group domain hardcodes protocol `"range"` regardless of what was negotiated (`raft/coordinator/groups.rs:31–32`).

#### 1.9 Admin-API correctness — **High**
- CreateTopics maps `TopicAlreadyExists` to success (`raft/coordinator/partition.rs:407–409`).
- CreateTopics fires partition acquisitions with `let _ =` — failures ignored, topic exists with no leaders, clients discover via `LeaderNotAvailable` at produce time (`handler/admin.rs:96–114`).
- Metadata reports `controller_id = self.broker_id` on every broker (`handler/metadata.rs:148–152`).

#### 1.10 Silent error swallowing on storage iterators — **Medium**
`src/cluster/partition_store.rs:841, 961, 1023, 1087` — every scan loop is `while let Ok(Some(item)) = iter.next().await`. A mid-scan storage error is indistinguishable from end-of-data: fetches silently truncate, `earliest_offset()` silently returns 0, recovery warms a partial index. Combined with 1.6's `OffsetOutOfRange` check (`fetch.rs:237–248`), a transient store error in `earliest_offset` can bounce consumers.
**Fix:** `while let Some(item) = iter.next().await.transpose()? { … }` — propagate errors.

#### 1.11 Corrupt batch stops fetch iteration — **Medium**
`partition_store.rs:857–866`. One bad value blocks delivery of all subsequent valid batches forever; consumers stall with no error code.
**Fix:** return an explicit error (or skip-with-alert) rather than `break` into a silent partial response.

---

### 2. Performance

#### 2.1 Redundant full-batch CRC recompute on every produce — **High impact, trivial fix**
`src/protocol.rs:206–220` (`patch_base_offset`). The Kafka v2 batch CRC covers bytes 21+ only; `base_offset` (bytes 0–8) is **not** in CRC range — the code's own test asserts this (`protocol.rs:419` "Base offset is NOT covered by CRC … But we recalculate it anyway for safety"). The broker therefore does a *second* full-payload CRC pass per produce, with a byte-at-a-time software table (`crc32c`, `protocol.rs:58–65`), on top of the optional validation pass.
**Fix:** delete the recompute in `patch_base_offset` (≈50 % CRC cost reduction per produce immediately), and replace the software CRC with the `crc32c`/`crc32fast` crate (SSE4.2/ARMv8 hardware instructions, ~10–20× faster) for the validation path.

#### 2.2 `earliest_offset()` storage scan on every fetched partition, every pass — **High**
`handler/fetch.rs:206–219` calls `store.earliest_offset()` (a SlateDB range scan) for *every partition on every `collect_fetch` pass* — and the long-poll loop re-runs `collect_fetch` on every HWM notify. Combined with the broker-wide `hwm_advanced` notify (one `Notify` for all partitions, `handler/fetch.rs:85`), a produce to any partition wakes **all** long-polling fetchers, each re-scanning earliest offsets of all their partitions. This is a thundering-herd of object-store reads that scales with consumers × partitions.
**Fix:** cache log-start offset in `PartitionStore` (an `AtomicI64`, invalidated by future retention); make the notify per-partition or shard it; only re-fetch partitions whose HWM actually advanced.

#### 2.3 `find_batch_start` falls back to a scan from offset 0 — **High**
`partition_store.rs:950–984`. On a batch-index miss with a mid-batch offset (lagging/replaying consumer, cold index after restart — LRU keeps only recent entries), the code scans from offset 0 to HWM. O(partition size) object-store reads per fetch.
**Fix:** scan from a bounded back-off window before `fetch_offset` (batches have bounded record counts), or persist a sparse offset index.

#### 2.4 Full log double-scan on every partition open — **High**
`partition_recovery.rs:77–123` + `warm_batch_index` (`partition_store.rs:1006–1067`): every open scans the full record keyspace twice (HWM recovery + index warm). Failover/rebalance latency grows linearly with log size, exactly when latency matters most.
**Fix:** periodically checkpoint `_hwm` (it's currently only written at recovery, finding 1.5/`partition_store.rs:707–712`), bound the recovery scan to keys ≥ checkpoint, and merge the two scans.

#### 2.5 Per-produce write lock spans epoch read + full I/O — **Medium**
`partition_store.rs:377–662`. Every append holds the partition mutex across: a `db.get(LEADER_EPOCH_KEY)` read, buffer assembly, the SlateDB write (including `await_durable=true` for acks≥1), and metrics. Per-partition produce throughput is capped by full storage round-trip latency.
**Fix:** the epoch get is served from memtable in steady state (cheap) but the durable-write wait need not hold the offset-allocation lock; consider allocating offsets under the lock and pipelining durability waits, if the fencing model allows. At minimum, document the ceiling.

#### 2.6 Raft state-machine apply holds the outer write lock for whole batches — **Medium**
`raft/storage.rs:751–777`. All coordinator reads block during apply batches; `apply_command` already takes the inner state lock.
**Fix:** drop the outer write lock; serialize only `last_applied_log`.

#### 2.7 RPC client connection cache vs one-shot server — **Medium**
`raft/network.rs:699–718, 940–1082`. The server handles one frame per connection; the client caches connections. Every second RPC on a cached socket times out (10 s) then reconnects — latency doubling and circuit-breaker churn under load.
**Fix:** add a server-side read loop or disable client-side caching.

#### 2.8 Smaller hot-path items — **Low**
- `Error::ParsingError(data.clone())` retains the full (up to 100 MB) request in the error (`request/mod.rs:292`).
- `parse_producer_info` parsed twice per append (`partition_store.rs:478, 646`).
- `parse_array` clones `NomBytes` per call (`parser.rs:48`); `bytes_to_string` allocates per name.
- Buffer pool is thread-local; tokio task migration lowers hit rate (`buffer_pool.rs`).
- Fetch metrics record `msg_count = 1` regardless of actual records (`handler/fetch.rs:254`).

---

### 3. Code Quality & Maintainability

#### 3.1 Dual protocol stacks: `codec/` vs `Request::parse` — **Medium**
The `server/codec/` module (with its own per-API version ranges) is unused in production; dispatch goes through `Request::parse` + a ~280-line match in `connection.rs:241–530`. The two have already drifted: `ProduceCodec` claims v0–9, `versions.rs` says 3–3. `ApiKey::response_style()` is `#[allow(dead_code)]` while encoding styles are hand-picked per arm — which is precisely how the InitProducerId bug (1.3) happened.
**Fix:** pick one path. Drive dispatch from a table keyed by `(ApiKey, version)` using `response_style()` and `uses_flexible_encoding()`; delete or test-gate `codec/`.

#### 3.2 Duplicated plain/TLS connection loops — **Medium**
`ClientConnection` (~680–898) and `TlsClientConnection` (~981–1134) in `connection.rs` are near-duplicates that already diverge (try_read vs read_exact). Generic over `AsyncRead + AsyncWrite` would halve the surface.

#### 3.3 God modules — **Low**
`metrics.rs` (3 606 lines), `config.rs` (2 861), `partition_manager.rs` (2 532), `error.rs` (2 538), `partition_store.rs` (1 665). Each mixes several concerns (e.g., `PartitionStore` = storage + idempotency + fetch + recovery + fencing + metrics). Split along the natural seams; recovery is already half-extracted.

#### 3.4 Test ballast inflating confidence — **Medium**
Hundreds of in-module "tests" assert struct-literal fields (e.g., `handler/produce.rs:299–796`, `handler/fetch.rs:318–1057` — `assert_eq!(response.partition_index, 0)` after constructing the struct with `partition_index: 0`). They add line coverage, not behavioral coverage, and bury the real tests.

#### 3.5 Misc — **Low**
- Duplicate `DEFAULT_MAX_MESSAGE_SIZE` constants (`constants.rs:100` vs `connection.rs:34`).
- `Error::IoError(ErrorKind)` discards the underlying error everywhere.
- Inconsistent UTF-8 policy: `from_utf8_lossy` in groups/admin parsers vs strict elsewhere.
- `PartitionState::Acquiring`/`Releasing` defined but never used — partitions jump straight to `Owned`.
- `WriteGuard::append` docs promise durability options that don't exist (`partition_handle.rs:26, 232`).
- Epoch-mismatch detection at open works by string-matching `"Epoch mismatch"` and loses the stored epoch (`partition_store.rs:1551–1561`).

---

### 4. Architecture & Design

#### 4.1 No log retention or truncation — **High**
There is no delete/truncate API on `PartitionStore`; `earliest_offset()` always returns the first record ever written. `retention.ms` in topic config is inert. Object-store cost grows monotonically and the log-start-offset semantics the fetch path relies on can never engage.
**Fix:** retention task per owned partition: range-delete record keys below the cutoff, persist a log-start-offset key, surface it through `earliest_offset()`.

#### 4.2 Config plumbing is a recurring failure mode — **High**
Three independent instances of "knob exists, knob does nothing":
- SlateDB tuning + `batch_index_max_size` never passed to stores (`partition_manager.rs:1183–1196, 1498–1509`);
- `configure_metrics()` never called → `enable_partition_metrics` / cardinality caps inert (`metrics.rs:1330–1347`);
- the documented `set_global_inflight_byte_budget` doesn't exist (`connection.rs:39–49`).
**Fix:** wire all three; add a startup log of *effective* config; add an integration test that opens a store via the manager with non-default config and asserts it took effect.

#### 4.3 Transactions are accepted but not implemented — **High**
`transactional_id` is parsed on Produce and InitProducerId and then ignored; there is no transaction coordinator, yet clients aren't rejected. EOS-expecting producers run with silently weaker guarantees.
**Fix:** explicitly reject non-null `transactional_id` until the feature exists.

#### 4.4 Read-path consistency model is implicit — **Medium**
`owns_partition_for_read` consults a 1 s-TTL owner cache and then the local SM without `ensure_linearizable()` (`raft/coordinator/partition.rs:243–300`); the cache is only invalidated on the broker that performs a transfer, not on followers applying the same command. Post-failover, brokers can serve fetch/metadata for partitions they just lost.
**Fix:** invalidate the owner cache from an SM apply hook on all nodes; use read-index for routing-critical metadata.

#### 4.5 Mock/real coordinator divergence — **Medium**
`MockCoordinator` (2 209 lines) implements different rebalance semantics than the Raft group domain (e.g., generation bumps on every join, `mock_coordinator.rs:460–467`). Most "distributed" test suites bind to the mock, so they validate the mock.
**Fix:** contract-test both implementations against one shared behavioral suite.

#### 4.6 Health/readiness split — **Low**
Zombie state reaches `/ready` via a 200 ms polling mirror task (`bin/kafkaesque.rs:187–193`) instead of sharing the `Arc<ZombieModeState>`; `/health` ignores zombie mode entirely; `/metrics` is unauthenticated on whatever interface health binds to.

---

### 5. Concurrency & Reliability

#### 5.1 Failure-detector cadence mismatch — **Critical**
`FailureDetectorConfig.heartbeat_interval` is set from `fast_heartbeat_interval_ms` (500 ms default; `partition_manager.rs:140`), but heartbeats are recorded only when `BrokerCommand::Heartbeat` entries are applied through Raft — and the heartbeat loop sends at `config.heartbeat_interval` = **5 s** (`partition_manager.rs:320–324`, `config.rs:807–811`). With `failure_threshold = 5`, failure is declared after ~2.5 s of "silence" that is in fact normal cadence. The production default oscillates brokers Healthy→Failed, triggering `handle_broker_failure` churn.
**Fix:** feed the detector the real send interval (or implement an actual 500 ms fast-heartbeat channel); validate at startup that `failure_threshold × detector_interval > heartbeat_interval + jitter`.

#### 5.2 Failure handling is not atomic and does not fence — **Critical**
`MarkBrokerFailed` only inserts into a `failed_brokers` map (`raft/domains/transfer.rs:236–263`); the broker remains `Active`, can renew leases and acquire partitions (`raft/domains/partition.rs:222–275` never checks broker status). Batch transfers are per-partition best-effort (`transfer.rs:266–358`): partial failure leaves mixed ownership, and "owned by an unexpected third broker" is silently skipped. Every broker runs the failover loop concurrently; if the first initiator crashes after marking, others see `BrokerAlreadyFailed` and skip the transfer — orphaned partitions.
**Fix:** one Raft command that fences + releases + marks atomically; leader-only failover initiation; persist transfer-completion state so retries resume.

#### 5.3 Lease lifecycle trusts wall clocks — **High**
Lease grant/renew/expiry compare client-supplied or local `timestamp_ms` (`raft/domains/partition.rs:332–343`); skew tolerance is applied only on the sweep side. Leader clock skew can mass-expire leases or extend split-brain. The `ExpireLeases` sweep is also issued from every broker and stalls under partitions.
**Fix:** derive all lease timestamps from the Raft leader's clock at apply time; leader-only sweeper.

#### 5.4 Raft storage durability edges — **High**
- Crash recovery reloads every `log/*.bin` without filtering against `last_purged_log_id`; purge/conflict deletes are best-effort `let _ = remove_file` (`raft/storage.rs:163–183, 695–713`) → resurrection of purged/forked entries violates openraft invariants.
- `install_snapshot` mutates the in-memory SM *before* persisting (`storage.rs:801–823`) → crash window regresses state.
- The S3 "atomic rename" fallback is copy+delete — readers can observe meta-without-data mid-crash (`storage.rs:563–615`).
- `restore()` panics on corrupt snapshot bytes (`state_machine.rs:181–185`).

#### 5.5 Zombie-mode reopen skips epoch fencing — **High**
The zombie-recovery reopen path omits `.leader_epoch(...)` (`partition_manager.rs:1406–1417`), unlike normal acquisition (1498–1508). The store opens with epoch validation disabled and inherits whatever epoch is stored — a broker that should be fenced passes per-write epoch checks. SlateDB single-writer fencing becomes the only guard.

#### 5.6 Server lifecycle gaps — **High**
- Graceful shutdown stops the accept loop but never cancels in-flight connections (`server/mod.rs:288–297`); the binary then waits 25 s and shrugs (`bin/kafkaesque.rs:316–323`).
- Handler timeout abandons but does not abort the handler future's work pattern — and parse/auth/timeout failures all close the TCP connection without a Kafka error response carrying the correlation ID (`connection.rs:701–728`), so clients see resets instead of typed errors.
- TLS handshake has no timeout while holding a connection slot (`server/mod.rs:644–671`) — trivial slot-exhaustion DoS.
- Connection-limit checks are check-then-act races (`server/mod.rs:315–363`).

#### 5.7 Fail-closed fencing classification — **Medium**
Unknown SlateDB error strings are classified as fencing (`error.rs:651–724`), so transient object-store glitches can trigger mass partition release / zombie mode. The circuit breaker dampens but does not eliminate churn. Also `is_not_leader()` omits `EpochMismatch` even though `to_kafka_code()` maps it to `NotLeaderForPartition` (`error.rs:289 vs 328–335`).

#### 5.8 SASL/auth state hygiene — **Medium**
- `sasl_post_auth` / `scram_sessions` are keyed by `SocketAddr` and never cleaned on disconnect (`handler/mod.rs:783–814`, `sasl_provider.rs:60–66`) — unbounded growth from abandoned handshakes; NAT address reuse can collide sessions.
- Pre-auth protocol-order violations are counted as auth *failures*, feeding the per-IP accept-time rate limiter (`connection.rs:278–293`) — a misconfigured client can get its NAT blocked.
- `SASL_REQUIRED=true` without `SASL_ENABLED=true` passes validation and bricks all clients (`config.rs:1454–1460`).
- SCRAM server nonces use `fastrand` (non-CSPRNG) (`scram.rs:302–310`).
- PLAIN's `require_tls` flag exists but is never enforced (`sasl.rs:167–169`).
- HMAC frames carry no nonce/timestamp → replayable within a term; the config comment claims replay defense that doesn't exist (`raft/auth.rs:186–254`, `raft/config.rs:105–107`).

---

### 6. Testing & Observability

#### 6.1 Test-fidelity gaps — **High**
- `distributed_systems_tests.rs` and `linearizability_tests.rs` exercise `MockCoordinator`/in-memory mocks, not multi-node Raft.
- No test for the failure-detector/heartbeat cadence (would have caught 5.1).
- No WAL crash-recovery test (kill between purge and disk delete; would have caught 5.4).
- No ACL or SASL enforcement integration tests (would have caught 1.3-security findings).
- No idempotent-producer wire-path E2E (InitProducerId → produce → dup retry → epoch bump).
- No long-poll test with `min_bytes > 1`; no `append_batch_durable` test; no recovery-gap (`fail_on_recovery_gap=true`) integration test; no fenced-broker re-acquire test.
- Crash-recovery tests call `flush()` before "crashing" — the actual WAL-loss window is untested.
- Loom tests exist but are not in CI; `sasl` feature is not compiled in default CI test jobs (`ci.yml:51–80`).
- Pervasive sleep-based timing in integration tests → flake risk.

#### 6.2 Fuzzing — **Medium**
One fuzz target (`fuzz_targets/request_parse.rs`) covering `Request::parse` only. No frame-wrapper, parser-primitive (varint/compact/tagged-fields), RecordBatch-header, or response-roundtrip targets. Given findings 1.1–1.4, a structured `(api_key, version, arbitrary body)` target would be high-yield.

#### 6.3 Metrics — **Medium**
- `configure_metrics()` never invoked: cardinality caps and `enable_partition_metrics` are dead config (`metrics.rs:1330–1347`).
- `produce_duration_seconds` / `fetch_duration_seconds` take raw topic labels with no overflow guard (`metrics.rs:1533–1551`) — auto-created topics explode Prometheus cardinality. `CONSUMER_LAG` and `SEQUENCE_HIGH_WATERMARK` similar (`(group,topic,partition)`, `(topic,partition,producer_id)`).
- The rich span helpers in `observability.rs:107–227` are unused by the actual hot paths.
- acks=0 drops (zombie, semaphore backpressure) are log-only — add a dropped-records counter (`handler/produce.rs:121–131, 270–272`).

#### 6.4 CI/CD — **Low–Medium**
Strong overall. Gaps: actions not SHA-pinned (`trivy-action@master`), `rust.yml` partially redundant with `ci.yml`, unbounded integration-test parallelism (port/static collisions), e2e script hardcodes `/app/target`, `Makefile` lacks `deny`/`audit`/`test` targets mirroring CI.

---

## Quick Wins (high impact, low effort)

1. **Delete the CRC recompute in `patch_base_offset`** (`protocol.rs:216–219`) — base offset is provably outside CRC range; halves per-produce CRC cost. Then swap the software CRC for the `crc32c` crate. *(~30 min)*
2. **Enforce `is_version_supported()` in `Request::parse`** and return `UnsupportedVersion` — closes the whole class of mis-parse bugs while per-version layouts are fixed. *(~1 h)*
3. **Fix `parse_string` to signed INT16** + fix `u16` test fixtures. *(~1 h)*
4. **Pass `.leader_epoch(...)` on the zombie-reopen path** (`partition_manager.rs:1406`). *(~30 min)*
5. **Wire `configure_metrics()` at handler init**; add topic-label overflow guards to the latency histograms. *(~1 h)*
6. **Fail startup when `SASL_REQUIRED && !SASL_ENABLED`**, and when `RAFT_CLUSTER_SECRET` is unset outside dev profiles. *(~1 h)*
7. **Set the failure detector interval from the real heartbeat interval** + add the startup invariant check (`failure_threshold × interval > heartbeat_interval`). *(~1 h, defuses the spurious-failover default)*
8. **Propagate scan errors** (`while let Ok(Some(_))` → `transpose()?`) in `fetch_from`, `find_batch_start`, `earliest_offset`, `warm_batch_index`. *(~1 h)*
9. **Cache `earliest_offset` in an `AtomicI64`** on `PartitionStore` — removes a storage scan per partition per fetch pass. *(~1 h)*
10. **SCRAM nonces from OS RNG**; add `EpochMismatch` to `is_not_leader()`; TLS handshake timeout; SHA-pin GitHub Actions; add `cargo test --all-features` CI job. *(~half day total)*
11. **Reject non-null `transactional_id`** on Produce/InitProducerId until transactions exist. *(~1 h)*
12. **Close partition stores on all ownership-loss paths** — extract the lease-renewal cleanup into a `close_and_remove(key)` helper and use it in `verify_ownership` and friends. *(~2 h)*

---

## Long-Term Recommendations

1. **Failover correctness overhaul.** Make broker failure a single atomic Raft transition (fence → release → mark), initiated only by the Raft leader, with persisted transfer progress and explicit incomplete-failover state. Drive all lease timestamps from leader time. Then build the test harness to prove it: 3-node real-Raft integration suites for leader kill, partition, concurrent failover, fenced re-acquire; a Jepsen-style linearizability check on ownership metadata.
2. **Protocol layer consolidation.** One table-driven dispatch path keyed by `(ApiKey, version)` that owns: version admission, request layout, response header style, and body encoding. Delete the parallel `codec/` stack or make it the single implementation. Expand fuzzing to structured per-API-version targets and response roundtrips.
3. **Security completion.** Enforce `cluster_operation_for_api` in dispatch; ACL-gate Metadata/ListOffsets/InitProducerId; learner-only Raft join with explicit promotion; HMAC nonce/replay-cache; mTLS for Raft RPC in production profiles; secure-by-default startup validation (refuse `acl_enabled=false && sasl_required=false` in the production profile). Add a deny/allow ACL test matrix to CI.
4. **Storage lifecycle.** Implement retention (time/size-based range deletes + persisted log-start offset), periodic `_hwm` checkpointing to bound recovery scans, and ListOffsets timestamp lookup. These three together fix unbounded growth, slow failover, and time-based consumer resets — the biggest operational gaps in the data plane.
5. **Idempotency hardening.** Persist retry-dedup state; consult persisted producer state on cache miss instead of evict-and-forget; add a wire-level idempotent-producer E2E suite (duplicate, out-of-order, epoch bump, broker restart).
6. **Observability as wired, not written.** Attach the existing span helpers to produce/fetch/ownership/Raft paths; counters for every silent-drop path (acks=0 drops, skipped transfers, swallowed deletes); make `/ready` read zombie state directly; document `/health` vs `/ready` semantics for load balancers.
7. **Test-suite restructuring.** Move struct-assertion tests out of handler modules; build one behavioral contract suite run against both `MockCoordinator` and the Raft coordinator; add loom and `--all-features` jobs to CI; replace sleeps with readiness polls.

---

## Finding Counts

| Category | Critical | High | Medium | Low |
|----------|----------|------|--------|-----|
| Protocol/wire | 1 | 8 | 8 | 12 |
| Server/connection | 0 | 4 | 9 | 11 |
| Storage/partition | 0 | 7 | 10 | 6 |
| Raft/coordination | 2 | 7 | 12 | 5 |
| Handlers/security | 4 | 8 | 12 | 8 |
| Testing/CI/observability | 0 | 3 | 9 | 6 |
| **Total** | **7** | **37** | **60** | **48** |

*Counts reflect deduplicated findings; several issues span categories and are counted once at their primary location.*
