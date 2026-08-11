# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Classic group API ceilings raised** (still short of flexible / static
  membership): FindCoordinator→2, Heartbeat/SyncGroup→3 (instance id
  parsed+ignored), LeaveGroup→3 (batch leave), JoinGroup→4 (KIP-394
  MemberIdRequired), OffsetCommit→6, OffsetFetch→5 (`committed_leader_epoch`
  always -1). JoinGroup v5+ and flexible group APIs remain refused.
- **Produce inner-record validation.** After decompress (or for
  uncompressed batches), the broker walks the records section and rejects
  batches whose header `records_count` does not match the walk
  (`InvalidRecord`).
- **Fetch session refuse path aligned** in P1 pins with
  `fetch_session_contract_tests` (`session_id≠0` → FetchSessionIdNotFound).
- **OffsetCommit advertised through v3** with a versioned response encoder.
  Throttle is emitted only for v3+; v0–v2 responses no longer prepend a
  spurious throttle that made negotiating clients mis-read the topics array.
- **OffsetFetch advertised through v3** (top-level `error_code` at v2,
  `throttle_time_ms` at v3). The encoder already handled these layouts; the
  broker now advertises them so Java/librdkafka negotiate matching framing.
- **Produce refuse-don't-lie at the RecordBatch layer.** CRC-valid batches
  with the transactional attribute bit, the control-batch bit, an undefined
  compression codec, or an undecompressible records section are rejected
  (`InvalidRequest` / `InvalidRecord` / `UnsupportedCompressionType` /
  `CorruptMessage`). `InvalidRecord` (87) added to `KafkaCode`.
- **Metrics isolation.** `Metrics::current()` / `scope_sync` / `scope_async`
  carry circuit-breaker state and connection counters. `RequestContext` and
  `KafkaServer` thread the handle; fencing circuit-breaker tests no longer
  need `#[serial]`.
- **Module carve.** `metrics`, `partition_store`, and `partition_manager`
  are directories modules (registration / circuit_breaker / raft / cardinality;
  append / fetch / builder; acquire / ownership / zombie / jitter).

### Added

- **Java `kafka-clients` smoke** (`java-smoke/` + `scripts/run-java-smoke.sh`)
  and required CI job `java-client-matrix` (Temurin 17 + Maven).
- librdkafka e2e: consumer-group OffsetCommit round-trip.
- Client compatibility matrix (README) + table lock tests + TCP wire tests
  (`client_compat_wire_tests`) asserting ApiVersions ceilings and JoinGroup
  v5 → UnsupportedVersion without closing the socket.
- `raft-chaos-nightly` CI job (schedule / workflow_dispatch): runs the
  ignored multi-node Raft linearizability test, scaled failover stress, and
  a longer loom preemption smoke.

## [0.2.0] - 2026-08-10

### Added

- **Sharded Raft metadata plane.** The single combined Raft group is replaced
  by a control group plus N metadata shard groups, all reachable on one
  TCP+HMAC port via a multiplexed frame (`Control` / `Shard(id)` /
  `JoinCluster` / `PromoteMember`). Shard count is pinned at bootstrap via
  `RAFT_METADATA_SHARDS`; routing uses a seed-pinned xxh3_64 hash that is part
  of the persisted cluster contract.
- **Multi-node cluster formation.** Brokers join an existing cluster with
  `RaftCoordinator::join_cluster`, which fans `JoinCluster` and
  `PromoteMember` across the control group and every shard. Voter-set changes
  are available internally via `change_membership_all_groups` (not exposed
  over any Kafka RPC).
- **Reconciler** propagating control-group decisions into shard state:
  partition seeding on topic create, purging on delete, and a per-shard
  broker-liveness shadow so the fencing gate on `AcquirePartition` /
  `RenewLease` runs locally without a control round-trip.
- **Replay-protected Raft frames.** Frames carry a timestamp and nonce, and
  each HMAC folds in the target group id, so a frame relabelled for another
  group fails authentication rather than merely failing to deserialize.
- **SCRAM-SHA-256 SASL**, alongside the existing PLAIN mechanism. SCRAM never
  puts the password (or anything it can be recovered from) on the wire.
  Per-connection mechanism commitment is recorded at handshake time so a
  client that negotiates SCRAM cannot then be authenticated as PLAIN.
- **ACL administration over the Kafka protocol**: `DescribeAcls`,
  `CreateAcls`, and `DeleteAcls`, with literal and prefixed resource-pattern
  types.
- **Config administration**: `DescribeConfigs`, `AlterConfigs`, and
  `IncrementalAlterConfigs`, plus a typed topic-config view that merges
  topic-level values over cluster-wide defaults and validates them at the
  write gate before anything is persisted.
- **`CreatePartitions`** for growing an existing topic's partition count.
- **`OffsetForLeaderEpoch`** (v0–v3) for consumer log-truncation detection.
- **Shared SlateDB resources**: one process-wide block cache
  (`SLATEDB_BLOCK_CACHE_BYTES`) and a dedicated compaction runtime
  (`SLATEDB_COMPACTION_WORKERS`) instead of per-database allocations.
- Consumer-group offset retention (`group_offset_retention_ms`) and a
  configurable Raft join timeout (`RAFT_JOIN_TIMEOUT_SECS`).

### Changed

- Wire-version coverage widened substantially: Produce v3–v9 (was v3),
  Fetch v4–v11 (was v4), and Metadata v0–v9 (was v0–v1). The v9 Metadata and
  v9 Produce paths use Kafka's flexible/compact encoding.
- Raft storage, network, and state-machine layers split into per-group
  modules (`state_machine/{control,shard}`, `mux{,_client,_server}`,
  `group`, `hash`) to support the sharded layout.
- Base images moved from Alpine 3.20 to Alpine 3.22 in `Dockerfile.minimal`
  and `Dockerfile.ci`. Alpine 3.20 reached end-of-support in May 2026, so it
  no longer receives package security fixes, and the `rust:*-alpine3.20`
  builder variant is no longer published for current Rust releases.
- `cleanup.policy=compact` (and `compact,delete`) is now **refused** with
  `INVALID_CONFIG` at the write gate. It previously parsed, validated, and
  persisted successfully, so a keyed topic looked compacted and grew forever.
  Compaction defaults are plumbed through config, but no cleaner exists yet.

### Fixed

- `Fetch` requests carrying `current_leader_epoch=0` are no longer fenced.

### Internal

- Clippy is clean under `-D warnings` across `--all-targets --all-features`.
- The CreateAcls v1 pattern-type test encoded mismatched string lengths, so it
  ran off the end of the payload instead of exercising the field it named.

### Supported Kafka APIs

Version ranges below are the versions the parsers and encoders actually
implement — they mirror `SUPPORTED_VERSIONS` in `src/server/versions.rs`,
which is what the broker advertises over `ApiVersions`. The
`changelog_api_versions_match_supported_versions` test in
`tests/changelog_contract_tests.rs` fails the build if this list drifts from
the code again.

- Produce (v3–v9)
- Fetch (v4–v11)
- ListOffsets (v0–v2)
- Metadata (v0–v9)
- OffsetCommit (v0–v6)
- OffsetFetch (v0–v5)
- FindCoordinator (v0–v2)
- JoinGroup (v0–v4)
- Heartbeat (v0–v3)
- LeaveGroup (v0–v3)
- SyncGroup (v0–v3)
- DescribeGroups (v0–v1)
- ListGroups (v0–v2)
- SaslHandshake (v0–v1)
- ApiVersions (v0–v3)
- CreateTopics (v0–v1)
- DeleteTopics (v0–v1)
- SaslAuthenticate (v0–v1)
- InitProducerId (v0–v4)
- DeleteGroups (v0–v1)
- DescribeConfigs (v0–v2)
- AlterConfigs (v0–v1)
- OffsetForLeaderEpoch (v0–v3)
- DescribeAcls (v0–v1)
- CreateAcls (v0–v1)
- DeleteAcls (v0–v1)
- CreatePartitions (v0–v1)
- IncrementalAlterConfigs (v0)

### Not implemented

Called out because the wire protocol makes them look available:

- **Replication.** Partitions are stored once in the object store and have a
  single live owner; durability comes from the bucket, not from peer brokers.
  `CreateTopics` rejects `replication_factor > 1` with
  `INVALID_REPLICATION_FACTOR` rather than accepting it and reporting a
  one-node ISR. `acks=all` is equivalent to `acks=1`.
- **Log compaction.** `cleanup.policy=compact` (and `compact,delete`) is
  rejected with `INVALID_CONFIG`; there is no cleaner, so accepting it would
  promise key-collapsing that never happens.
- **Incremental fetch sessions (KIP-227).** Fetch v7+ is supported for its
  other fields; responses always carry `session_id = 0`, the spec's
  sessionless path, and a `session_id` the broker never issued is answered
  with `FETCH_SESSION_ID_NOT_FOUND`.
- **Transactions.** `transactional_id` is rejected at produce time, and
  RecordBatch attribute bit 4 (transactional) / bit 5 (control) are
  refused on the produce path.

## [0.1.0] - 2026-01-01

### Added

- Initial release of Kafkaesque
- Kafka protocol server with full wire protocol compatibility
- SlateDB-backed cluster storage with object store support (S3, GCS, Azure, local)
- Embedded Raft consensus for distributed coordination
- Consumer group management with rebalancing
- Idempotent producer support with sequence number tracking
- Partition leasing with automatic failover
- Health check endpoints (`/health`, `/ready`, `/live`, `/metrics`)
- Prometheus metrics integration
- TLS support (optional feature: `tls`)
- SASL PLAIN authentication (optional feature: `sasl`)
- OpenTelemetry tracing (optional feature: `otel`)
- Kubernetes deployment manifests and Helm chart
- Terraform modules for AWS, GCP, and Azure
- Comprehensive test suite (38 test files)

[Unreleased]: https://github.com/samdvr/kafkaesque/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/samdvr/kafkaesque/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/samdvr/kafkaesque/releases/tag/v0.1.0
