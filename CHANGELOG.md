# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- OffsetCommit (v0–v2)
- OffsetFetch (v0–v1)
- FindCoordinator (v0–v1)
- JoinGroup (v0–v2)
- Heartbeat (v0–v1)
- LeaveGroup (v0–v1)
- SyncGroup (v0–v1)
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
- **Transactions.** `transactional_id` is rejected at produce time.

[Unreleased]: https://github.com/samdvr/kafkaesque/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/samdvr/kafkaesque/releases/tag/v0.1.0
