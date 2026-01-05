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

- ApiVersions (v3)
- Metadata (v12)
- Produce (v9)
- Fetch (v13)
- ListOffsets (v7)
- OffsetCommit (v8)
- OffsetFetch (v8)
- FindCoordinator (v4)
- JoinGroup (v9)
- SyncGroup (v5)
- Heartbeat (v4)
- LeaveGroup (v5)
- DescribeGroups (v5)
- ListGroups (v4)
- DeleteGroups (v2)
- CreateTopics (v7)
- DeleteTopics (v6)
- SaslHandshake (v1)
- SaslAuthenticate (v2)
- InitProducerId (v4)

[Unreleased]: https://github.com/samdvr/kafkaesque/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/samdvr/kafkaesque/releases/tag/v0.1.0
