# Kafkaesque

[![CI](https://github.com/samdvr/kafkaesque/actions/workflows/ci.yml/badge.svg)](https://github.com/samdvr/kafkaesque/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A cloud-native, Rust-based Kafka-compatible broker backed by object storage
(S3, GCS, Azure) with embedded Raft consensus.

Kafkaesque speaks the Kafka wire protocol, so standard Kafka clients work
without modification. It uses object storage for durability and embedded
Raft for coordination — no Zookeeper, no external metadata service, no
local-disk reliability requirement.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Kafka Client   │     │  Kafka Client   │     │  Kafka Client   │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Kafkaesque Broker(s)  │
                    └────────────┬────────────┘
                                 │
                   ┌─────────────┼─────────────┐
                   │             │             │
         ┌─────────▼──────┐  ┌───▼────────┐  ┌▼──────────────┐
         │  Raft Layer    │  │ SlateDB    │  │ Object Store  │
         │ (Metadata &    │  │ (LSM, hot  │  │ (S3, GCS,     │
         │  Coordination) │  │  data)     │  │  Azure)       │
         └────────────────┘  └────────────┘  └───────────────┘
```

**Key features:**
- **Kafka compatible** — works with librdkafka, Java, sarama, kcat, …
- **Object-store native** — durability comes from S3/GCS/Azure; brokers are
  effectively stateless.
- **Embedded Raft** — no Zookeeper. Brokers coordinate leader election and
  metadata over an authenticated internal Raft channel.
- **Single binary** — zero external dependencies at runtime.
- **LSM-tree storage** — built on [SlateDB](https://slatedb.io/) for
  efficient writes against object stores.

## Quick Start

### Run locally (standalone)

```bash
CLUSTER_PROFILE=development cargo run --release --bin kafkaesque
```

The broker listens on `localhost:9092`. Data is stored under
`/tmp/kafkaesque-data`.

`CLUSTER_PROFILE=development` opts into an unauthenticated Raft control
plane for local use. Outside the development profile the broker refuses to
start unless `RAFT_CLUSTER_SECRET` is set — see [Security](#security).

### Run with Docker

```bash
docker run --rm -p 9092:9092 -p 8080:8080 \
  -e CLUSTER_PROFILE=development \
  -e OBJECT_STORE_TYPE=local \
  -e DATA_PATH=/data \
  -v /tmp/kafkaesque-data:/data \
  kafkaesque:latest
```

### Smoke test with `kcat`

```bash
echo "hello" | kcat -P -b localhost:9092 -t demo
kcat -C -b localhost:9092 -t demo -e
```

## Configuration

Configuration is handled entirely via environment variables. Common knobs:

| Category | Variable | Default | Description |
|----------|----------|---------|-------------|
| **Core** | `BROKER_ID` | `0` | Unique broker ID; must be persistent across restarts. |
| | `HOST` | `0.0.0.0` | Bind address for the Kafka listener. |
| | `PORT` | `9092` | Kafka protocol port. |
| | `CLUSTER_ID` | `kafkaesque` | Logical cluster identifier. |
| **Storage** | `OBJECT_STORE_TYPE` | `local` | `local`, `s3`, `gcs`, or `azure`. |
| | `DATA_PATH` | `/tmp/kafkaesque-data` | Path or object-store prefix for data. |
| **Raft** | `RAFT_PEERS` | (empty) | Peer list, e.g. `0=host:port,1=host:port`. |
| | `RAFT_LISTEN_ADDR` | `127.0.0.1:9093` | Internal Raft RPC address. |
| **Security** | `CLUSTER_PROFILE` | `production` | Deployment profile. Only `development` permits running without Raft auth. |
| | `RAFT_CLUSTER_SECRET` | *(required outside dev)* | Shared HMAC key authenticating all Raft RPC traffic. Identical on every node. |
| **S3** | `S3_BUCKET` | | AWS S3 bucket name. |
| | `AWS_REGION` | | AWS region (e.g. `us-east-1`). |
| **Telemetry** | `LOG_FORMAT` | `pretty` | `pretty` or `json` (for log aggregators). |
| | `RUST_LOG` | `info` | Standard `tracing-subscriber` filter. |
| | `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP collector endpoint. Requires `--features otel`. |
| | `OTEL_SERVICE_NAME` | `kafkaesque-kafka-broker` | Service name attached to exported spans. |

See `src/config.rs` for the full set.

## Security

The Raft RPC port accepts cluster-membership changes and coordination
commands, so it must never be exposed unauthenticated. The broker is
secure by default: startup fails unless `RAFT_CLUSTER_SECRET` is set to a
non-empty value, or you explicitly opt out with
`CLUSTER_PROFILE=development` for local work.

```bash
# Generate once, distribute the same value to every node:
export RAFT_CLUSTER_SECRET="$(openssl rand -base64 32)"
```

For the public Kafka listener, enable SASL/PLAIN and TLS at compile time:

```bash
cargo build --release --features sasl,tls
```

## Deployment

### Multi-broker cluster

For a 3-node cluster, give each broker a unique `BROKER_ID` and the full
`RAFT_PEERS` list:

```bash
BROKER_ID=0 \
RAFT_LISTEN_ADDR=0.0.0.0:9093 \
RAFT_PEERS="0=node0:9093,1=node1:9093,2=node2:9093" \
RAFT_CLUSTER_SECRET="$SHARED_SECRET" \
OBJECT_STORE_TYPE=s3 \
S3_BUCKET=my-kafka-data \
cargo run --release --bin kafkaesque
```

### Kubernetes

Manifests are in `deploy/kubernetes/`:

```bash
kubectl apply -k deploy/kubernetes/
```

This deploys a 3-broker StatefulSet.

## Cargo features

| Feature | Pulls in | Purpose |
|---|---|---|
| `sasl` | (no extra deps) | SASL/PLAIN and SCRAM-SHA-256 authentication. |
| `tls` | `tokio-rustls`, `rustls`, `rustls-pemfile` | TLS termination on the Kafka listener. |
| `otel` | `opentelemetry`, `opentelemetry-otlp`, `tracing-opentelemetry` | OTLP span export over gRPC. |
| `loom` | `loom` | Loom-based concurrency tests (dev only). |
| `test-utilities` | (no extra deps) | Exposes `MockCoordinator` and other helpers for integration tests. |

## Supported APIs

Kafkaesque implements the core Kafka protocol used by modern clients:

- **Produce / Fetch** — basic messaging.
- **Metadata** — topic and partition discovery.
- **Consumer groups** — `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`,
  `OffsetCommit`, `OffsetFetch`, `DescribeGroups`, `ListGroups`,
  `DeleteGroups`, `FindCoordinator`.
- **Admin** — `CreateTopics`, `DeleteTopics`, `InitProducerId`.
- **API negotiation** — `ApiVersions`.
- **Auth** — `SaslHandshake`, `SaslAuthenticate` (PLAIN, SCRAM-SHA-256;
  requires `--features sasl`).

Not yet supported: transactions, log compaction, idempotent-producer
deduplication beyond the per-session `producer_id`.

## Testing

```bash
# Unit + integration tests (mirrors CI's split):
cargo test --lib
cargo test --tests

# Doc tests:
cargo test --doc

# End-to-end (requires kcat):
./scripts/run-e2e.sh             # single-node
./scripts/run-cluster-e2e.sh     # 3-node, simulates failover
./scripts/run-k8s-e2e.sh         # in-cluster

# All CI checks (fmt, clippy, tests, audit, deny):
make ci
```

## Fuzzing

The `fuzz/` directory holds [`cargo-fuzz`](https://rust-fuzz.github.io/book/cargo-fuzz.html)
targets covering every attacker-reachable parser, decoder, and validator —
Kafka request parsers, the connection frame reader, RecordBatch header
validation, SCRAM, postcard decoders for cluster RPC, and the identifier
validators.

```bash
cargo install cargo-fuzz
cargo +nightly fuzz list
cargo +nightly fuzz run api_produce -- -max_total_time=60
```

See [`fuzz/README.md`](fuzz/README.md) for the full target catalogue,
reproduction workflow, and corpus / coverage management.

## Development

- **Format**: `cargo fmt --all`
- **Lint**: `cargo clippy --all-targets -- -D warnings`
- **Benchmarks**: `cargo bench` (Criterion; results in `target/criterion/`)
- **Audit**: `make audit` (RustSec advisories)
- **Deny**: `make deny` (license / banned-crate policy in `deny.toml`)

## License

Apache-2.0
