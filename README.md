# Kafkaesque

[![CI](https://github.com/samdvr/kafkaesque/actions/workflows/ci.yml/badge.svg)](https://github.com/samdvr/kafkaesque/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A cloud-native, Rust-based Kafka-compatible broker backed by object storage
(S3, GCS, Azure) with embedded Raft consensus.

Kafkaesque speaks the Kafka wire protocol, so standard Kafka clients work
without modification. It uses object storage for durability and embedded
Raft for coordination — no Zookeeper, no external metadata service, no
local-disk reliability requirement.

## Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Workspace layout](#workspace-layout)
- [Configuration](#configuration)
- [Health & monitoring](#health--monitoring)
- [Durability contract](#durability-contract)
- [Security](#security)
- [Deployment](#deployment)
- [Cargo features](#cargo-features)
- [Supported APIs](#supported-apis)
- [Testing](#testing)
- [Fuzzing](#fuzzing)
- [Development](#development)
- [License](#license)

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

Building from source needs the Rust toolchain pinned in
[`rust-toolchain.toml`](rust-toolchain.toml) (Rust 1.89, edition 2024).
`rustup` picks it up automatically. The pre-built Docker image below needs
only Docker.

### Run locally (standalone)

```bash
CLUSTER_PROFILE=development cargo run --release -p kafkaesque-bin --bin kafkaesque
```

The broker listens on `localhost:9092` for Kafka traffic and `localhost:8080`
for health and metrics. Data is stored under `/tmp/kafkaesque-data`.

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

## Workspace layout

The repository is a Cargo workspace with three crates:

| Crate | Purpose |
|---|---|
| `kafkaesque-protocol` (in `crates/kafkaesque-protocol/`) | Runtime-independent Kafka wire-protocol layer — parser, encoder, types, constants, wire-error. No tokio/slatedb/openraft, so it can be reused on its own. |
| `kafkaesque` (root crate) | The umbrella: cluster coordination, server, runtime, telemetry. Re-exports the protocol layer at the same module paths it used to live at, so existing `use kafkaesque::{parser, encode, types, ...}` paths keep working. |
| `kafkaesque-bin` (in `crates/kafkaesque-bin/`) | The production broker binary. Build via `cargo build -p kafkaesque-bin --bin kafkaesque`. |

`MockCoordinator` and other test helpers live behind the
`test-utilities` feature on the umbrella crate


## Configuration

Configuration is handled entirely via environment variables. Common knobs:

| Category | Variable | Default | Description |
|----------|----------|---------|-------------|
| **Core** | `BROKER_ID` | `0` | Unique broker ID; must be persistent across restarts. |
| | `HOST` | `0.0.0.0` | Bind address for the Kafka listener. |
| | `PORT` | `9092` | Kafka protocol port. |
| | `ADVERTISED_HOST` | (value of `HOST`) | Hostname returned to clients in `Metadata` responses. Set this when the broker is reachable at a different address than it binds to (NAT, load balancer, Kubernetes). |
| | `CLUSTER_ID` | `kafkaesque` | Logical cluster identifier. |
| **Storage** | `OBJECT_STORE_TYPE` | `local` | `local`, `s3`, `gcs`, or `azure`. |
| | `DATA_PATH` | `/tmp/kafkaesque-data` | Path or object-store prefix for data. |
| **Raft** | `RAFT_PEERS` | (empty) | Peer list, e.g. `0=host:port,1=host:port`. |
| | `RAFT_LISTEN_ADDR` | `127.0.0.1:9093` | Internal Raft RPC address. |
| **Security** | `CLUSTER_PROFILE` | `production` | Deployment profile. Only `development` permits running without Raft auth. |
| | `RAFT_CLUSTER_SECRET` | *(required outside dev)* | Shared HMAC key authenticating all Raft RPC traffic. Identical on every node. |
| | `SASL_ENABLED` | `false` | Enable SASL auth on the Kafka listener (needs `--features sasl`). |
| | `SASL_USERS_FILE` | | Path to the SASL user/credential file. |
| | `TLS_ENABLED` | `false` | Enable TLS on the Kafka listener (needs `--features tls`). |
| | `TLS_CERT_PATH` / `TLS_KEY_PATH` | | PEM cert and key for TLS termination. |
| **S3** | `S3_BUCKET` | | AWS S3 bucket name. |
| | `AWS_REGION` | | AWS region (e.g. `us-east-1`). |
| **GCS** | `GCS_BUCKET` | | Google Cloud Storage bucket name. |
| | `GOOGLE_APPLICATION_CREDENTIALS` | | Path to the GCP service-account JSON. |
| **Azure** | `AZURE_STORAGE_ACCOUNT` | | Azure storage account name. |
| | `AZURE_CONTAINER` | | Azure blob container name. |
| **Health** | `HEALTH_PORT` | `8080` | Port for `/health`, `/ready`, `/live`, `/metrics`. Set to `0` to disable. |
| | `METRICS_AUTH_TOKEN` | | When set, `/metrics` requires `Authorization: Bearer <token>`. |
| **Telemetry** | `LOG_FORMAT` | `pretty` | `pretty` or `json` (for log aggregators). |
| | `RUST_LOG` | `info` | Standard `tracing-subscriber` filter. |
| | `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP collector endpoint. Requires `--features otel`. |
| | `OTEL_SERVICE_NAME` | `kafkaesque-kafka-broker` | Service name attached to exported spans. |

This is a curated subset; see [`src/cluster/config.rs`](src/cluster/config.rs)
for the full set of tunables (thread pools, cache TTLs, retention, flush
windows, ACLs, …).

## Health & monitoring

Each broker runs a lightweight HTTP server on `HEALTH_PORT` (default `8080`,
bound to `HOST`) exposing:

| Endpoint | Purpose | Notes |
|---|---|---|
| `GET /health` (`/healthz`) | Liveness | Always `200` while the process is running. Use as the Kubernetes liveness probe. |
| `GET /live` (`/livez`) | Liveness | Alias of `/health`. |
| `GET /ready` (`/readyz`) | Readiness | `503` when the broker is in zombie mode (fenced / not serving); `200` otherwise. Use as the readiness probe. |
| `GET /metrics` | Prometheus metrics | Text exposition format. Gated by `METRICS_AUTH_TOKEN` when set. |

Set `HEALTH_PORT=0` to disable the health server entirely.

## Durability contract

Kafkaesque honors the standard Kafka `acks` semantics. Every produce
request opts into one of three durability levels by setting `acks`:

| `acks` | Behavior | What can be lost on a hard kill |
|---|---|---|
| `0` | Fire-and-forget. Broker replies immediately and writes asynchronously. | Up to ~100 ms of unacked records (one SlateDB flush window). |
| `1` | Broker waits for the WAL to fsync before acking. | Nothing the broker has acked. |
| `all` | Same as `acks=1` for a single broker; identical durability across replicas because the data plane is the object store, not peer brokers. | Nothing the broker has acked. |

Beyond the `acks` flag, two additional internal writes are always
durable, regardless of the producer's choice:

- **Idempotent producer state.** When a record batch carries a
  `producer_id`, the broker persists the producer's last sequence
  number and base offset in the same SlateDB write batch as the
  records themselves, with `await_durable: true`. This ensures the
  exactly-once dedup window survives crashes — an idle producer whose
  in-memory state was evicted cannot have a fresh-looking sequence-0
  batch admitted as new after a restart.
- **Leader-epoch fencing token, log-start offset, and snapshot
  pointer.** All written with `await_durable: true`.

### Graceful shutdown

`PartitionManager::shutdown` flushes every owned partition store
before releasing its lease and unregistering the broker. SIGTERM /
SIGINT trigger this path; a hard kill (SIGKILL, OOM-kill) does not. On
a hard kill, only the `acks=0` window can lose acknowledged data.

### Property test

`tests/durability_contract_props.rs` drives a randomized produce
schedule (varying batch sizes and `acks` levels) interleaved with
randomized "crashes" — re-opening the SlateDB instance against the
same backing object store without first running graceful shutdown.
The test asserts that **every offset returned by `append_batch_durable`
is still readable after the crash**. Run with:

```bash
cargo test --test durability_contract_props
```

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
cargo run --release -p kafkaesque-bin --bin kafkaesque
```

### Kubernetes

Raw manifests are in [`deploy/kubernetes/`](deploy/kubernetes/):

```bash
kubectl apply -k deploy/kubernetes/
```

This deploys a 3-broker StatefulSet. Replace the placeholder secrets in
`secret.yaml` before production use — see
[`deploy/kubernetes/README.md`](deploy/kubernetes/README.md).

### Helm

A chart with dev and production value presets is in
[`deploy/helm/kafkaesque/`](deploy/helm/kafkaesque/):

```bash
helm install kafkaesque ./deploy/helm/kafkaesque -n kafkaesque --create-namespace \
  -f deploy/helm/kafkaesque/values-production.yaml \
  --set raftAuth.clusterSecret="$(openssl rand -base64 32)"
```

See [`deploy/helm/kafkaesque/README.md`](deploy/helm/kafkaesque/README.md) for
the full values reference.

### Cloud infrastructure (Terraform)

Modules under [`deploy/terraform/`](deploy/terraform/) provision the object
store and IAM/identity wiring for **AWS**, **GCP**, and **Azure** (e.g. an S3
bucket plus an IRSA role for EKS). See
[`deploy/terraform/README.md`](deploy/terraform/README.md).

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
- **OffsetForLeaderEpoch** — log-truncation fencing for consumer rebalances (KIP-320).
- **Consumer groups** — `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`,
  `OffsetCommit`, `OffsetFetch`, `DescribeGroups`, `ListGroups`,
  `DeleteGroups`, `FindCoordinator`.
- **Admin** — `CreateTopics`, `DeleteTopics`, `CreatePartitions`,
  `DescribeConfigs`, `AlterConfigs`, `IncrementalAlterConfigs`,
  `InitProducerId`.
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
