# Kafkaesque

[![CI](https://github.com/samdvr/kafkaesque/actions/workflows/ci.yml/badge.svg)](https://github.com/samdvr/kafkaesque/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A cloud-native, Rust-based Kafka-compatible broker backed by object storage (S3, GCS, Azure) with embedded Raft consensus.

Kafkaesque provides a 100% Kafka wire protocol compatible interface, meaning standard Kafka clients work without modification. It leverages object storage for durability and embedded Raft for coordination, removing the need for Zookeeper or external metadata services.

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
         │ (Metadata &    │  │ (Storage)  │  │ (S3, GCS,     │
         │  Coordination) │  │            │  │  Azure)       │
         └────────────────┘  └────────────┘  └───────────────┘
```

**Key Features:**
*   **Kafka Compatible**: Works with standard clients (librdkafka, Java, sarama, etc.).
*   **Object Store Native**: Data is persisted to S3/GCS/Azure. No local disk reliability required.
*   **Embedded Raft**: No Zookeeper. Brokers coordinate leader election and metadata via internal Raft.
*   **Zero External Deps**: Single binary deployment.
*   **LSM-Tree Storage**: Built on [SlateDB](https://slatedb.io/) for efficient storage on object stores.

## Quick Start

### Run Locally (Standalone)

Run a single broker using local filesystem as "object storage":

```bash
cargo run --release --example cluster
```

The broker listens on `localhost:9092`. Data is stored in `/tmp/kafkaesque-data`.

### Run with Docker

```bash
docker run --rm -p 9092:9092 -p 8080:8080 \
  -e OBJECT_STORE_TYPE=local \
  -e DATA_PATH=/data \
  -v /tmp/kafkaesque-data:/data \
  kafkaesque:latest
```

## Configuration

Configuration is handled entirely via environment variables.

| Category | Variable | Default | Description |
|----------|----------|---------|-------------|
| **Core** | `BROKER_ID` | `0` | Unique broker ID (must be persistent). |
| | `HOST` | `0.0.0.0` | Bind address. |
| | `PORT` | `9092` | Kafka protocol port. |
| | `CLUSTER_ID` | `kafkaesque` | Logical cluster identifier. |
| **Storage** | `OBJECT_STORE_TYPE` | `local` | `local`, `s3`, `gcs`, or `azure`. |
| | `DATA_PATH` | `/tmp...` | Path or prefix for data. |
| **Raft** | `RAFT_PEERS` | (empty) | Peer list: `0=host:port,1=host:port`. |
| | `RAFT_LISTEN_ADDR` | `127.0.0.1:9093` | Internal Raft RPC address. |
| **S3** | `S3_BUCKET` | | AWS S3 Bucket name. |
| | `AWS_REGION` | | AWS Region (e.g. `us-east-1`). |

*See `src/config.rs` for all available options.*

## Deployment

### Multi-Broker Cluster

For a 3-node cluster, ensure each broker has a unique `BROKER_ID` and the full `RAFT_PEERS` list.

**Example (Broker 0):**
```bash
BROKER_ID=0 \
RAFT_LISTEN_ADDR=0.0.0.0:9093 \
RAFT_PEERS="0=node0:9093,1=node1:9093,2=node2:9093" \
OBJECT_STORE_TYPE=s3 \
S3_BUCKET=my-kafka-data \
cargo run --release --example cluster
```

### Kubernetes

Manifests are provided in `deploy/kubernetes/`.

```bash
kubectl apply -k deploy/kubernetes/
```

This deploys a StatefulSet with 3 brokers.

## Testing

**Unit & Integration Tests:**
```bash
cargo test
```

**End-to-End Tests:**
Requires `kcat` (kafkacat).
```bash
# Single node test
./scripts/run-e2e.sh

# 3-Node Cluster test (simulates failover)
./scripts/run-cluster-e2e.sh
```

## Supported APIs

Kafkaesque supports the core Kafka protocol (v3+):
*   **Produce/Fetch**: Basic messaging.
*   **Metadata**: Topic/partition discovery.
*   **Consumer Groups**: JoinGroup, SyncGroup, Heartbeat, OffsetCommit/Fetch.
*   **Admin**: CreateTopics, DeleteTopics.
*   **SASL/TLS**: PLAIN auth and TLS encryption (compile with `--features sasl,tls`).

*Note: Transactions and Log Compaction are not yet supported.*

## Development

*   **Format**: `cargo fmt`
*   **Lint**: `cargo clippy`
*   **Benchmarks**: `cargo bench`

## License

Apache-2.0
