//! SlateDB cluster server example.
//!
//! This example demonstrates a horizontally scalable Kafka-compatible server
//! using SlateDB for storage and Raft for coordination.
//!
//! ## Running
//!
//! Single broker (local storage):
//! ```bash
//! cargo run --example cluster
//! ```
//!
//! With JSON logging for production:
//! ```bash
//! LOG_FORMAT=json RUST_LOG=info cargo run --example cluster
//! ```
//!
//! Multiple brokers (in separate terminals):
//! ```bash
//! BROKER_ID=0 PORT=9092 RAFT_LISTEN_ADDR=127.0.0.1:9093 cargo run --example cluster
//! BROKER_ID=1 PORT=9094 RAFT_LISTEN_ADDR=127.0.0.1:9095 RAFT_PEERS="0=127.0.0.1:9093" cargo run --example cluster
//! BROKER_ID=2 PORT=9096 RAFT_LISTEN_ADDR=127.0.0.1:9097 RAFT_PEERS="0=127.0.0.1:9093,1=127.0.0.1:9095" cargo run --example cluster
//! ```
//!
//! With S3 storage (MinIO example):
//! ```bash
//! OBJECT_STORE_TYPE=s3 \
//! S3_BUCKET=kafka-data \
//! AWS_ENDPOINT=http://localhost:9000 \
//! AWS_ACCESS_KEY_ID=minioadmin \
//! AWS_SECRET_ACCESS_KEY=minioadmin \
//! AWS_REGION=us-east-1 \
//! cargo run --example cluster
//! ```
//!
//! Then connect with any Kafka client:
//! ```bash
//! kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
//! kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
//! ```
//!
//! ## Health Endpoints
//!
//! By default, health endpoints are exposed on port 8080:
//! - `GET /health` - Liveness check (always 200 if running)
//! - `GET /ready` - Readiness check (503 if in zombie mode)
//! - `GET /metrics` - Prometheus metrics
//!
//! Set `HEALTH_PORT=0` to disable the health server.
//!
//! ## Runtime Configuration
//!
//! The broker uses separate tokio runtimes for control plane (Raft, heartbeats)
//! and data plane (client connections, produce/fetch) to prevent cold reads from
//! starving Raft heartbeats.
//!
//! - `CONTROL_PLANE_THREADS`: Number of control plane threads (default: 2)
//! - `DATA_PLANE_THREADS`: Number of data plane threads (default: num_cpus)

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
use kafkaesque::runtime::{BrokerRuntimes, RuntimeConfig};
use kafkaesque::server::KafkaServer;
use kafkaesque::server::health::HealthServer;
use kafkaesque::telemetry::{LogFormat, init_logging};
use tracing::info;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging with format from LOG_FORMAT env var (default: pretty)
    // Set LOG_FORMAT=json for JSON output suitable for log aggregators
    init_logging(LogFormat::from_env()).map_err(|e| -> Box<dyn std::error::Error> { e })?;

    // Read configuration from environment
    let config = ClusterConfig::from_env()?;

    // Create runtime configuration from cluster config
    let runtime_config = RuntimeConfig {
        control_plane_threads: config.control_plane_threads,
        data_plane_threads: config.data_plane_threads,
        ..Default::default()
    };

    // Create separate runtimes for control plane and data plane
    let runtimes = BrokerRuntimes::new(runtime_config)?;
    let handles = runtimes.handles();

    info!(
        control_plane_threads = handles.control.metrics().num_workers(),
        data_plane_threads = handles.data.metrics().num_workers(),
        "Created separate control plane and data plane runtimes"
    );

    // Run the broker on the control plane runtime
    runtimes.block_on_control(run_broker(config, handles))
}

async fn run_broker(
    config: ClusterConfig,
    runtime_handles: kafkaesque::runtime::RuntimeHandles,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        broker_id = config.broker_id,
        host = %config.host,
        port = config.port,
        health_port = config.health_port,
        raft_addr = %config.raft_listen_addr,
        object_store = ?config.object_store,
        data_path = %config.object_store_path,
        "Starting SlateDB cluster server with Raft coordination"
    );

    // Shared zombie mode flag for health server
    // Note: The handler internally manages zombie mode, this is just for the health server
    let zombie_mode = Arc::new(AtomicBool::new(false));

    // Start health server if configured (before creating handler to start fast)
    let health_handle = if config.health_port > 0 {
        let health_addr = format!("{}:{}", config.host, config.health_port);
        match HealthServer::with_broker_id(
            &health_addr,
            zombie_mode.clone(),
            Some(config.broker_id),
        )
        .await
        {
            Ok(server) => {
                info!(
                    broker_id = config.broker_id,
                    health_addr = %health_addr,
                    "Health server started"
                );
                // Health server runs on control plane runtime
                Some(runtime_handles.control.spawn(async move {
                    let _ = server.run().await;
                }))
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to start health server - continuing without it");
                None
            }
        }
    } else {
        info!("Health server disabled (HEALTH_PORT=0)");
        None
    };

    // Create handler with runtime handles for control/data plane separation
    let handler =
        SlateDBClusterHandler::with_runtime_handles(config.clone(), runtime_handles.clone())
            .await?;

    // Create server with data plane runtime for client connections
    let addr = format!("{}:{}", config.host, config.port);
    let server = KafkaServer::with_config(
        &addr,
        handler,
        kafkaesque::constants::DEFAULT_MAX_CONNECTIONS_PER_IP,
        kafkaesque::constants::DEFAULT_MAX_TOTAL_CONNECTIONS,
        runtime_handles.data.clone(),
    )
    .await?;

    info!(
        broker_id = config.broker_id,
        address = %addr,
        "Server is running. Connect with any Kafka client."
    );
    info!(
        "Example producer: kafka-console-producer.sh --bootstrap-server {}:{} --topic test",
        config.host, config.port
    );
    info!(
        "Example consumer: kafka-console-consumer.sh --bootstrap-server {}:{} --topic test --from-beginning",
        config.host, config.port
    );

    server.run().await?;

    // Cleanup health server if running
    if let Some(handle) = health_handle {
        handle.abort();
    }

    Ok(())
}
