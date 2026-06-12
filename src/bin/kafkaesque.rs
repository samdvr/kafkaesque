//! SlateDB cluster server.
//!
//! This is the deployed Kafkaesque broker binary: a horizontally scalable
//! Kafka-compatible server using SlateDB for storage and Raft for coordination.
//!
//! ## Running
//!
//! Single broker (local storage):
//! ```bash
//! CLUSTER_PROFILE=development RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE=true cargo run --bin kafkaesque
//! ```
//!
//! The `RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE=true` opt-in is required when
//! `RAFT_PEERS` is unset to prevent a bootstrap race where
//! two nodes with no peers configured would each form their own one-node
//! cluster against the same object store.
//!
//! ## Security posture
//!
//! Outside the development profile (`CLUSTER_PROFILE=development`), startup
//! fails unless `RAFT_CLUSTER_SECRET` is set: the Raft RPC port accepts
//! cluster joins and coordination commands, so it must be authenticated on
//! any real network. Set the same strong random value on every node:
//! ```bash
//! RAFT_CLUSTER_SECRET="$(openssl rand -base64 32)"   # same value cluster-wide
//! ```
//!
//! With JSON logging for production:
//! ```bash
//! LOG_FORMAT=json RUST_LOG=info cargo run --bin kafkaesque
//! ```
//!
//! Multiple brokers (in separate terminals, sharing one secret):
//! ```bash
//! export RAFT_CLUSTER_SECRET=local-cluster-secret
//! BROKER_ID=0 PORT=9092 RAFT_LISTEN_ADDR=127.0.0.1:9093 cargo run --bin kafkaesque
//! BROKER_ID=1 PORT=9094 RAFT_LISTEN_ADDR=127.0.0.1:9095 RAFT_PEERS="0=127.0.0.1:9093" cargo run --bin kafkaesque
//! BROKER_ID=2 PORT=9096 RAFT_LISTEN_ADDR=127.0.0.1:9097 RAFT_PEERS="0=127.0.0.1:9093,1=127.0.0.1:9095" cargo run --bin kafkaesque
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
//! cargo run --bin kafkaesque
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
use std::time::Duration;

use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
use kafkaesque::runtime::{BrokerRuntimes, RuntimeConfig};
use kafkaesque::server::KafkaServer;
#[cfg(feature = "tls")]
use kafkaesque::server::TlsKafkaServer;
use kafkaesque::server::health::HealthServer;
#[cfg(feature = "tls")]
use kafkaesque::server::tls::TlsConfig;
use kafkaesque::telemetry::{LogFormat, init_logging, shutdown_telemetry};
use tokio::signal::unix::{SignalKind, signal};
use tracing::{info, warn};

/// Validate TLS configuration before the broker starts.
/// Three failure modes worth distinguishing because the operator-facing
/// fix is different:
/// - the binary wasn't built with `--features tls`
/// - cert/key paths are missing
/// - cert/key files are unreadable (handled later by the TLS server)
#[cfg(feature = "tls")]
fn validate_tls_config(config: &ClusterConfig) -> Result<(), Box<dyn std::error::Error>> {
    if config.tls_cert_path.is_none() || config.tls_key_path.is_none() {
        return Err(
            "TLS_ENABLED=true requires both TLS_CERT_PATH and TLS_KEY_PATH to be set".into(),
        );
    }
    Ok(())
}

#[cfg(not(feature = "tls"))]
fn validate_tls_config(_config: &ClusterConfig) -> Result<(), Box<dyn std::error::Error>> {
    Err(
        "TLS_ENABLED=true but kafkaesque was built without --features tls. Rebuild with \
         `cargo build --features tls` (or use the published release image which includes it) \
         to enable TLS support."
            .into(),
    )
}

/// Either a plain or TLS-wrapped Kafka listener. Both server types expose
/// the same `run` / `shutdown_and_wait` / `handler` surface, so the binary
/// drives them through a single match instead of duplicating the lifecycle.
enum Listener<H: kafkaesque::server::Handler + Send + Sync + 'static> {
    Plain(Arc<KafkaServer<H>>),
    #[cfg(feature = "tls")]
    Tls(Arc<TlsKafkaServer<H>>),
}

impl<H: kafkaesque::server::Handler + Send + Sync + 'static> Listener<H> {
    async fn shutdown_and_wait(&self, timeout: Duration) -> bool {
        match self {
            Listener::Plain(s) => s.shutdown_and_wait(timeout).await,
            #[cfg(feature = "tls")]
            Listener::Tls(s) => s.shutdown_and_wait(timeout).await,
        }
    }

    fn handler(&self) -> Arc<H> {
        match self {
            Listener::Plain(s) => s.handler(),
            #[cfg(feature = "tls")]
            Listener::Tls(s) => s.handler(),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging(LogFormat::from_env()).map_err(|e| -> Box<dyn std::error::Error> { e })?;

    let config = ClusterConfig::from_env()?;

    let runtime_config = RuntimeConfig {
        control_plane_threads: config.control_plane_threads,
        data_plane_threads: config.data_plane_threads,
        ..Default::default()
    };

    let runtimes = BrokerRuntimes::new(runtime_config)?;
    let handles = runtimes.handles();

    info!(
        control_plane_threads = handles.control.metrics().num_workers(),
        data_plane_threads = handles.data.metrics().num_workers(),
        "Created separate control plane and data plane runtimes"
    );

    runtimes.block_on_control(run_broker(config, handles))
}

async fn run_broker(
    config: ClusterConfig,
    runtime_handles: kafkaesque::runtime::RuntimeHandles,
) -> Result<(), Box<dyn std::error::Error>> {
    if config.tls_enabled {
        validate_tls_config(&config)?;
    }

    // Wire the process-wide inbound-frame memory budget before any client
    // connection is accepted. Without this call the budget silently stayed at
    // the 1 GiB default regardless of operator configuration.
    if !kafkaesque::server::set_global_inflight_byte_budget(config.global_inflight_byte_budget) {
        warn!(
            budget_bytes = config.global_inflight_byte_budget,
            "Global inflight byte budget was already fixed; config value ignored"
        );
    }

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

    let handler =
        SlateDBClusterHandler::with_runtime_handles(config.clone(), runtime_handles.clone())
            .await?;

    // Share the real `Arc<ZombieModeState>` from the partition manager with
    // the health server so probes observe transitions on the next request
    // rather than on the next polling tick.
    let zombie_state = handler.zombie_state();

    let health_handle = if config.health_port > 0 {
        let health_addr = format!("{}:{}", config.host, config.health_port);
        match HealthServer::with_broker_id(
            &health_addr,
            zombie_state.clone(),
            Some(config.broker_id),
        )
        .await
        {
            Ok(server) => {
                let server = server.with_metrics_auth(config.metrics_auth_token.clone());
                info!(
                    broker_id = config.broker_id,
                    health_addr = %health_addr,
                    metrics_auth_enabled = config.metrics_auth_token.is_some(),
                    "Health server started"
                );
                Some(runtime_handles.control.spawn(async move {
                    use futures::FutureExt;
                    use std::panic::AssertUnwindSafe;
                    // Catch panics in the health server so a bug in the
                    // health endpoint doesn't silently leave K8s liveness
                    // probes passing at the LB while the metrics endpoint
                    // is dead and nothing logs.
                    if let Err(panic_payload) =
                        AssertUnwindSafe(server.run()).catch_unwind().await
                    {
                        tracing::error!(
                            task = "health_server",
                            payload = ?panic_payload,
                            "Background task panicked"
                        );
                    }
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

    let addr = format!("{}:{}", config.host, config.port);
    let listener: Listener<_> = if config.tls_enabled {
        #[cfg(feature = "tls")]
        {
            let cert = config.tls_cert_path.as_ref().expect("validated above");
            let key = config.tls_key_path.as_ref().expect("validated above");
            let tls_config = TlsConfig::from_pem_files(cert, key)
                .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
            let server = TlsKafkaServer::with_full_config(
                &addr,
                handler,
                tls_config,
                kafkaesque::constants::DEFAULT_MAX_CONNECTIONS_PER_IP,
                kafkaesque::constants::DEFAULT_MAX_TOTAL_CONNECTIONS,
                config.max_message_size,
                runtime_handles.data.clone(),
            )
            .await?;
            info!(
                broker_id = config.broker_id,
                address = %addr,
                "TLS Kafka listener active. Plain TCP connections will be rejected."
            );
            Listener::Tls(Arc::new(server))
        }
        #[cfg(not(feature = "tls"))]
        {
            unreachable!("TLS feature disabled; validate_tls_config should have rejected")
        }
    } else {
        let server = KafkaServer::with_full_config(
            &addr,
            handler,
            kafkaesque::constants::DEFAULT_MAX_CONNECTIONS_PER_IP,
            kafkaesque::constants::DEFAULT_MAX_TOTAL_CONNECTIONS,
            config.max_message_size,
            runtime_handles.data.clone(),
        )
        .await?;
        Listener::Plain(Arc::new(server))
    };

    info!(
        broker_id = config.broker_id,
        address = %addr,
        tls_enabled = config.tls_enabled,
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

    let mut sigterm = signal(SignalKind::terminate()).map_err(Box::new)?;
    let mut sigint = signal(SignalKind::interrupt()).map_err(Box::new)?;
    let run_task = match &listener {
        Listener::Plain(s) => {
            let s = s.clone();
            runtime_handles.data.spawn(async move { s.run().await })
        }
        #[cfg(feature = "tls")]
        Listener::Tls(s) => {
            let s = s.clone();
            runtime_handles.data.spawn(async move { s.run().await })
        }
    };

    tokio::select! {
        run_result = async {
            match run_task.await {
                Ok(r) => r,
                Err(e) => Err(kafkaesque::error::Error::IoError(
                    kafkaesque::error::PreservedIoError {
                        kind: if e.is_cancelled() {
                            std::io::ErrorKind::Interrupted
                        } else {
                            std::io::ErrorKind::Other
                        },
                        message: e.to_string(),
                    },
                )),
            }
        } => {
            if let Err(e) = run_result {
                warn!(error = %e, "Kafka server exited with error");
            }
        }
        _ = sigterm.recv() => {
            info!("SIGTERM received, beginning graceful shutdown");
        }
        _ = sigint.recv() => {
            info!("SIGINT received, beginning graceful shutdown");
        }
    }

    // Drain: stop accepting, finish in-flight requests, then shut down handler.
    // 25s connection-drain budget; K8s default terminationGracePeriod is 30s
    // so this leaves ~5s for handler.shutdown() to flush stores and release leases.
    info!("Stopping Kafka listener and draining in-flight connections");
    let drained = listener.shutdown_and_wait(Duration::from_secs(25)).await;
    if !drained {
        warn!("Connection drain timed out; some clients may see broken connections");
    }

    info!("Flushing partition stores and releasing leases");
    if let Err(e) = listener.handler().shutdown().await {
        warn!(error = %e, "Handler shutdown returned an error");
    }

    if let Some(handle) = health_handle {
        handle.abort();
    }

    // Flush any pending OTLP spans before exit. The batch exporter buffers
    // up to a few hundred ms of spans; without an explicit shutdown those
    // are dropped on process exit.
    shutdown_telemetry();

    info!("Graceful shutdown complete");
    Ok(())
}
