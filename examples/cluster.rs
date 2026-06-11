//! SlateDB cluster server example.
//!
//! This example demonstrates a horizontally scalable Kafka-compatible server
//! using SlateDB for storage and Raft for coordination.
//!
//! ## Running
//!
//! Single broker (local storage):
//! ```bash
//! RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE=true cargo run --example cluster
//! ```
//!
//! The `RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE=true` opt-in is required when
//! `RAFT_PEERS` is unset to prevent a bootstrap race where
//! two nodes with no peers configured would each form their own one-node
//! cluster against the same object store.
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
use kafkaesque::runtime::{BrokerRuntimes, RuntimeConfig};
use kafkaesque::server::KafkaServer;
#[cfg(feature = "tls")]
use kafkaesque::server::TlsKafkaServer;
#[cfg(feature = "tls")]
use kafkaesque::server::tls::TlsConfig;
use kafkaesque::server::health::HealthServer;
use kafkaesque::telemetry::{LogFormat, init_logging};
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
    // Validate TLS configuration up front. Three failure modes
    // worth distinguishing because the operator-facing fix is different:
    // (a) `TLS_ENABLED=true` but the binary wasn't built with `--features tls`,
    // (b) `TLS_ENABLED=true` but cert/key paths are missing,
    // (c) the build is fine but TLS isn't on (no-op).
    if config.tls_enabled {
        validate_tls_config(&config)?;
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

    // Create handler first so the health server can be wired to the real
    // zombie-mode flag the PartitionManager toggles.
    let handler =
        SlateDBClusterHandler::with_runtime_handles(config.clone(), runtime_handles.clone())
            .await?;

    // Mirror the real ZombieModeState into an AtomicBool the HealthServer can
    // load on every probe. Polling on a 200ms tick is fast enough for K8s
    // readiness (which probes on the order of seconds) and avoids changing
    // HealthServer's public API.
    let zombie_mode_flag = Arc::new(AtomicBool::new(false));
    let zombie_state = handler.zombie_state();
    let zombie_flag_clone = zombie_mode_flag.clone();
    let zombie_mirror_task = runtime_handles.control.spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        loop {
            tick.tick().await;
            zombie_flag_clone.store(zombie_state.is_active(), Ordering::SeqCst);
        }
    });

    // Start health server with the mirrored flag.
    let health_handle = if config.health_port > 0 {
        let health_addr = format!("{}:{}", config.host, config.health_port);
        match HealthServer::with_broker_id(
            &health_addr,
            zombie_mode_flag.clone(),
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

    // Create server with data plane runtime for client connections. When
    // TLS is enabled, wrap the listener in `TlsKafkaServer`; otherwise use
    // the plain server. Both share the same `Listener` surface so the
    // shutdown drain logic below stays single-path.
    let addr = format!("{}:{}", config.host, config.port);
    let listener: Listener<_> = if config.tls_enabled {
        #[cfg(feature = "tls")]
        {
            // Unwraps are safe: validate_tls_config above rejected the case
            // where either path is None.
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
            // Unreachable: validate_tls_config returns Err under
            // not(feature = "tls"), and the call above already short-circuited.
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

    // Run the Kafka server until SIGTERM/SIGINT, then drain. Without this,
    // every `kubectl rollout restart` would be a data-loss event:
    // the process would die with un-flushed SlateDB writes, un-released partition
    // leases, and no Raft deregistration.
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
                    if e.is_cancelled() { std::io::ErrorKind::Interrupted } else { std::io::ErrorKind::Other },
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
    // Use a 25s connection-drain budget; K8s default terminationGracePeriod is
    // 30s so this leaves ~5s for handler.shutdown() to flush stores and
    // release leases.
    info!("Stopping Kafka listener and draining in-flight connections");
    let drained = listener.shutdown_and_wait(Duration::from_secs(25)).await;
    if !drained {
        warn!("Connection drain timed out; some clients may see broken connections");
    }

    info!("Flushing partition stores and releasing leases");
    if let Err(e) = listener.handler().shutdown().await {
        warn!(error = %e, "Handler shutdown returned an error");
    }

    // Cleanup health server and zombie mirror.
    if let Some(handle) = health_handle {
        handle.abort();
    }
    zombie_mirror_task.abort();

    info!("Graceful shutdown complete");
    Ok(())
}
