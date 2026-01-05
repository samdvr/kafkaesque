//! Runtime separation for control plane and data plane.
//!
//! This module provides separate tokio runtimes for:
//! - **Control plane**: Raft consensus, heartbeats, lease management, coordination
//! - **Data plane**: Client connections, produce/fetch handlers
//!
//! Separating these prevents data plane saturation from starving control plane tasks,
//! which could cause missed heartbeats and unnecessary leader elections.
//!
//! # Example
//!
//! ```rust,no_run
//! use kafkaesque::runtime::{BrokerRuntimes, RuntimeConfig};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = RuntimeConfig::default();
//!     let runtimes = BrokerRuntimes::new(config)?;
//!
//!     // Run broker on control plane, pass handles to components
//!     runtimes.block_on_control(async {
//!         // ... initialize and run broker with runtimes.handles()
//!     });
//!
//!     Ok(())
//! }
//! ```

use std::io;
use tokio::runtime::{Builder, Handle, Runtime};

/// Configuration for the dual-runtime setup.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Number of worker threads for control plane.
    ///
    /// Control plane handles Raft consensus, heartbeats, and coordination.
    /// These are low-throughput but latency-sensitive tasks.
    ///
    /// Default: 2
    pub control_plane_threads: usize,

    /// Number of worker threads for data plane.
    ///
    /// Data plane handles client connections and produce/fetch requests.
    /// These are high-throughput I/O-heavy tasks.
    ///
    /// Default: number of CPU cores
    pub data_plane_threads: usize,

    /// Thread name prefix for control plane threads.
    ///
    /// Default: "ctrl"
    pub control_plane_thread_name: String,

    /// Thread name prefix for data plane threads.
    ///
    /// Default: "data"
    pub data_plane_thread_name: String,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            control_plane_threads: 2,
            data_plane_threads: std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4),
            control_plane_thread_name: "ctrl".to_string(),
            data_plane_thread_name: "data".to_string(),
        }
    }
}

impl RuntimeConfig {
    /// Create configuration from environment variables.
    ///
    /// - `CONTROL_PLANE_THREADS`: Number of control plane worker threads (default: 2)
    /// - `DATA_PLANE_THREADS`: Number of data plane worker threads (default: num_cpus)
    pub fn from_env() -> Self {
        let defaults = Self::default();

        let control_plane_threads = std::env::var("CONTROL_PLANE_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.control_plane_threads);

        let data_plane_threads = std::env::var("DATA_PLANE_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.data_plane_threads);

        Self {
            control_plane_threads,
            data_plane_threads,
            ..defaults
        }
    }
}

/// Handles for both runtimes, enabling task spawning on the correct plane.
///
/// This struct is cheap to clone and can be passed to components that need
/// to spawn tasks on specific runtimes.
#[derive(Clone)]
pub struct RuntimeHandles {
    /// Control plane runtime handle for Raft and coordination tasks.
    pub control: Handle,

    /// Data plane runtime handle for client connections and I/O.
    pub data: Handle,
}

impl RuntimeHandles {
    /// Create handles from the current runtime (for backward compatibility).
    ///
    /// This creates handles that both point to the current tokio runtime,
    /// which is useful for tests or when runtime separation is not desired.
    pub fn from_current() -> Self {
        let current = Handle::current();
        Self {
            control: current.clone(),
            data: current,
        }
    }
}

/// Owned runtimes for the broker.
///
/// This struct owns both runtimes and should be held by the main entry point.
/// Use `handles()` to get cloneable handles for passing to components.
pub struct BrokerRuntimes {
    /// Control plane runtime (Raft, heartbeats, coordination).
    control: Runtime,

    /// Data plane runtime (client connections, produce/fetch).
    data: Runtime,

    /// Cloneable handles for spawning tasks.
    handles: RuntimeHandles,
}

impl BrokerRuntimes {
    /// Create both runtimes with the given configuration.
    pub fn new(config: RuntimeConfig) -> io::Result<Self> {
        let control = Builder::new_multi_thread()
            .worker_threads(config.control_plane_threads)
            .thread_name(&config.control_plane_thread_name)
            .enable_all()
            .build()?;

        let data = Builder::new_multi_thread()
            .worker_threads(config.data_plane_threads)
            .thread_name(&config.data_plane_thread_name)
            .enable_all()
            .build()?;

        let handles = RuntimeHandles {
            control: control.handle().clone(),
            data: data.handle().clone(),
        };

        Ok(Self {
            control,
            data,
            handles,
        })
    }

    /// Get cloneable handles for spawning tasks on each runtime.
    pub fn handles(&self) -> RuntimeHandles {
        self.handles.clone()
    }

    /// Get a reference to the control plane runtime.
    pub fn control(&self) -> &Runtime {
        &self.control
    }

    /// Get a reference to the data plane runtime.
    pub fn data(&self) -> &Runtime {
        &self.data
    }

    /// Block on a future using the control plane runtime.
    ///
    /// This is typically used in main() to run the broker's async entry point.
    pub fn block_on_control<F: std::future::Future>(&self, future: F) -> F::Output {
        self.control.block_on(future)
    }

    /// Gracefully shutdown both runtimes.
    ///
    /// Shuts down data plane first (stops accepting new work), then control plane.
    pub fn shutdown(self) {
        // Shutdown data plane first to stop accepting new client work
        drop(self.data);
        // Then shutdown control plane
        drop(self.control);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_config_default() {
        let config = RuntimeConfig::default();
        assert_eq!(config.control_plane_threads, 2);
        assert!(config.data_plane_threads >= 1);
        assert_eq!(config.control_plane_thread_name, "ctrl");
        assert_eq!(config.data_plane_thread_name, "data");
    }

    #[test]
    fn test_runtime_config_clone() {
        let config = RuntimeConfig::default();
        let cloned = config.clone();
        assert_eq!(config.control_plane_threads, cloned.control_plane_threads);
        assert_eq!(config.data_plane_threads, cloned.data_plane_threads);
    }

    #[test]
    fn test_broker_runtimes_creation() {
        let config = RuntimeConfig {
            control_plane_threads: 1,
            data_plane_threads: 1,
            ..Default::default()
        };

        let runtimes = BrokerRuntimes::new(config).expect("Failed to create runtimes");
        let handles = runtimes.handles();

        // Verify handles are usable
        assert!(!handles.control.id().to_string().is_empty());
        assert!(!handles.data.id().to_string().is_empty());

        // Verify they are different runtimes
        assert_ne!(handles.control.id(), handles.data.id());
    }

    #[test]
    fn test_runtime_handles_clone() {
        let config = RuntimeConfig {
            control_plane_threads: 1,
            data_plane_threads: 1,
            ..Default::default()
        };

        let runtimes = BrokerRuntimes::new(config).expect("Failed to create runtimes");
        let handles1 = runtimes.handles();
        let handles2 = handles1.clone();

        assert_eq!(handles1.control.id(), handles2.control.id());
        assert_eq!(handles1.data.id(), handles2.data.id());
    }

    #[tokio::test]
    async fn test_runtime_handles_from_current() {
        let handles = RuntimeHandles::from_current();
        // Both should point to the same runtime
        assert_eq!(handles.control.id(), handles.data.id());
    }

    #[test]
    fn test_block_on_control() {
        let config = RuntimeConfig {
            control_plane_threads: 1,
            data_plane_threads: 1,
            ..Default::default()
        };

        let runtimes = BrokerRuntimes::new(config).expect("Failed to create runtimes");
        let result = runtimes.block_on_control(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_spawn_on_different_runtimes() {
        let config = RuntimeConfig {
            control_plane_threads: 1,
            data_plane_threads: 1,
            ..Default::default()
        };

        let runtimes = BrokerRuntimes::new(config).expect("Failed to create runtimes");
        let handles = runtimes.handles();

        // Spawn tasks on different runtimes and verify they execute
        let control_result = runtimes.block_on_control(async move {
            let control_task = handles.control.spawn(async { "control" });
            let data_task = handles.data.spawn(async { "data" });

            let c = control_task.await.expect("Control task failed");
            let d = data_task.await.expect("Data task failed");
            (c, d)
        });

        assert_eq!(control_result, ("control", "data"));
    }
}
