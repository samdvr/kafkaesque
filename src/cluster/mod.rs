//! SlateDB storage backend for Kafka-compatible storage.
//!
//! This module provides a horizontally scalable Kafka-compatible storage backend using:
//! - **SlateDB**: LSM tree-based key-value store with object storage backend
//! - **Raft**: Embedded consensus layer for partition ownership, consumer groups, and offsets
//!
//! # Architecture
//!
//! ```text
//!                     ┌─────────────┐
//!                     │ Kafka Client│
//!                     └──────┬──────┘
//!                            │
//!              ┌─────────────┼─────────────┐
//!              ▼             ▼             ▼
//!        ┌─────────┐   ┌─────────┐   ┌─────────┐
//!        │ Broker 0│   │ Broker 1│   │ Broker 2│
//!        └────┬────┘   └────┬────┘   └────┬────┘
//!             │             │             │
//!             └─────────────┼─────────────┘
//!                           ▼
//!                    ┌────────────┐
//!                    │    Raft    │ ← Coordination (embedded)
//!                    └────────────┘
//!                           │
//!         ┌─────────────────┼─────────────────┐
//!         ▼                 ▼                 ▼
//!   ┌──────────┐      ┌──────────┐      ┌──────────┐
//!   │ SlateDB  │      │ SlateDB  │      │ SlateDB  │
//!   │ Part 0,3 │      │ Part 1,4 │      │ Part 2,5 │
//!   └──────────┘      └──────────┘      └──────────┘
//!         │                 │                 │
//!         └─────────────────┼─────────────────┘
//!                           ▼
//!                    ┌────────────┐
//!                    │Object Store│ ← S3, GCS, Local
//!                    └────────────┘
//! ```
//!
//! # Features
//!
//! - **Horizontal Scaling**: Multiple brokers share partitions via Raft coordination
//! - **High Availability**: Automatic failover with Raft leader election
//! - **Writer Fencing**: SlateDB's built-in fencing prevents split-brain
//! - **Cloud Native**: Object storage backend (S3, GCS, Azure, or local filesystem)
//! - **Zero External Dependencies**: Raft consensus is embedded, fully self-contained
//!
//! # Usage
//!
//! ```rust,no_run
//! use kafkaesque::cluster::{SlateDBClusterHandler, ClusterConfig};
//! use kafkaesque::server::KafkaServer;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ClusterConfig::from_env()?;
//!     let handler = SlateDBClusterHandler::new(config).await?;
//!     let server = KafkaServer::new("127.0.0.1:9092", handler).await?;
//!     server.run().await?;
//!     Ok(())
//! }
//! ```

pub mod auto_balancer;
pub mod background_tasks;
pub mod buffer_pool;
mod config;
mod coordinator;
mod error;
pub mod failure_detector;
mod handler;
mod keys;
pub mod load_metrics;
pub mod metrics;
mod object_store;
pub mod observability;
pub mod ownership_guard;
mod partition_handle;
mod partition_manager;
mod partition_recovery;
mod partition_state;
mod partition_store;
mod partition_store_pool;
pub mod raft;
pub mod rebalance_coordinator;
pub mod retry;
#[cfg(feature = "sasl")]
mod sasl_provider;
mod traits;
mod validation;
pub mod zombie_mode;

#[cfg(any(test, feature = "test-utilities"))]
pub mod mock_coordinator;
#[cfg(test)]
mod partition_store_tests;

use std::sync::Arc;

pub use background_tasks::{BackgroundTaskRegistry, SharedTaskRegistry, TaskStatus};
pub use config::{ClusterConfig, ClusterProfile, ObjectStoreType};
pub use coordinator::{
    BrokerInfo, consistent_hash_assignment, validate_group_id, validate_topic_name,
};
pub use error::{
    FencingDetectionMethod, HeartbeatResult, SlateDBError, SlateDBResult,
    detect_fencing_from_message,
};
pub use handler::{HealthStatus, SlateDBClusterHandler};
#[cfg(any(test, feature = "test-utilities"))]
pub use mock_coordinator::MockCoordinator;
pub use ownership_guard::{
    FencingError, OwnershipConfig, OwnershipGuard, OwnershipGuardBuilder, WritePermit,
};
pub use partition_handle::{PartitionHandle, PartitionId, ReadGuard, WriteGuard};
pub use partition_manager::PartitionManager;
pub use partition_state::PartitionState;
pub use partition_store::PartitionStore;
pub use partition_store_pool::{PartitionStorePool, PoolConfig};
pub use raft::{RaftConfig, RaftCoordinator, RaftNode};
pub use rebalance_coordinator::{
    CoordinatorExecutorAdapter, RebalanceCoordinator, RebalanceCoordinatorConfig,
};
pub use traits::{
    ClusterCoordinator, ConsumerGroupCoordinator, PartitionCoordinator,
    PartitionTransferCoordinator, ProducerCoordinator,
};
pub use zombie_mode::ZombieModeState;

/// Type alias for partition key (topic_name, partition_index).
///
/// Uses `Arc<str>` for the topic name to enable O(1) cloning instead of O(n) string allocation.
/// This is critical for performance since partition keys are cloned frequently in:
/// - Background task loops (heartbeat, lease renewal, ownership checks)
/// - State machine command processing (every Raft log entry)
/// - Partition lookups and iterations
pub type PartitionKey = (Arc<str>, i32);
