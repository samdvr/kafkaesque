//! Raft consensus layer for Kafkaesque cluster coordination.
//!
//! This module provides an embedded Raft consensus layer for
//! all cluster coordination needs, including:
//!
//! - Broker registration and heartbeat
//! - Partition ownership (lease-based)
//! - Topic metadata
//! - Consumer group coordination
//! - Committed offsets
//! - Producer ID allocation
//!
//! # Architecture
//!
//! ```text
//!                     ┌─────────────────────────────────────────────────────────────┐
//!                     │                      Kafkaesque Cluster                     │
//!                     │                                                             │
//!                     │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
//!                     │  │  Broker 0   │    │  Broker 1   │    │  Broker 2   │      │
//!                     │  │  (Leader)   │◄──►│ (Follower)  │◄──►│ (Follower)  │      │
//!                     │  │             │    │             │    │             │      │
//!                     │  │ ┌─────────┐ │    │ ┌─────────┐ │    │ ┌─────────┐ │      │
//!                     │  │ │  Raft   │ │    │ │  Raft   │ │    │ │  Raft   │ │      │
//!                     │  │ │  Node   │ │    │ │  Node   │ │    │ │  Node   │ │      │
//!                     │  │ └────┬────┘ │    │ └────┬────┘ │    │ └────┬────┘ │      │
//!                     │  │      │      │    │      │      │    │      │      │      │
//!                     │  │ ┌────▼────┐ │    │ ┌────▼────┐ │    │ ┌────▼────┐ │      │
//!                     │  │ │  State  │ │    │ │  State  │ │    │ │  State  │ │      │
//!                     │  │ │ Machine │ │    │ │ Machine │ │    │ │ Machine │ │      │
//!                     │  │ └─────────┘ │    │ └─────────┘ │    │ └─────────┘ │      │
//!                     │  └─────────────┘    └─────────────┘    └─────────────┘      │
//!                     └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Zero external dependencies**: Fully self-contained, no external coordination services
//! - **Linearizable consistency**: Strongest consistency guarantees
//! - **Built-in fault tolerance**: Tolerates N/2 node failures
//! - **Automatic leader election**: No manual intervention needed
//!
//! # Usage
//!
//! ```rust,no_run
//! use kafkaesque::cluster::raft::{RaftCoordinator, RaftConfig};
//! use std::sync::Arc;
//! use tokio::runtime::Handle;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = RaftConfig::default();
//!     // Create an object store (local, S3, GCS, or Azure)
//!     let object_store: Arc<dyn object_store::ObjectStore> =
//!         Arc::new(object_store::memory::InMemory::new());
//!     let coordinator = RaftCoordinator::new(config, object_store, Handle::current()).await?;
//!     // Use coordinator for cluster operations...
//!     Ok(())
//! }
//! ```

mod commands;
mod config;
mod coordinator;
pub mod domains;
pub mod group_state_machine;
mod network;
mod node;
mod state_machine;
mod storage;
mod types;

pub use commands::{CoordinationCommand, CoordinationResponse};
pub use config::RaftConfig;
pub use coordinator::RaftCoordinator;
pub use domains::{
    BrokerCommand, BrokerDomainState, BrokerResponse, BrokerStatus, GroupCommand, GroupDomainState,
    GroupResponse, MemberDescription, PartitionCommand, PartitionDomainState, PartitionResponse,
    PartitionTransfer, ProducerCommand, ProducerDomainState, ProducerResponse, TransferCommand,
    TransferDomainState, TransferReason, TransferResponse,
};
pub use network::{RaftNetworkFactoryImpl, RaftRpcMessage, RaftRpcResponse, request_cluster_join};
pub use node::RaftNode;
pub use state_machine::CoordinationStateMachine;
pub use storage::RaftStore;
pub use types::{RaftNodeId, TypeConfig};
