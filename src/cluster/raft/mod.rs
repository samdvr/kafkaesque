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

mod auth;
mod cluster;
mod commands;
mod config;
mod coordinator;
pub mod domains;
mod group;
pub(crate) mod hash;
mod mux;
mod mux_client;
mod mux_server;
mod network;
mod node;
mod state_machine;
mod storage;
mod tls;
mod types;

pub use auth::RaftAuthKeys;
pub use commands::{CoordinationCommand, CoordinationResponse};
pub use config::RaftConfig;
pub use coordinator::RaftCoordinator;
pub use domains::{
    AclBinding, AclCommand, AclDecision, AclDomainState, AclFilter, AclOperation, AclPatternType,
    AclPermissionType, AclResourceType, AclResponse, BrokerCommand, BrokerDomainState,
    BrokerResponse, BrokerStatus, GroupCommand, GroupDomainState, GroupResponse, MemberDescription,
    PartitionCommand, PartitionDomainState, PartitionResponse, PartitionTransfer, ProducerCommand,
    ProducerDomainState, ProducerResponse, TransferCommand, TransferDomainState, TransferReason,
    TransferResponse,
};
pub use network::{RaftNetworkFactoryImpl, request_cluster_join};
#[doc(hidden)]
pub use network::{RaftRpcMessage, RaftRpcResponse};
pub use node::RaftNode;
#[doc(hidden)]
pub use state_machine::{CoordinationStateMachine, OwnershipCacheInvalidation};
#[doc(hidden)]
pub use storage::{LegacyRaftStore as RaftStore, RaftStore as RaftStoreGeneric};
pub use tls::RaftTlsConfig;
pub use types::{RaftNodeId, TypeConfig};
