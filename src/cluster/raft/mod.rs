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
mod reconciler;
mod state_machine;
mod storage;
mod tls;
mod types;

pub use auth::RaftAuthKeys;
pub use commands::{ControlCommand, ControlResponse, ShardCommand, ShardResponse};
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
#[doc(hidden)]
pub use state_machine::OwnershipCacheInvalidation;
// Exported for `fuzz/fuzz_targets/`, which drives the postcard decoders a
// hostile peer can reach: `mux::{MuxRaftRpcMessage, MuxRaftRpcResponse}`
// are the wire frames (they replaced the legacy `network::RaftRpcMessage`
// pair), and the two state machines decode `InstallSnapshot` payloads
// (they replaced the single `CoordinationStateMachine`). Not part of the
// supported API — see the `#[doc(hidden)]` items above.
#[doc(hidden)]
pub use mux::{MuxRaftRpcMessage, MuxRaftRpcResponse};
#[doc(hidden)]
pub use state_machine::control::ControlStateMachine;
#[doc(hidden)]
pub use state_machine::shard::ShardStateMachine;
#[doc(hidden)]
pub use storage::RaftStore;
pub use tls::RaftTlsConfig;
pub use types::{RaftNodeId, ShardId};
