//! Domain-specific state machine components.
//!
//! This module organizes the Raft state machine into logical domains:
//!
//! - **Broker**: Broker registration, heartbeat, fencing
//! - **Partition**: Topic management, partition ownership, leases
//! - **Group**: Consumer group coordination, assignments, offsets
//! - **Producer**: Producer ID allocation, idempotency state
//! - **Transfer**: Fast failover, batch partition transfers
//! - **Acl**: Authorization bindings
//!
//! Each domain provides:
//! - Command enum defining operations
//! - Response enum for operation results
//! - State struct holding domain-specific data
//! - Apply function for deterministic state transitions
//!
//! The main `CoordinationState` composes these domains.

pub mod acl;
pub mod broker;
pub mod group;
pub mod partition;
pub mod producer;
pub mod transfer;

pub use acl::{
    AclBinding, AclCommand, AclDecision, AclDomainState, AclFilter, AclOperation, AclPatternType,
    AclPermissionType, AclResourceType, AclResponse,
};
pub use broker::{BrokerCommand, BrokerDomainState, BrokerResponse, BrokerStatus};
pub use group::{GroupCommand, GroupDomainState, GroupResponse, MemberDescription};
pub use partition::{PartitionCommand, PartitionDomainState, PartitionResponse};
pub use producer::{ProducerCommand, ProducerDomainState, ProducerResponse};
pub use transfer::{
    PartitionTransfer, TransferCommand, TransferDomainState, TransferReason, TransferResponse,
};
