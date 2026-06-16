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
pub mod partition_state;
pub mod producer;
pub(crate) mod serde_helpers;
pub mod topic_registry;
pub mod transfer;

pub use acl::{
    AclBinding, AclCommand, AclDecision, AclDomainState, AclFilter, AclOperation, AclPatternType,
    AclPermissionType, AclResourceType, AclResponse,
};
pub use broker::{BrokerCommand, BrokerDomainState, BrokerInfo, BrokerResponse, BrokerStatus};
pub use group::{GroupCommand, GroupDomainState, GroupResponse, MemberDescription};
pub use partition::{
    BatchRenewOutcome, PartitionCommand, PartitionDomainState, PartitionInfo, PartitionResponse,
    TopicInfo,
};
pub use partition_state::{PartitionStateCommand, PartitionStateDomain, PartitionStateResponse};
pub use producer::{ProducerCommand, ProducerDomainState, ProducerResponse};
pub use topic_registry::{TopicRegistryCommand, TopicRegistryResponse, TopicRegistryState};
pub use transfer::{
    PartitionTransfer, TransferCommand, TransferDomainState, TransferReason, TransferResponse,
};
