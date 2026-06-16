//! Raft state machines.
//!
//! The cluster is migrating from a single coordination state machine
//! ([`legacy::CoordinationStateMachine`]) to a sharded layout with a control
//! group state machine ([`control::ControlStateMachine`]) and N shard group
//! state machines ([`shard::ShardStateMachine`]). The legacy SM is preserved
//! verbatim while the rest of the system is migrated; once everything routes
//! through `control` + `shard`, the `legacy` module is deleted (sharding plan
//! step 10).

pub mod control;
mod legacy;
pub mod shard;

pub use legacy::{
    CoordinationState, CoordinationStateMachine, HeartbeatHook, OwnershipCacheInvalidation,
    OwnershipChangeHook,
};
