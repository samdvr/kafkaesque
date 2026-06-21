//! Raft state machines.
//!
//! - [`control::ControlStateMachine`] — cluster-wide state (broker registry,
//!   ACLs, topic registry, producer-id allocator).
//! - [`shard::ShardStateMachine`] — per-shard slice of partition state,
//!   consumer groups, per-producer idempotency, transfers, plus a
//!   reconciler-fed broker-liveness shadow.
//!
//! Hooks shared between the two SMs (heartbeat, ownership-change) live
//! here so the coordinator wires them up uniformly.

pub mod control;
pub mod shard;

use std::sync::Arc;

/// Sink for applied broker-heartbeat events. Wired into the local failure
/// detector so fast-failover doesn't have to wait on lease-TTL expiry.
pub type HeartbeatHook = Arc<dyn Fn(i32) + Send + Sync>;

/// Sink for owner-cache invalidation events emitted from the SM apply path.
/// The cache is process-wide; the same hook installs on control + every
/// shard, all dispatching into the same `RaftCoordinator` invalidator.
pub type OwnershipChangeHook = Arc<dyn Fn(OwnershipCacheInvalidation) + Send + Sync>;

/// What the read-path owner cache should invalidate after an applied
/// state-machine command. `Partition` is the per-key fast path
/// (acquire / release / transfer); `All` is the rare bulk-invalidate
/// (broker fence / unregister, leader change, snapshot install).
#[derive(Debug, Clone)]
pub enum OwnershipCacheInvalidation {
    /// One specific partition's owner has changed; invalidate that key only.
    Partition { topic: String, partition: i32 },
    /// Some sweeping change (broker exit, leader change) — invalidate the
    /// whole cache.
    All,
}
