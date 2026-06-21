//! State accessor helper for the multi-group Raft coordinator.
//!
//! Wraps the cluster handle so callers don't have to reach across
//! `coordinator.cluster()` for the routine "write to control" / "write to
//! shard for X" / "ensure linearizable" paths. The actual SM read pattern
//! still happens inline at the call site (lifetime-bound to the read guard
//! the caller wants to hold), but the write helper saves a layer of indirection.

use std::sync::Arc;

use super::super::cluster::RaftCluster;
use super::super::commands::{ControlCommand, ControlResponse, ShardCommand, ShardResponse};
use super::super::types::ShardId;
use crate::cluster::error::SlateDBResult;

/// Helper for accessing Raft coordinator state.
///
/// Holds a borrow of the cluster so the legacy single-call sites
/// (`coordinator.state_accessor().write(...)`) keep working post-migration
/// without carrying the cluster `Arc` themselves.
pub struct StateAccessor<'a> {
    cluster: &'a Arc<RaftCluster>,
}

impl<'a> StateAccessor<'a> {
    /// Create a new state accessor.
    #[inline]
    pub fn new(cluster: &'a Arc<RaftCluster>) -> Self {
        Self { cluster }
    }

    /// Write a command to the control group.
    ///
    /// Same forwarding semantics as [`super::RaftCoordinator::cluster`]'s
    /// `write_control` — backpressure semaphore, propose, surface forwarding
    /// errors. Use for cluster-wide state: broker registry, ACLs, topic
    /// registry, producer-id allocation.
    #[inline]
    pub async fn write_control(&self, command: ControlCommand) -> SlateDBResult<ControlResponse> {
        self.cluster.write_control(command).await
    }

    /// Write a command to the shard owning `topic`. Picks the shard with
    /// `hash(topic) % metadata_shards`.
    #[inline]
    pub async fn write_shard_for_topic(
        &self,
        topic: &str,
        command: ShardCommand,
    ) -> SlateDBResult<ShardResponse> {
        let id = self.cluster.router().shard_for_topic(topic);
        self.cluster.write_shard(id, command).await
    }

    /// Write a command to the shard owning consumer group `group_id`.
    #[inline]
    pub async fn write_shard_for_group(
        &self,
        group_id: &str,
        command: ShardCommand,
    ) -> SlateDBResult<ShardResponse> {
        let id = self.cluster.router().shard_for_group(group_id);
        self.cluster.write_shard(id, command).await
    }

    /// Write a command to the shard owning `producer_id`.
    #[inline]
    pub async fn write_shard_for_producer(
        &self,
        producer_id: i64,
        command: ShardCommand,
    ) -> SlateDBResult<ShardResponse> {
        let id = self.cluster.router().shard_for_producer(producer_id);
        self.cluster.write_shard(id, command).await
    }

    /// Write a command directly to a specific shard id.
    #[inline]
    pub async fn write_shard(
        &self,
        shard_id: ShardId,
        command: ShardCommand,
    ) -> SlateDBResult<ShardResponse> {
        self.cluster.write_shard(shard_id, command).await
    }

    /// Ensure linearizable state on the **control** group before reading.
    ///
    /// Per-shard linearizable reads use
    /// `cluster.shard(id).raft().ensure_linearizable()` directly because each
    /// shard's read-index barrier is independent.
    pub async fn ensure_linearizable_control(&self) -> SlateDBResult<()> {
        use crate::cluster::error::SlateDBError;
        self.cluster
            .control()
            .raft()
            .ensure_linearizable()
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to ensure linearizable: {}", e)))?;
        Ok(())
    }

    /// Underlying cluster handle. Useful when a caller needs direct access
    /// (e.g. iterating over every shard for an admin read).
    #[inline]
    pub fn cluster(&self) -> &Arc<RaftCluster> {
        self.cluster
    }
}

#[cfg(test)]
mod tests {
    // Tests for StateAccessor would require a full RaftCluster setup
    // which is covered by integration tests
}
