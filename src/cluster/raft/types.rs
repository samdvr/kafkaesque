//! Type definitions for the Raft consensus layer.

use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use super::commands::{ControlCommand, ControlResponse, ShardCommand, ShardResponse};

/// Node ID type for Raft nodes.
pub type RaftNodeId = u64;

/// Identifier for a sharded metadata Raft group, 0..N-1 where N is the
/// cluster-wide `metadata_shards` constant pinned at bootstrap.
pub type ShardId = u16;

/// Identifies which Raft group a message / command targets.
///
/// The control group holds cluster-wide state (broker registry, ACLs, topic
/// registry, producer-id allocation). Each shard group owns a slice of the
/// hot-path per-entity state (per-partition leases, consumer groups,
/// per-producer idempotency).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GroupId {
    Control,
    Shard(ShardId),
}

openraft::declare_raft_types!(
    pub ControlConfig:
        D = ControlCommand,
        R = ControlResponse,
        NodeId = RaftNodeId,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

openraft::declare_raft_types!(
    pub ShardConfig:
        D = ShardCommand,
        R = ShardResponse,
        NodeId = RaftNodeId,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);
