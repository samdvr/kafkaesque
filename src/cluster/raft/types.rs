//! Type definitions for the Raft consensus layer.

use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use super::commands::{CoordinationCommand, CoordinationResponse};

/// Node ID type for Raft nodes.
pub type RaftNodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = CoordinationCommand,
        R = CoordinationResponse,
        NodeId = RaftNodeId,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Information about a Raft node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftNodeInfo {
    /// The node's Raft ID.
    pub node_id: RaftNodeId,
    /// The node's network address for Raft communication.
    pub raft_addr: String,
    /// The node's Kafka broker ID (for mapping between Raft and Kafka IDs).
    pub broker_id: i32,
    /// Whether this node is a voter (participates in quorum).
    pub is_voter: bool,
}
