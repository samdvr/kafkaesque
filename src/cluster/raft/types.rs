//! Type definitions for the Raft consensus layer.

use openraft::BasicNode;
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
