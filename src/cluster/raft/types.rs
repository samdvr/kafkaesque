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

/// Width of the on-wire group tag that binds a multiplexed Raft frame to
/// exactly one Raft group. See [`GroupId::auth_tag`].
pub const GROUP_TAG_LEN: usize = 4;

impl GroupId {
    /// Canonical byte encoding of this group, mixed into every multiplexed
    /// frame's HMAC input and carried verbatim in the frame header.
    ///
    /// Layout: byte 0 is the group kind (1 = control, 2 = shard), byte 1 is
    /// reserved (always 0), bytes 2..4 are the shard id big-endian (0 for
    /// control). Because the kind byte differs, no shard id can ever
    /// collide with control — including `u16::MAX`.
    ///
    /// **Stable on the wire — never re-number these tags.** Both peers
    /// derive the HMAC over this encoding, so changing it is a breaking
    /// protocol change that makes every existing peer fail authentication.
    pub const fn auth_tag(self) -> [u8; GROUP_TAG_LEN] {
        match self {
            GroupId::Control => [1, 0, 0, 0],
            GroupId::Shard(id) => {
                let [hi, lo] = id.to_be_bytes();
                [2, 0, hi, lo]
            }
        }
    }

    /// Inverse of [`GroupId::auth_tag`]. Returns `None` for any encoding we
    /// did not produce, so an unknown kind byte is rejected as malformed
    /// rather than silently coerced to a real group.
    pub const fn from_auth_tag(tag: [u8; GROUP_TAG_LEN]) -> Option<Self> {
        match tag {
            [1, 0, 0, 0] => Some(GroupId::Control),
            [2, 0, hi, lo] => Some(GroupId::Shard(u16::from_be_bytes([hi, lo]))),
            _ => None,
        }
    }
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
