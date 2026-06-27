//! Multiplexed wire frame for the metadata Raft port.
//!
//! Today the Raft RPC port (see [`super::network`]) carries one `Raft<TypeConfig>`'s
//! traffic per node: a single combined log + state machine. The sharded
//! layout (sharding plan, step 4) introduces a control group plus N shard
//! groups, all reachable on the same TCP+HMAC port. Every frame gains an
//! outer tag — `Control` / `Shard(ShardId)` / `JoinCluster` /
//! `PromoteMember { group }` — and the multiplexed server matches that
//! tag *before* deserialising the inner payload.
//!
//! This module defines the new wire frame and provides pure-logic
//! dispatch helpers + tests. The actual TCP listener that runs it lands
//! alongside [`super::cluster::RaftCluster`] in step 5; this is the
//! foundation it builds on.
//!
//! # Risk #2 mitigation — silent group misdispatch
//!
//! The plan's risk #2 calls out that a frame routed to the wrong `Raft`
//! instance whose inner payload happens to deserialize would silently
//! corrupt that group's log. Two defences:
//!
//! 1. **Outer tag is matched first.** [`MuxRaftRpcMessage`] is a tagged
//!    `serde` enum. Postcard encodes the discriminant before the inner
//!    payload bytes, so the dispatcher *must* inspect the variant before
//!    handing bytes to a specific group's RPC handler. The
//!    `dispatch_target` helper makes that ordering explicit.
//! 2. **Per-group HMAC purpose mix-in (planned).** The plan calls for
//!    each frame's HMAC to fold in the target group id so a control
//!    frame replayed under a shard tag fails authentication, not just
//!    deserialisation. The hook for that lives in [`auth_purpose_for`]
//!    here — wiring it through [`super::auth`] is a follow-up alongside
//!    the actual server in step 5.

#![allow(dead_code)] // wired in step 5 when RaftCluster runs the listener

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

use super::commands::{ControlCommand, ControlResponse, ShardCommand, ShardResponse};
use super::network::RpcErrorInfo;
use super::types::{ControlConfig, GroupId, RaftNodeId, ShardConfig, ShardId};

/// Per-group payload of a control RPC.
///
/// Mirrors the variants of the legacy `RaftRpcMessage` but parameterised
/// to [`ControlConfig`]. The multiplexed server sees these only after the
/// outer [`MuxRaftRpcMessage::Control`] tag has been matched.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRpcMessage {
    AppendEntries(AppendEntriesRequest<ControlConfig>),
    Vote(VoteRequest<RaftNodeId>),
    InstallSnapshot(InstallSnapshotRequest<ControlConfig>),
    /// Forwarded client write with leader-term validation. Same shape as the
    /// legacy `RaftRpcMessage::ClientWriteWithTerm`; the rationale (term
    /// check + hop count) is documented there.
    ClientWriteWithTerm {
        command: ControlCommand,
        expected_term: u64,
        forward_hops: u8,
    },
}

/// Per-group payload of a shard RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShardRpcMessage {
    AppendEntries(AppendEntriesRequest<ShardConfig>),
    Vote(VoteRequest<RaftNodeId>),
    InstallSnapshot(InstallSnapshotRequest<ShardConfig>),
    ClientWriteWithTerm {
        command: ShardCommand,
        expected_term: u64,
        forward_hops: u8,
    },
}

/// Multiplexed wire frame for the metadata Raft port.
///
/// The outer tag is the dispatch key — see module docs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MuxRaftRpcMessage {
    /// Frame addressed at the cluster-wide control group.
    #[serde(rename = "mux_control_v1")]
    Control(ControlRpcMessage),

    /// Frame addressed at the shard with the given id.
    #[serde(rename = "mux_shard_v1")]
    Shard(ShardId, ShardRpcMessage),

    /// Bootstrap join — adds the new broker as a **learner** on `group`.
    /// Sent once per group during the join sequence (sharding plan
    /// "Bootstrap & membership"). Each group's leader handles its own
    /// JoinCluster; cross-group fan-out is the *joining* broker's
    /// responsibility, not a control-group coordinator job, to keep the
    /// control leader off the critical path of every join.
    #[serde(rename = "mux_join_v2")]
    JoinCluster {
        node_id: RaftNodeId,
        raft_addr: String,
        group: GroupId,
    },

    /// Promote an existing learner on `group` to a voting member.
    #[serde(rename = "mux_promote_v1")]
    PromoteMember { node_id: RaftNodeId, group: GroupId },
}

/// Multiplexed RPC response. The inner variants mirror the openraft RPC
/// shapes plus the two client-write outcomes (one per group flavour).
///
/// Not `Clone` because openraft's `AppendEntriesResponse` /
/// `InstallSnapshotResponse` aren't `Clone` either; the legacy
/// [`super::network::RaftRpcResponse`] is also non-Clone for the same
/// reason. Responses are produced once per request and consumed in
/// place, so the missing impl doesn't cost anything.
#[derive(Debug, Serialize, Deserialize)]
pub enum MuxRaftRpcResponse {
    AppendEntries(AppendEntriesResponse<RaftNodeId>),
    Vote(VoteResponse<RaftNodeId>),
    InstallSnapshot(InstallSnapshotResponse<RaftNodeId>),
    /// Forwarded client write committed by the control leader.
    ControlClientWriteOk(ControlResponse),
    /// Forwarded client write committed by a shard leader.
    ShardClientWriteOk(ShardResponse),
    /// Bootstrap join accepted.
    JoinClusterOk,
    /// Promotion accepted.
    PromoteMemberOk,
    /// Structured error with retry semantics. Reuses the legacy
    /// [`RpcErrorInfo`] type so client retry logic is identical.
    Error(RpcErrorInfo),
}

/// Logical dispatch target for an inbound multiplexed frame.
///
/// `RaftCluster` (step 5) holds an `Arc<Raft<ControlConfig>>` and a
/// `Vec<Arc<Raft<ShardConfig>>>`. Resolving an incoming frame to one of
/// these — or to a control-plane handler for join / promote — is what
/// the dispatcher does. The enum makes the routing decision *visible*
/// at the type level so a bug in the routing table can be caught by a
/// fuzz test (see `dispatch_target_is_unambiguous`) instead of
/// corrupting a group's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchTarget {
    /// Route to the control-group `Raft` instance.
    Control,
    /// Route to the shard at the given index. Caller must validate the
    /// id against the configured `metadata_shards` count.
    Shard(ShardId),
    /// Run on the cluster-control plane (no Raft instance involved
    /// for the join handshake itself — the learner add happens via
    /// `Raft::change_membership` on the control group, but the
    /// dispatch decision is independent of that).
    ClusterControl,
}

/// Decide where this frame should be routed *before* its inner payload
/// is touched.
///
/// Pure function on the outer tag — no Raft state is inspected, no
/// network is touched. Correctness here is critical: a bug that routes
/// `Shard(3, ...)` to control would let bytes drawn for one group's
/// state machine land in another group's log.
pub fn dispatch_target(msg: &MuxRaftRpcMessage) -> DispatchTarget {
    match msg {
        MuxRaftRpcMessage::Control(_) => DispatchTarget::Control,
        MuxRaftRpcMessage::Shard(id, _) => DispatchTarget::Shard(*id),
        // Per-group join (mux_join_v2): the dispatch target is the group
        // the learner is being added to, not a "cluster-control" plane.
        // This is what lets the joining broker target each group's leader
        // independently — without it the control leader would have to
        // coordinate every shard's add_learner.
        MuxRaftRpcMessage::JoinCluster { group, .. } => match group {
            GroupId::Control => DispatchTarget::ClusterControl,
            GroupId::Shard(id) => DispatchTarget::Shard(*id),
        },
        MuxRaftRpcMessage::PromoteMember { group, .. } => match group {
            GroupId::Control => DispatchTarget::Control,
            GroupId::Shard(id) => DispatchTarget::Shard(*id),
        },
    }
}

/// Per-group HMAC purpose mix-in.
///
/// The legacy [`super::auth::FramePurpose`] is a flat enum. Per the
/// sharding plan (Risk #2 mitigation), the multiplexed port should fold
/// the target group into the HMAC input so a frame signed for one group
/// fails authentication when interpreted as another. Until the auth
/// module gains the new shape, this helper produces the per-group
/// distinguishing bytes that step 5's wiring will mix into the HMAC
/// input.
pub fn auth_purpose_for(group: GroupId) -> [u8; 4] {
    // Layout: tag byte 0 = group kind (1 = control, 2 = shard), then
    // the shard id big-endian (or zero for control). This is what the
    // sign/verify path will mix into the HMAC alongside the existing
    // `FramePurpose` byte. Stable on the wire — never re-number tags.
    match group {
        GroupId::Control => [1, 0, 0, 0],
        GroupId::Shard(id) => {
            let [hi, lo] = id.to_be_bytes();
            [2, 0, hi, lo]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::network::RpcErrorKind;
    use super::*;
    use openraft::Vote;

    fn vote_request(node: u64, term: u64) -> VoteRequest<RaftNodeId> {
        VoteRequest::new(Vote::new(term, node), None)
    }

    fn sample_control() -> MuxRaftRpcMessage {
        MuxRaftRpcMessage::Control(ControlRpcMessage::Vote(vote_request(1, 1)))
    }

    fn sample_shard(id: ShardId) -> MuxRaftRpcMessage {
        MuxRaftRpcMessage::Shard(id, ShardRpcMessage::Vote(vote_request(1, 1)))
    }

    fn sample_join() -> MuxRaftRpcMessage {
        MuxRaftRpcMessage::JoinCluster {
            node_id: 7,
            raft_addr: "127.0.0.1:9191".into(),
            group: GroupId::Control,
        }
    }

    fn sample_join_shard(id: ShardId) -> MuxRaftRpcMessage {
        MuxRaftRpcMessage::JoinCluster {
            node_id: 7,
            raft_addr: "127.0.0.1:9191".into(),
            group: GroupId::Shard(id),
        }
    }

    fn sample_promote(group: GroupId) -> MuxRaftRpcMessage {
        MuxRaftRpcMessage::PromoteMember { node_id: 7, group }
    }

    #[test]
    fn dispatch_target_is_correct_for_each_outer_tag() {
        assert_eq!(dispatch_target(&sample_control()), DispatchTarget::Control);
        assert_eq!(dispatch_target(&sample_shard(0)), DispatchTarget::Shard(0));
        assert_eq!(dispatch_target(&sample_shard(7)), DispatchTarget::Shard(7));
        // Control join: still routed to control-plane handler.
        assert_eq!(
            dispatch_target(&sample_join()),
            DispatchTarget::ClusterControl
        );
        // Shard join: routed to the targeted shard's Raft handle.
        assert_eq!(
            dispatch_target(&sample_join_shard(3)),
            DispatchTarget::Shard(3)
        );
        assert_eq!(
            dispatch_target(&sample_promote(GroupId::Control)),
            DispatchTarget::Control
        );
        assert_eq!(
            dispatch_target(&sample_promote(GroupId::Shard(3))),
            DispatchTarget::Shard(3)
        );
    }

    #[test]
    fn outer_tag_changes_routing_even_with_same_inner_payload() {
        // Same inner Vote(...) payload bytes, but different outer tag —
        // dispatch target must follow the outer tag. This is the core
        // Risk #2 invariant: a serialised Shard(3, Vote) must not
        // dispatch to control just because its inner payload is also
        // a valid Vote.
        let same_inner = vote_request(1, 1);
        let as_control = MuxRaftRpcMessage::Control(ControlRpcMessage::Vote(same_inner.clone()));
        let as_shard = MuxRaftRpcMessage::Shard(3, ShardRpcMessage::Vote(same_inner));
        assert_ne!(
            dispatch_target(&as_control),
            dispatch_target(&as_shard),
            "outer tag must override inner payload similarity"
        );
    }

    #[test]
    fn wire_frame_roundtrips_for_each_variant() {
        for msg in [
            sample_control(),
            sample_shard(0),
            sample_shard(255),
            sample_join(),
            sample_promote(GroupId::Control),
            sample_promote(GroupId::Shard(42)),
        ] {
            let bytes = postcard::to_stdvec(&msg).expect("encode");
            let back: MuxRaftRpcMessage = postcard::from_bytes(&bytes).expect("decode");
            // The whole frame must round-trip and dispatch to the same
            // target. We compare via dispatch target rather than `==`
            // because the inner openraft request types don't impl
            // PartialEq.
            assert_eq!(dispatch_target(&msg), dispatch_target(&back));
        }
    }

    #[test]
    fn shard_bytes_do_not_decode_as_control() {
        // The plan's frame-multiplexing fuzz: bytes intended for shard 3
        // must not decode (or dispatch) as control. Postcard's tagged
        // enum encoding makes this strict — the discriminant byte
        // differs — and the test pins that contract.
        let shard_bytes = postcard::to_stdvec(&sample_shard(3)).unwrap();
        let decoded: MuxRaftRpcMessage = postcard::from_bytes(&shard_bytes).unwrap();
        assert!(matches!(decoded, MuxRaftRpcMessage::Shard(3, _)));
        assert_ne!(dispatch_target(&decoded), DispatchTarget::Control);
    }

    #[test]
    fn control_bytes_do_not_decode_as_shard() {
        let control_bytes = postcard::to_stdvec(&sample_control()).unwrap();
        let decoded: MuxRaftRpcMessage = postcard::from_bytes(&control_bytes).unwrap();
        assert!(matches!(decoded, MuxRaftRpcMessage::Control(_)));
        assert!(!matches!(decoded, MuxRaftRpcMessage::Shard(_, _)));
    }

    #[test]
    fn fuzzed_outer_discriminants_either_decode_or_reject() {
        // Frame-multiplexing fuzz — the plan's risk #2 test. For every
        // possible outer discriminant byte (postcard varint-encodes the
        // enum index), either decoding fails OR the decoded variant's
        // dispatch is consistent with its discriminant.
        //
        // Concretely: scan a small space of synthetic outer-tag bytes
        // (0x00..0x10) prepended to a known-good shard-style payload,
        // and assert no byte produces both a successful decode AND an
        // unintended target. A real adversarial payload would be larger;
        // this is a smoke version that pins the invariant.
        let inner_payload = postcard::to_stdvec(&sample_shard(7)).unwrap();
        // The first byte is the outer enum discriminant. Replace it
        // with each value in 0x00..0x10 and observe.
        for tag in 0u8..=0x0f {
            let mut bytes = inner_payload.clone();
            bytes[0] = tag;
            match postcard::from_bytes::<MuxRaftRpcMessage>(&bytes) {
                Ok(msg) => {
                    let target = dispatch_target(&msg);
                    // The decoded target must be consistent with the
                    // decoded variant. Mismatched-byte input that
                    // accidentally parsed as `Control` and dispatched
                    // to a shard would be the Risk #2 corruption mode.
                    match (&msg, target) {
                        (MuxRaftRpcMessage::Control(_), DispatchTarget::Control) => {}
                        (MuxRaftRpcMessage::Shard(id, _), DispatchTarget::Shard(t)) => {
                            assert_eq!(*id, t)
                        }
                        (
                            MuxRaftRpcMessage::JoinCluster {
                                group: GroupId::Control,
                                ..
                            },
                            DispatchTarget::ClusterControl,
                        ) => {}
                        (
                            MuxRaftRpcMessage::JoinCluster {
                                group: GroupId::Shard(id),
                                ..
                            },
                            DispatchTarget::Shard(t),
                        ) => assert_eq!(*id, t),
                        (
                            MuxRaftRpcMessage::PromoteMember {
                                group: GroupId::Control,
                                ..
                            },
                            DispatchTarget::Control,
                        ) => {}
                        (
                            MuxRaftRpcMessage::PromoteMember {
                                group: GroupId::Shard(id),
                                ..
                            },
                            DispatchTarget::Shard(t),
                        ) => assert_eq!(*id, t),
                        (m, t) => panic!(
                            "fuzz: mismatched variant {:?} dispatched to {:?}",
                            std::mem::discriminant(m),
                            t
                        ),
                    }
                }
                Err(_) => {
                    // Decode failure is the safe outcome — callers
                    // surface it as an InvalidRequest error.
                }
            }
        }
    }

    #[test]
    fn auth_purpose_distinguishes_groups() {
        let control = auth_purpose_for(GroupId::Control);
        let shard_3 = auth_purpose_for(GroupId::Shard(3));
        let shard_5 = auth_purpose_for(GroupId::Shard(5));

        assert_ne!(control, shard_3, "control vs shard purposes must differ");
        assert_ne!(shard_3, shard_5, "different shards must differ");

        // Stable on the wire — these byte patterns are part of the
        // bootstrap contract once they're mixed into HMAC input.
        assert_eq!(control, [1, 0, 0, 0]);
        assert_eq!(shard_3, [2, 0, 0, 3]);
        assert_eq!(shard_5, [2, 0, 0, 5]);
    }

    #[test]
    fn auth_purpose_for_shard_max_does_not_collide_with_control() {
        // u16::MAX shards all derive purposes distinct from control.
        for id in 0u16..=u16::MAX {
            let p = auth_purpose_for(GroupId::Shard(id));
            assert_ne!(
                p[0], 1,
                "shard {} purpose must not start with control tag",
                id
            );
        }
    }

    #[test]
    fn rpc_error_info_can_be_carried_in_mux_response() {
        // We reuse the legacy [`RpcErrorInfo`] / [`RpcErrorKind`]
        // structures so existing client retry logic works unchanged.
        // This tests the wiring; the retry-semantics tests live with
        // those types directly.
        let resp = MuxRaftRpcResponse::Error(RpcErrorInfo::new(
            RpcErrorKind::LeadershipChanged,
            "term moved on".to_string(),
        ));
        let bytes = postcard::to_stdvec(&resp).expect("encode");
        let back: MuxRaftRpcResponse = postcard::from_bytes(&bytes).expect("decode");
        match back {
            MuxRaftRpcResponse::Error(info) => {
                assert_eq!(info.message, "term moved on");
                assert!(matches!(info.kind, RpcErrorKind::LeadershipChanged));
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }
}
