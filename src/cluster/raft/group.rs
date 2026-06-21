//! Per-Raft-group polymorphism.
//!
//! `RaftStore<G>` is generic over a [`GroupKind`] trait that bundles the
//! openraft `TypeConfig`, the SM wrapper, the SM state type, and a few
//! thin operations (apply, snapshot, deserialize, new). Two impls:
//!
//! - [`ControlGroupKind`] — wraps [`control::ControlStateMachine`].
//! - [`ShardGroupKind`] — wraps [`shard::ShardStateMachine`]. Each shard's
//!   storage instance is parameterised by its `shard_id` at construction;
//!   the kind itself is shared across all shard groups since openraft
//!   needs a single `'static` type per `Raft<Cfg>`.

use std::io::Cursor;
use std::sync::Arc;

use openraft::BasicNode;
use tokio::sync::RwLock;

use super::commands::{ControlCommand, ControlResponse, ShardCommand, ShardResponse};
use super::state_machine::{
    control::{ControlState, ControlStateMachine},
    shard::{ShardState, ShardStateMachine},
};
use super::types::{ControlConfig, RaftNodeId, ShardConfig, ShardId};

/// Bundles the per-group types the storage layer needs.
///
/// Both impls produce an openraft-compatible `RaftTypeConfig` whose
/// node id is [`RaftNodeId`], node type is [`BasicNode`], snapshot
/// transport is `Cursor<Vec<u8>>`, and entry type is `Entry<Cfg>` (the
/// macro default). The shard kind layers a runtime `shard_id` on top of
/// the shared static `ShardConfig`.
pub trait GroupKind: Sized + Send + Sync + 'static {
    /// The openraft type config for this group. Pinned to
    /// `(RaftNodeId, BasicNode, Cursor<Vec<u8>>)` because the rest of the
    /// codebase (network, storage paths) hard-codes those. Pinning
    /// `Entry = openraft::Entry<Self::Cfg>` lets the storage layer use the
    /// concrete `Entry<Cfg>` type (with its inherent `log_id`/`payload`
    /// fields) without dropping into trait-method-only access.
    ///
    /// `D: Clone` is required because the apply path clones the command
    /// out of an immutable `EntryPayload::Normal(ref cmd)` ref to feed it
    /// into the `async fn apply` (which takes the command by value so the
    /// SM can match-by-move).
    type Cfg: openraft::RaftTypeConfig<
            NodeId = RaftNodeId,
            Node = BasicNode,
            SnapshotData = Cursor<Vec<u8>>,
            Entry = openraft::Entry<Self::Cfg>,
            D: Clone,
        >;

    /// State-machine wrapper. Holds the `Arc<RwLock<State>>` plus any hooks.
    /// Cloneable so the storage layer can hand a clone to snapshot
    /// builders without taking the lock.
    type Sm: Clone + Send + Sync + 'static;

    /// State held inside the SM wrapper. This is what gets serialised into
    /// snapshots and what `install_snapshot` replaces. Carries `Default`
    /// so a fresh node can construct an empty state.
    type State: Default
        + Clone
        + Send
        + Sync
        + 'static
        + serde::Serialize
        + serde::de::DeserializeOwned;

    /// Construction parameter for [`Self::new_sm`]. For the control kind
    /// this is `()`; for the shard kind it carries the shard id so each
    /// shard's SM is wired with its own identity.
    type SmInit: Clone + Send + Sync + 'static;

    /// Build a fresh state machine.
    fn new_sm(init: Self::SmInit) -> Self::Sm;

    /// Borrow the inner `Arc<RwLock<State>>` for atomic state transitions.
    /// Storage takes a write guard on this when installing a snapshot or
    /// applying state in lock-step with the index advance.
    fn sm_state_arc(sm: &Self::Sm) -> Arc<RwLock<Self::State>>;

    /// Decode snapshot bytes WITHOUT mutating the SM. On corrupt input
    /// returns `InvalidData`; storage propagates that as a
    /// `StorageError` so an open-raft snapshot install rejects the bad
    /// bytes instead of leaving the SM half-restored.
    fn deserialize_state(bytes: &[u8]) -> std::io::Result<Self::State>;

    /// Serialise the current state.
    ///
    /// Returns an `impl Future + Send` (rather than `async fn`) so trait
    /// impls' returned futures are guaranteed `Send`. Without this the
    /// storage layer cannot satisfy openraft's `RaftStorage` trait
    /// (`apply_to_state_machine`, `build_snapshot`) which require Send
    /// futures via openraft's `add_async_trait` annotation.
    fn snapshot(sm: &Self::Sm) -> impl std::future::Future<Output = Vec<u8>> + Send + '_;

    /// Apply a single committed log entry's command to the SM and return
    /// its response.
    fn apply(
        sm: &Self::Sm,
        cmd: <Self::Cfg as openraft::RaftTypeConfig>::D,
    ) -> impl std::future::Future<Output = <Self::Cfg as openraft::RaftTypeConfig>::R> + Send + '_;

    /// Generic Ok response for `EntryPayload::Blank` and
    /// `EntryPayload::Membership` entries.
    fn ok_response() -> <Self::Cfg as openraft::RaftTypeConfig>::R;

    /// Per-group on-disk subdirectory under `{raft_log_dir}/node-{id}/`.
    /// Used by storage to avoid log/snapshot collisions across groups.
    /// Examples: `"control"`, `"shard-3"`.
    fn dir_segment(init: &Self::SmInit) -> String;
}

// ============================================================================
// ControlGroupKind — control group of the sharded layout.
// ============================================================================

/// Kind for the cluster-wide control group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlGroupKind;

impl GroupKind for ControlGroupKind {
    type Cfg = ControlConfig;
    type Sm = ControlStateMachine;
    type State = ControlState;
    type SmInit = ();

    fn new_sm(_init: ()) -> Self::Sm {
        ControlStateMachine::new()
    }

    fn sm_state_arc(sm: &Self::Sm) -> Arc<RwLock<Self::State>> {
        sm.state_arc()
    }

    fn deserialize_state(bytes: &[u8]) -> std::io::Result<Self::State> {
        ControlStateMachine::deserialize_state(bytes)
    }

    async fn snapshot(sm: &Self::Sm) -> Vec<u8> {
        sm.snapshot().await
    }

    async fn apply(sm: &Self::Sm, cmd: ControlCommand) -> ControlResponse {
        sm.apply_command(cmd).await
    }

    fn ok_response() -> ControlResponse {
        ControlResponse::Ok
    }

    fn dir_segment(_init: &Self::SmInit) -> String {
        "control".to_string()
    }
}

// ============================================================================
// ShardGroupKind — one of N shard groups in the sharded layout.
// ============================================================================

/// Kind for a shard group in the sharded layout.
///
/// `Sm` is constructed with a [`ShardId`] passed in via [`Self::SmInit`];
/// the storage and reconciler keep that id alongside their own state so
/// per-group files can be co-located by name (`shard-{i}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardGroupKind;

impl GroupKind for ShardGroupKind {
    type Cfg = ShardConfig;
    type Sm = ShardStateMachine;
    type State = ShardState;
    type SmInit = ShardId;

    fn new_sm(init: ShardId) -> Self::Sm {
        ShardStateMachine::new(init)
    }

    fn sm_state_arc(sm: &Self::Sm) -> Arc<RwLock<Self::State>> {
        sm.state_arc()
    }

    fn deserialize_state(bytes: &[u8]) -> std::io::Result<Self::State> {
        ShardStateMachine::deserialize_state(bytes)
    }

    async fn snapshot(sm: &Self::Sm) -> Vec<u8> {
        sm.snapshot().await
    }

    async fn apply(sm: &Self::Sm, cmd: ShardCommand) -> ShardResponse {
        sm.apply_command(cmd).await
    }

    fn ok_response() -> ShardResponse {
        ShardResponse::Ok
    }

    fn dir_segment(init: &Self::SmInit) -> String {
        format!("shard-{}", init)
    }
}

#[cfg(test)]
mod tests {
    //! Smoke tests confirming every `GroupKind` impl round-trips.

    use super::*;

    fn control_sm() -> ControlStateMachine {
        ControlGroupKind::new_sm(())
    }

    fn shard_sm(id: ShardId) -> ShardStateMachine {
        ShardGroupKind::new_sm(id)
    }

    #[tokio::test]
    async fn control_kind_apply_and_snapshot_roundtrip() {
        let sm = control_sm();
        let resp = ControlGroupKind::apply(&sm, ControlCommand::Noop).await;
        assert!(matches!(resp, ControlResponse::Ok));
        let bytes = ControlGroupKind::snapshot(&sm).await;
        let state = ControlGroupKind::deserialize_state(&bytes).unwrap();
        assert_eq!(state.version, 1);
    }

    #[tokio::test]
    async fn shard_kind_apply_and_snapshot_roundtrip() {
        let sm = shard_sm(7);
        let resp = ShardGroupKind::apply(&sm, ShardCommand::Noop).await;
        assert!(matches!(resp, ShardResponse::Ok));
        let bytes = ShardGroupKind::snapshot(&sm).await;
        let state = ShardGroupKind::deserialize_state(&bytes).unwrap();
        assert_eq!(state.shard_id, 7);
        assert_eq!(state.version, 1);
    }

    #[test]
    fn dir_segments_are_distinct_per_group() {
        assert_eq!(ControlGroupKind::dir_segment(&()), "control");
        assert_eq!(ShardGroupKind::dir_segment(&3), "shard-3");
        assert_eq!(ShardGroupKind::dir_segment(&0), "shard-0");
    }

    #[test]
    fn ok_response_per_kind_is_ok_variant() {
        match ControlGroupKind::ok_response() {
            ControlResponse::Ok => {}
            _ => panic!("expected Ok"),
        }
        match ShardGroupKind::ok_response() {
            ShardResponse::Ok => {}
            _ => panic!("expected Ok"),
        }
    }

    #[tokio::test]
    async fn deserialize_state_rejects_corrupt_bytes() {
        for label in ["control", "shard"] {
            let result: std::io::Result<()> = match label {
                "control" => ControlGroupKind::deserialize_state(&[0xff; 16]).map(|_| ()),
                "shard" => ShardGroupKind::deserialize_state(&[0xff; 16]).map(|_| ()),
                _ => unreachable!(),
            };
            let err = result.unwrap_err();
            assert_eq!(
                err.kind(),
                std::io::ErrorKind::InvalidData,
                "{} kind must reject corrupt bytes with InvalidData",
                label
            );
        }
    }
}
