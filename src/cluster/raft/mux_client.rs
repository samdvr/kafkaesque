//! Client side of the multiplexed Raft RPC port.
//!
//! Each Raft group inside [`super::cluster::RaftCluster`] needs to drive
//! outbound `AppendEntries` / `Vote` / `InstallSnapshot` RPCs against
//! peers. The legacy [`super::network::RaftNetworkFactoryImpl`] provides
//! that for the single-group `RaftNode`; this module is the multiplexed
//! analog. Two factories are exported — one per group kind — both pointing
//! at the *same* address book so a broker registered into the cluster
//! shows up to every group's network at once.
//!
//! # Layout
//!
//! ```text
//!          MuxFactoryShared { addrs, auth_keys, tls }    ◄── one per cluster
//!                  ▲                  ▲
//!                  │                  │
//!         MuxControlFactory     MuxShardFactory { shard_id: i }   (× N)
//!                  │                  │
//!                  ▼                  ▼
//!         MuxControlClient      MuxShardClient { shard_id: i }
//!         (one per peer)        (one per peer per shard)
//! ```
//!
//! Each `MuxXxxClient` holds a single cached TCP connection to its peer.
//! The plan calls for *per-group* connections (control vs shard-i live on
//! independent sockets) so a slow control message can't park heartbeats
//! for shard 3, and vice versa. Since openraft constructs one network
//! client per `(group, peer)` automatically when each group has its own
//! factory, the layout above falls out for free.
//!
//! # Wire framing
//!
//! Every outbound payload is wrapped in a [`MuxRaftRpcMessage`]:
//!
//! - Control client → `MuxRaftRpcMessage::Control(ControlRpcMessage::*)`
//! - Shard client (id=i) → `MuxRaftRpcMessage::Shard(i,
//!   ShardRpcMessage::*)`
//!
//! HMAC + TLS framing is identical to the legacy path — same
//! [`super::auth::write_rpc_frame`] / [`super::auth::read_rpc_frame`]
//! helpers, same TCP+TLS connect path. The frame *contents* are the only
//! thing that differs, so the legacy path's wire compatibility is not
//! perturbed.
//!
//! # What's left for follow-up
//!
//! This commit gets openraft talking over the multiplexed wire. It
//! deliberately **omits**:
//!
//! - Per-target retry with exponential backoff.
//! - Circuit breaker.
//! - Dual control/bulk lanes (one socket per priority class).
//! - `forward_client_write_with_term` and `request_cluster_join` on the
//!   mux port.
//!
//! Those land alongside the runnable bootstrap in step 5b — at that point
//! we'll be able to test resilience under churn end-to-end. Splitting
//! them out keeps this commit's surface area auditable.

#![allow(dead_code)] // wired in step 5b when RaftCluster::new bootstraps a real cluster

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tokio::sync::{Mutex, RwLock};

use super::auth::{
    FramePurpose, MAX_RPC_MESSAGE_BYTES, PeerScope, RaftAuthKeys, read_rpc_frame, write_rpc_frame,
};
use super::mux::{ControlRpcMessage, MuxRaftRpcMessage, MuxRaftRpcResponse, ShardRpcMessage};
use super::network::{MaybeTlsStreamClient, RpcErrorInfo, RpcErrorKind, connect_raft};
use super::tls::RaftTlsConfig;
use super::types::{ControlConfig, RaftNodeId, ShardConfig, ShardId};

// ============================================================================
// Address book + factory shared state.
// ============================================================================

/// Shared address map: `node_id → raft_addr`. One per cluster; cloned into
/// every group's factory so a write to it (broker join, factory's
/// `new_client` callback) is visible to every group's network at once.
pub type MuxAddrBook = Arc<RwLock<BTreeMap<RaftNodeId, String>>>;

/// Build a fresh, empty address book.
pub fn new_addr_book() -> MuxAddrBook {
    Arc::new(RwLock::new(BTreeMap::new()))
}

/// Cluster-wide network state shared by every group's factory.
///
/// Holding this in an `Arc<MuxFactoryShared>` and giving every
/// `MuxControlFactory` / `MuxShardFactory` a clone means:
///
/// - One address book — every factory sees the same broker addresses.
/// - One auth key handle — the cluster secret / join token rotation
///   reaches every group.
/// - One TLS configuration — there is no per-group TLS.
pub struct MuxFactoryShared {
    addrs: MuxAddrBook,
    auth_keys: Arc<RaftAuthKeys>,
    tls: Option<RaftTlsConfig>,
}

impl MuxFactoryShared {
    /// Bundle the shared state.
    pub fn new(addrs: MuxAddrBook, auth_keys: Arc<RaftAuthKeys>, tls: Option<RaftTlsConfig>) -> Self {
        Self {
            addrs,
            auth_keys,
            tls,
        }
    }

    /// Borrow the address book — useful for callers that need to pre-seed
    /// it from `RaftConfig::cluster_members` before any factory hands out
    /// a client.
    pub fn addrs(&self) -> &MuxAddrBook {
        &self.addrs
    }
}

/// Build the per-group factories for a cluster with `metadata_shards`
/// shards. The control factory plus every shard factory share one
/// [`MuxFactoryShared`] under the hood.
///
/// Returned in `(control, [shard_0, shard_1, ..., shard_{N-1}])` order.
/// Callers feed each factory into the corresponding `Raft<Cfg>::new` call.
pub fn build_mux_factories(
    shared: Arc<MuxFactoryShared>,
    metadata_shards: u16,
) -> (MuxControlFactory, Vec<MuxShardFactory>) {
    let control = MuxControlFactory {
        shared: shared.clone(),
    };
    let shards = (0..metadata_shards)
        .map(|id| MuxShardFactory {
            shared: shared.clone(),
            shard_id: id,
        })
        .collect();
    (control, shards)
}

// ============================================================================
// Factories.
// ============================================================================

/// `RaftNetworkFactory<ControlConfig>` for the cluster-wide control group.
#[derive(Clone)]
pub struct MuxControlFactory {
    shared: Arc<MuxFactoryShared>,
}

impl MuxControlFactory {
    /// Update the address for `node_id`. Same `add_node` shape as the
    /// legacy [`super::network::RaftNetworkFactoryImpl`] so the
    /// coordinator's `add_learner` path stays uniform.
    pub async fn add_node(&self, node_id: RaftNodeId, addr: String) {
        self.shared.addrs.write().await.insert(node_id, addr);
    }

    pub async fn get_node_addr(&self, node_id: RaftNodeId) -> Option<String> {
        self.shared.addrs.read().await.get(&node_id).cloned()
    }
}

/// `RaftNetworkFactory<ShardConfig>` for one shard group. The `shard_id`
/// is the outer wire-frame tag every outbound RPC carries.
#[derive(Clone)]
pub struct MuxShardFactory {
    shared: Arc<MuxFactoryShared>,
    shard_id: ShardId,
}

impl MuxShardFactory {
    /// The shard id this factory tags its outbound frames with.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub async fn add_node(&self, node_id: RaftNodeId, addr: String) {
        self.shared.addrs.write().await.insert(node_id, addr);
    }

    pub async fn get_node_addr(&self, node_id: RaftNodeId) -> Option<String> {
        self.shared.addrs.read().await.get(&node_id).cloned()
    }
}

impl RaftNetworkFactory<ControlConfig> for MuxControlFactory {
    type Network = MuxControlClient;

    async fn new_client(&mut self, target: RaftNodeId, node: &BasicNode) -> Self::Network {
        // Same idempotent-add behaviour as the legacy factory: openraft
        // calls `new_client` whenever it needs to talk to a peer, and
        // we use that as a signal to record / refresh the address.
        self.shared
            .addrs
            .write()
            .await
            .insert(target, node.addr.clone());
        MuxControlClient {
            inner: MuxConnInner::new(node.addr.clone(), self.shared.clone()),
        }
    }
}

impl RaftNetworkFactory<ShardConfig> for MuxShardFactory {
    type Network = MuxShardClient;

    async fn new_client(&mut self, target: RaftNodeId, node: &BasicNode) -> Self::Network {
        self.shared
            .addrs
            .write()
            .await
            .insert(target, node.addr.clone());
        MuxShardClient {
            inner: MuxConnInner::new(node.addr.clone(), self.shared.clone()),
            shard_id: self.shard_id,
        }
    }
}

// ============================================================================
// Connection internals (shared between control and shard clients).
// ============================================================================

/// Per-peer connection state shared between [`MuxControlClient`] and
/// [`MuxShardClient`].
///
/// Holds one cached TCP connection per peer using a take-and-commit
/// pattern: the cell empties for the duration of an in-flight RPC and is
/// either re-populated with the same stream on success or left empty on
/// any failure (including cancellation). This matches the cancellation
/// safety of the legacy connection (`network::RaftNetworkConnection`):
/// without the take-and-commit, a cancelled future would leave a
/// half-written stream in the cell and the next caller would read
/// another caller's response bytes.
struct MuxConnInner {
    target_addr: String,
    cached_conn: Mutex<Option<MaybeTlsStreamClient>>,
    shared: Arc<MuxFactoryShared>,
}

impl MuxConnInner {
    fn new(target_addr: String, shared: Arc<MuxFactoryShared>) -> Self {
        Self {
            target_addr,
            cached_conn: Mutex::new(None),
            shared,
        }
    }

    /// Send a multiplexed frame and read back the response.
    async fn send(&self, frame: MuxRaftRpcMessage) -> std::io::Result<MuxRaftRpcResponse> {
        let data = postcard::to_stdvec(&frame)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut guard = self.cached_conn.lock().await;
        let mut stream = match guard.take() {
            Some(s) => s,
            None => connect_raft(&self.target_addr, self.shared.tls.as_ref()).await?,
        };

        match Self::do_rpc(&mut stream, &data, &self.shared.auth_keys).await {
            Ok(resp) => {
                // Commit the still-clean stream back to the cache.
                *guard = Some(stream);
                Ok(resp)
            }
            Err(e) => {
                // Drop the stream — its frame buffer may be in a partial
                // state. Next caller reconnects from scratch.
                Err(e)
            }
        }
    }

    /// Perform one request/response round-trip on an existing stream.
    /// Mirrors `network::RaftNetworkConnection::do_rpc` but reads back a
    /// [`MuxRaftRpcResponse`].
    async fn do_rpc(
        stream: &mut MaybeTlsStreamClient,
        data: &[u8],
        auth_keys: &RaftAuthKeys,
    ) -> std::io::Result<MuxRaftRpcResponse> {
        // `is_join = false` — multiplexed `JoinCluster` frames travel
        // through their own helper (lands with the bootstrap commit) and
        // don't reach `RaftNetwork`'s RPC verbs. Steady-state Raft RPCs
        // always use the cluster purpose.
        write_rpc_frame(stream, auth_keys, false, data).await?;
        let (_purpose, response_buf) = read_rpc_frame(
            stream,
            auth_keys,
            &PeerScope::unknown(),
            MAX_RPC_MESSAGE_BYTES,
        )
        .await?;
        postcard::from_bytes(&response_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

// ============================================================================
// Per-group `RaftNetwork` impls.
// ============================================================================

/// `RaftNetwork<ControlConfig>` connection. Wraps every outbound request
/// in a [`MuxRaftRpcMessage::Control`] frame.
pub struct MuxControlClient {
    inner: MuxConnInner,
}

impl RaftNetwork<ControlConfig> for MuxControlClient {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<ControlConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let frame = MuxRaftRpcMessage::Control(ControlRpcMessage::AppendEntries(req));
        let resp = self.inner.send(frame).await.map_err(network_error)?;
        match resp {
            MuxRaftRpcResponse::AppendEntries(r) => Ok(r),
            MuxRaftRpcResponse::Error(info) => Err(rpc_to_network(info)),
            other => Err(unexpected_variant("AppendEntries", &other)),
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let frame = MuxRaftRpcMessage::Control(ControlRpcMessage::Vote(req));
        let resp = self.inner.send(frame).await.map_err(network_error)?;
        match resp {
            MuxRaftRpcResponse::Vote(r) => Ok(r),
            MuxRaftRpcResponse::Error(info) => Err(rpc_to_network(info)),
            other => Err(unexpected_variant("Vote", &other)),
        }
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<ControlConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>>,
    > {
        let frame = MuxRaftRpcMessage::Control(ControlRpcMessage::InstallSnapshot(req));
        let resp = self.inner.send(frame).await.map_err(network_error_install)?;
        match resp {
            MuxRaftRpcResponse::InstallSnapshot(r) => Ok(r),
            MuxRaftRpcResponse::Error(info) => Err(rpc_to_network_install(info)),
            other => Err(unexpected_variant_install("InstallSnapshot", &other)),
        }
    }
}

/// `RaftNetwork<ShardConfig>` connection. Wraps every outbound request in
/// a [`MuxRaftRpcMessage::Shard(shard_id, …)`] frame.
pub struct MuxShardClient {
    inner: MuxConnInner,
    shard_id: ShardId,
}

impl RaftNetwork<ShardConfig> for MuxShardClient {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<ShardConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let frame =
            MuxRaftRpcMessage::Shard(self.shard_id, ShardRpcMessage::AppendEntries(req));
        let resp = self.inner.send(frame).await.map_err(network_error)?;
        match resp {
            MuxRaftRpcResponse::AppendEntries(r) => Ok(r),
            MuxRaftRpcResponse::Error(info) => Err(rpc_to_network(info)),
            other => Err(unexpected_variant("AppendEntries", &other)),
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let frame = MuxRaftRpcMessage::Shard(self.shard_id, ShardRpcMessage::Vote(req));
        let resp = self.inner.send(frame).await.map_err(network_error)?;
        match resp {
            MuxRaftRpcResponse::Vote(r) => Ok(r),
            MuxRaftRpcResponse::Error(info) => Err(rpc_to_network(info)),
            other => Err(unexpected_variant("Vote", &other)),
        }
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<ShardConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>>,
    > {
        let frame =
            MuxRaftRpcMessage::Shard(self.shard_id, ShardRpcMessage::InstallSnapshot(req));
        let resp = self.inner.send(frame).await.map_err(network_error_install)?;
        match resp {
            MuxRaftRpcResponse::InstallSnapshot(r) => Ok(r),
            MuxRaftRpcResponse::Error(info) => Err(rpc_to_network_install(info)),
            other => Err(unexpected_variant_install("InstallSnapshot", &other)),
        }
    }
}

// ============================================================================
// Error mapping helpers.
// ============================================================================

/// Map an I/O error from the framing layer onto openraft's RPC error.
fn network_error<E>(e: std::io::Error) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, E>>
where
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&e))
}

/// `install_snapshot` uses a different inner error parameter than vote /
/// append-entries, so it needs its own mapper.
fn network_error_install(
    e: std::io::Error,
) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>> {
    RPCError::Network(NetworkError::new(&e))
}

/// Map a structured RPC error from the server onto openraft's RPC error.
/// We surface the structured kind via the wrapped `io::Error`'s kind so
/// the caller can recover retry semantics — same approach the legacy
/// path takes.
fn rpc_to_network<E>(
    info: RpcErrorInfo,
) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, E>>
where
    E: std::error::Error,
{
    let io_kind = match info.kind {
        RpcErrorKind::LeadershipChanged | RpcErrorKind::NotLeader { .. } => {
            std::io::ErrorKind::NotConnected
        }
        RpcErrorKind::ForwardLoopDetected => std::io::ErrorKind::ConnectionRefused,
        RpcErrorKind::InvalidRequest => std::io::ErrorKind::InvalidInput,
        RpcErrorKind::Internal => std::io::ErrorKind::Other,
        RpcErrorKind::Timeout => std::io::ErrorKind::TimedOut,
        RpcErrorKind::Network => std::io::ErrorKind::ConnectionAborted,
    };
    RPCError::Network(NetworkError::new(&std::io::Error::new(io_kind, info.message)))
}

fn rpc_to_network_install(
    info: RpcErrorInfo,
) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>> {
    let io_kind = match info.kind {
        RpcErrorKind::LeadershipChanged | RpcErrorKind::NotLeader { .. } => {
            std::io::ErrorKind::NotConnected
        }
        RpcErrorKind::ForwardLoopDetected => std::io::ErrorKind::ConnectionRefused,
        RpcErrorKind::InvalidRequest => std::io::ErrorKind::InvalidInput,
        RpcErrorKind::Internal => std::io::ErrorKind::Other,
        RpcErrorKind::Timeout => std::io::ErrorKind::TimedOut,
        RpcErrorKind::Network => std::io::ErrorKind::ConnectionAborted,
    };
    RPCError::Network(NetworkError::new(&std::io::Error::new(io_kind, info.message)))
}

fn unexpected_variant<E>(
    expected: &str,
    actual: &MuxRaftRpcResponse,
) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, E>>
where
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "unexpected mux response: expected {}, got {:?}",
            expected,
            std::mem::discriminant(actual)
        ),
    )))
}

fn unexpected_variant_install(
    expected: &str,
    actual: &MuxRaftRpcResponse,
) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>> {
    RPCError::Network(NetworkError::new(&std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "unexpected mux response: expected {}, got {:?}",
            expected,
            std::mem::discriminant(actual)
        ),
    )))
}

// `FramePurpose` is intentionally unused at this layer for now — the
// purpose tag is selected inside `write_rpc_frame` via `auth_keys`. Kept
// imported so a future refactor that lifts the purpose decision into the
// caller doesn't have to re-add the import.
const _: Option<FramePurpose> = None;

#[cfg(test)]
mod tests {
    //! These tests cover the address-book sharing contract — the part of
    //! the factory that's exercisable without TCP. The full
    //! send/receive path needs a live `RaftRpcServer` running the mux
    //! dispatcher; that integration test lands with the runnable
    //! bootstrap in step 5b.
    //!
    //! What we pin here:
    //!
    //! - One shared `MuxAddrBook` is visible across control + every shard
    //!   factory (the cluster-wide invariant).
    //! - Adding a node through any factory makes it visible from every
    //!   other factory (no per-factory copies).
    //! - Shard factories carry their distinct shard ids.
    //! - `build_mux_factories(N)` produces factories for shard ids
    //!   `0..N` in order.
    //!
    //! Without these, a broker that joins via control would not be
    //! reachable from any shard's network until the next factory
    //! callback — an unbounded delay.
    use super::*;

    fn shared_state() -> Arc<MuxFactoryShared> {
        Arc::new(MuxFactoryShared::new(
            new_addr_book(),
            Arc::new(RaftAuthKeys::dev_unauthenticated()),
            None,
        ))
    }

    #[tokio::test]
    async fn build_mux_factories_emits_correct_shard_ids() {
        let (control, shards) = build_mux_factories(shared_state(), 8);
        let _ = control;
        assert_eq!(shards.len(), 8);
        for (i, f) in shards.iter().enumerate() {
            assert_eq!(f.shard_id(), i as ShardId);
        }
    }

    #[tokio::test]
    async fn build_mux_factories_with_one_shard() {
        let (_, shards) = build_mux_factories(shared_state(), 1);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id(), 0);
    }

    #[tokio::test]
    async fn build_mux_factories_with_zero_shards_returns_empty() {
        // ShardRouter clamps 0 to 1 for routing safety, but the factory
        // builder is a lower-level helper that should faithfully emit
        // what was asked. Config validation rejects 0 metadata_shards;
        // this is a type-level contract that 0 → empty fleet.
        let (_, shards) = build_mux_factories(shared_state(), 0);
        assert!(shards.is_empty());
    }

    #[tokio::test]
    async fn add_node_through_control_is_visible_to_shards() {
        // Cluster-wide invariant: address writes propagate across every
        // group's network at once, not after the next factory callback.
        let shared = shared_state();
        let (control, shards) = build_mux_factories(shared.clone(), 4);
        control.add_node(7, "127.0.0.1:7000".into()).await;
        for (i, f) in shards.iter().enumerate() {
            assert_eq!(
                f.get_node_addr(7).await,
                Some("127.0.0.1:7000".into()),
                "shard {} did not see address registered via control",
                i
            );
        }
    }

    #[tokio::test]
    async fn add_node_through_shard_is_visible_to_control_and_other_shards() {
        // Inverse direction — addresses registered against any shard
        // must be visible across the cluster, including the control
        // group. Otherwise broker-discovery via shard heartbeat couldn't
        // backfill control's address book.
        let (control, shards) = build_mux_factories(shared_state(), 4);
        shards[2].add_node(11, "127.0.0.1:9999".into()).await;
        assert_eq!(
            control.get_node_addr(11).await,
            Some("127.0.0.1:9999".into())
        );
        for (i, f) in shards.iter().enumerate() {
            assert_eq!(
                f.get_node_addr(11).await,
                Some("127.0.0.1:9999".into()),
                "shard {} did not see address registered via shard 2",
                i
            );
        }
    }

    #[tokio::test]
    async fn factories_share_one_address_book_handle() {
        // Pin the shape: factories must not snapshot the address book at
        // construction (a `BTreeMap::clone()` would silently break the
        // shared-visibility contract above). We verify by checking the
        // shared `addrs` Arc returned by `MuxFactoryShared::addrs()` is
        // the same allocation across factories.
        let shared = shared_state();
        let (_control, shards) = build_mux_factories(shared.clone(), 3);
        let book_via_shared = shared.addrs();
        // The shards' factories don't expose their inner shared, but we
        // can still verify the propagation contract via the address-book
        // round trip above. Here we just assert the shared handle is
        // singular — `Arc::strong_count >= 1 + N + 1` (1 for `shared`,
        // N shard factories, 1 control factory). Bind the control
        // factory above (not `_`) so it isn't dropped before the count
        // is taken.
        assert!(Arc::strong_count(&shared) >= 1 + shards.len() + 1);
        // And the address book is still the same allocation we gave it.
        assert!(Arc::ptr_eq(&shared.addrs, book_via_shared));
    }

    #[tokio::test]
    async fn get_node_addr_returns_none_for_unknown_node() {
        let (control, shards) = build_mux_factories(shared_state(), 2);
        assert_eq!(control.get_node_addr(404).await, None);
        for f in &shards {
            assert_eq!(f.get_node_addr(404).await, None);
        }
    }

    #[tokio::test]
    async fn add_node_overwrites_existing_address() {
        // Re-registering a node id with a new address (e.g. broker
        // restarted on a different port) updates the entry. Regression
        // guard: a `or_insert_with` would silently keep the stale addr.
        let (control, _) = build_mux_factories(shared_state(), 1);
        control.add_node(5, "127.0.0.1:1000".into()).await;
        control.add_node(5, "127.0.0.1:2000".into()).await;
        assert_eq!(
            control.get_node_addr(5).await,
            Some("127.0.0.1:2000".into())
        );
    }
}
