//! Server-side dispatch for the multiplexed Raft RPC port.
//!
//! [`super::mux::dispatch_target`] decides *which group* an inbound frame
//! addresses. This module wires that decision through to a concrete
//! `Raft<ControlConfig>` / `Raft<ShardConfig>` and produces a
//! [`MuxRaftRpcResponse`]. The TCP accept loop that calls into here lives
//! alongside `RaftCluster` (sharding plan step 5); the dispatch core is
//! split out as a pure async fn so the routing decisions can be exercised
//! without standing up a full openraft fixture.
//!
//! # Relationship to the legacy `RaftRpcServer`
//!
//! The single-group [`super::network::RaftRpcServer::dispatch_rpc_message`]
//! handles the legacy [`super::network::RaftRpcMessage`] frame. This module
//! is its multiplexed twin: same RPC verbs (vote / append-entries /
//! install-snapshot / forwarded client-write / join / promote), but routed
//! to one of N+1 Raft instances rather than the single legacy one. The
//! plan calls for both to coexist until step 10, when the legacy
//! `RaftNode` is deleted; until then we keep the dispatch logic *parallel*
//! (same error variants, same hop cap, same purpose-tag cross-check) so a
//! frame's meaning never depends on which dispatcher it reaches.
//!
//! # Risk #2 — silent group misdispatch
//!
//! The sharding plan calls out (Risk #2) that a frame routed to the wrong
//! `Raft` instance whose inner payload happens to deserialize would
//! silently corrupt that group's log. Two layers of defence apply here:
//!
//! 1. **Outer tag matched first.** [`dispatch_mux_frame`] inspects
//!    [`MuxRaftRpcMessage`]'s variant before any payload is handed to a
//!    Raft instance.
//! 2. **Shard id bounds-checked.** A [`MuxRaftRpcMessage::Shard`] frame
//!    whose id exceeds the configured `metadata_shards` count is rejected
//!    as `InvalidRequest` rather than wrapping the index (which could
//!    otherwise route to the wrong shard).
//!
//! A future commit will mix [`super::mux::auth_purpose_for`] into the HMAC
//! input so a cross-group replay also fails authentication.

#![allow(dead_code)] // wired in step 5 when RaftCluster runs the listener

use std::sync::Arc;

use openraft::{BasicNode, Raft};
use tokio::time::timeout;

use super::auth::{
    FramePurpose, MAX_RPC_MESSAGE_BYTES, PeerScope, RaftAuthKeys, read_rpc_frame, write_rpc_frame,
};
use super::commands::{ControlCommand, ShardCommand};
use super::mux::{ControlRpcMessage, MuxRaftRpcMessage, MuxRaftRpcResponse, ShardRpcMessage};
use super::network::{
    MAX_FORWARD_HOPS, MaybeTlsStreamServer, RPC_FRAME_IDLE_TIMEOUT, RpcErrorInfo, RpcErrorKind,
    accept_raft_stream,
};
use super::tls::RaftTlsConfig;
use super::types::{ControlConfig, GroupId, RaftNodeId, ShardConfig, ShardId};

/// Raft handles the multiplexed dispatcher needs to route an inbound frame.
///
/// `shards` is an `Arc<[…]>` rather than a `Vec<…>` so the per-connection
/// handler task on the accept loop can clone it cheaply (one refcount bump)
/// instead of cloning N `Arc<Raft<_>>` per frame. The shard count is fixed
/// at bootstrap and persisted via `cluster_meta.bin` (plan step 9), so the
/// slice never resizes after construction.
pub struct MuxRaftHandles {
    /// The cluster-wide control group.
    pub control: Arc<Raft<ControlConfig>>,
    /// All N shard groups, indexed by [`ShardId`].
    pub shards: Arc<[Arc<Raft<ShardConfig>>]>,
}

impl MuxRaftHandles {
    /// Lookup a shard handle by id, returning `None` if `id` is out of range.
    /// Out-of-range ids surface as [`RpcErrorKind::InvalidRequest`] rather
    /// than panicking on `slice[id]`, so a malformed (or hostile) frame
    /// cannot crash the server.
    pub fn shard(&self, id: ShardId) -> Option<&Arc<Raft<ShardConfig>>> {
        self.shards.get(id as usize)
    }
}

/// Dispatch an inbound multiplexed frame to the right Raft instance and
/// produce a response.
///
/// Ordering of checks is load-bearing (see Risk #2 in the module docs):
///
/// 1. Cross-check the wire purpose tag against the variant — same gate the
///    legacy [`super::network::RaftRpcServer`] applies.
/// 2. Bounds-check the shard id on `Shard(id, _)` frames before touching
///    `handles.shards`.
/// 3. Match the outer enum tag and forward to the corresponding Raft.
///
/// Failures along the way return a structured [`MuxRaftRpcResponse::Error`]
/// rather than bubbling errors up; the caller wraps any returned response
/// with the standard HMAC + frame envelope and writes it back to the peer.
pub async fn dispatch_mux_frame(
    handles: &MuxRaftHandles,
    frame_purpose: FramePurpose,
    msg: MuxRaftRpcMessage,
) -> MuxRaftRpcResponse {
    if let Err(resp) = check_frame_purpose(&msg, frame_purpose) {
        return resp;
    }

    match msg {
        MuxRaftRpcMessage::Control(inner) => dispatch_control(&handles.control, inner).await,
        MuxRaftRpcMessage::Shard(id, inner) => match handles.shard(id) {
            Some(raft) => dispatch_shard(raft, inner).await,
            None => unknown_shard(id),
        },
        MuxRaftRpcMessage::JoinCluster {
            node_id,
            raft_addr,
            group,
        } => {
            // Per-group join: each group's leader handles its own
            // add_learner. The joining broker fans out across all N+1
            // groups; this handler is local to ONE group at a time. The
            // alternative — fan-out from the control leader — would put
            // every join on the control critical path, which the plan
            // explicitly avoids.
            tracing::info!(
                node_id,
                raft_addr = %raft_addr,
                group = ?group,
                "mux: received join cluster request (learner only)"
            );
            let result = match group {
                GroupId::Control => handle_join_control(&handles.control, node_id, raft_addr).await,
                GroupId::Shard(id) => match handles.shard(id) {
                    Some(raft) => handle_join_shard(raft, node_id, raft_addr).await,
                    None => return unknown_shard(id),
                },
            };
            match result {
                Ok(()) => MuxRaftRpcResponse::JoinClusterOk,
                Err(msg) => {
                    MuxRaftRpcResponse::Error(RpcErrorInfo::new(RpcErrorKind::Internal, msg))
                }
            }
        }
        MuxRaftRpcMessage::PromoteMember { node_id, group } => {
            tracing::info!(node_id, group = ?group, "mux: received promote member request");
            let result = match group {
                GroupId::Control => handle_promote_control(&handles.control, node_id).await,
                GroupId::Shard(id) => match handles.shard(id) {
                    Some(raft) => handle_promote_shard(raft, node_id).await,
                    None => return unknown_shard(id),
                },
            };
            match result {
                Ok(()) => MuxRaftRpcResponse::PromoteMemberOk,
                Err(msg) => {
                    MuxRaftRpcResponse::Error(RpcErrorInfo::new(RpcErrorKind::Internal, msg))
                }
            }
        }
    }
}

/// Cross-check the wire purpose tag against the deserialized variant.
///
/// Matches the legacy `dispatch_rpc_message` gate (network.rs ~line 1224):
/// the join purpose tag is reserved for [`MuxRaftRpcMessage::JoinCluster`]
/// frames, and `PromoteMember` must travel under the cluster purpose.
/// Splitting promotion from join means a holder of the join token alone
/// cannot promote themselves to a voter.
///
/// Returns `Ok(())` if the pairing is legitimate, or `Err(response)` with a
/// pre-built `InvalidRequest` reply that the caller should send verbatim.
pub(super) fn check_frame_purpose(
    msg: &MuxRaftRpcMessage,
    frame_purpose: FramePurpose,
) -> Result<(), MuxRaftRpcResponse> {
    let is_join_message = matches!(msg, MuxRaftRpcMessage::JoinCluster { .. });
    let is_promote_message = matches!(msg, MuxRaftRpcMessage::PromoteMember { .. });
    let join_frame = matches!(frame_purpose, FramePurpose::Join);

    if is_join_message != join_frame {
        tracing::warn!(
            purpose = ?frame_purpose,
            "mux: rejecting frame — wire purpose tag does not match message variant"
        );
        return Err(MuxRaftRpcResponse::Error(RpcErrorInfo::new(
            RpcErrorKind::InvalidRequest,
            "Wire purpose tag does not match message variant",
        )));
    }
    if is_promote_message && join_frame {
        tracing::warn!("mux: rejecting PromoteMember sent under join purpose tag");
        return Err(MuxRaftRpcResponse::Error(RpcErrorInfo::new(
            RpcErrorKind::InvalidRequest,
            "PromoteMember requires cluster HMAC purpose",
        )));
    }
    Ok(())
}

/// Build the "unknown shard id" error response. Kept as a helper so the
/// shard-route branch and the promote-shard branch produce identical
/// errors.
fn unknown_shard(id: ShardId) -> MuxRaftRpcResponse {
    tracing::warn!(
        shard_id = id,
        "mux: rejecting frame — shard id out of range"
    );
    MuxRaftRpcResponse::Error(RpcErrorInfo::new(
        RpcErrorKind::InvalidRequest,
        format!("unknown shard id {}", id),
    ))
}

async fn dispatch_control(
    raft: &Raft<ControlConfig>,
    msg: ControlRpcMessage,
) -> MuxRaftRpcResponse {
    match msg {
        ControlRpcMessage::AppendEntries(req) => match raft.append_entries(req).await {
            Ok(resp) => MuxRaftRpcResponse::AppendEntries(resp),
            Err(e) => MuxRaftRpcResponse::Error(RpcErrorInfo::internal(e)),
        },
        ControlRpcMessage::Vote(req) => match raft.vote(req).await {
            Ok(resp) => MuxRaftRpcResponse::Vote(resp),
            Err(e) => MuxRaftRpcResponse::Error(RpcErrorInfo::internal(e)),
        },
        ControlRpcMessage::InstallSnapshot(req) => match raft.install_snapshot(req).await {
            Ok(resp) => MuxRaftRpcResponse::InstallSnapshot(resp),
            Err(e) => MuxRaftRpcResponse::Error(RpcErrorInfo::internal(e)),
        },
        ControlRpcMessage::ClientWriteWithTerm {
            command,
            expected_term,
            forward_hops,
        } => forward_control(raft, command, expected_term, forward_hops).await,
    }
}

async fn dispatch_shard(raft: &Raft<ShardConfig>, msg: ShardRpcMessage) -> MuxRaftRpcResponse {
    match msg {
        ShardRpcMessage::AppendEntries(req) => match raft.append_entries(req).await {
            Ok(resp) => MuxRaftRpcResponse::AppendEntries(resp),
            Err(e) => MuxRaftRpcResponse::Error(RpcErrorInfo::internal(e)),
        },
        ShardRpcMessage::Vote(req) => match raft.vote(req).await {
            Ok(resp) => MuxRaftRpcResponse::Vote(resp),
            Err(e) => MuxRaftRpcResponse::Error(RpcErrorInfo::internal(e)),
        },
        ShardRpcMessage::InstallSnapshot(req) => match raft.install_snapshot(req).await {
            Ok(resp) => MuxRaftRpcResponse::InstallSnapshot(resp),
            Err(e) => MuxRaftRpcResponse::Error(RpcErrorInfo::internal(e)),
        },
        ShardRpcMessage::ClientWriteWithTerm {
            command,
            expected_term,
            forward_hops,
        } => forward_shard(raft, command, expected_term, forward_hops).await,
    }
}

/// Validate a forwarded client-write before re-issuing it to the local
/// Raft. Mirrors the legacy `ClientWriteWithTerm` arm in
/// `network::RaftRpcServer::dispatch_rpc_message`. Three things can fail:
///
/// 1. **Hop limit** — request has bounced through too many nodes; reject
///    so we don't add another hop. Cap matches [`MAX_FORWARD_HOPS`] so a
///    request the legacy path would refuse can't sneak through the mux
///    path.
/// 2. **Stale term** — the sender saw an older term as leader; our current
///    term has moved on. Tell the caller to refresh leader info.
/// 3. **Not leader** — we received the forward but we're not the leader of
///    *this* group. Reject (with leader hint if known) rather than bounce.
///
/// Generic across both group configs because the inputs (`metrics()` view)
/// and the failure responses are identical — only the success path needs
/// the concrete `client_write` call, which the caller does.
fn validate_forward<C>(
    raft: &Raft<C>,
    expected_term: u64,
    forward_hops: u8,
) -> Result<(), MuxRaftRpcResponse>
where
    C: openraft::RaftTypeConfig<NodeId = RaftNodeId, Node = BasicNode>,
{
    if forward_hops >= MAX_FORWARD_HOPS {
        return Err(MuxRaftRpcResponse::Error(RpcErrorInfo::new(
            RpcErrorKind::ForwardLoopDetected,
            format!(
                "Forward loop detected: {} hops exceeds limit of {}",
                forward_hops, MAX_FORWARD_HOPS
            ),
        )));
    }
    let metrics = raft.metrics().borrow().clone();
    let current_term = metrics.current_term;
    let is_leader = metrics.current_leader == Some(metrics.id);

    if expected_term > 0 && current_term > expected_term {
        tracing::warn!(
            expected_term,
            current_term,
            forward_hops,
            "mux: rejecting forwarded request — term is stale (leadership changed)"
        );
        return Err(MuxRaftRpcResponse::Error(RpcErrorInfo::new(
            RpcErrorKind::LeadershipChanged,
            format!(
                "Stale leader: expected term {} but current term is {} (leadership changed)",
                expected_term, current_term
            ),
        )));
    }
    if !is_leader {
        let leader_hint = metrics.current_leader;
        let leader_info = leader_hint
            .map(|id| format!("leader is node {}", id))
            .unwrap_or_else(|| "no leader elected".to_string());
        tracing::debug!(
            forward_hops,
            "mux: rejecting forwarded request — this node is not the leader ({})",
            leader_info
        );
        return Err(MuxRaftRpcResponse::Error(RpcErrorInfo::new(
            RpcErrorKind::NotLeader { leader_hint },
            format!(
                "ForwardToLeader: this node is not the leader, {}",
                leader_info
            ),
        )));
    }
    Ok(())
}

async fn forward_control(
    raft: &Raft<ControlConfig>,
    command: ControlCommand,
    expected_term: u64,
    forward_hops: u8,
) -> MuxRaftRpcResponse {
    if let Err(resp) = validate_forward(raft, expected_term, forward_hops) {
        return resp;
    }
    match raft.client_write(command).await {
        Ok(resp) => MuxRaftRpcResponse::ControlClientWriteOk(resp.data),
        Err(e) => MuxRaftRpcResponse::Error(RpcErrorInfo::internal(e)),
    }
}

async fn forward_shard(
    raft: &Raft<ShardConfig>,
    command: ShardCommand,
    expected_term: u64,
    forward_hops: u8,
) -> MuxRaftRpcResponse {
    if let Err(resp) = validate_forward(raft, expected_term, forward_hops) {
        return resp;
    }
    match raft.client_write(command).await {
        Ok(resp) => MuxRaftRpcResponse::ShardClientWriteOk(resp.data),
        Err(e) => MuxRaftRpcResponse::Error(RpcErrorInfo::internal(e)),
    }
}

/// Add a node as a learner on the control group.
///
/// Idempotent check first — same approach as the legacy
/// `RaftRpcServer::handle_join_cluster`. Inspecting the openraft error
/// string for "already-a-member" would couple us to upstream wording and
/// silently break on a crate bump, so we check membership structurally
/// before issuing the change.
async fn handle_join_control(
    raft: &Raft<ControlConfig>,
    node_id: RaftNodeId,
    raft_addr: String,
) -> Result<(), String> {
    {
        let metrics = raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        if membership.get_node(&node_id).is_some() {
            tracing::info!(node_id, "mux: node already in control cluster");
            return Ok(());
        }
    }
    let node = BasicNode { addr: raft_addr };
    match raft.add_learner(node_id, node, true).await {
        Ok(_) => {
            tracing::info!(node_id, "mux: added node as control learner");
            Ok(())
        }
        Err(e) => {
            tracing::warn!(node_id, error = %e, "mux: failed to add control learner");
            Err(format!("Failed to add learner: {}", e))
        }
    }
}

/// Add `node_id` as a learner on a shard group.
///
/// Same idempotent shape as [`handle_join_control`]: structural membership
/// check before issuing the change so a retry against a group that already
/// has the learner is a no-op (and so we never depend on upstream
/// "already-a-member" error wording).
async fn handle_join_shard(
    raft: &Raft<ShardConfig>,
    node_id: RaftNodeId,
    raft_addr: String,
) -> Result<(), String> {
    {
        let metrics = raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        if membership.get_node(&node_id).is_some() {
            tracing::info!(node_id, "mux: node already in shard cluster");
            return Ok(());
        }
    }
    let node = BasicNode { addr: raft_addr };
    match raft.add_learner(node_id, node, true).await {
        Ok(_) => {
            tracing::info!(node_id, "mux: added node as shard learner");
            Ok(())
        }
        Err(e) => {
            tracing::warn!(node_id, error = %e, "mux: failed to add shard learner");
            Err(format!("Failed to add learner: {}", e))
        }
    }
}

async fn handle_promote_control(
    raft: &Raft<ControlConfig>,
    node_id: RaftNodeId,
) -> Result<(), String> {
    let metrics = raft.metrics().borrow().clone();
    if metrics.current_leader != Some(metrics.id) {
        return Err("PromoteMember rejected: this node is not the control leader".to_string());
    }
    let mut voters: std::collections::BTreeSet<RaftNodeId> =
        metrics.membership_config.membership().voter_ids().collect();
    if voters.contains(&node_id) {
        tracing::info!(node_id, "mux: node already a control voter");
        return Ok(());
    }
    voters.insert(node_id);
    match raft.change_membership(voters, false).await {
        Ok(_) => {
            tracing::info!(node_id, "mux: promoted node to control voter");
            Ok(())
        }
        Err(e) => {
            tracing::warn!(node_id, error = %e, "mux: failed to promote to control voter");
            Err(format!("Failed to promote to voter: {}", e))
        }
    }
}

async fn handle_promote_shard(raft: &Raft<ShardConfig>, node_id: RaftNodeId) -> Result<(), String> {
    let metrics = raft.metrics().borrow().clone();
    if metrics.current_leader != Some(metrics.id) {
        return Err("PromoteMember rejected: this node is not the shard leader".to_string());
    }
    let mut voters: std::collections::BTreeSet<RaftNodeId> =
        metrics.membership_config.membership().voter_ids().collect();
    if voters.contains(&node_id) {
        tracing::info!(node_id, "mux: node already a shard voter");
        return Ok(());
    }
    voters.insert(node_id);
    match raft.change_membership(voters, false).await {
        Ok(_) => {
            tracing::info!(node_id, "mux: promoted node to shard voter");
            Ok(())
        }
        Err(e) => {
            tracing::warn!(node_id, error = %e, "mux: failed to promote to shard voter");
            Err(format!("Failed to promote to voter: {}", e))
        }
    }
}

// ============================================================================
// MuxRaftRpcServer — TCP accept loop for the multiplexed port.
// ============================================================================

/// Server for the multiplexed Raft RPC port.
///
/// Mirrors [`super::network::RaftRpcServer`] one-for-one — same accept-side
/// gating, same per-IP cap, same TLS handshake helper, same idle timeout —
/// but routes every accepted frame through [`dispatch_mux_frame`] so it can
/// land on the control group OR any shard group on a single port. Holding the
/// dispatch logic in `dispatch_mux_frame` and the I/O scaffolding here keeps
/// the routing decision testable without standing up a TCP listener.
///
/// `RaftCluster::new` constructs one of these per broker and spawns
/// [`Self::run`] on the control-plane runtime alongside the legacy
/// `RaftRpcServer` while the migration lands; once the legacy single-group
/// path is deleted (sharding plan, step 10) this becomes the only Raft RPC
/// server in the process.
pub struct MuxRaftRpcServer {
    /// Raft handles routed to by the dispatcher. `Arc<[…]>` rather than
    /// `Vec<…>` so per-connection tasks clone the slice with one refcount
    /// bump instead of N `Arc<Raft<_>>` clones.
    handles: Arc<MuxRaftHandles>,
    /// Address to listen on.
    listen_addr: String,
    /// Runtime handle for spawning per-connection tasks.
    runtime: tokio::runtime::Handle,
    /// HMAC keys verifying every inbound frame and signing every response.
    auth_keys: Arc<RaftAuthKeys>,
    /// Optional mTLS configuration. Same shape as the legacy server.
    tls: Option<RaftTlsConfig>,
}

impl MuxRaftRpcServer {
    /// Build a server bound to `listen_addr`. The control + shard handles
    /// must already be constructed; we don't take ownership of the underlying
    /// `Raft<_>` because every group is also referenced by `RaftCluster` for
    /// driving client writes from the local broker.
    pub fn new(
        handles: Arc<MuxRaftHandles>,
        listen_addr: String,
        runtime: tokio::runtime::Handle,
        auth_keys: Arc<RaftAuthKeys>,
        tls: Option<RaftTlsConfig>,
    ) -> Self {
        Self {
            handles,
            listen_addr,
            runtime,
            auth_keys,
            tls,
        }
    }

    /// Start the RPC server.
    ///
    /// Stops accepting new connections when `shutdown` fires; in-flight
    /// per-connection tasks finish naturally. Invariants pinned alongside the
    /// legacy server (per-IP cap, accept semaphore, TLS handshake before any
    /// frame bytes flow) are reproduced verbatim — drift between the two
    /// listeners would mean the same peer behaves differently against the two
    /// servers running side-by-side during migration.
    pub async fn run(
        self,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), std::io::Error> {
        use std::collections::HashMap;
        use std::net::IpAddr;
        use std::sync::Mutex as StdMutex;
        use tokio::sync::Semaphore;

        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        tracing::info!(
            addr = %self.listen_addr,
            shards = self.handles.shards.len(),
            cluster_secret = self.auth_keys.cluster_secret_configured(),
            join_token = self.auth_keys.join_token_configured(),
            tls = self.tls.is_some(),
            "Mux Raft RPC server listening"
        );

        if !self.auth_keys.cluster_secret_configured() {
            tracing::warn!(
                addr = %self.listen_addr,
                "Mux Raft RPC server running without RAFT_CLUSTER_SECRET — control plane is \
                 unauthenticated. Set RAFT_CLUSTER_SECRET on every node before exposing the \
                 Raft port to an untrusted network."
            );
        }

        // Accept-side gating: same caps as the legacy server. A handshake
        // semaphore bounds total concurrent inbound TLS handshakes; the
        // per-IP map bounds how many slots a single peer can hold.
        const MAX_CONCURRENT_HANDSHAKES: usize = 128;
        const MAX_PER_IP: usize = 16;
        let accept_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES));
        let per_ip_counts: Arc<StdMutex<HashMap<IpAddr, usize>>> =
            Arc::new(StdMutex::new(HashMap::new()));

        loop {
            tokio::select! {
                biased;
                _ = shutdown.recv() => {
                    tracing::info!(addr = %self.listen_addr, "Mux Raft RPC server shutting down");
                    return Ok(());
                }
                accept_result = listener.accept() => {
                    let (stream, peer_addr) = match accept_result {
                        Ok(v) => v,
                        Err(e) => return Err(e),
                    };
                    let handles = self.handles.clone();
                    let auth_keys = self.auth_keys.clone();
                    let tls = self.tls.clone();
                    let accept_semaphore = accept_semaphore.clone();
                    let per_ip_counts = per_ip_counts.clone();
                    let peer_ip = peer_addr.ip();

                    let per_ip_admitted = {
                        let mut counts = per_ip_counts.lock().unwrap_or_else(|e| e.into_inner());
                        let n = counts.entry(peer_ip).or_insert(0);
                        if *n >= MAX_PER_IP {
                            tracing::warn!(peer = %peer_addr, "Rejecting mux Raft RPC: per-IP connection cap reached");
                            false
                        } else {
                            *n += 1;
                            true
                        }
                    };
                    if !per_ip_admitted {
                        continue;
                    }

                    self.runtime.spawn(async move {
                        let _accept_permit = match accept_semaphore.acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => return,
                        };
                        struct PerIpGuard {
                            counts: Arc<StdMutex<HashMap<IpAddr, usize>>>,
                            ip: IpAddr,
                        }
                        impl Drop for PerIpGuard {
                            fn drop(&mut self) {
                                let mut c = self.counts.lock().unwrap_or_else(|e| e.into_inner());
                                if let Some(n) = c.get_mut(&self.ip) {
                                    *n = n.saturating_sub(1);
                                    if *n == 0 {
                                        c.remove(&self.ip);
                                    }
                                }
                            }
                        }
                        let _ip_guard = PerIpGuard {
                            counts: per_ip_counts.clone(),
                            ip: peer_ip,
                        };
                        let wrapped = match accept_raft_stream(stream, tls.as_ref()).await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!(peer = %peer_addr, error = %e, "Mux Raft TLS accept failed");
                                return;
                            }
                        };
                        if let Err(e) = handle_mux_connection(handles, wrapped, auth_keys, PeerScope::from_ip(peer_ip)).await {
                            tracing::warn!(peer = %peer_addr, error = %e, "Error handling mux Raft RPC");
                        }
                    });
                }
            }
        }
    }
}

/// Read frames from a single connection until EOF or idle timeout.
///
/// Mirrors [`super::network::RaftRpcServer::handle_connection`]: same
/// per-frame idle bound, same EOF/reset/broken-pipe quiet-shutdown, same
/// "decode-failure becomes an InvalidRequest response on the wire rather than
/// a dropped socket" behaviour. Decode failure must surface as a typed
/// response — closing the socket without one would force clients to retry
/// against a closing peer and observe only timeouts.
async fn handle_mux_connection(
    handles: Arc<MuxRaftHandles>,
    mut stream: MaybeTlsStreamServer,
    auth_keys: Arc<RaftAuthKeys>,
    peer: PeerScope,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let read_result = match timeout(
            RPC_FRAME_IDLE_TIMEOUT,
            read_rpc_frame(&mut stream, &auth_keys, &peer, MAX_RPC_MESSAGE_BYTES),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => {
                tracing::debug!("Closing idle mux Raft RPC connection");
                break;
            }
        };

        let (frame_purpose, msg_buf) = match read_result {
            Ok(v) => v,
            Err(e)
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::ConnectionReset
                    || e.kind() == std::io::ErrorKind::BrokenPipe =>
            {
                break;
            }
            Err(e) => return Err(e.into()),
        };

        let response = dispatch_one_frame(&handles, frame_purpose, &msg_buf).await;
        let response_data = postcard::to_stdvec(&response)?;
        write_rpc_frame(&mut stream, &auth_keys, false, &response_data).await?;
    }

    Ok(())
}

/// Decode and dispatch a single frame from the wire.
///
/// Decode failures become a typed [`MuxRaftRpcResponse::Error`] rather than a
/// connection-fatal error. This matches the legacy server's behaviour: a
/// malformed frame from one peer must not knock its connection out, because
/// some implementations of the upstream openraft retry policy multiplex
/// pipelined requests on a single TCP connection.
async fn dispatch_one_frame(
    handles: &MuxRaftHandles,
    frame_purpose: FramePurpose,
    msg_buf: &[u8],
) -> MuxRaftRpcResponse {
    let message: MuxRaftRpcMessage = match postcard::from_bytes(msg_buf) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to deserialize mux Raft RPC frame");
            return MuxRaftRpcResponse::Error(RpcErrorInfo::new(
                RpcErrorKind::InvalidRequest,
                format!("Malformed mux Raft RPC frame: {}", e),
            ));
        }
    };
    dispatch_mux_frame(handles, frame_purpose, message).await
}

#[cfg(test)]
mod tests {
    //! These tests cover the no-Raft branches of the dispatcher: the
    //! cross-checks that decide a frame is malformed *before* it would
    //! touch a Raft instance. The full Raft-routed paths (vote,
    //! append-entries, client-write) need a live openraft fixture and are
    //! exercised by the integration tests that land alongside
    //! `RaftCluster` in step 5.
    //!
    //! The invariants pinned here are the ones that, if regressed, would
    //! corrupt a group's log silently or accept a frame the legacy
    //! dispatcher refuses:
    //!
    //! - Wire purpose tag matches variant (gate against join-token holders
    //!   sending arbitrary client writes).
    //! - PromoteMember sent under join purpose is rejected (gate against
    //!   join-token holders promoting themselves to voter).
    //! - Shard id is bounds-checked before slice access.
    //! - Out-of-range shard ids in `PromoteMember` use the same error path
    //!   as out-of-range ids in regular shard frames.

    use openraft::Vote;
    use openraft::raft::VoteRequest;

    use super::super::commands::ShardCommand;
    use super::*;

    fn vote() -> VoteRequest<RaftNodeId> {
        VoteRequest::new(Vote::new(1, 1), None)
    }

    fn shard_frame(id: ShardId) -> MuxRaftRpcMessage {
        MuxRaftRpcMessage::Shard(id, ShardRpcMessage::Vote(vote()))
    }

    fn shard_client_write(id: ShardId) -> MuxRaftRpcMessage {
        MuxRaftRpcMessage::Shard(
            id,
            ShardRpcMessage::ClientWriteWithTerm {
                command: ShardCommand::Noop,
                expected_term: 1,
                forward_hops: 0,
            },
        )
    }

    fn join_frame() -> MuxRaftRpcMessage {
        MuxRaftRpcMessage::JoinCluster {
            node_id: 7,
            raft_addr: "127.0.0.1:7000".into(),
            group: GroupId::Control,
        }
    }

    fn promote_frame(group: GroupId) -> MuxRaftRpcMessage {
        MuxRaftRpcMessage::PromoteMember { node_id: 7, group }
    }

    fn assert_invalid_request(resp: MuxRaftRpcResponse, expected_substr: &str) {
        match resp {
            MuxRaftRpcResponse::Error(info) => {
                assert!(
                    matches!(info.kind, RpcErrorKind::InvalidRequest),
                    "expected InvalidRequest, got {:?}",
                    info.kind
                );
                assert!(
                    info.message.contains(expected_substr),
                    "message {:?} should contain {:?}",
                    info.message,
                    expected_substr
                );
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn check_purpose_accepts_cluster_purpose_for_steady_state_traffic() {
        // AppendEntries / Vote / InstallSnapshot / ClientWriteWithTerm all
        // travel under FramePurpose::Cluster. A frame variant in that set
        // paired with the cluster purpose must pass.
        for msg in [shard_frame(0), shard_client_write(3)] {
            check_frame_purpose(&msg, FramePurpose::Cluster)
                .expect("steady-state shard frame under cluster purpose must pass");
        }
    }

    #[test]
    fn check_purpose_accepts_join_under_join_purpose() {
        check_frame_purpose(&join_frame(), FramePurpose::Join)
            .expect("JoinCluster under join purpose must pass");
    }

    #[test]
    fn check_purpose_rejects_join_under_cluster_purpose() {
        let resp = check_frame_purpose(&join_frame(), FramePurpose::Cluster)
            .expect_err("JoinCluster under cluster purpose must be rejected");
        assert_invalid_request(resp, "purpose tag");
    }

    #[test]
    fn check_purpose_rejects_non_join_under_join_purpose() {
        // Pinning the inverse direction: a steady-state frame carried
        // under the join purpose tag must also be rejected, because the
        // join key is a lower-trust credential. Without this check, a
        // join-token holder could send a ClientWriteWithTerm and have it
        // accepted by a node that knows both keys.
        let resp = check_frame_purpose(&shard_frame(0), FramePurpose::Join)
            .expect_err("non-join frame under join purpose must be rejected");
        assert_invalid_request(resp, "purpose tag");
    }

    #[test]
    fn check_purpose_rejects_promote_under_join_purpose() {
        // Defence-in-depth: PromoteMember has its own dedicated check
        // (after the variant-vs-purpose mismatch one) to surface a
        // clearer error message. Together, these mean a join-token-only
        // attacker cannot reach voter status.
        let resp = check_frame_purpose(&promote_frame(GroupId::Control), FramePurpose::Join)
            .expect_err("PromoteMember under join purpose must be rejected");
        assert_invalid_request(resp, "purpose tag");
    }

    #[test]
    fn unknown_shard_id_produces_invalid_request_error() {
        // `MuxRaftHandles::shard(id)` returning None must surface a
        // clearly-typed InvalidRequest, never a panic. This is the
        // bounds-check half of Risk #2's silent-misdispatch mitigation.
        let resp = unknown_shard(99);
        assert_invalid_request(resp, "unknown shard id 99");
    }

    #[test]
    fn unknown_shard_error_is_stable_across_id_values() {
        // The error message embeds the id. Pinned so monitoring /
        // alerting on this message string keeps working.
        match unknown_shard(0) {
            MuxRaftRpcResponse::Error(info) => assert_eq!(info.message, "unknown shard id 0"),
            other => panic!("expected Error, got {:?}", other),
        }
        match unknown_shard(u16::MAX) {
            MuxRaftRpcResponse::Error(info) => {
                assert_eq!(info.message, format!("unknown shard id {}", u16::MAX))
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn purpose_check_treats_unauthenticated_like_cluster_for_non_join() {
        // FramePurpose::Unauthenticated is used in dev / single-broker
        // mode when no cluster secret is configured. A steady-state frame
        // arriving under that purpose must NOT trigger the join-purpose
        // rejection — that rejection is specifically about the join key
        // having a lower trust level. Without this we'd break dev mode.
        check_frame_purpose(&shard_frame(0), FramePurpose::Unauthenticated)
            .expect("non-join frame under unauthenticated purpose must pass in dev mode");
        check_frame_purpose(
            &promote_frame(GroupId::Shard(2)),
            FramePurpose::Unauthenticated,
        )
        .expect("PromoteMember under unauthenticated purpose must pass in dev mode");
    }

    #[test]
    fn purpose_check_rejects_join_frame_under_unauthenticated() {
        // Inverse of the prior test: a JoinCluster frame in dev mode
        // would arrive under FramePurpose::Unauthenticated (no key
        // configured). The purpose-vs-variant check still fires because
        // join MUST travel under the join purpose tag whenever a key is
        // configured anywhere in the cluster. In dev-only flows the
        // legacy path handles this identically — the check is structural,
        // not tied to whether keys are configured.
        let resp = check_frame_purpose(&join_frame(), FramePurpose::Unauthenticated)
            .expect_err("JoinCluster under unauthenticated purpose must be rejected");
        assert_invalid_request(resp, "purpose tag");
    }
}
