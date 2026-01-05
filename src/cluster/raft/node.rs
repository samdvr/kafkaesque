//! Raft node wrapper providing a simpler interface.
//!
//! This module wraps the openraft Raft node and provides higher-level
//! operations for cluster coordination.

use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::ObjectStore;
use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft};
use tokio::runtime::Handle;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::timeout;
use tracing::info;

use super::commands::{CoordinationCommand, CoordinationResponse};
use super::config::RaftConfig;
use super::network::{RaftNetworkFactoryImpl, RaftRpcServer};
use super::state_machine::CoordinationStateMachine;
use super::storage::RaftStore;
use super::types::{RaftNodeId, TypeConfig};

use crate::cluster::error::{SlateDBError, SlateDBResult};

/// A Raft node for cluster coordination.
pub struct RaftNode {
    /// The underlying openraft node.
    raft: Arc<Raft<TypeConfig>>,
    /// The state machine (shared with the store).
    state_machine: Arc<RwLock<CoordinationStateMachine>>,
    /// Network factory for creating connections.
    network: Arc<RwLock<RaftNetworkFactoryImpl>>,
    /// This node's ID.
    node_id: RaftNodeId,
    /// Configuration.
    config: RaftConfig,
    /// Shutdown signal sender.
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    /// Semaphore for proposal backpressure.
    /// Limits the number of concurrent pending proposals to prevent memory exhaustion.
    proposal_semaphore: Arc<Semaphore>,
}

impl RaftNode {
    /// Create and start a new Raft node.
    ///
    /// # Arguments
    /// * `config` - Raft configuration
    /// * `object_store` - Object store for durable snapshot persistence
    /// * `runtime` - Runtime handle for spawning control plane tasks
    pub async fn new(
        config: RaftConfig,
        object_store: Arc<dyn ObjectStore>,
        runtime: Handle,
    ) -> SlateDBResult<Self> {
        // Validate config
        if let Err(errors) = config.validate() {
            return Err(SlateDBError::Config(format!(
                "Invalid Raft config: {}",
                errors.join(", ")
            )));
        }

        // Create snapshot path prefix based on node ID
        let snapshot_prefix = format!(
            "{}/raft/snapshots/node-{}",
            config.snapshot_dir, config.node_id
        );

        // Create components
        let openraft_config = Arc::new(config.to_openraft_config());
        let store = RaftStore::new(object_store.clone(), &snapshot_prefix);

        // Try to restore from existing snapshot
        match store.load_snapshot_from_store().await {
            Ok(true) => {
                info!(node_id = config.node_id, "Restored state from snapshot");
            }
            Ok(false) => {
                info!(
                    node_id = config.node_id,
                    "No existing snapshot found, starting fresh"
                );
            }
            Err(e) => {
                // Log but don't fail - the cluster can still function
                tracing::warn!(
                    node_id = config.node_id,
                    error = %e,
                    "Failed to load snapshot, starting with empty state"
                );
            }
        }

        let state_machine = store.state_machine();

        let network = Arc::new(RwLock::new(RaftNetworkFactoryImpl::new()));

        // Add known nodes to network
        for (node_id, addr) in &config.cluster_members {
            network.write().await.add_node(*node_id, addr.clone()).await;
        }

        // Create the Raft node using Adaptor to convert v1 API to v2
        let (log_store, sm_store) = Adaptor::new(store);

        let raft = Raft::new(
            config.node_id,
            openraft_config,
            network.write().await.clone(),
            log_store,
            sm_store,
        )
        .await
        .map_err(|e| SlateDBError::Config(format!("Failed to create Raft node: {}", e)))?;

        let raft = Arc::new(raft);
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        // Initialize proposal backpressure semaphore
        let proposal_semaphore = Arc::new(Semaphore::new(config.max_pending_proposals));

        let node = Self {
            raft: raft.clone(),
            state_machine,
            network,
            node_id: config.node_id,
            config: config.clone(),
            shutdown_tx,
            proposal_semaphore,
        };

        // Start RPC server on control plane runtime
        let rpc_server =
            RaftRpcServer::new(raft.clone(), config.raft_addr.clone(), runtime.clone());
        let mut shutdown_rx = node.shutdown_tx.subscribe();
        runtime.spawn(async move {
            tokio::select! {
                result = rpc_server.run() => {
                    if let Err(e) = result {
                        tracing::error!(error = %e, "Raft RPC server error");
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Raft RPC server shutting down");
                }
            }
        });

        info!(
            node_id = config.node_id,
            raft_addr = %config.raft_addr,
            snapshot_path = %snapshot_prefix,
            "Raft node started with object store snapshot persistence"
        );

        Ok(node)
    }

    /// Initialize the cluster with this node as the leader.
    ///
    /// This should only be called on the first node to bootstrap the cluster.
    /// The node initializes as a single-node cluster first, then other nodes
    /// join dynamically via the JoinCluster RPC.
    pub async fn initialize_cluster(&self) -> SlateDBResult<()> {
        let mut members = BTreeMap::new();

        // Only add ourselves as initial member - other nodes join dynamically
        // This allows the cluster to bootstrap with just one node initially,
        // and other nodes will request to join when they start up.
        members.insert(
            self.node_id,
            BasicNode {
                addr: self.config.raft_addr.clone(),
            },
        );

        self.raft
            .initialize(members)
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to initialize cluster: {}", e)))?;

        info!(
            node_id = self.node_id,
            "Cluster initialized as single node, other nodes will join dynamically"
        );
        Ok(())
    }

    /// Add a new node to the cluster as a learner.
    pub async fn add_learner(&self, node_id: RaftNodeId, addr: String) -> SlateDBResult<()> {
        self.network
            .write()
            .await
            .add_node(node_id, addr.clone())
            .await;

        self.raft
            .add_learner(node_id, BasicNode { addr }, true)
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to add learner: {}", e)))?;

        info!(node_id, "Added learner to cluster");
        Ok(())
    }

    /// Promote a learner to a voter.
    pub async fn change_membership(
        &self,
        members: impl IntoIterator<Item = RaftNodeId>,
    ) -> SlateDBResult<()> {
        let members: std::collections::BTreeSet<_> = members.into_iter().collect();

        self.raft
            .change_membership(members, false)
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to change membership: {}", e)))?;

        info!("Membership changed");
        Ok(())
    }

    /// Write a command to the Raft log.
    ///
    /// This will replicate the command to the cluster and return the response
    /// once it has been committed and applied. If this node is not the leader,
    /// the request will be forwarded to the leader.
    ///
    /// # Backpressure
    ///
    /// This method enforces backpressure to prevent memory exhaustion under heavy
    /// metadata operation load. If `max_pending_proposals` concurrent proposals are
    /// already in flight, this method will wait up to `proposal_timeout` for a slot
    /// to become available before failing.
    ///
    /// Leader Election Race Prevention
    ///
    /// When forwarding to the leader, this method now includes:
    /// 1. The current Raft term to detect stale leader views
    /// 2. A hop counter to prevent infinite forwarding loops
    pub async fn write(&self, command: CoordinationCommand) -> SlateDBResult<CoordinationResponse> {
        use std::time::Instant;

        let _permit = match timeout(
            self.config.proposal_timeout,
            self.proposal_semaphore.acquire(),
        )
        .await
        {
            Ok(Ok(permit)) => {
                crate::cluster::metrics::record_raft_backpressure("acquired");
                permit
            }
            Ok(Err(_)) => {
                // Semaphore closed - shouldn't happen
                return Err(SlateDBError::Raft(
                    "Proposal semaphore closed unexpectedly".to_string(),
                ));
            }
            Err(_) => {
                crate::cluster::metrics::record_raft_backpressure("timeout");
                crate::cluster::metrics::COORDINATOR_FAILURES
                    .with_label_values(&["proposal_backpressure_timeout"])
                    .inc();
                return Err(SlateDBError::Raft(format!(
                    "Proposal backpressure timeout: too many pending proposals (max {})",
                    self.config.max_pending_proposals
                )));
            }
        };

        let pending = self.pending_proposals();
        crate::cluster::metrics::set_raft_pending_proposals(pending as i64);

        let start = Instant::now();
        let result = self.raft.client_write(command.clone()).await;

        let duration = start.elapsed().as_secs_f64();
        let status = if result.is_ok() { "success" } else { "error" };
        crate::cluster::metrics::record_raft_proposal(status, duration);

        match result {
            Ok(response) => Ok(response.data),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("forward request to") || err_str.contains("ForwardToLeader") {
                    let metrics = self.raft.metrics().borrow().clone();
                    let current_term = metrics.current_term;

                    if let Some(leader_id) = self.current_leader().await
                        && let Some(leader_addr) =
                            self.network.read().await.get_node_addr(leader_id).await
                    {
                        tracing::debug!(
                            node_id = self.node_id,
                            leader_id = leader_id,
                            leader_addr = %leader_addr,
                            current_term = current_term,
                            "Forwarding write to leader with term validation"
                        );
                        match super::network::forward_client_write_with_term(
                            &leader_addr,
                            command,
                            current_term,
                            0, // Initial hop count
                        )
                        .await
                        {
                            Ok(response) => return Ok(response),
                            Err(e) => {
                                return Err(SlateDBError::Storage(format!(
                                    "Failed to forward to leader: {}",
                                    e
                                )));
                            }
                        }
                    }
                    Err(SlateDBError::Storage(format!("Not the Raft leader: {}", e)))
                } else {
                    Err(SlateDBError::Storage(format!("Raft write failed: {}", e)))
                }
            }
        }
    }

    /// Ensure we can read linearizable state.
    ///
    /// This verifies that we are the leader and have applied all committed entries.
    pub async fn ensure_linearizable(&self) -> SlateDBResult<()> {
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to ensure linearizable: {}", e)))?;

        Ok(())
    }

    /// Get the current leader ID.
    ///
    /// Uses borrow to avoid cloning the entire metrics struct.
    pub async fn current_leader(&self) -> Option<RaftNodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// Check if this node is the leader.
    pub async fn is_leader(&self) -> bool {
        self.current_leader().await == Some(self.node_id)
    }

    /// Get the state machine for local reads.
    ///
    /// Note: Local reads may be stale. For linearizable reads, use `ensure_linearizable()`
    /// before reading.
    pub fn state_machine(&self) -> Arc<RwLock<CoordinationStateMachine>> {
        self.state_machine.clone()
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> RaftNodeId {
        self.node_id
    }

    /// Get this node's broker ID.
    pub fn broker_id(&self) -> i32 {
        self.config.broker_id
    }

    /// Shutdown the Raft node.
    pub async fn shutdown(&self) -> SlateDBResult<()> {
        let _ = self.shutdown_tx.send(());

        self.raft
            .shutdown()
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to shutdown Raft: {}", e)))?;

        info!(node_id = self.node_id, "Raft node shut down");
        Ok(())
    }

    /// Get cluster metrics.
    pub fn metrics(&self) -> openraft::RaftMetrics<RaftNodeId, BasicNode> {
        self.raft.metrics().borrow().clone()
    }

    /// Check if the cluster is already initialized.
    ///
    /// Returns true if the cluster has existing membership (from a restored snapshot
    /// or previous initialization). This should be checked before calling
    /// `initialize_cluster()` to avoid re-initialization errors on restart.
    pub fn is_initialized(&self) -> bool {
        let metrics_watch = self.raft.metrics();
        let metrics = metrics_watch.borrow();
        // Check if there are any voters in the membership config
        // An uninitialized cluster has no voters
        metrics
            .membership_config
            .membership()
            .voter_ids()
            .next()
            .is_some()
    }

    /// Get the number of available proposal slots.
    ///
    /// Returns the number of additional proposals that can be submitted
    /// before backpressure kicks in.
    pub fn available_proposal_slots(&self) -> usize {
        self.proposal_semaphore.available_permits()
    }

    /// Get the number of pending proposals.
    ///
    /// Returns the number of proposals currently in flight (waiting for commit).
    pub fn pending_proposals(&self) -> usize {
        self.config.max_pending_proposals - self.proposal_semaphore.available_permits()
    }
}
