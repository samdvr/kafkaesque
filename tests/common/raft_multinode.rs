//! In-process multi-node `RaftCoordinator` harness.
//!
//! This is the infrastructure the P2 gap pins in
//! `tests/p2_multinode_gap_pins_tests.rs` were waiting on: a way to bring
//! up N real Raft coordinators that share an object store, form a voter
//! set, and exercise adversarial properties (split-brain elections,
//! membership changes, network partitions) inside `cargo test`.
//!
//! # Bootstrap strategies
//!
//! - [`MultiNodeRaft::spawn`] — leader-driven: node 1 initializes, then
//!   `add_learner_all_groups` + `change_membership_all_groups` promotes
//!   every peer. Same path the in-crate `RaftCluster` smoke tests use.
//! - [`MultiNodeRaft::spawn_via_join`] — joiner-driven: node 1 initializes,
//!   each subsequent node calls [`RaftCoordinator::join_cluster`]. Exercises
//!   the mux `JoinCluster` / `PromoteMember` fan-out.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use kafkaesque::cluster::raft::{ControlCommand, RaftAuthKeys};
use kafkaesque::cluster::{PartitionCoordinator, RaftConfig, RaftCoordinator};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::runtime::Handle;
use tokio::time::sleep;

use super::next_port;

/// Shared HMAC material so `join_cluster` frames carry a real join-purpose
/// tag. Mux rejects unauthenticated JoinCluster frames.
fn test_auth_keys() -> Arc<RaftAuthKeys> {
    Arc::new(RaftAuthKeys::from_strings(
        Some("test-cluster-secret-padded-to-32-bytes!!".to_string()),
        Some("test-join-token-padded-to-32-bytes!!!!".to_string()),
    ))
}

fn multinode_config(
    node_id: u64,
    raft_addr: String,
    members: Vec<(u64, String)>,
    root: &Path,
    auth: Arc<RaftAuthKeys>,
    metadata_shards: u16,
) -> RaftConfig {
    RaftConfig {
        node_id,
        broker_id: node_id as i32,
        host: "127.0.0.1".to_string(),
        port: 9090 + node_id as i32,
        raft_addr,
        cluster_members: members,
        raft_log_dir: root.join("log").to_string_lossy().into_owned(),
        snapshot_dir: root.join("snapshots").to_string_lossy().into_owned(),
        // Fast timers — same shape as the in-crate RaftCluster smoke harness.
        heartbeat_interval: Duration::from_millis(50),
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        max_payload_entries: 100,
        snapshot_threshold: 100,
        is_voter: true,
        lease_duration: Duration::from_secs(5),
        lease_renewal_interval: Duration::from_secs(1),
        broker_heartbeat_interval: Duration::from_millis(200),
        broker_heartbeat_ttl: Duration::from_secs(1),
        default_session_timeout_ms: 5_000,
        session_timeout_check_interval: Duration::from_millis(500),
        auto_create_topics: true,
        max_partitions_per_topic: 100,
        max_pending_proposals: 100,
        proposal_timeout: Duration::from_secs(5),
        auth_keys: auth,
        tls: None,
        clock_skew_tolerance_ms: 5_000,
        metadata_shards,
    }
}

/// A live N-node Raft cluster of `RaftCoordinator`s.
pub struct MultiNodeRaft {
    pub nodes: Vec<Arc<RaftCoordinator>>,
    pub addrs: Vec<String>,
    _temps: Vec<tempfile::TempDir>,
    _store: Arc<dyn ObjectStore>,
}

impl MultiNodeRaft {
    /// Bring up `n` coordinators and form a voter set via leader-driven
    /// membership changes. Uses 2 metadata shards (enough to exercise the
    /// fan-out, cheap enough for PR CI).
    pub async fn spawn(n: usize) -> Self {
        Self::spawn_with_shards(n, 2).await
    }

    /// Like [`Self::spawn`] but with an explicit shard count.
    pub async fn spawn_with_shards(n: usize, metadata_shards: u16) -> Self {
        assert!(n >= 1, "need at least one node");
        let (nodes, addrs, temps, store, _auth) =
            build_nodes(n, metadata_shards, test_auth_keys()).await;

        // Node 1 is the bootstrap voter.
        nodes[0]
            .initialize_cluster()
            .await
            .expect("initialize_cluster on bootstrap node");
        wait_until_has_leader(&nodes[0], Duration::from_secs(5)).await;

        // Add every other node as learner, then promote the full set.
        for i in 1..n {
            let id = (i as u64) + 1;
            nodes[0]
                .cluster()
                .add_learner_all_groups(id, addrs[i].clone())
                .await
                .unwrap_or_else(|e| panic!("add_learner node {id}: {e}"));
        }
        if n > 1 {
            let voters: Vec<u64> = (1..=n as u64).collect();
            nodes[0]
                .cluster()
                .change_membership_all_groups(voters)
                .await
                .expect("change_membership to full voter set");
        }

        wait_for_agreed_leader(&nodes, Duration::from_secs(15)).await;

        for node in &nodes {
            node.register_broker()
                .await
                .expect("register_broker after cluster form");
        }

        Self {
            nodes,
            addrs,
            _temps: temps,
            _store: store,
        }
    }

    /// Bring up `n` coordinators where nodes 2..N join via
    /// [`RaftCoordinator::join_cluster`] against node 1's raft address.
    pub async fn spawn_via_join(n: usize) -> Self {
        assert!(n >= 2, "join path needs a leader and at least one joiner");
        let metadata_shards = 2u16;
        let (nodes, addrs, temps, store, _auth) =
            build_nodes(n, metadata_shards, test_auth_keys()).await;

        nodes[0]
            .initialize_cluster()
            .await
            .expect("initialize_cluster on bootstrap node");
        wait_until_has_leader(&nodes[0], Duration::from_secs(5)).await;

        // Seed the leader's address book so replication can dial joiners
        // as soon as they appear as learners (join RPC also does this on
        // the leader side, but seeding early avoids a race on the first
        // AppendEntries).
        for i in 1..n {
            nodes[0]
                .cluster()
                .add_node((i as u64) + 1, addrs[i].clone())
                .await;
            nodes[i].cluster().add_node(1, addrs[0].clone()).await;
        }

        for i in 1..n {
            nodes[i]
                .join_cluster(&addrs[0])
                .await
                .unwrap_or_else(|e| panic!("join_cluster node {}: {e}", i + 1));
        }

        wait_for_agreed_leader(&nodes, Duration::from_secs(20)).await;

        for node in &nodes {
            node.register_broker()
                .await
                .expect("register_broker after join");
        }

        Self {
            nodes,
            addrs,
            _temps: temps,
            _store: store,
        }
    }

    pub fn node(&self, idx: usize) -> &Arc<RaftCoordinator> {
        &self.nodes[idx]
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Control-group leader id currently agreed by every live node, or
    /// `None` if views diverge / no leader yet.
    pub async fn agreed_leader(&self) -> Option<u64> {
        agreed_leader_among(&self.nodes).await
    }

    /// Index of the node that currently believes it is the control leader.
    pub async fn leader_index(&self) -> Option<usize> {
        for (i, n) in self.nodes.iter().enumerate() {
            if n.is_leader().await {
                return Some(i);
            }
        }
        None
    }

    /// Index of any non-leader node. Panics if the cluster has fewer than
    /// two nodes or no follower is visible yet.
    pub async fn follower_index(&self) -> usize {
        let leader = self
            .leader_index()
            .await
            .expect("cluster must have a leader before asking for a follower");
        (0..self.nodes.len())
            .find(|&i| i != leader)
            .expect("cluster must have a follower")
    }

    /// Simulate a full network partition isolating `idx` from every other
    /// node by scrubbing peer addresses out of every address book.
    pub async fn isolate(&self, idx: usize) {
        let victim = self.nodes[idx].cluster().node_id();
        for (i, node) in self.nodes.iter().enumerate() {
            if i == idx {
                for (j, peer) in self.nodes.iter().enumerate() {
                    if j != i {
                        node.cluster().remove_node(peer.cluster().node_id()).await;
                    }
                }
            } else {
                node.cluster().remove_node(victim).await;
            }
        }
    }

    /// Restore every peer address so isolated nodes can dial again.
    pub async fn heal(&self) {
        for (i, node) in self.nodes.iter().enumerate() {
            for (j, _peer) in self.nodes.iter().enumerate() {
                if i != j {
                    node.cluster()
                        .add_node((j as u64) + 1, self.addrs[j].clone())
                        .await;
                }
            }
        }
    }

    /// Propose a control Noop through `idx`. Returns Ok on commit
    /// (including via leader-forward).
    pub async fn write_noop(&self, idx: usize) -> Result<(), String> {
        self.nodes[idx]
            .cluster()
            .write_control(ControlCommand::Noop)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// Graceful shutdown of every coordinator.
    pub async fn shutdown(self) {
        for node in &self.nodes {
            let _ = node.shutdown().await;
        }
    }
}

async fn build_nodes(
    n: usize,
    metadata_shards: u16,
    auth: Arc<RaftAuthKeys>,
) -> (
    Vec<Arc<RaftCoordinator>>,
    Vec<String>,
    Vec<tempfile::TempDir>,
    Arc<dyn ObjectStore>,
    Arc<RaftAuthKeys>,
) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut temps = Vec::with_capacity(n);
    let mut addrs = Vec::with_capacity(n);
    for _ in 0..n {
        temps.push(tempfile::tempdir().expect("tempdir"));
        addrs.push(format!("127.0.0.1:{}", next_port()));
    }

    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        let node_id = (i as u64) + 1;
        let members: Vec<(u64, String)> = (0..n)
            .filter(|&j| j != i)
            .map(|j| ((j as u64) + 1, addrs[j].clone()))
            .collect();
        let config = multinode_config(
            node_id,
            addrs[i].clone(),
            members,
            temps[i].path(),
            auth.clone(),
            metadata_shards,
        );
        let coord = RaftCoordinator::new(config, store.clone(), Handle::current())
            .await
            .unwrap_or_else(|e| panic!("RaftCoordinator::new node {node_id}: {e}"));
        // Brief pause so the mux RPC server is accepting before peers dial.
        sleep(Duration::from_millis(20)).await;
        nodes.push(Arc::new(coord));
    }

    (nodes, addrs, temps, store, auth)
}

async fn wait_until_has_leader(node: &RaftCoordinator, timeout: Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if node.get_leader().await.is_some() {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("node never observed a control-group leader within {timeout:?}");
}

async fn agreed_leader_among(nodes: &[Arc<RaftCoordinator>]) -> Option<u64> {
    let mut seen: Option<u64> = None;
    for n in nodes {
        match n.get_leader().await {
            Some(id) => {
                if let Some(prev) = seen {
                    if prev != id {
                        return None;
                    }
                } else {
                    seen = Some(id);
                }
            }
            None => return None,
        }
    }
    seen
}

/// Poll until every node reports the same control-group leader.
pub async fn wait_for_agreed_leader(nodes: &[Arc<RaftCoordinator>], timeout: Duration) -> u64 {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Some(id) = agreed_leader_among(nodes).await {
            return id;
        }
        sleep(Duration::from_millis(50)).await;
    }
    let views: Vec<_> = {
        let mut v = Vec::new();
        for n in nodes {
            v.push(n.get_leader().await);
        }
        v
    };
    panic!("nodes never agreed on a control leader within {timeout:?}; views={views:?}");
}
