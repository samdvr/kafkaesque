//! SlateDB cluster handler implementing the Kafka Handler trait.
//!
//! This module is split into several submodules by handler category:
//! - `metadata` - Metadata request handling
//! - `produce` - Produce request handling
//! - `fetch` - Fetch request handling
//! - `offsets` - Offset listing, commit, and fetch
//! - `groups` - Consumer group coordination
//! - `admin` - Topic creation and deletion
//! - `producer_id` - Producer ID initialization

mod admin;
mod fetch;
mod groups;
mod metadata;
mod offsets;
mod produce;
mod producer_id;

use async_trait::async_trait;
use moka::sync::Cache;
use object_store::ObjectStore;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::error::KafkaCode;
use crate::runtime::RuntimeHandles;
use crate::server::request::*;
use crate::server::response::*;
use crate::server::{Handler, RequestContext};

type DynFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Sharded dispatch pool for fire-and-forget acks=0 produces.
///
/// Each shard owns a bounded mpsc channel. The hot path picks a shard
/// (modulo partition hash) and `try_send`s the work future — no mutex,
/// no global synchronization, the bounded queue is the backpressure.
/// Each worker drains its channel and `tokio::spawn`s the work so the
/// shard itself never blocks behind a slow write.
pub(crate) struct FireAndForgetPool {
    shards: Vec<mpsc::Sender<DynFuture>>,
}

impl FireAndForgetPool {
    /// Build a pool with `n_shards` workers, each sized to `per_shard`
    /// queued items. Total worst-case in-flight = n_shards * per_shard
    /// queued plus N actively-spawned tasks.
    pub fn new(n_shards: usize, per_shard: usize) -> Self {
        let mut shards = Vec::with_capacity(n_shards);
        for _ in 0..n_shards {
            let (tx, mut rx) = mpsc::channel::<DynFuture>(per_shard);
            shards.push(tx);
            tokio::spawn(async move {
                while let Some(fut) = rx.recv().await {
                    // Spawn so a slow individual write doesn't block the
                    // shard's drain. The shard sees a steady stream of
                    // small per-partition writes; each one is a separate
                    // tokio task once it's been admitted.
                    tokio::spawn(fut);
                }
            });
        }
        Self { shards }
    }

    /// Try to enqueue `fut` on the shard that owns `shard_key`. Returns
    /// `false` when the shard's queue is full or closed — the caller
    /// should treat that as backpressure (acks=0 semantics: drop the
    /// write, bump a metric).
    pub fn try_dispatch<F>(&self, shard_key: i32, fut: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.shards.is_empty() {
            return false;
        }
        // Map negative i32 to a non-negative bucket index without paying a
        // signed-rem branch on the hot path.
        let idx = (shard_key as u32 as usize) % self.shards.len();
        self.shards[idx].try_send(Box::pin(fut)).is_ok()
    }
}

/// Number of fire-and-forget shards. 16 keeps per-shard channel pressure
/// low while staying well below the typical CPU-core count cap on shard
/// fan-out for cache-line-affinity reasons.
const FIRE_AND_FORGET_SHARDS: usize = 16;
/// Per-shard bounded queue depth. Sized so 16 * 64 = 1024 matches the
/// previous global `MAX_FIRE_AND_FORGET_CONCURRENT = 1000` ceiling.
const FIRE_AND_FORGET_PER_SHARD: usize = 64;
use crate::types::BrokerId;

use super::config::ClusterConfig;
use super::error::{SlateDBError, SlateDBResult};
use super::object_store::create_object_store;
use super::partition_manager::PartitionManager;
use super::raft::{RaftConfig, RaftCoordinator, request_cluster_join};
use super::traits::PartitionCoordinator;

/// Health status of the cluster handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Handler is healthy and accepting requests.
    Healthy,
    /// Handler is in zombie mode (lost Raft coordination).
    Zombie,
    /// Handler is degraded (some partitions unavailable).
    Degraded,
    /// Handler is shutting down.
    ShuttingDown,
}

/// SlateDB cluster handler for Kafka protocol.
///
/// This handler uses:
/// - SlateDB for partition data storage (one instance per partition)
/// - Raft for coordination, consumer groups, and offsets
/// - Object storage (S3, GCS, or local) for durability
pub struct SlateDBClusterHandler {
    /// Partition manager handles ownership and storage.
    pub(crate) partition_manager: Arc<PartitionManager<RaftCoordinator>>,

    /// Raft coordinator for consumer groups and metadata.
    pub(crate) coordinator: Arc<RaftCoordinator>,

    /// Broker ID for this server.
    pub(crate) broker_id: BrokerId,

    /// Host address for this broker.
    pub(crate) host: String,

    /// Port for this broker.
    pub(crate) port: i32,

    /// Whether to auto-create topics on first reference.
    /// If false, metadata requests for unknown topics return UnknownTopicOrPartition.
    pub(crate) auto_create_topics: bool,

    /// Whether to validate CRC-32C checksums on incoming record batches.
    /// When enabled, corrupted batches are rejected with CorruptMessage.
    pub(crate) validate_record_crc: bool,

    /// Maximum concurrent partition writes per produce request.
    pub(crate) max_concurrent_partition_writes: usize,

    /// Maximum concurrent partition reads per fetch request.
    pub(crate) max_concurrent_partition_reads: usize,

    /// Cache for topic name Arc<str> allocations to reduce hot path allocations.
    topic_name_cache: Cache<String, Arc<str>>,

    /// Whether SASL must complete before any non-handshake API key is served.
    /// Mirrors `ClusterConfig::sasl_required`. The connection-accept loop
    /// reads this via `Handler::sasl_required` and arms the per-connection
    /// `AuthGate`.
    pub(crate) sasl_required: bool,

    /// Authorizer used by the per-handler ACL checks. Always
    /// `Some` — set to `AllowAllAuthorizer` when ACL enforcement is off so
    /// the call sites don't need to special-case the disabled path.
    pub(crate) authorizer: Arc<dyn crate::cluster::authorizer::Authorizer>,

    /// Per-partition notifies that fire after every successful append.
    /// Fetch handlers waiting under `max_wait_ms` / `min_bytes` listen on
    /// the notifies of exactly the partitions they requested.
    ///
    /// This used to be a single broker-wide `Notify`: every produce woke
    /// *every* long-polling fetcher, each of which re-checked all of its
    /// partitions — a thundering herd that scaled with consumers x
    /// partitions. Entries are created lazily and are tiny (an `Arc<Notify>`
    /// per partition this broker has served), so the map is bounded by the
    /// partition count.
    pub(crate) hwm_notifiers: dashmap::DashMap<(Arc<str>, i32), Arc<tokio::sync::Notify>>,

    /// In-flight acks=0 produce dispatch pool.
    ///
    /// Replaces the old global `Arc<std::sync::Mutex<JoinSet>>` that every
    /// acks=0 partition produce had to lock. Under the documented 1k
    /// concurrent target that single mutex was THE contended sync mutex on
    /// the produce hot path. This pool fans work out to N independent
    /// shards, each with its own bounded mpsc channel; the produce hot path
    /// hashes by partition and `try_send`s — no mutex acquired, the
    /// bounded queue is the backpressure.
    pub(crate) fire_and_forget_pool: Arc<FireAndForgetPool>,

    /// SASL provider. Holds the in-memory user table when the
    /// `sasl` feature is compiled in. `None` when the feature is off.
    #[cfg(feature = "sasl")]
    pub(crate) sasl_provider: Option<Arc<crate::cluster::sasl_provider::SaslProvider>>,

    /// Pending SASL post-authenticate state, keyed by client `SocketAddr`.
    /// The cluster handler stashes the result of the most
    /// recent `handle_sasl_authenticate` here so the connection
    /// dispatcher's `take_sasl_post_auth` can read out whether the
    /// handshake is complete and what principal was authenticated. Used
    /// only by SCRAM today (PLAIN is single-step) but always populated for
    /// uniformity.
    #[cfg(feature = "sasl")]
    pub(crate) sasl_post_auth: dashmap::DashMap<std::net::SocketAddr, crate::server::SaslPostAuth>,
}

impl SlateDBClusterHandler {
    /// Create a new SlateDB cluster handler.
    ///
    /// Uses the `object_store` field from config to determine storage backend.
    /// Uses the current tokio runtime for all tasks (backwards compatible).
    pub async fn new(config: ClusterConfig) -> SlateDBResult<Self> {
        let object_store = create_object_store(&config)?;
        let runtime_handles = RuntimeHandles::from_current();
        Self::with_runtimes(config, object_store, runtime_handles).await
    }

    /// Create a new handler with a custom object store.
    ///
    /// Uses the current tokio runtime for all tasks (backwards compatible).
    pub async fn with_object_store(
        config: ClusterConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> SlateDBResult<Self> {
        let runtime_handles = RuntimeHandles::from_current();
        Self::with_runtimes(config, object_store, runtime_handles).await
    }

    /// Create a new handler with custom runtime handles.
    ///
    /// Creates the object store from config. This is the recommended constructor
    /// when using separate control/data plane runtimes.
    pub async fn with_runtime_handles(
        config: ClusterConfig,
        runtime_handles: RuntimeHandles,
    ) -> SlateDBResult<Self> {
        let object_store = create_object_store(&config)?;
        Self::with_runtimes(config, object_store, runtime_handles).await
    }

    /// Create a new handler with custom object store and runtime handles.
    ///
    /// This is the full constructor allowing customization of both storage and runtimes.
    pub async fn with_runtimes(
        config: ClusterConfig,
        object_store: Arc<dyn ObjectStore>,
        runtime_handles: RuntimeHandles,
    ) -> SlateDBResult<Self> {
        // Initialize circuit breaker config from ClusterConfig
        super::metrics::init_circuit_breaker_config(
            config.circuit_breaker_threshold,
            config.circuit_breaker_base_reset_window_ms,
            config.circuit_breaker_max_reset_window_ms,
        );

        // Wire metrics cardinality settings. Without this call the
        // `enable_partition_metrics` / `max_metric_cardinality` knobs were
        // parsed, validated... and silently ignored.
        super::metrics::configure_metrics(
            config.enable_partition_metrics,
            config.max_metric_cardinality,
        );

        // Create Raft coordinator with object store for snapshot persistence
        let raft_config = RaftConfig::from_cluster_config(&config);
        let coordinator = Arc::new(
            RaftCoordinator::new(
                raft_config.clone(),
                object_store.clone(),
                runtime_handles.control.clone(),
            )
            .await?,
        );

        // Check if the cluster is already initialized from persisted state.
        // This handles the restart case where snapshots were loaded.
        let already_initialized = coordinator.is_initialized();

        // Initialize cluster:
        // - Skip if cluster is already initialized (restart with persisted state)
        // - Single node (no peers): require explicit RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE
        //   to permit single-node bootstrap. Without this gate, every fresh node
        //   on every host with no RAFT_PEERS happily forms its own one-node
        //   cluster against the same object-store prefix.
        // - Multi-node: only the node with the lowest broker_id initializes on fresh start
        let should_initialize = if already_initialized {
            info!(
                broker_id = config.broker_id,
                "Cluster already initialized from persisted state, skipping initialization"
            );
            false
        } else {
            match &config.raft_peers {
                None => {
                    let allow_single = std::env::var("RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE")
                        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
                        .unwrap_or(false);
                    if !allow_single {
                        return Err(SlateDBError::Config(
                            "Refusing to bootstrap: RAFT_PEERS is unset and \
                             RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE is not enabled. \
                             Set RAFT_PEERS for multi-node deployments, or set \
                             RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE=true if a one-node \
                             cluster is intended (e.g. local development). \
                             Without this gate two fresh brokers can race to \
                             form independent one-node clusters against the same \
                             object store."
                                .to_string(),
                        ));
                    }
                    true
                }
                Some(peers) => {
                    // Parse peer IDs from format "node_id=host:port,node_id=host:port,..."
                    let peer_ids: Vec<i32> = peers
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .filter_map(|s| {
                            let parts: Vec<&str> = s.split('=').collect();
                            if parts.len() == 2 {
                                parts[0].parse::<i32>().ok()
                            } else {
                                None
                            }
                        })
                        .collect();

                    // This broker should initialize if its ID is lower than all peer IDs
                    let min_peer_id = peer_ids.iter().min().copied().unwrap_or(i32::MAX);
                    config.broker_id < min_peer_id
                }
            }
        };

        if should_initialize {
            info!(broker_id = config.broker_id, "Initializing Raft cluster");
            coordinator.initialize_cluster().await?;
        }

        // Start background tasks
        coordinator.start_background_tasks().await;

        // For non-initializing nodes on fresh start (no persisted state),
        // we need to join the existing cluster by adding ourselves as a
        // learner and then promoting to voter.
        // Skip joining if we already have persisted state - we're already a member.
        if !should_initialize && !already_initialized {
            info!(
                broker_id = config.broker_id,
                "Joining existing Raft cluster as new member"
            );

            // Try to join the cluster via any known peer
            let node_id = config.broker_id as u64;
            let raft_addr = config.raft_listen_addr.clone();

            // Try each peer until we successfully join
            if let Some(peers) = &config.raft_peers {
                let mut joined = false;
                for peer_spec in peers.split(',').filter(|s| !s.is_empty()) {
                    let parts: Vec<&str> = peer_spec.split('=').collect();
                    if parts.len() == 2 {
                        let peer_addr = parts[1];
                        info!(
                            broker_id = config.broker_id,
                            peer_addr = %peer_addr,
                            "Requesting to join cluster via peer"
                        );

                        // Send join request to the peer (who will forward to leader if needed)
                        match request_cluster_join(
                            peer_addr,
                            node_id,
                            &raft_addr,
                            &raft_config.auth_keys,
                            raft_config.tls.as_ref(),
                        )
                        .await
                        {
                            Ok(()) => {
                                info!(
                                    broker_id = config.broker_id,
                                    "Successfully requested to join cluster"
                                );
                                joined = true;
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    broker_id = config.broker_id,
                                    peer_addr = %peer_addr,
                                    error = %e,
                                    "Failed to join via peer, trying next"
                                );
                            }
                        }
                    }
                }

                if !joined {
                    // Best-effort, warn-only joins would let a node
                    // run as a non-member of any cluster, then later get
                    // misclassified as a fresh init candidate. Fail closed —
                    // the operator must restart once the cluster is reachable.
                    return Err(SlateDBError::Config(format!(
                        "Failed to join Raft cluster: tried all peers in \
                         RAFT_PEERS={:?} and none accepted the join request. \
                         Verify the seed peers are running and routable, then \
                         restart this broker.",
                        peers
                    )));
                }
            }
        }

        // Wait for leader election before registering
        // This ensures the cluster is ready for writes
        coordinator.wait_for_leader().await?;

        // Register this broker
        coordinator.register_broker().await?;

        // Create partition manager
        let partition_manager = Arc::new(PartitionManager::new(
            coordinator.clone(),
            object_store,
            config.clone(),
            runtime_handles.control.clone(),
        ));

        // Install a heartbeat hook on the Raft state machine so
        // every applied broker heartbeat refreshes the local failure
        // detector. Without this, fast-failover would rely on lease-TTL expiry
        // (~60s) instead of the configured ~2.5s heartbeat budget.
        if let Some(rebalance_coord) = partition_manager.rebalance_coordinator() {
            let rc = rebalance_coord.clone();
            let sm_arc = coordinator.node().state_machine();
            sm_arc
                .read()
                .await
                .set_heartbeat_hook(Arc::new(move |broker_id| {
                    rc.record_heartbeat(broker_id);
                }));
        }

        // Invalidate the local owner cache whenever the replicated state
        // machine applies an ownership change. Without this, followers (and
        // the broker that lost a partition) can serve fetch/metadata from a
        // stale 1s-TTL cache after failover.
        {
            let coordinator_for_hook = coordinator.clone();
            let runtime = runtime_handles.control.clone();
            let sm_arc = coordinator.node().state_machine();
            sm_arc
                .read()
                .await
                .set_ownership_change_hook(Arc::new(move |invalidation| {
                    let coordinator = coordinator_for_hook.clone();
                    runtime.spawn(async move {
                        match invalidation {
                            super::raft::OwnershipCacheInvalidation::Partition {
                                topic,
                                partition,
                            } => {
                                coordinator
                                    .invalidate_ownership_cache(&topic, partition)
                                    .await;
                            }
                            super::raft::OwnershipCacheInvalidation::All => {
                                coordinator.invalidate_all_ownership_cache().await;
                            }
                        }
                    });
                }));
        }

        // Start background tasks
        partition_manager.start().await;

        info!(
            broker_id = config.broker_id,
            bind_host = %config.host,
            advertised_host = %config.advertised_host,
            port = config.port,
            auto_create_topics = config.auto_create_topics,
            validate_record_crc = config.validate_record_crc,
            "SlateDB cluster handler initialized with Raft coordination"
        );

        // Pick the authorizer. When ACL enforcement is off we
        // route every request through `AllowAllAuthorizer` so the call sites
        // can be unconditional. When on, the Raft-backed authorizer reads
        // bindings from the local state machine and consults the configured
        // super-user list.
        let authorizer: Arc<dyn crate::cluster::authorizer::Authorizer> = if config.acl_enabled {
            // Bootstrap the ACL state from a file on the leader. Followers
            // pick up the bindings via Raft replication. Idempotent: a
            // restart re-applies the same file (CreateAcls deduplicates).
            if let Some(path) = config.acl_bootstrap_file.as_ref()
                && coordinator.is_leader().await
            {
                match std::fs::read_to_string(path) {
                    Ok(contents) => {
                        match serde_json::from_str::<Vec<crate::cluster::raft::AclBinding>>(
                            &contents,
                        ) {
                            Ok(bindings) => {
                                let n = bindings.len();
                                match coordinator.create_acls(bindings).await {
                                    Ok(created) => info!(
                                        file = %path,
                                        entries = n,
                                        new_bindings = created,
                                        "Bootstrapped ACL bindings from file"
                                    ),
                                    Err(e) => warn!(
                                        file = %path,
                                        error = %e,
                                        "Failed to apply ACL bootstrap"
                                    ),
                                }
                            }
                            Err(e) => warn!(
                                file = %path,
                                error = %e,
                                "Failed to parse ACL bootstrap file"
                            ),
                        }
                    }
                    Err(e) => warn!(
                        file = %path,
                        error = %e,
                        "Failed to read ACL bootstrap file"
                    ),
                }
            }
            Arc::new(crate::cluster::authorizer::RaftAclAuthorizer::new(
                coordinator.clone(),
                config.super_users.clone(),
                config.acl_deny_by_default,
            ))
        } else {
            Arc::new(crate::cluster::authorizer::AllowAllAuthorizer)
        };

        Ok(Self {
            partition_manager,
            coordinator,
            broker_id: BrokerId::new(config.broker_id),
            host: config.advertised_host.clone(),
            port: config.port,
            auto_create_topics: config.auto_create_topics,
            validate_record_crc: config.validate_record_crc,
            max_concurrent_partition_writes: config.max_concurrent_partition_writes,
            max_concurrent_partition_reads: config.max_concurrent_partition_reads,
            topic_name_cache: Cache::new(10_000),
            sasl_required: config.sasl_required,
            authorizer,
            hwm_notifiers: dashmap::DashMap::new(),
            fire_and_forget_pool: Arc::new(FireAndForgetPool::new(
                FIRE_AND_FORGET_SHARDS,
                FIRE_AND_FORGET_PER_SHARD,
            )),
            #[cfg(feature = "sasl")]
            sasl_provider: crate::cluster::sasl_provider::SaslProvider::from_config(&config)
                .await
                .map(Arc::new),
            #[cfg(feature = "sasl")]
            sasl_post_auth: dashmap::DashMap::new(),
        })
    }

    /// Shutdown the handler gracefully.
    pub async fn shutdown(&self) -> SlateDBResult<()> {
        // Fire-and-forget pool drains via channel-close: the pool is held in
        // `Arc`, so on the last drop the worker `recv().await` returns
        // `None` and the worker exits. We don't await individual spawned
        // tasks here — acks=0 semantics already accept silent drop on
        // shutdown, and waiting would gate clean shutdown on a slow write.
        self.partition_manager.shutdown().await?;
        self.coordinator.shutdown().await
    }

    /// Check if the broker is in zombie mode.
    ///
    /// Zombie mode indicates the broker has lost coordination with Raft
    /// and cannot reliably determine partition ownership. In this state,
    /// all write operations are rejected to prevent split-brain scenarios.
    ///
    /// # Returns
    /// `true` if the broker is in zombie mode and writes should be rejected.
    pub fn is_zombie(&self) -> bool {
        self.partition_manager.is_zombie()
    }

    /// Get the underlying zombie-mode state.
    ///
    /// Used to wire the K8s readiness probe to the real flag the
    /// `PartitionManager` toggles. Returning a separate `Arc<AtomicBool>` here
    /// would yield a flag that is never updated.
    pub fn zombie_state(&self) -> Arc<crate::cluster::zombie_mode::ZombieModeState> {
        self.partition_manager.zombie_state()
    }

    /// Get the Raft coordinator as a `RaftStateProvider`.
    ///
    /// Used to wire the K8s readiness probe to query the coordinator
    /// directly, bypassing the up-to-1s lag of the global `RAFT_STATE`
    /// gauge.
    pub fn raft_coordinator(&self) -> Arc<dyn crate::server::health::RaftStateProvider> {
        self.coordinator.clone()
    }

    /// Get a cached Arc<str> for a topic name.
    ///
    /// This method returns a cached Arc<str> for the topic name,
    /// or creates and caches a new one if not present. This avoids allocation
    /// pressure from creating `Arc::from(topic.name.as_str())` on every request.
    ///
    /// `moka::sync::Cache::get_with` takes the key by value, so the previous
    /// implementation allocated a `String` on every call regardless of cache
    /// state — defeating the cache. We probe with `get` (borrowed key) first
    /// and only allocate when populating on a miss.
    pub(crate) fn cached_topic_name(&self, topic: &str) -> Arc<str> {
        if let Some(cached) = self.topic_name_cache.get(topic) {
            return cached;
        }
        let owned: Arc<str> = Arc::from(topic);
        self.topic_name_cache
            .insert(topic.to_string(), owned.clone());
        owned
    }

    /// The HWM-advance notify for one partition, created lazily.
    ///
    /// Producers call `notify_waiters()` on it after a successful append;
    /// long-poll fetches wait on the notifies of their requested partitions.
    pub(crate) fn hwm_notifier(
        &self,
        topic: &Arc<str>,
        partition: i32,
    ) -> Arc<tokio::sync::Notify> {
        // Steady-state hot path: shard read lock only. `entry()` always takes
        // a shard write lock, so two threads producing/fetching against the
        // same partition would contend on a hashmap *read*. Probe with `get()`
        // first; only fall back to `entry()` on the cold-path miss.
        let key = (Arc::clone(topic), partition);
        if let Some(existing) = self.hwm_notifiers.get(&key) {
            return existing.value().clone();
        }
        self.hwm_notifiers.entry(key).or_default().clone()
    }

    /// Cluster-level ACL gate, driven by the
    /// `cluster_operation_for_api` table. Returns `true` when the API key
    /// needs no cluster-level check, or when the principal holds the
    /// required operation on the `kafka-cluster` resource.
    pub(crate) async fn authorize_cluster_api(
        &self,
        ctx: &RequestContext,
        api_key: crate::server::request::ApiKey,
    ) -> bool {
        use crate::cluster::authorizer::{
            AuthorizeRequest, AuthorizeResult, CLUSTER_RESOURCE_NAME, cluster_operation_for_api,
        };
        let Some(operation) = cluster_operation_for_api(api_key) else {
            return true;
        };
        self.authorizer
            .authorize(AuthorizeRequest {
                principal: &ctx.principal,
                host: &ctx.client_host,
                operation,
                resource_type: crate::cluster::raft::AclResourceType::Cluster,
                resource_name: CLUSTER_RESOURCE_NAME,
            })
            .await
            == AuthorizeResult::Allowed
    }

    /// Topic-level ACL check. Returns `true` when the principal
    /// may perform `operation` on `topic`.
    pub(crate) async fn topic_authorized(
        &self,
        ctx: &RequestContext,
        operation: crate::cluster::raft::AclOperation,
        topic: &str,
    ) -> bool {
        use crate::cluster::authorizer::{AuthorizeRequest, AuthorizeResult};
        self.authorizer
            .authorize(AuthorizeRequest {
                principal: &ctx.principal,
                host: &ctx.client_host,
                operation,
                resource_type: crate::cluster::raft::AclResourceType::Topic,
                resource_name: topic,
            })
            .await
            == AuthorizeResult::Allowed
    }

    /// Build topic metadata response for a single topic.
    ///
    /// Queries the coordinator once for all partition owners on this topic
    /// (a single Raft read-index barrier), then assembles partition entries
    /// from the result. The previous implementation issued N sequential
    /// `get_partition_owner` calls — at e.g. 1 ms per Raft read this turned
    /// a 1k-partition Metadata refresh into a 1-second sequential stall.
    pub(crate) async fn build_topic_metadata(&self, topic: &str) -> TopicMetadata {
        let owners = self
            .coordinator
            .get_partition_owners(topic)
            .await
            .unwrap_or_default();

        // Fall back to `get_partition_count` only if the bulk read returned
        // nothing — e.g. an unknown topic, or a coordinator backend that
        // reports an empty owner list for a freshly created topic that has
        // not yet been assigned. We still want a partition-shaped response
        // so clients see the topic exists.
        let partition_count = if owners.is_empty() {
            self.coordinator
                .get_partition_count(topic)
                .await
                .ok()
                .flatten()
                .unwrap_or(1)
        } else {
            owners.len() as i32
        };

        let mut by_partition: std::collections::HashMap<i32, Option<i32>> =
            std::collections::HashMap::with_capacity(owners.len());
        for (partition, owner) in owners {
            by_partition.insert(partition, owner);
        }

        let mut partitions = Vec::with_capacity(partition_count as usize);
        for p in 0..partition_count {
            let owner = by_partition.get(&p).copied().unwrap_or(None);

            // When no owner is known, return LeaderNotAvailable instead of
            // falsely claiming this broker is the leader. This prevents clients
            // from sending produce requests to brokers that don't own the partition,
            // which would result in NOT_LEADER_OR_FOLLOWER errors.
            let (leader_id, error_code) = match owner {
                Some(id) => (id, KafkaCode::None),
                None => (-1, KafkaCode::LeaderNotAvailable),
            };

            partitions.push(PartitionMetadata {
                error_code,
                partition_index: p,
                leader_id,
                replica_nodes: if leader_id >= 0 {
                    vec![leader_id]
                } else {
                    vec![]
                },
                isr_nodes: if leader_id >= 0 {
                    vec![leader_id]
                } else {
                    vec![]
                },
            });
        }

        TopicMetadata {
            error_code: KafkaCode::None,
            name: topic.to_string(),
            // Kafka marks system topics (those reserved by the broker for
            // its own bookkeeping, e.g. `__consumer_offsets`) with a
            // double-underscore prefix and exposes them as internal in
            // metadata. Admin tools and clients filter on this flag.
            is_internal: topic.starts_with("__"),
            partitions,
        }
    }
}

#[async_trait]
impl Handler for SlateDBClusterHandler {
    fn sasl_required(&self) -> bool {
        self.sasl_required
    }

    /// Handle SaslHandshake by advertising the SaslProvider's mechanism list
    /// when the `sasl` feature is built in.
    #[cfg(feature = "sasl")]
    async fn handle_sasl_handshake(
        &self,
        ctx: &RequestContext,
        request: SaslHandshakeRequestData,
    ) -> SaslHandshakeResponseData {
        let Some(provider) = &self.sasl_provider else {
            return SaslHandshakeResponseData {
                error_code: KafkaCode::UnsupportedSaslMechanism,
                mechanisms: vec![],
            };
        };
        let mechanisms: Vec<String> = provider.supported_mechanisms().to_vec();
        let mech_upper = request.mechanism.to_uppercase();
        let supported = mechanisms
            .iter()
            .any(|m| m.eq_ignore_ascii_case(&mech_upper));
        if supported {
            // Bind the chosen mechanism to this connection. The
            // SaslAuthenticate handler enforces that subsequent auth bytes
            // match the committed mechanism; without that binding, a
            // client can handshake SCRAM and then send PLAIN bytes,
            // eliciting cleartext credentials from a flow the client
            // believed was SCRAM.
            provider.record_negotiated_mechanism(ctx.client_addr, &mech_upper);
        }
        SaslHandshakeResponseData {
            error_code: if supported {
                KafkaCode::None
            } else {
                KafkaCode::UnsupportedSaslMechanism
            },
            mechanisms,
        }
    }

    /// Handle SaslAuthenticate via the SaslProvider when the `sasl` feature
    /// is on. Successful authentication flips the connection-level
    /// `AuthGate` in `dispatch_request_common`.
    ///
    /// Mechanism is sniffed from the wire bytes: a SCRAM
    /// session in progress for this connection wins; otherwise a leading
    /// `n,,` / `y,,` / `c=` indicates SCRAM, while a leading NUL byte
    /// indicates PLAIN. The post-auth state for this connection is
    /// stashed in `self.sasl_post_auth` so the dispatcher knows whether
    /// the handshake is complete.
    #[cfg(feature = "sasl")]
    async fn handle_sasl_authenticate(
        &self,
        ctx: &RequestContext,
        request: SaslAuthenticateRequestData,
    ) -> SaslAuthenticateResponseData {
        use crate::server::SaslPostAuth;
        let Some(provider) = &self.sasl_provider else {
            self.sasl_post_auth.insert(
                ctx.client_addr,
                SaslPostAuth {
                    principal: None,
                    complete: false,
                },
            );
            return SaslAuthenticateResponseData {
                error_code: KafkaCode::SaslAuthenticationFailed,
                error_message: Some("SASL not configured".to_string()),
                auth_bytes: bytes::Bytes::new(),
            };
        };

        let mechanism = pick_sasl_mechanism(
            provider.has_scram_session(ctx.client_addr).await,
            &request.auth_bytes,
        );

        // Enforce the mechanism committed at handshake time. The byte-
        // sniffing in `pick_sasl_mechanism` is necessarily heuristic —
        // PLAIN's first byte is NUL while SCRAM client-first carries a
        // GS2 header — so a malicious client can present SCRAM bytes after
        // claiming PLAIN (or vice-versa) to elicit credentials from a flow
        // the legitimate client believed was the other mechanism. Reject
        // the mismatch instead of authenticating against the wrong
        // mechanism.
        if let Some(committed) = provider.negotiated_mechanism_for(ctx.client_addr)
            && !committed.eq_ignore_ascii_case(mechanism)
        {
            self.sasl_post_auth.insert(
                ctx.client_addr,
                SaslPostAuth {
                    principal: None,
                    complete: false,
                },
            );
            return SaslAuthenticateResponseData {
                error_code: KafkaCode::SaslAuthenticationFailed,
                error_message: Some(format!(
                    "SaslAuthenticate payload does not match mechanism committed in \
                     SaslHandshake (expected {}, got bytes consistent with {})",
                    committed, mechanism
                )),
                auth_bytes: bytes::Bytes::new(),
            };
        }

        let (ok, principal, response_bytes) = provider
            .authenticate_with_session(
                ctx.client_addr,
                mechanism,
                &request.auth_bytes,
                ctx.transport_tls,
            )
            .await;

        // Decide whether the handshake is complete. SCRAM has two stages:
        // the first call returns `ok=false, principal=None` even on the
        // happy path (intermediate challenge), and the second returns
        // `ok=true` with the principal. PLAIN is single-step.
        let intermediate = !ok
            && principal.is_none()
            && !response_bytes.is_empty()
            && !response_bytes.starts_with(b"e=");
        self.sasl_post_auth.insert(
            ctx.client_addr,
            SaslPostAuth {
                principal: principal.clone(),
                complete: ok,
            },
        );

        if ok || intermediate {
            // Both happy-path final and intermediate use error_code=None
            // per the Kafka SASL framing; the dispatcher tells them apart
            // via `take_sasl_post_auth`.
            SaslAuthenticateResponseData {
                error_code: KafkaCode::None,
                error_message: None,
                auth_bytes: bytes::Bytes::from(response_bytes),
            }
        } else {
            SaslAuthenticateResponseData {
                error_code: KafkaCode::SaslAuthenticationFailed,
                error_message: Some("Authentication failed".to_string()),
                auth_bytes: bytes::Bytes::from(response_bytes),
            }
        }
    }

    #[cfg(feature = "sasl")]
    async fn take_sasl_post_auth(
        &self,
        client_addr: std::net::SocketAddr,
    ) -> Option<crate::server::SaslPostAuth> {
        self.sasl_post_auth.remove(&client_addr).map(|(_k, v)| v)
    }

    async fn on_connection_closed(&self, _client_addr: std::net::SocketAddr) {
        #[cfg(feature = "sasl")]
        {
            self.sasl_post_auth.remove(&_client_addr);
            if let Some(provider) = &self.sasl_provider {
                provider.clear_session(_client_addr).await;
            }
        }
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id))]
    async fn handle_metadata(
        &self,
        ctx: &RequestContext,
        request: MetadataRequestData,
    ) -> MetadataResponseData {
        metadata::handle_metadata(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id, topic_count = request.topics.len()))]
    async fn handle_produce(
        &self,
        ctx: &RequestContext,
        request: ProduceRequestData,
    ) -> ProduceResponseData {
        produce::handle_produce(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id, topic_count = request.topics.len()))]
    async fn handle_fetch(
        &self,
        ctx: &RequestContext,
        request: FetchRequestData,
    ) -> FetchResponseData {
        fetch::handle_fetch(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id))]
    async fn handle_list_offsets(
        &self,
        ctx: &RequestContext,
        request: ListOffsetsRequestData,
    ) -> ListOffsetsResponseData {
        offsets::handle_list_offsets(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id))]
    async fn handle_find_coordinator(
        &self,
        ctx: &RequestContext,
        request: FindCoordinatorRequestData,
    ) -> FindCoordinatorResponseData {
        groups::handle_find_coordinator(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id, group_id = %request.group_id))]
    async fn handle_join_group(
        &self,
        ctx: &RequestContext,
        request: JoinGroupRequestData,
    ) -> JoinGroupResponseData {
        groups::handle_join_group(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id, group_id = %request.group_id))]
    async fn handle_sync_group(
        &self,
        ctx: &RequestContext,
        request: SyncGroupRequestData,
    ) -> SyncGroupResponseData {
        groups::handle_sync_group(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id, group_id = %request.group_id))]
    async fn handle_heartbeat(
        &self,
        ctx: &RequestContext,
        request: HeartbeatRequestData,
    ) -> HeartbeatResponseData {
        groups::handle_heartbeat(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id, group_id = %request.group_id))]
    async fn handle_leave_group(
        &self,
        ctx: &RequestContext,
        request: LeaveGroupRequestData,
    ) -> LeaveGroupResponseData {
        groups::handle_leave_group(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id, group_id = %request.group_id))]
    async fn handle_offset_commit(
        &self,
        ctx: &RequestContext,
        request: OffsetCommitRequestData,
    ) -> OffsetCommitResponseData {
        offsets::handle_offset_commit(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id, group_id = %request.group_id))]
    async fn handle_offset_fetch(
        &self,
        ctx: &RequestContext,
        request: OffsetFetchRequestData,
    ) -> OffsetFetchResponseData {
        offsets::handle_offset_fetch(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id))]
    async fn handle_describe_groups(
        &self,
        ctx: &RequestContext,
        request: DescribeGroupsRequestData,
    ) -> DescribeGroupsResponseData {
        groups::handle_describe_groups(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id))]
    async fn handle_list_groups(
        &self,
        ctx: &RequestContext,
        request: ListGroupsRequestData,
    ) -> ListGroupsResponseData {
        groups::handle_list_groups(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id))]
    async fn handle_delete_groups(
        &self,
        ctx: &RequestContext,
        request: DeleteGroupsRequestData,
    ) -> DeleteGroupsResponseData {
        groups::handle_delete_groups(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id))]
    async fn handle_create_topics(
        &self,
        ctx: &RequestContext,
        request: CreateTopicsRequestData,
    ) -> CreateTopicsResponseData {
        admin::handle_create_topics(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id))]
    async fn handle_delete_topics(
        &self,
        ctx: &RequestContext,
        request: DeleteTopicsRequestData,
    ) -> DeleteTopicsResponseData {
        admin::handle_delete_topics(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id))]
    async fn handle_init_producer_id(
        &self,
        ctx: &RequestContext,
        request: InitProducerIdRequestData,
    ) -> InitProducerIdResponseData {
        producer_id::handle_init_producer_id(self, ctx, request).await
    }
}

/// Pick the SASL mechanism for an incoming `auth_bytes` payload.
///
/// Kafka clients commit to a mechanism in `SaslHandshake` and the server
/// is expected to remember it for the subsequent `SaslAuthenticate`
/// requests, but our handler trait is intentionally stateless so we
/// instead detect the mechanism from the bytes:
///
/// - If a SCRAM session is in flight for this connection, the bytes are
///   interpreted as SCRAM `client-final-message`, regardless of leading
///   characters.
/// - Otherwise leading `n,,` / `y,,` / `c=` is SCRAM `client-first-message`
///   (RFC 5802 GS2 header).
/// - Otherwise the message is PLAIN (`[authzid] \0 authcid \0 password`).
#[cfg(feature = "sasl")]
fn pick_sasl_mechanism(has_scram_session: bool, auth_bytes: &[u8]) -> &'static str {
    if has_scram_session {
        return "SCRAM-SHA-256";
    }
    if auth_bytes.starts_with(b"n,,")
        || auth_bytes.starts_with(b"y,,")
        || auth_bytes.starts_with(b"c=")
    {
        "SCRAM-SHA-256"
    } else {
        "PLAIN"
    }
}

#[cfg(test)]
#[path = "handler_tests.rs"]
mod tests;
