//! PartitionCoordinator implementation for RaftCoordinator.
//!
//! Routing summary:
//!
//! - Broker registry / heartbeat / unregister → **control** group
//!   (`ControlCommand::Broker`).
//! - Topic registry (create / delete / list / partition count) → **control**
//!   group (`ControlCommand::TopicRegistry`).
//! - Per-partition lease ops (acquire / renew / release / expire) → **shard**
//!   group `hash(topic) % metadata_shards`
//!   (`ShardCommand::Partition(PartitionStateCommand::*)`).
//! - Read-side state spans both: ownership reads from a shard's
//!   `partition_state.partitions` map; broker liveness reads from control's
//!   `broker_domain.brokers`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tracing::{debug, info};

use super::super::commands::{ControlCommand, ControlResponse, ShardCommand, ShardResponse};
use super::super::domains::{
    BrokerCommand, BrokerResponse, BrokerStatus, PartitionStateCommand, PartitionStateResponse,
    TopicRegistryCommand, TopicRegistryResponse,
};
use super::{RaftCoordinator, current_time_ms};

use crate::cluster::coordinator::BrokerInfo;
use crate::cluster::error::{SlateDBError, SlateDBResult};
use crate::cluster::metrics;
use crate::cluster::traits::PartitionCoordinator;

#[async_trait]
impl PartitionCoordinator for RaftCoordinator {
    fn broker_id(&self) -> i32 {
        self.broker_id
    }

    // Raft node IDs are broker IDs (`config.broker_id as u64` at join
    // time), so the current Raft leader's node id IS the controller's
    // broker id. Returns None during elections, which callers report as
    // controller_id = -1 per Kafka convention.
    //
    // The "controller" in the multi-group layout is the **control**
    // group's leader — it's the node that owns broker registry and topic
    // registry decisions, which is what every "find the controller"
    // protocol path is asking about.
    async fn current_leader_id(&self) -> SlateDBResult<Option<i32>> {
        Ok(self
            .cluster()
            .control()
            .current_leader()
            .map(|id| id as i32))
    }

    async fn register_broker(&self) -> SlateDBResult<()> {
        // Retry with backoff since non-leaders need to forward to leader
        // and the leader may not be ready immediately after election.
        for attempt in 0..30 {
            let command = ControlCommand::Broker(BrokerCommand::Register {
                broker_id: self.broker_id,
                host: self.broker_info.host.clone(),
                port: self.broker_info.port,
                timestamp_ms: current_time_ms(),
            });

            match self.cluster().write_control(command).await {
                Ok(response) => match response {
                    ControlResponse::Broker(BrokerResponse::Registered { .. }) => {
                        info!(broker_id = self.broker_id, "Broker registered with Raft");
                        return Ok(());
                    }
                    other => {
                        return Err(SlateDBError::Storage(format!(
                            "Unexpected response: {:?}",
                            other
                        )));
                    }
                },
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("forward")
                        || err_str.contains("NotLeader")
                        || err_str.contains("Not the Raft leader")
                    {
                        debug!(
                            broker_id = self.broker_id,
                            attempt = attempt,
                            "Register broker needs forwarding, retrying..."
                        );
                        tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        Err(SlateDBError::Storage(
            "Failed to register broker after retries".to_string(),
        ))
    }

    async fn heartbeat(&self) -> SlateDBResult<()> {
        let now = current_time_ms();
        let command = ControlCommand::Broker(BrokerCommand::Heartbeat {
            broker_id: self.broker_id,
            timestamp_ms: now,
            reported_local_timestamp_ms: now,
        });

        let response = self.cluster().write_control(command).await?;
        match response {
            ControlResponse::Broker(BrokerResponse::HeartbeatAck) => Ok(()),
            ControlResponse::Broker(BrokerResponse::NotFound { broker_id: _ }) => {
                // Re-register
                self.register_broker().await
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn get_live_brokers(&self) -> SlateDBResult<Vec<BrokerInfo>> {
        let start = Instant::now();
        let sm = self.cluster().control().state_machine();
        let state = sm.state().await;

        let now = current_time_ms();
        let ttl_ms = self.config.broker_heartbeat_ttl.as_millis() as u64;

        let brokers: Vec<BrokerInfo> = state
            .broker_domain
            .brokers
            .values()
            .filter(|b| b.status == BrokerStatus::Active && now - b.last_heartbeat_ms < ttl_ms)
            .map(|b| BrokerInfo {
                broker_id: b.broker_id,
                host: b.host.clone(),
                port: b.port,
                registered_at: b.registered_at_ms as i64,
            })
            .collect();

        metrics::record_raft_query("get_brokers", start.elapsed().as_secs_f64());
        Ok(brokers)
    }

    async fn unregister_broker(&self) -> SlateDBResult<()> {
        let command = ControlCommand::Broker(BrokerCommand::Unregister {
            broker_id: self.broker_id,
        });

        self.cluster().write_control(command).await?;
        Ok(())
    }

    async fn acquire_partition(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<bool> {
        match self
            .acquire_partition_with_epoch(topic, partition, lease_secs)
            .await
        {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn acquire_partition_with_epoch(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<Option<i32>> {
        let command = ShardCommand::Partition(PartitionStateCommand::AcquirePartition {
            topic: topic.to_string(),
            partition,
            broker_id: self.broker_id,
            lease_duration_ms: lease_secs * 1000,
            timestamp_ms: current_time_ms(),
        });

        let response = self.cluster().write_shard_for_topic(topic, command).await?;
        match response {
            ShardResponse::Partition(PartitionStateResponse::PartitionAcquired {
                topic,
                partition,
                leader_epoch,
                ..
            }) => {
                // Update cache. Take the stamp now (after the Raft write
                // returned and the hook fired synchronously on this replica).
                // `insert_if_fresh` refuses if a *new* invalidation slips in
                // between this stamp and the insert.
                let key = (Arc::from(topic.as_str()), partition);
                let stamp = self.owner_cache_read_stamp(&key);
                self.owner_cache_insert_if_fresh(key, self.broker_id, stamp)
                    .await;
                Ok(Some(leader_epoch))
            }
            ShardResponse::Partition(PartitionStateResponse::PartitionOwnedByOther { .. }) => {
                Ok(None)
            }
            // This broker has been fenced (e.g. marked failed by the fast
            // failure detector while it was partitioned away) — it must not
            // re-acquire ownership until it re-registers. The broker
            // liveness shadow on each shard (reconciler-fed from control)
            // is what gates this locally.
            ShardResponse::Partition(PartitionStateResponse::BrokerNotActive { .. }) => Ok(None),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn renew_partition_lease(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<bool> {
        let command = ShardCommand::Partition(PartitionStateCommand::RenewLease {
            topic: topic.to_string(),
            partition,
            broker_id: self.broker_id,
            lease_duration_ms: lease_secs * 1000,
            timestamp_ms: current_time_ms(),
        });

        let response = self.cluster().write_shard_for_topic(topic, command).await?;
        match response {
            ShardResponse::Partition(PartitionStateResponse::LeaseRenewed { .. }) => Ok(true),
            ShardResponse::Partition(PartitionStateResponse::PartitionNotOwned { .. }) => {
                self.owner_cache_invalidate(&(Arc::from(topic), partition))
                    .await;
                Ok(false)
            }
            // Fenced broker: lease renewal is refused outright.
            ShardResponse::Partition(PartitionStateResponse::BrokerNotActive { .. }) => {
                self.owner_cache_invalidate(&(Arc::from(topic), partition))
                    .await;
                Ok(false)
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    /// Renew leases for many partitions in a single Raft proposal.
    ///
    /// Replaces N round trips for N owned partitions with one — *per shard*.
    /// In the sharded layout we group the request by `hash(topic) %
    /// metadata_shards`, then one batch proposal to each affected shard.
    /// For a broker owning partitions across all 8 shards we pay 8 commits
    /// instead of N. Same per-batch FSM advantage (one fsync, one apply
    /// that internally loops over partitions) is preserved within each shard.
    async fn renew_partition_leases_batch(
        &self,
        partitions: Vec<(Arc<str>, i32)>,
        lease_secs: u64,
    ) -> SlateDBResult<Vec<(Arc<str>, i32, bool)>> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }

        // Group by destination shard and remember the request index so
        // the result vector is in the caller's submitted order.
        let metadata_shards = self.cluster().metadata_shards();
        let mut by_shard: HashMap<u16, Vec<(usize, Arc<str>, i32)>> = HashMap::new();
        for (idx, (topic, partition)) in partitions.iter().enumerate() {
            let shard_id = self.cluster().router().shard_for_topic(topic);
            by_shard
                .entry(shard_id)
                .or_default()
                .push((idx, topic.clone(), *partition));
        }
        // Pre-fill the result with `false` so a shard that returns
        // `BrokerNotActive` (the whole batch refused) leaves them as
        // `not renewed`. This is the same outcome the legacy single-shard
        // path produced.
        let mut out: Vec<(Arc<str>, i32, bool)> = partitions
            .iter()
            .map(|(t, p)| (t.clone(), *p, false))
            .collect();

        let lease_duration_ms = lease_secs * 1000;
        let timestamp_ms = current_time_ms();
        let _ = metadata_shards; // kept for clarity; loop uses `by_shard.into_iter()` directly

        for (shard_id, items) in by_shard {
            let request_partitions: Vec<(String, i32)> =
                items.iter().map(|(_, t, p)| (t.to_string(), *p)).collect();
            let command = ShardCommand::Partition(PartitionStateCommand::RenewLeases {
                broker_id: self.broker_id,
                partitions: request_partitions,
                lease_duration_ms,
                timestamp_ms,
            });
            let response = self.cluster().write_shard(shard_id, command).await?;
            match response {
                ShardResponse::Partition(PartitionStateResponse::LeasesRenewed { results }) => {
                    if results.len() != items.len() {
                        return Err(SlateDBError::Storage(format!(
                            "RenewLeases on shard {} returned {} results for {} partitions",
                            shard_id,
                            results.len(),
                            items.len()
                        )));
                    }
                    for ((idx, topic, partition), outcome) in items.into_iter().zip(results) {
                        let renewed = match outcome {
                            crate::cluster::raft::domains::BatchRenewOutcome::Renewed {
                                ..
                            } => true,
                            crate::cluster::raft::domains::BatchRenewOutcome::NotOwned => {
                                self.owner_cache_invalidate(&(topic.clone(), partition))
                                    .await;
                                false
                            }
                        };
                        out[idx] = (topic, partition, renewed);
                    }
                }
                // Whole-batch refusal because this broker is fenced — the
                // pre-filled `false` entries already match what we return.
                ShardResponse::Partition(PartitionStateResponse::BrokerNotActive { .. }) => {}
                other => {
                    return Err(SlateDBError::Storage(format!(
                        "Unexpected response on shard {}: {:?}",
                        shard_id, other
                    )));
                }
            }
        }
        Ok(out)
    }

    async fn release_partition(&self, topic: &str, partition: i32) -> SlateDBResult<()> {
        let command = ShardCommand::Partition(PartitionStateCommand::ReleasePartition {
            topic: topic.to_string(),
            partition,
            broker_id: self.broker_id,
        });

        self.cluster().write_shard_for_topic(topic, command).await?;
        self.owner_cache_invalidate(&(Arc::from(topic), partition))
            .await;
        Ok(())
    }

    async fn get_partition_owner(&self, topic: &str, partition: i32) -> SlateDBResult<Option<i32>> {
        let start = Instant::now();
        let key = (Arc::from(topic), partition);
        if let Some(cached_owner) = self.owner_cache_get(&key).await
            && self
                .verify_cached_owner(topic, partition, cached_owner)
                .await
        {
            metrics::record_raft_query("get_owner", start.elapsed().as_secs_f64());
            return Ok(Some(cached_owner));
        }

        // Take the cache-generation snapshot BEFORE the linearizable read.
        // If a tombstone bump fires in the window between the read and the
        // insert, `owner_cache_insert_if_fresh` will refuse the insert
        // rather than land a stale entry as fresh.
        let stamp = self.owner_cache_read_stamp(&key);

        // Cache miss on routing-critical metadata: confirm we've applied
        // through the leader's commit index before consulting local state.
        // The lease/ownership data lives in the shard owning this topic.
        let shard = self.cluster().shard_for_topic(topic);
        shard
            .raft()
            .ensure_linearizable()
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to ensure linearizable: {}", e)))?;

        let sm = shard.state_machine();
        let state = sm.state().await;

        // Use the *replicated* lease clock (`state.lease_clock_ms`) rather
        // than the local wall clock. Local `SystemTime::now()` skew would
        // make this broker disagree with the rest of the cluster about
        // whether a lease has expired.
        let now_ms = state.lease_clock_ms;
        let owner = state
            .partition_state
            .partitions
            .get(&(Arc::from(topic), partition))
            .and_then(|p| {
                if p.lease_expires_at_ms > now_ms {
                    p.owner_broker_id
                } else {
                    None
                }
            });

        if let Some(owner_id) = owner {
            self.owner_cache_insert_if_fresh((Arc::from(topic), partition), owner_id, stamp)
                .await;
        }

        metrics::record_raft_query("get_owner", start.elapsed().as_secs_f64());
        Ok(owner)
    }

    async fn owns_partition_for_read(&self, topic: &str, partition: i32) -> SlateDBResult<bool> {
        let start = Instant::now();
        let key = (Arc::from(topic), partition);
        if let Some(cached_owner) = self.owner_cache_get(&key).await
            && self
                .verify_cached_owner(topic, partition, cached_owner)
                .await
        {
            metrics::record_raft_query("owns_partition", start.elapsed().as_secs_f64());
            return Ok(cached_owner == self.broker_id);
        }

        // Stamp BEFORE the linearizable read so a concurrent invalidation
        // can't slip into a fresh-looking insert.
        let stamp = self.owner_cache_read_stamp(&key);

        let shard = self.cluster().shard_for_topic(topic);
        shard
            .raft()
            .ensure_linearizable()
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to ensure linearizable: {}", e)))?;

        let sm = shard.state_machine();
        let state = sm.state().await;

        // See `get_partition_owner` above for why we use the replicated
        // `lease_clock_ms` rather than `current_time_ms()`.
        let now_ms = state.lease_clock_ms;
        let owns = state
            .partition_state
            .partitions
            .get(&(Arc::from(topic), partition))
            .map(|p| p.owner_broker_id == Some(self.broker_id) && p.lease_expires_at_ms > now_ms)
            .unwrap_or(false);

        if owns {
            self.owner_cache_insert_if_fresh(key, self.broker_id, stamp)
                .await;
        }

        metrics::record_raft_query("owns_partition", start.elapsed().as_secs_f64());
        Ok(owns)
    }

    async fn owns_partition_for_write(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<u64> {
        // For writes, ensure linearizable state on the topic's shard before
        // checking ownership.
        self.cluster()
            .shard_for_topic(topic)
            .raft()
            .ensure_linearizable()
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to ensure linearizable: {}", e)))?;

        // Now verify and extend lease atomically
        self.verify_and_extend_lease(topic, partition, lease_secs)
            .await
    }

    async fn verify_and_extend_lease(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<u64> {
        let command = ShardCommand::Partition(PartitionStateCommand::RenewLease {
            topic: topic.to_string(),
            partition,
            broker_id: self.broker_id,
            lease_duration_ms: lease_secs * 1000,
            timestamp_ms: current_time_ms(),
        });

        let response = self.cluster().write_shard_for_topic(topic, command).await?;
        match response {
            ShardResponse::Partition(PartitionStateResponse::LeaseRenewed {
                lease_expires_at_ms,
                ..
            }) => {
                let remaining = lease_expires_at_ms.saturating_sub(current_time_ms()) / 1000;
                Ok(remaining)
            }
            ShardResponse::Partition(PartitionStateResponse::PartitionNotOwned {
                topic,
                partition,
            }) => {
                self.owner_cache_invalidate(&(Arc::from(topic.as_str()), partition))
                    .await;
                Err(SlateDBError::NotOwned { topic, partition })
            }
            // Fenced broker: treat as not owned so the write path returns
            // NotLeaderForPartition to the client instead of retrying.
            ShardResponse::Partition(PartitionStateResponse::BrokerNotActive { .. }) => {
                self.owner_cache_invalidate(&(Arc::from(topic), partition))
                    .await;
                Err(SlateDBError::NotOwned {
                    topic: topic.to_string(),
                    partition,
                })
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn invalidate_ownership_cache(&self, topic: &str, partition: i32) {
        self.owner_cache_invalidate(&(Arc::from(topic), partition))
            .await;
    }

    async fn invalidate_all_ownership_cache(&self) {
        self.owner_cache_invalidate_all().await;
    }

    async fn should_own_partition(&self, topic: &str, partition: i32) -> SlateDBResult<bool> {
        use crate::cluster::coordinator::consistent_hash_assignment;

        // Liveness lives on control; this is the same broker list
        // `get_live_brokers` exposes.
        let sm = self.cluster().control().state_machine();
        let state = sm.state().await;

        let brokers: Vec<BrokerInfo> = state
            .broker_domain
            .brokers
            .values()
            .filter(|b| b.status == BrokerStatus::Active)
            .map(|b| BrokerInfo {
                broker_id: b.broker_id,
                host: b.host.clone(),
                port: b.port,
                registered_at: b.registered_at_ms as i64,
            })
            .collect();

        if brokers.is_empty() {
            return Ok(false);
        }

        let designated_owner = consistent_hash_assignment(topic, partition, &brokers);
        Ok(designated_owner == self.broker_id)
    }

    async fn register_topic(&self, topic: &str, partitions: i32) -> SlateDBResult<()> {
        let command = ControlCommand::TopicRegistry(TopicRegistryCommand::CreateTopic {
            name: topic.to_string(),
            partitions,
            config: HashMap::new(),
            timestamp_ms: current_time_ms(),
        });

        // Topic existence is cluster-wide; the per-partition state seeds
        // (InitPartition) are propagated by the reconciler in the next
        // step of the sharding plan. Until the reconciler ships, partition
        // entries appear lazily as brokers AcquirePartition them — same
        // legacy behaviour for the steady-state hot path, only `CreateTopic`
        // itself is no longer the seeding point.
        let response = self.cluster().write_control(command).await?;
        match response {
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicCreated { .. }) => Ok(()),
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicAlreadyExists {
                ..
            }) => Ok(()),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn register_topic_with_config(
        &self,
        topic: &str,
        partitions: i32,
        config: HashMap<String, String>,
    ) -> SlateDBResult<()> {
        let command = ControlCommand::TopicRegistry(TopicRegistryCommand::CreateTopic {
            name: topic.to_string(),
            partitions,
            config,
            timestamp_ms: current_time_ms(),
        });

        let response = self.cluster().write_control(command).await?;
        match response {
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicCreated { .. }) => Ok(()),
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicAlreadyExists {
                ..
            }) => Ok(()),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn get_topic_config(
        &self,
        topic: &str,
    ) -> SlateDBResult<Option<HashMap<String, String>>> {
        let start = Instant::now();
        let sm = self.cluster().control().state_machine();
        let state = sm.state().await;
        let cfg = state
            .topic_registry
            .topics
            .get(&Arc::from(topic) as &Arc<str>)
            .map(|t| t.config.clone());
        metrics::record_raft_query("get_topic_config", start.elapsed().as_secs_f64());
        Ok(cfg)
    }

    async fn update_topic_config(
        &self,
        topic: &str,
        config: HashMap<String, String>,
    ) -> SlateDBResult<bool> {
        let command = ControlCommand::TopicRegistry(TopicRegistryCommand::UpdateTopicConfig {
            name: topic.to_string(),
            config,
        });
        let response = self.cluster().write_control(command).await?;
        match response {
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicConfigUpdated {
                ..
            }) => Ok(true),
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicNotFound { .. }) => {
                Ok(false)
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn get_topics(&self) -> SlateDBResult<Vec<String>> {
        let start = Instant::now();
        let sm = self.cluster().control().state_machine();
        let state = sm.state().await;

        let topics = state
            .topic_registry
            .topics
            .keys()
            .map(|k| k.to_string())
            .collect();
        metrics::record_raft_query("get_topics", start.elapsed().as_secs_f64());
        Ok(topics)
    }

    async fn topic_exists(&self, topic: &str) -> SlateDBResult<bool> {
        let start = Instant::now();
        let sm = self.cluster().control().state_machine();
        let state = sm.state().await;

        let exists = state
            .topic_registry
            .topics
            .contains_key(&Arc::from(topic) as &Arc<str>);
        metrics::record_raft_query("topic_exists", start.elapsed().as_secs_f64());
        Ok(exists)
    }

    async fn get_partition_count(&self, topic: &str) -> SlateDBResult<Option<i32>> {
        let start = Instant::now();
        let sm = self.cluster().control().state_machine();
        let state = sm.state().await;

        let count = state
            .topic_registry
            .topics
            .get(topic)
            .map(|t| t.partition_count);
        metrics::record_raft_query("get_partition_count", start.elapsed().as_secs_f64());
        Ok(count)
    }

    async fn delete_topic(&self, topic: &str) -> SlateDBResult<bool> {
        let command = ControlCommand::TopicRegistry(TopicRegistryCommand::DeleteTopic {
            name: topic.to_string(),
        });

        let response = self.cluster().write_control(command).await?;
        match response {
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicDeleted { .. }) => Ok(true),
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicNotFound { .. }) => {
                Ok(false)
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn get_assigned_partitions(&self) -> SlateDBResult<Vec<(String, i32)>> {
        use crate::cluster::coordinator::BrokerRing;

        // Brokers + topic list both live on control; no shard fan-out.
        // The hash-ring assignment is computed locally without consulting
        // any per-partition state.
        let sm = self.cluster().control().state_machine();
        let state = sm.state().await;

        let brokers: Vec<BrokerInfo> = state
            .broker_domain
            .brokers
            .values()
            .filter(|b| b.status == BrokerStatus::Active)
            .map(|b| BrokerInfo {
                broker_id: b.broker_id,
                host: b.host.clone(),
                port: b.port,
                registered_at: b.registered_at_ms as i64,
            })
            .collect();

        if brokers.is_empty() {
            return Ok(Vec::new());
        }

        let ring = match BrokerRing::from_brokers(&brokers) {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };
        let mut assigned = Vec::new();
        for topic in state.topic_registry.topics.values() {
            for partition in 0..topic.partition_count {
                let owner = ring.assign(&topic.name, partition);
                if owner == self.broker_id {
                    assigned.push((topic.name.to_string(), partition));
                }
            }
        }

        Ok(assigned)
    }

    async fn get_partition_owners(&self, topic: &str) -> SlateDBResult<Vec<(i32, Option<i32>)>> {
        let start = Instant::now();
        // Topic existence + partition count → control. Per-partition
        // ownership → shard owning this topic. Two SM reads, one each.
        let control_sm = self.cluster().control().state_machine();
        let topic_state = {
            let state = control_sm.state().await;
            match state
                .topic_registry
                .topics
                .get(&Arc::from(topic) as &Arc<str>)
            {
                Some(t) => t.clone(),
                None => {
                    metrics::record_raft_query(
                        "get_partition_owners",
                        start.elapsed().as_secs_f64(),
                    );
                    return Ok(Vec::new());
                }
            }
        };

        let shard = self.cluster().shard_for_topic(topic);
        let shard_sm = shard.state_machine();
        let shard_state = shard_sm.state().await;

        // Use the shard's replicated lease clock for ownership gating.
        let now = shard_state.lease_clock_ms;
        let mut owners = Vec::new();
        for partition in 0..topic_state.partition_count {
            let owner = shard_state
                .partition_state
                .partitions
                .get(&(Arc::from(topic), partition))
                .and_then(|p| {
                    if p.lease_expires_at_ms > now {
                        p.owner_broker_id
                    } else {
                        None
                    }
                });
            owners.push((partition, owner));
        }

        metrics::record_raft_query("get_partition_owners", start.elapsed().as_secs_f64());
        Ok(owners)
    }

    async fn get_partition_leader_epoch(
        &self,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<Option<(Option<i32>, i32)>> {
        // Linearizable read on the topic's shard so we don't return a
        // stale leader_epoch right after a takeover. OffsetForLeaderEpoch
        // sits on the rebalance hot path; the read-index barrier is
        // cheap relative to a stale answer that drives a consumer to
        // truncate.
        let shard = self.cluster().shard_for_topic(topic);
        shard
            .raft()
            .ensure_linearizable()
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to ensure linearizable: {}", e)))?;

        let sm = shard.state_machine();
        let state = sm.state().await;

        // Return the entry as long as it exists; an expired lease still
        // tells the consumer "I had this leader_epoch", which is the
        // honest answer. The handler decides whether to ALSO require
        // current ownership (it does, to fence stale fetches).
        let entry = state
            .partition_state
            .partitions
            .get(&(Arc::from(topic), partition))
            .map(|p| (p.owner_broker_id, p.leader_epoch));
        Ok(entry)
    }

    async fn grow_topic_partitions(&self, topic: &str, new_count: i32) -> SlateDBResult<bool> {
        // Implemented as ControlCommand::TopicRegistry::GrowPartitions.
        // The state machine validates `new_count > existing` and rejects
        // shrinks, so racing growers converge to the largest count.
        use super::super::commands::ControlCommand;
        use super::super::domains::TopicRegistryCommand;
        let command = ControlCommand::TopicRegistry(TopicRegistryCommand::GrowPartitions {
            name: topic.to_string(),
            new_count,
        });
        let response = self.cluster().write_control(command).await?;
        match response {
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicGrown { .. }) => Ok(true),
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicNotFound { .. }) => {
                Ok(false)
            }
            ControlResponse::TopicRegistry(TopicRegistryResponse::InvalidPartitionCount {
                ..
            }) => Err(SlateDBError::Storage(
                "new_count must be strictly greater than the current partition count".to_string(),
            )),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }
}

impl RaftCoordinator {
    /// Confirm a cached owner entry still matches local state-machine data.
    ///
    /// The owner entry lives in the shard owning this topic. We use that
    /// shard's replicated `lease_clock_ms` for the freshness check — the
    /// control group's clock is unrelated.
    async fn verify_cached_owner(&self, topic: &str, partition: i32, cached_owner: i32) -> bool {
        let key = (Arc::from(topic), partition);
        let still_valid = {
            let sm = self.cluster().shard_for_topic(topic).state_machine();
            let state = sm.state().await;
            let now_ms = state.lease_clock_ms;
            state.partition_state.partitions.get(&key).is_some_and(|p| {
                p.lease_expires_at_ms > now_ms && p.owner_broker_id == Some(cached_owner)
            })
        };
        if !still_valid {
            self.owner_cache_invalidate(&key).await;
        }
        still_valid
    }
}
