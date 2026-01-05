//! PartitionCoordinator implementation for RaftCoordinator.
//!
//! This module handles:
//! - Broker registration and heartbeat
//! - Partition ownership (acquire/renew/release)
//! - Topic metadata (create/delete/list)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{debug, info};

use super::super::commands::{CoordinationCommand, CoordinationResponse};
use super::super::domains::{
    BrokerCommand, BrokerResponse, BrokerStatus, PartitionCommand, PartitionResponse,
};
use super::{RaftCoordinator, current_time_ms};

use crate::cluster::coordinator::BrokerInfo;
use crate::cluster::error::{SlateDBError, SlateDBResult};
use crate::cluster::traits::PartitionCoordinator;

#[async_trait]
impl PartitionCoordinator for RaftCoordinator {
    fn broker_id(&self) -> i32 {
        self.broker_id
    }

    async fn register_broker(&self) -> SlateDBResult<()> {
        // Retry with backoff since non-leaders need to forward to leader
        // and the leader may not be ready immediately after election
        for attempt in 0..30 {
            let command = CoordinationCommand::BrokerDomain(BrokerCommand::Register {
                broker_id: self.broker_id,
                host: self.broker_info.host.clone(),
                port: self.broker_info.port,
                timestamp_ms: current_time_ms(),
            });

            match self.node.write(command).await {
                Ok(response) => match response {
                    CoordinationResponse::BrokerDomainResponse(BrokerResponse::Registered {
                        ..
                    }) => {
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
                    // Retry on forwarding errors (not the leader)
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
        let command = CoordinationCommand::BrokerDomain(BrokerCommand::Heartbeat {
            broker_id: self.broker_id,
            timestamp_ms: now,
            reported_local_timestamp_ms: now,
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::BrokerDomainResponse(BrokerResponse::HeartbeatAck) => Ok(()),
            CoordinationResponse::BrokerDomainResponse(BrokerResponse::NotFound {
                broker_id: _,
            }) => {
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
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let now = current_time_ms();
        let ttl_ms = self.config.broker_heartbeat_ttl.as_millis() as u64;

        let brokers: Vec<BrokerInfo> = inner_state
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

        Ok(brokers)
    }

    async fn unregister_broker(&self) -> SlateDBResult<()> {
        let command = CoordinationCommand::BrokerDomain(BrokerCommand::Unregister {
            broker_id: self.broker_id,
        });

        self.node.write(command).await?;
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
        let command = CoordinationCommand::PartitionDomain(PartitionCommand::AcquirePartition {
            topic: topic.to_string(),
            partition,
            broker_id: self.broker_id,
            lease_duration_ms: lease_secs * 1000,
            timestamp_ms: current_time_ms(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::PartitionDomainResponse(
                PartitionResponse::PartitionAcquired {
                    topic,
                    partition,
                    leader_epoch,
                    ..
                },
            ) => {
                // Update cache
                self.owner_cache
                    .insert((Arc::from(topic.as_str()), partition), self.broker_id)
                    .await;
                Ok(Some(leader_epoch))
            }
            CoordinationResponse::PartitionDomainResponse(
                PartitionResponse::PartitionOwnedByOther { .. },
            ) => Ok(None),
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
        let command = CoordinationCommand::PartitionDomain(PartitionCommand::RenewLease {
            topic: topic.to_string(),
            partition,
            broker_id: self.broker_id,
            lease_duration_ms: lease_secs * 1000,
            timestamp_ms: current_time_ms(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeaseRenewed {
                ..
            }) => Ok(true),
            CoordinationResponse::PartitionDomainResponse(
                PartitionResponse::PartitionNotOwned { .. },
            ) => {
                self.owner_cache
                    .invalidate(&(Arc::from(topic), partition))
                    .await;
                Ok(false)
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn release_partition(&self, topic: &str, partition: i32) -> SlateDBResult<()> {
        let command = CoordinationCommand::PartitionDomain(PartitionCommand::ReleasePartition {
            topic: topic.to_string(),
            partition,
            broker_id: self.broker_id,
        });

        self.node.write(command).await?;
        self.owner_cache
            .invalidate(&(Arc::from(topic), partition))
            .await;
        Ok(())
    }

    async fn get_partition_owner(&self, topic: &str, partition: i32) -> SlateDBResult<Option<i32>> {
        // Check cache first (for read-path operations that can tolerate slight staleness)
        if let Some(owner) = self.owner_cache.get(&(Arc::from(topic), partition)).await {
            return Ok(Some(owner));
        }

        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let owner = inner_state
            .partition_domain
            .partitions
            .get(&(Arc::from(topic), partition))
            .and_then(|p| {
                if p.lease_expires_at_ms > current_time_ms() {
                    p.owner_broker_id
                } else {
                    None
                }
            });

        if let Some(owner_id) = owner {
            self.owner_cache
                .insert((Arc::from(topic), partition), owner_id)
                .await;
        }

        Ok(owner)
    }

    async fn owns_partition_for_read(&self, topic: &str, partition: i32) -> SlateDBResult<bool> {
        // Check cache first (fast path for read operations)
        if let Some(owner) = self.owner_cache.get(&(Arc::from(topic), partition)).await {
            return Ok(owner == self.broker_id);
        }

        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let owns = inner_state
            .partition_domain
            .partitions
            .get(&(Arc::from(topic), partition))
            .map(|p| {
                p.owner_broker_id == Some(self.broker_id)
                    && p.lease_expires_at_ms > current_time_ms()
            })
            .unwrap_or(false);

        if owns {
            self.owner_cache
                .insert((Arc::from(topic), partition), self.broker_id)
                .await;
        }

        Ok(owns)
    }

    async fn owns_partition_for_write(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<u64> {
        // For writes, ensure linearizable state before checking
        self.node.ensure_linearizable().await?;

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
        let command = CoordinationCommand::PartitionDomain(PartitionCommand::RenewLease {
            topic: topic.to_string(),
            partition,
            broker_id: self.broker_id,
            lease_duration_ms: lease_secs * 1000,
            timestamp_ms: current_time_ms(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeaseRenewed {
                lease_expires_at_ms,
                ..
            }) => {
                let remaining = lease_expires_at_ms.saturating_sub(current_time_ms()) / 1000;
                Ok(remaining)
            }
            CoordinationResponse::PartitionDomainResponse(
                PartitionResponse::PartitionNotOwned { topic, partition },
            ) => {
                self.owner_cache
                    .invalidate(&(Arc::from(topic.as_str()), partition))
                    .await;
                Err(SlateDBError::NotOwned { topic, partition })
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn invalidate_ownership_cache(&self, topic: &str, partition: i32) {
        self.owner_cache
            .invalidate(&(Arc::from(topic), partition))
            .await;
    }

    async fn invalidate_all_ownership_cache(&self) {
        self.owner_cache.invalidate_all();
        self.owner_cache.run_pending_tasks().await;
    }

    async fn should_own_partition(&self, topic: &str, partition: i32) -> SlateDBResult<bool> {
        use crate::cluster::coordinator::consistent_hash_assignment;

        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let brokers: Vec<BrokerInfo> = inner_state
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
        let command = CoordinationCommand::PartitionDomain(PartitionCommand::CreateTopic {
            name: topic.to_string(),
            partitions,
            config: HashMap::new(),
            timestamp_ms: current_time_ms(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicCreated {
                ..
            }) => Ok(()),
            CoordinationResponse::PartitionDomainResponse(
                PartitionResponse::TopicAlreadyExists { .. },
            ) => Ok(()),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn get_topics(&self) -> SlateDBResult<Vec<String>> {
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        Ok(inner_state
            .partition_domain
            .topics
            .keys()
            .map(|k| k.to_string())
            .collect())
    }

    async fn topic_exists(&self, topic: &str) -> SlateDBResult<bool> {
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        Ok(inner_state
            .partition_domain
            .topics
            .contains_key(&Arc::from(topic) as &Arc<str>))
    }

    async fn get_partition_count(&self, topic: &str) -> SlateDBResult<Option<i32>> {
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        Ok(inner_state
            .partition_domain
            .topics
            .get(topic)
            .map(|t| t.partition_count))
    }

    async fn delete_topic(&self, topic: &str) -> SlateDBResult<bool> {
        let command = CoordinationCommand::PartitionDomain(PartitionCommand::DeleteTopic {
            name: topic.to_string(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicDeleted {
                ..
            }) => Ok(true),
            CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicNotFound {
                ..
            }) => Ok(false),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn get_assigned_partitions(&self) -> SlateDBResult<Vec<(String, i32)>> {
        use crate::cluster::coordinator::consistent_hash_assignment;

        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let brokers: Vec<BrokerInfo> = inner_state
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

        let mut assigned = Vec::new();
        for topic in inner_state.partition_domain.topics.values() {
            for partition in 0..topic.partition_count {
                let owner = consistent_hash_assignment(&topic.name, partition, &brokers);
                if owner == self.broker_id {
                    assigned.push((topic.name.to_string(), partition));
                }
            }
        }

        Ok(assigned)
    }

    async fn get_partition_owners(&self, topic: &str) -> SlateDBResult<Vec<(i32, Option<i32>)>> {
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let topic_state = match inner_state
            .partition_domain
            .topics
            .get(&Arc::from(topic) as &Arc<str>)
        {
            Some(t) => t,
            None => return Ok(Vec::new()),
        };

        let now = current_time_ms();
        let mut owners = Vec::new();
        for partition in 0..topic_state.partition_count {
            let owner = inner_state
                .partition_domain
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

        Ok(owners)
    }
}
