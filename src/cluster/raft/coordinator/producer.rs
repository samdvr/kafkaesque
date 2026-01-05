//! ProducerCoordinator implementation for RaftCoordinator.
//!
//! This module handles:
//! - Producer ID allocation
//! - Producer state storage and retrieval

use std::collections::HashMap;

use async_trait::async_trait;

use super::super::commands::{CoordinationCommand, CoordinationResponse};
use super::super::domains::{ProducerCommand, ProducerResponse};
use super::{RaftCoordinator, current_time_ms};

use crate::cluster::coordinator::producer::PersistedProducerState;
use crate::cluster::error::{SlateDBError, SlateDBResult};
use crate::cluster::traits::ProducerCoordinator;

#[async_trait]
impl ProducerCoordinator for RaftCoordinator {
    async fn next_producer_id(&self) -> SlateDBResult<i64> {
        let command = CoordinationCommand::ProducerDomain(ProducerCommand::AllocateProducerId);
        let response = self.node.write(command).await?;

        match response {
            CoordinationResponse::ProducerDomainResponse(
                ProducerResponse::ProducerIdAllocated { producer_id, .. },
            ) => Ok(producer_id),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn init_producer_id(
        &self,
        transactional_id: Option<&str>,
        requested_producer_id: i64,
        requested_epoch: i16,
    ) -> SlateDBResult<(i64, i16)> {
        let command = CoordinationCommand::ProducerDomain(ProducerCommand::InitProducerId {
            transactional_id: transactional_id.map(|s| s.to_string()),
            producer_id: requested_producer_id,
            epoch: requested_epoch,
            timeout_ms: 30_000,
            timestamp_ms: current_time_ms(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::ProducerDomainResponse(
                ProducerResponse::ProducerIdAllocated { producer_id, epoch },
            ) => Ok((producer_id, epoch)),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn store_producer_state(
        &self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        last_sequence: i32,
        producer_epoch: i16,
    ) -> SlateDBResult<()> {
        let command = CoordinationCommand::ProducerDomain(ProducerCommand::StoreProducerState {
            topic: topic.to_string(),
            partition,
            producer_id,
            last_sequence,
            producer_epoch,
            timestamp_ms: current_time_ms(),
        });

        self.node.write(command).await?;
        Ok(())
    }

    async fn load_producer_states(
        &self,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<HashMap<i64, PersistedProducerState>> {
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let mut result = HashMap::new();
        for ((t, p, pid), state) in &inner_state.producer_domain.producer_states {
            if t.as_ref() == topic && *p == partition {
                result.insert(
                    *pid,
                    PersistedProducerState {
                        last_sequence: state.last_sequence,
                        producer_epoch: state.producer_epoch,
                    },
                );
            }
        }

        Ok(result)
    }
}
