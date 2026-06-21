//! ProducerCoordinator implementation for RaftCoordinator.
//!
//! This module handles:
//! - Producer ID allocation (control group — cluster-wide monotonic counter)
//! - Per-producer epoch / idempotency state (shard group, by `hash(producer_id)`)
//! - Producer state for delivery semantics (shard group, by `hash(producer_id)`)
//!
//! The split is per the sharding plan: only the **counter** is global; every
//! per-id table is sharded so a 100k-producer cluster doesn't write all its
//! producer-state commits through a single Raft log.

use std::collections::HashMap;

use async_trait::async_trait;

use super::super::commands::{ControlCommand, ControlResponse, ShardCommand, ShardResponse};
use super::super::domains::{ProducerCommand, ProducerResponse};
use super::{RaftCoordinator, current_time_ms};

use crate::cluster::coordinator::producer::PersistedProducerState;
use crate::cluster::error::{SlateDBError, SlateDBResult};
use crate::cluster::traits::ProducerCoordinator;

#[async_trait]
impl ProducerCoordinator for RaftCoordinator {
    async fn next_producer_id(&self) -> SlateDBResult<i64> {
        // Per-call request token: openraft's `client_write` is not
        // idempotent, so a leader-side commit followed by a crash before
        // reply causes the proposer to retry and a duplicate entry to
        // commit. The FSM caches recent (token, producer_id) pairs and
        // returns the cached id on replay, collapsing the duplicate.
        let token = uuid::Uuid::new_v4().as_u128();
        let response = self
            .cluster()
            .write_control(ControlCommand::AllocateProducerId {
                request_token: Some(token),
            })
            .await?;

        match response {
            ControlResponse::Producer(ProducerResponse::ProducerIdAllocated {
                producer_id,
                ..
            }) => Ok(producer_id),
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
        // Mint the token at THIS boundary (not inside the FSM). openraft's
        // `client_write` is not idempotent: a leader-side commit followed
        // by a crash before reply causes the proposer to retry, which
        // commits a SECOND `InitProducerId` entry. With a stable token
        // attached to both entries, the FSM returns the originally-minted
        // (producer_id, epoch) pair on replay instead of burning a fresh
        // one. The token is per-call here; client-SDK retries above the
        // broker still mint distinct tokens, but at least the openraft-
        // layer dup is collapsed.
        let token = uuid::Uuid::new_v4().as_u128();
        let command = ShardCommand::Producer(ProducerCommand::InitProducerId {
            transactional_id: transactional_id.map(|s| s.to_string()),
            producer_id: requested_producer_id,
            epoch: requested_epoch,
            timeout_ms: 30_000,
            timestamp_ms: current_time_ms(),
            request_token: Some(token),
        });

        // Per-producer state lives on the shard owning this producer id.
        // `requested_producer_id == -1` (caller hasn't allocated one yet)
        // hashes deterministically too, so the FSM can mint a fresh id and
        // have its state on the same shard the caller will look it up on
        // afterwards.
        let response = self
            .cluster()
            .write_shard_for_producer(requested_producer_id, command)
            .await?;
        match response {
            ShardResponse::Producer(ProducerResponse::ProducerIdAllocated {
                producer_id,
                epoch,
            }) => Ok((producer_id, epoch)),
            // The FSM's ProducerFenced reply maps to a typed FencedProducer
            // error so the handler emits InvalidProducerEpoch (47) on the
            // wire. Returning a generic Storage error would surface as the
            // generic Unknown code and the client would retry indefinitely
            // with the fenced identity.
            ShardResponse::Producer(ProducerResponse::ProducerFenced {
                stored_producer_id,
                stored_epoch,
            }) => Err(SlateDBError::FencedProducer {
                producer_id: stored_producer_id,
                expected_epoch: stored_epoch,
                actual_epoch: requested_epoch,
            }),
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
        let command = ShardCommand::Producer(ProducerCommand::StoreProducerState {
            topic: topic.to_string(),
            partition,
            producer_id,
            last_sequence,
            producer_epoch,
            timestamp_ms: current_time_ms(),
        });

        // Per-producer state is sharded by producer id, not by topic — that
        // way every produce request from the same producer hits one shard
        // for its idempotency state regardless of which topic it writes to.
        self.cluster()
            .write_shard_for_producer(producer_id, command)
            .await?;
        Ok(())
    }

    async fn load_producer_states(
        &self,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<HashMap<i64, PersistedProducerState>> {
        // Producer state could live on any shard (sharding is by
        // producer_id, not by topic), so loading state for a (topic, part)
        // pair has to scan every shard. This is a startup-time recovery
        // path, not a hot path, so the O(N) fan-out is acceptable.
        let mut result = HashMap::new();
        for shard in self.cluster().shards() {
            let sm = shard.state_machine();
            let state = sm.state().await;
            for ((t, p, pid), s) in &state.producer_state.producer_states {
                if t.as_ref() == topic && *p == partition {
                    result.insert(
                        *pid,
                        PersistedProducerState {
                            last_sequence: s.last_sequence,
                            producer_epoch: s.producer_epoch,
                        },
                    );
                }
            }
        }

        Ok(result)
    }
}
