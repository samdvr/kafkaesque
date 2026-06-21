//! Produce-during-leader-failover durability test.
//!
//! The audit flagged this as the highest-priority test gap: "the most
//! important durability invariant in the system is unverified". The
//! invariant is:
//!
//!   _Every record `acks=-1` returns success for, before a leader
//!   failover, must be readable by the new leader after the failover._
//!
//! Kafkaesque uses object storage (S3-class) for replication rather than
//! Raft replication of partition data, so "leader failover" here means
//! ownership of the partition transfers between brokers. The data is
//! already in object storage; the new owner just opens a `PartitionStore`
//! on the same path and serves it.
//!
//! That makes the durability invariant testable end-to-end with two
//! co-located `PartitionStore` instances over a shared in-memory object
//! store — the same wire path real brokers use, minus the network.

use bytes::Bytes;
use kafkaesque::cluster::{MockCoordinator, PartitionCoordinator, SlateDBError};

mod common;
use common::{create_object_store, next_port};

const TOPIC: &str = "leader-failover";
const PARTITION: i32 = 0;
const BASE_PATH: &str = "leader-failover-test";

/// CRC-32C (Castagnoli). Mirrors the helper in
/// `tests/linearizability_real_tests.rs` — kept inline so this file is
/// self-contained.
fn crc32c(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut t = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut crc = i as u32;
            let mut j = 0;
            while j < 8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0x82F63B78;
                } else {
                    crc >>= 1;
                }
                j += 1;
            }
            t[i] = crc;
            i += 1;
        }
        t
    };
    let mut crc = !0u32;
    for &b in data {
        let idx = ((crc ^ b as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[idx];
    }
    !crc
}

/// Build a non-idempotent record batch (`producer_id = -1`). The framing
/// matches what the produce handler hands to `PartitionStore::append_batch_durable`.
fn build_batch(record_count: i32) -> Bytes {
    let mut batch = vec![0u8; 100];
    batch[0..8].copy_from_slice(&0i64.to_be_bytes()); // base_offset
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes()); // batch_length
    batch[12..16].copy_from_slice(&0i32.to_be_bytes()); // partition_leader_epoch
    batch[16] = 2; // magic
    batch[21..23].copy_from_slice(&0i16.to_be_bytes()); // attributes
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes()); // last_offset_delta
    batch[27..35].copy_from_slice(&0i64.to_be_bytes()); // base_timestamp
    batch[35..43].copy_from_slice(&0i64.to_be_bytes()); // max_timestamp
    batch[43..51].copy_from_slice(&(-1i64).to_be_bytes()); // producer_id (non-idempotent)
    batch[51..53].copy_from_slice(&0i16.to_be_bytes()); // producer_epoch
    batch[53..57].copy_from_slice(&0i32.to_be_bytes()); // base_sequence
    batch[57..61].copy_from_slice(&record_count.to_be_bytes()); // records_count
    let crc = crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(batch)
}

/// Bootstrap a two-broker mock cluster sharing coordination state.
/// Both brokers see the same topic/partition/owner/epoch maps so an
/// acquire on broker 1 is visible (and refused) on broker 2 until the
/// lease expires.
async fn make_two_broker_cluster() -> (MockCoordinator, MockCoordinator) {
    let broker1 = MockCoordinator::new(1, "localhost", next_port() as i32);
    let broker2 = MockCoordinator {
        broker_id: 2,
        host: "localhost".to_string(),
        port: next_port() as i32,
        brokers: broker1.brokers.clone(),
        topics: broker1.topics.clone(),
        topic_configs: broker1.topic_configs.clone(),
        partition_owners: broker1.partition_owners.clone(),
        partition_epochs: broker1.partition_epochs.clone(),
        consumer_groups: Default::default(),
        offsets: Default::default(),
        next_producer_id: Default::default(),
        next_member_id: Default::default(),
        producer_states: Default::default(),
        mock_raft_index: Default::default(),
    };
    broker1.register_broker().await.unwrap();
    broker2.register_broker().await.unwrap();
    broker1.register_topic(TOPIC, 1).await.unwrap();
    (broker1, broker2)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn produced_records_survive_leader_failover() {
    const N: i64 = 32;

    let object_store = create_object_store();
    let (broker1, broker2) = make_two_broker_cluster().await;

    // Broker 1 acquires partition.
    let acquired = broker1
        .acquire_partition(TOPIC, PARTITION, 60)
        .await
        .unwrap();
    assert!(acquired, "broker 1 must acquire partition");

    // Broker 1 opens a PartitionStore against the shared object store and
    // produces N batches with `acks=-1` semantics (durable writes).
    use kafkaesque::cluster::PartitionStore;
    let store_b1 = PartitionStore::open(object_store.clone(), BASE_PATH, TOPIC, PARTITION)
        .await
        .expect("broker 1 opens partition store");

    let mut acked_offsets = Vec::with_capacity(N as usize);
    for _ in 0..N {
        let base = store_b1
            .append_batch_durable(&build_batch(1))
            .await
            .expect("durable produce");
        acked_offsets.push(base);
    }
    // Offsets must be contiguous from 0.
    assert_eq!(acked_offsets.first().copied(), Some(0));
    for (i, &o) in acked_offsets.iter().enumerate() {
        assert_eq!(o, i as i64, "offset gap on the produce side");
    }
    // Drop broker 1's handle to the partition store: a real failover
    // wouldn't have the old owner gracefully close, but releasing the
    // SlateDB handle ensures broker 2's open is the only live writer.
    drop(store_b1);

    // Force failover: broker 1's leases expire (simulates: process death,
    // network partition, lease-renewal storm — all funnel here).
    broker1.expire_my_leases().await;

    // Broker 2 takes over.
    let acquired = broker2
        .acquire_partition(TOPIC, PARTITION, 60)
        .await
        .unwrap();
    assert!(
        acquired,
        "broker 2 must acquire after broker 1's lease expired"
    );

    let owner = broker2.get_partition_owner(TOPIC, PARTITION).await.unwrap();
    assert_eq!(owner, Some(2), "post-failover owner must be broker 2");

    // Broker 2 opens the SAME path on the shared object store and reads
    // back. Every acked record must be visible.
    let store_b2 = PartitionStore::open(object_store.clone(), BASE_PATH, TOPIC, PARTITION)
        .await
        .expect("broker 2 opens partition store after failover");

    let mut next_fetch = 0i64;
    let mut total_records = 0i64;
    while next_fetch < N {
        let (next_offset, payload) = store_b2
            .fetch_from(next_fetch)
            .await
            .expect("fetch after failover");
        match payload {
            Some(_bytes) => {
                // Each batch we produced contained 1 record, so the next-
                // offset advance equals the records read in this batch.
                assert!(
                    next_offset > next_fetch,
                    "fetch must make progress: next_offset={next_offset}, fetch_offset={next_fetch}"
                );
                total_records += next_offset - next_fetch;
                next_fetch = next_offset;
            }
            None => break,
        }
    }
    assert_eq!(
        total_records, N,
        "every acks=-1 record produced before failover must be readable after failover"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_leader_writes_after_failover_are_fenced() {
    // The dual to the durability test: once ownership has moved, the
    // *prior* owner must not be able to silently keep appending to its
    // local PartitionStore handle. The lease/epoch check on the
    // coordinator side fences the broker, and writes from a fenced
    // coordinator must surface as `Fenced` / `NotOwned` rather than
    // succeeding behind the new leader's back.
    let (broker1, broker2) = make_two_broker_cluster().await;

    let acquired = broker1
        .acquire_partition(TOPIC, PARTITION, 60)
        .await
        .unwrap();
    assert!(acquired);

    // Verify works while broker 1 owns the partition.
    let r = broker1.verify_and_extend_lease(TOPIC, PARTITION, 60).await;
    assert!(r.is_ok(), "verify must succeed for the live owner");

    // Failover.
    broker1.expire_my_leases().await;
    broker2
        .acquire_partition(TOPIC, PARTITION, 60)
        .await
        .unwrap();

    // Broker 1's verify must now fail with `Fenced` — the produce
    // handler's pre-write `verify_and_extend_lease` is the gate that
    // catches a stale leader before it touches storage.
    let r = broker1.verify_and_extend_lease(TOPIC, PARTITION, 60).await;
    match r {
        Err(SlateDBError::Fenced) => {}
        other => panic!("expected Fenced after failover, got {other:?}"),
    }
}

/// In-flight produce during failover: a single iteration that races a
/// spawned `append_batch_durable` against `expire_my_leases` with a
/// barrier. The spawned future must resolve to one of two valid
/// outcomes:
///
///   - `Ok(offset)` — the append committed before fencing. Post-failover
///     the offset must be readable.
///   - `Err(Fenced)` — the append was rejected before write. Post-failover
///     the offset must NOT appear in the new leader's view.
///
/// The third state — Ok with the offset *missing* on the post-failover
/// reader — would be silent data loss and is the exact failure mode the
/// produce/failover invariant is meant to rule out.
async fn run_inflight_produce_failover_iteration(iteration: u64) {
    use kafkaesque::cluster::PartitionStore;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let object_store = create_object_store();
    let topic = format!("inflight-failover-{iteration}");
    let base_path = format!("inflight-failover-{iteration}-path");
    let (broker1, broker2) = make_two_broker_cluster_named(&topic).await;

    let acquired = broker1
        .acquire_partition(&topic, PARTITION, 60)
        .await
        .unwrap();
    assert!(acquired);

    let store_b1 = Arc::new(
        PartitionStore::open(object_store.clone(), &base_path, &topic, PARTITION)
            .await
            .expect("broker 1 opens partition store"),
    );

    // Pre-seed one batch so the post-failover reader has a known prefix.
    let prefix_offset = store_b1
        .append_batch_durable(&build_batch(1))
        .await
        .expect("seed");
    assert_eq!(prefix_offset, 0);

    let barrier = Arc::new(Barrier::new(2));
    let store_for_appender = store_b1.clone();
    let barrier_appender = barrier.clone();
    let appender = tokio::spawn(async move {
        barrier_appender.wait().await;
        store_for_appender
            .append_batch_durable(&build_batch(1))
            .await
    });

    let broker_for_failover = broker1.clone();
    let barrier_failover = barrier.clone();
    let failover = tokio::spawn(async move {
        barrier_failover.wait().await;
        broker_for_failover.expire_my_leases().await;
    });

    let append_result = appender.await.expect("appender join");
    failover.await.expect("failover join");

    drop(store_b1);

    let acquired = broker2
        .acquire_partition(&topic, PARTITION, 60)
        .await
        .unwrap();
    assert!(acquired, "broker 2 must acquire post-failover");

    let store_b2 = PartitionStore::open(object_store.clone(), &base_path, &topic, PARTITION)
        .await
        .expect("broker 2 opens partition store");

    // Drain everything broker 2 can see; collect read offsets.
    let mut next_fetch = 0i64;
    let mut total_records = 0i64;
    while let Ok((next_offset, payload)) = store_b2.fetch_from(next_fetch).await {
        match payload {
            Some(_) => {
                assert!(next_offset > next_fetch, "fetch must make progress");
                total_records += next_offset - next_fetch;
                next_fetch = next_offset;
            }
            None => break,
        }
    }

    match append_result {
        Ok(offset) => {
            // Append claims to have committed at `offset`. Post-failover
            // every claimed offset must be present in the new leader's view.
            assert!(
                offset < total_records,
                "append acked offset {offset} but post-failover broker 2 only sees {total_records} records — silent data loss",
            );
        }
        Err(SlateDBError::Fenced) => {
            // Fenced before write. The pre-seed (offset 0) must be visible,
            // but no extra record from the racing append.
            assert_eq!(
                total_records, 1,
                "Fenced append must not have written a record; post-failover total={total_records}"
            );
        }
        Err(other) => {
            // Any error other than Fenced is a third-state outcome: the
            // append did not succeed, and neither did the lease check.
            // The contract is binary — Ok or Fenced. Anything else
            // points at a regression.
            panic!("unexpected error from in-flight append: {other:?}");
        }
    }
}

/// Helper: same as `make_two_broker_cluster` but parameterized by topic
/// so each iteration of the in-flight test gets fresh coordination state.
async fn make_two_broker_cluster_named(topic: &str) -> (MockCoordinator, MockCoordinator) {
    let broker1 = MockCoordinator::new(1, "localhost", next_port() as i32);
    let broker2 = MockCoordinator {
        broker_id: 2,
        host: "localhost".to_string(),
        port: next_port() as i32,
        brokers: broker1.brokers.clone(),
        topics: broker1.topics.clone(),
        topic_configs: broker1.topic_configs.clone(),
        partition_owners: broker1.partition_owners.clone(),
        partition_epochs: broker1.partition_epochs.clone(),
        consumer_groups: Default::default(),
        offsets: Default::default(),
        next_producer_id: Default::default(),
        next_member_id: Default::default(),
        producer_states: Default::default(),
        mock_raft_index: Default::default(),
    };
    broker1.register_broker().await.unwrap();
    broker2.register_broker().await.unwrap();
    broker1.register_topic(topic, 1).await.unwrap();
    (broker1, broker2)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn produce_in_flight_during_failover_is_fenced_or_durable() {
    // The "thousands of iterations" the audit asks for runs in the nightly
    // chaos workflow; PR CI runs a smoke pass so the test stays in budget.
    // The test body itself is the same — set ITERATIONS via env to scale up.
    let iterations: u64 = std::env::var("INFLIGHT_FAILOVER_ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    for i in 0..iterations {
        run_inflight_produce_failover_iteration(i).await;
    }
}
