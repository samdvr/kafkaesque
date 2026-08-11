//! `CreateTopics` seeds per-partition shard state eagerly.
//!
//! Background. Topic *existence* is recorded in the **control** Raft
//! group, but per-partition lease state lives in a **shard** group. These
//! used to be two independent steps: `register_topic` wrote `CreateTopic`
//! to control and left `InitPartition` to the reconciler's next tick.
//!
//! That gap was client-visible. `RenewLease` (and any lease op) against a
//! partition the shard state machine has never seen falls through to
//! `PartitionNotOwned`, which the handler maps to `NotLeaderForPartition`.
//! A producer that wrote immediately after `CreateTopics` — which is what
//! every "create topic then produce" test and most real client bootstraps
//! do — could lose the race and see a spurious error on a topic the broker
//! had just confirmed creating. The two fsyncs per Raft apply that used to
//! hide the window disappeared when applies got faster, which is when it
//! started showing up.
//!
//! Scope, measured rather than assumed: this closes the control→shard
//! seeding gap, and it is *not* what made
//! `tests/offset_for_leader_epoch_tests.rs` flaky. That target failed 2 of
//! 20 full-target runs both with and without the eager seeding; its cause
//! was a 5s setup deadline that the partition manager's ownership sweep
//! can miss when 9 brokers boot concurrently (see `TOPIC_READY_TIMEOUT`
//! there). Don't cite this fix as the reason that target went green.
//!
//! These tests pin the fix: after `register_topic` / `grow_topic_partitions`
//! returns, the shard SM already holds an entry for every partition, with
//! **no reconciler pass in between**. The helper deliberately does not call
//! `start_background_tasks`, so nothing but the create path itself can seed
//! — if these pass, the seeding is eager rather than merely eventual.
//!
//! The reconciler remains the backstop (see
//! `reconciler_seeds_partitions_for_topic_created_on_control` in
//! `src/cluster/raft/cluster.rs`), which is why the assertions here are
//! about *timing*, not about which component is capable of seeding.

use std::sync::Arc;

use kafkaesque::cluster::PartitionCoordinator;

mod common;
use common::build_single_node_raft;

/// Partition keys the shard owning `topic` currently holds.
///
/// Reads the local state machine directly rather than going through a
/// lease op, because a lease op would auto-create the entry as a legacy
/// fallback and mask exactly the gap under test.
async fn seeded_partitions(coord: &kafkaesque::cluster::RaftCoordinator, topic: &str) -> Vec<i32> {
    let sm = coord.cluster().shard_for_topic(topic).state_machine();
    let state = sm.state().await;
    let mut found: Vec<i32> = state
        .partition_state
        .partitions
        .keys()
        .filter(|(t, _)| &**t == topic)
        .map(|(_, p)| *p)
        .collect();
    found.sort_unstable();
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_topic_seeds_every_partition_before_returning() {
    let coord = build_single_node_raft().await;
    let topic = "eager-seed";
    let partitions = 6i32;

    // Pre-condition: nothing seeded for a topic that doesn't exist yet.
    assert!(
        seeded_partitions(&coord, topic).await.is_empty(),
        "shard already holds partitions for a topic that was never created"
    );

    coord.register_topic(topic, partitions).await.unwrap();

    // No reconciler pass, no sleep, no retry loop: the entries must be
    // there the instant register_topic returns.
    assert_eq!(
        seeded_partitions(&coord, topic).await,
        (0..partitions).collect::<Vec<_>>(),
        "register_topic returned before seeding every partition — a produce \
         racing this window gets NotLeaderForPartition on a topic the \
         broker just said it created"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_topic_seeds_partition_zero_for_single_partition_topic() {
    // Partition 0 is the case the control→shard gap hit hardest: unlike
    // partitions 1..N (which the ownership loop acquires lazily), the
    // broker believes it owns partition 0 immediately, so `get_store`
    // succeeds while the shard SM has no entry at all.
    let coord = build_single_node_raft().await;
    let topic = "eager-seed-single";

    coord.register_topic(topic, 1).await.unwrap();

    assert_eq!(
        seeded_partitions(&coord, topic).await,
        vec![0],
        "partition 0 not seeded on return from register_topic"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_register_topic_is_idempotent() {
    // A retried CreateTopics takes the `TopicAlreadyExists` arm. That arm
    // must still seed: the first attempt may have committed the control
    // entry and then failed partway through seeding, and a client's retry
    // is the natural repair point. `InitPartition` is idempotent, so this
    // is cheap.
    let coord = build_single_node_raft().await;
    let topic = "eager-seed-retry";
    let partitions = 3i32;

    coord.register_topic(topic, partitions).await.unwrap();
    let after_first = seeded_partitions(&coord, topic).await;

    coord.register_topic(topic, partitions).await.unwrap();

    assert_eq!(
        seeded_partitions(&coord, topic).await,
        after_first,
        "second register_topic changed the seeded set — InitPartition is \
         supposed to be a no-op for existing entries"
    );
    assert_eq!(after_first, (0..partitions).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grow_topic_partitions_seeds_the_added_partitions() {
    // CreatePartitions has the same control→shard gap as CreateTopics: the
    // added partitions need shard entries before anything can acquire
    // them.
    let coord = build_single_node_raft().await;
    let topic = "eager-seed-grow";

    coord.register_topic(topic, 2).await.unwrap();
    assert_eq!(seeded_partitions(&coord, topic).await, vec![0, 1]);

    let grown = coord.grow_topic_partitions(topic, 5).await.unwrap();
    assert!(grown, "grow_topic_partitions reported the topic as missing");

    assert_eq!(
        seeded_partitions(&coord, topic).await,
        vec![0, 1, 2, 3, 4],
        "grow_topic_partitions returned before seeding the added partitions"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeded_partition_is_immediately_acquirable_and_renewable() {
    // Acquire-then-renew with no delay after create, for every partition.
    //
    // Unlike the tests above, this one also passed *before* the eager
    // seeding: on a single node `AcquirePartition` auto-creates a missing
    // entry as a legacy fallback, so the renew that follows finds one. It
    // is kept as a forward-looking guard — the seeding must not corrupt or
    // pre-claim the entries it creates (e.g. seeding with a non-`None`
    // owner would make this acquire return `false`) — not as a reproducer
    // for the race. The reproducers are the state-machine assertions
    // above; the client-visible multi-broker path is covered by
    // `tests/offset_for_leader_epoch_tests.rs` and the e2e targets.
    let coord = build_single_node_raft().await;
    let topic = "eager-seed-lease";
    let partitions = 4i32;

    coord.register_topic(topic, partitions).await.unwrap();

    const LEASE_SECS: u64 = 30;

    for partition in 0..partitions {
        let acquired = coord
            .acquire_partition(topic, partition, LEASE_SECS)
            .await
            .unwrap_or_else(|e| panic!("acquire ({topic}, {partition}) failed: {e}"));
        assert!(
            acquired,
            "could not acquire ({topic}, {partition}) right after create"
        );

        let renewed = coord
            .renew_partition_lease(topic, partition, LEASE_SECS)
            .await
            .unwrap_or_else(|e| panic!("renew ({topic}, {partition}) failed: {e}"));
        assert!(
            renewed,
            "renew ({topic}, {partition}) reported lost ownership immediately \
             after a successful acquire"
        );

        assert_eq!(
            coord.get_partition_owner(topic, partition).await.unwrap(),
            Some(coord.broker_id()),
            "owner mismatch for ({topic}, {partition}) after acquire"
        );
    }

    // Keep the Arc alive to the end so background lease work can't drop
    // the coordinator mid-assertion.
    drop(Arc::clone(&coord));
}
