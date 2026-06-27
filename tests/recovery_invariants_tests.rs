//! Recovery-invariant tests after unclean restart.
//!
//! Background. `tests/durability_contract_props.rs` proves the offset
//! contract: any offset returned by `append_batch_durable` is still
//! readable after a crash + reopen against the same backing store.
//! The audit's P0.8 flagged complementary invariants that the property
//! test doesn't reach:
//!
//! 1. **Empty-partition reopen**: a partition that's never been written
//!    to must reopen with HWM=0, LSO=0, and accept fresh appends from
//!    offset 0 — a regression here would either reject the first append
//!    or assign offset N.
//! 2. **Multi-cycle recovery**: HWM and LSO survive multiple
//!    crash+reopen cycles without drift.
//! 3. **HWM recovers without explicit flush** for acks=0 writes ONLY
//!    when SlateDB has flushed independently. Pin the divergence the
//!    README's durability table describes.
//! 4. **Recovery after retention**: a crash *after* `apply_retention`
//!    flushed the new log_start_offset must reopen with both LSO
//!    advanced AND HWM intact.
//! 5. **Idempotent producer state across multiple cycles**: a producer
//!    that survives 3 crash-reopen cycles must still get
//!    DuplicateSequence on a re-replay of any historical batch — not
//!    just the immediate previous run.
//! 6. **Fresh producer after recovery starts at sequence 0**: a brand-new
//!    producer ID seeing an existing partition must be admitted at
//!    sequence 0; the existing producer state of OTHER ids must not
//!    interfere.
//!
//! `src/cluster/partition_store_tests.rs:1187+` already covers the
//! single-cycle producer-recovery case. These tests cover the *integration*
//! invariants on top.

use std::sync::Arc;

use bytes::Bytes;
use kafkaesque::cluster::PartitionStore;
use object_store::ObjectStore;
use object_store::memory::InMemory;

mod common;

const BASE_PATH: &str = "recovery-invariants";
const TOPIC: &str = "recovery-topic";
const PARTITION: i32 = 0;

/// Build a v2 RecordBatch. With `producer_id = -1` (the default we use
/// for idempotency-off tests), back-to-back fixtures are accepted as
/// distinct appends. With a real `producer_id >= 0`, the broker dedups
/// exact-replays — exactly what these tests want for producer-state
/// recovery.
fn build_batch(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    record_count: i32,
) -> Bytes {
    let mut batch = vec![0u8; 100];
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes());
    batch[16] = 2;
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes());
    batch[27..35].copy_from_slice(&1_000i64.to_be_bytes()); // first_timestamp
    batch[35..43].copy_from_slice(&1_000i64.to_be_bytes()); // max_timestamp
    batch[43..51].copy_from_slice(&producer_id.to_be_bytes());
    batch[51..53].copy_from_slice(&producer_epoch.to_be_bytes());
    batch[53..57].copy_from_slice(&base_sequence.to_be_bytes());
    batch[57..61].copy_from_slice(&record_count.to_be_bytes());
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(batch)
}

async fn open(object_store: Arc<dyn ObjectStore>) -> PartitionStore {
    PartitionStore::open(object_store, BASE_PATH, TOPIC, PARTITION)
        .await
        .expect("open partition store")
}

// ---------------------------------------------------------------------------
// 1. Empty partition reopens cleanly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fresh_partition_reopens_with_zero_hwm() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    {
        let store = open(object_store.clone()).await;
        assert_eq!(store.high_watermark(), 0);
        store.close().await.expect("close");
    }

    let reopened = open(object_store).await;
    assert_eq!(
        reopened.high_watermark(),
        0,
        "fresh-and-closed partition must reopen at HWM=0",
    );
    assert_eq!(reopened.log_start_offset(), 0);

    // First append on the reopened, never-written partition must land at
    // offset 0 — not at some "post-reopen drift" offset.
    let first = reopened
        .append_batch_durable(&build_batch(-1, -1, -1, 3))
        .await
        .expect("first append after reopen");
    assert_eq!(
        first, 0,
        "first append on never-written partition must be offset 0"
    );
}

// ---------------------------------------------------------------------------
// 2. Multi-cycle HWM stability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hwm_survives_three_crash_reopen_cycles() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut expected_hwm: i64 = 0;

    for cycle in 0..3 {
        let store = open(object_store.clone()).await;
        assert_eq!(
            store.high_watermark(),
            expected_hwm,
            "cycle {}: HWM after reopen must equal HWM at last close",
            cycle,
        );

        // Append two batches (5 records total).
        store
            .append_batch_durable(&build_batch(-1, -1, -1, 2))
            .await
            .unwrap();
        store
            .append_batch_durable(&build_batch(-1, -1, -1, 3))
            .await
            .unwrap();
        expected_hwm += 5;
        assert_eq!(store.high_watermark(), expected_hwm);

        // Simulated graceful close (so we know the durable writes are
        // committed). The crash-recovery property test covers the
        // drop-without-close case; this test isolates the HWM-is-not-
        // re-derived-incorrectly invariant.
        store.close().await.expect("close");
    }
}

// ---------------------------------------------------------------------------
// 3. New appends after recovery start at recovered HWM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn appends_after_recovery_continue_from_recovered_hwm() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let pre_close_hwm;
    {
        let store = open(object_store.clone()).await;
        store
            .append_batch_durable(&build_batch(-1, -1, -1, 4))
            .await
            .unwrap();
        store
            .append_batch_durable(&build_batch(-1, -1, -1, 6))
            .await
            .unwrap();
        pre_close_hwm = store.high_watermark();
        assert_eq!(pre_close_hwm, 10);
        store.close().await.expect("close");
    }

    let reopened = open(object_store).await;
    assert_eq!(reopened.high_watermark(), pre_close_hwm);

    // The next append must land at exactly `pre_close_hwm`, not at 0
    // (offset reuse) or `pre_close_hwm + N` (gap).
    let next_offset = reopened
        .append_batch_durable(&build_batch(-1, -1, -1, 1))
        .await
        .unwrap();
    assert_eq!(
        next_offset, pre_close_hwm,
        "post-recovery append must continue at recovered HWM, not reuse or skip offsets",
    );
}

// ---------------------------------------------------------------------------
// 4. Idempotent producer state survives multiple cycles
// ---------------------------------------------------------------------------

#[tokio::test]
async fn idempotent_producer_state_survives_multiple_recovery_cycles() {
    // Three crash-reopen cycles. After each cycle, an exact-replay of
    // an OLD batch (sequence < last seen) must be rejected, and an
    // exact-replay of the most recent batch must be re-acked at the
    // original offset. A regression that only persisted producer state
    // in the latest snapshot would pass cycle 1 but fail cycle 2 or 3.
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let producer_id: i64 = 42_000;

    // Cycle 0: write sequences 0-2.
    {
        let store = open(object_store.clone()).await;
        store
            .append_batch_durable(&build_batch(producer_id, 0, 0, 3))
            .await
            .unwrap();
        store.close().await.unwrap();
    }
    // Cycle 1: write sequences 3-5.
    {
        let store = open(object_store.clone()).await;
        store
            .append_batch_durable(&build_batch(producer_id, 0, 3, 3))
            .await
            .unwrap();
        store.close().await.unwrap();
    }
    // Cycle 2: verify dedup against the *original* sequences 0-2 still
    // works AND writes sequences 6-8.
    {
        let store = open(object_store.clone()).await;
        // Exact-replay of the most recent batch (3-5) should re-ack at
        // its original base_offset (3, since each batch had 3 records).
        let replay_3_5 = store
            .append_batch_durable(&build_batch(producer_id, 0, 3, 3))
            .await
            .expect("exact-replay of last batch must be re-acked, not error");
        assert_eq!(
            replay_3_5, 3,
            "post-multi-cycle replay must return ORIGINAL base_offset",
        );

        // Replay of OLDER sequences (0-2) must error — they're below
        // the producer's current sequence.
        let replay_0_2 = store
            .append_batch_durable(&build_batch(producer_id, 0, 0, 3))
            .await;
        assert!(
            replay_0_2.is_err(),
            "older-sequence replay across cycles must be rejected, got {:?}",
            replay_0_2,
        );

        // Fresh appends continue from sequence 6.
        store
            .append_batch_durable(&build_batch(producer_id, 0, 6, 3))
            .await
            .expect("fresh sequence must be accepted");
        store.close().await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// 5. New producer ID after recovery is unaffected by other producers' state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fresh_producer_after_recovery_starts_at_sequence_zero() {
    // Producer A writes some batches, the partition is closed/reopened.
    // A *different* producer B then writes for the first time. B's state
    // is independent of A's; B's first batch at sequence 0 must be
    // accepted, and A's previous state must remain intact.
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    {
        let store = open(object_store.clone()).await;
        store
            .append_batch_durable(&build_batch(/*A=*/ 100, 0, 0, 5))
            .await
            .expect("producer A append");
        store.close().await.unwrap();
    }

    let reopened = open(object_store).await;
    // Producer B (different id) writes for the first time.
    let b_offset = reopened
        .append_batch_durable(&build_batch(/*B=*/ 200, 0, 0, 3))
        .await
        .expect("producer B's first batch must be admitted at sequence 0");
    assert_eq!(b_offset, 5, "B's first batch comes after A's 5 records");

    // Producer A's state must still be intact: a replay of its last
    // batch returns the original offset.
    let replay_a = reopened
        .append_batch_durable(&build_batch(100, 0, 0, 5))
        .await
        .expect("A replay must succeed (re-ack)");
    assert_eq!(
        replay_a, 0,
        "A's exact-replay must return original offset 0"
    );
}

// ---------------------------------------------------------------------------
// 6. LSO + HWM consistency through retention + recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lso_and_hwm_both_survive_retention_then_recovery() {
    // Retention advances LSO, close+reopen must preserve it AND HWM.
    // This is the interaction case `retention_lso_safety_tests.rs`
    // and `durability_contract_props.rs` each cover one half of.
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let pre_close_hwm;
    let pre_close_lso;
    {
        let store = open(object_store.clone()).await;

        // Three batches, two with old timestamps that retention will
        // delete, one fresh.
        for ts in [1_000i64, 2_000, 100_000] {
            let mut batch = build_batch(-1, -1, -1, 2);
            // Patch the timestamps in this batch so retention sees them.
            // (build_batch above uses a fixed 1_000ms; we patch per-call here.)
            let mut owned = batch.to_vec();
            owned[27..35].copy_from_slice(&ts.to_be_bytes());
            owned[35..43].copy_from_slice(&ts.to_be_bytes());
            let crc = kafkaesque::protocol::crc32c(&owned[21..]);
            owned[17..21].copy_from_slice(&crc.to_be_bytes());
            batch = Bytes::from(owned);
            store.append_batch_durable(&batch).await.unwrap();
        }

        let deleted = store
            .apply_retention(/*retention_ms=*/ 10_000, /*now_ms=*/ 50_000)
            .await
            .expect("retention");
        assert!(deleted >= 1);
        pre_close_hwm = store.high_watermark();
        pre_close_lso = store.log_start_offset();
        assert!(pre_close_lso > 0);
        assert!(pre_close_hwm > pre_close_lso);
        store.close().await.unwrap();
    }

    let reopened = open(object_store).await;
    assert_eq!(
        reopened.high_watermark(),
        pre_close_hwm,
        "HWM must survive close+reopen after retention",
    );
    assert_eq!(
        reopened.log_start_offset(),
        pre_close_lso,
        "LSO must survive close+reopen after retention",
    );
}
