//! Log-retention correctness tests.
//!
//! The audit flagged "Compaction can delete current keys; nothing covers
//! it." Kafkaesque does not implement Kafka-style log compaction (the
//! `cleanup.policy=compact` mode that retains the latest value per key) —
//! the retention path here is *time-based*: `apply_retention(retention_ms, now_ms)`
//! deletes the contiguous prefix of batches whose `max_timestamp` is
//! older than `now_ms - retention_ms`. The same correctness concern
//! applies: a buggy retention pass could delete batches whose timestamp
//! is still within the window, or advance `log_start_offset` past records
//! that are still durable.
//!
//! These tests pin the contract:
//!
//! 1. Batches inside the retention window are NEVER deleted.
//! 2. `apply_retention` stops at the first non-expired batch — even if a
//!    later batch in the log is also expired, the contiguous-prefix rule
//!    prevents creating a hole below the new LSO.
//! 3. After deletion, `fetch_from(0)` advances cleanly to the new LSO and
//!    returns only the surviving batches.
//! 4. `retention_ms = 0` disables retention entirely.
//! 5. Retention against a zombie-mode broker is refused so a stale owner
//!    cannot delete records the new owner has acked reads on.

use std::sync::Arc;

use bytes::Bytes;
use kafkaesque::cluster::{PartitionStore, ZombieModeState};
use object_store::ObjectStore;
use object_store::memory::InMemory;

mod common;

const TOPIC: &str = "log-retention";
const PARTITION: i32 = 0;
const BASE_PATH: &str = "log-retention-test";

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

/// Build a batch with the given `max_timestamp` (and matching base
/// timestamp). Retention reads `max_timestamp` to decide whether a batch
/// is expired.
fn build_batch_at(record_count: i32, max_timestamp_ms: i64) -> Bytes {
    let mut batch = vec![0u8; 100];
    batch[0..8].copy_from_slice(&0i64.to_be_bytes()); // base_offset
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes()); // batch_length
    batch[12..16].copy_from_slice(&0i32.to_be_bytes()); // partition_leader_epoch
    batch[16] = 2; // magic
    batch[21..23].copy_from_slice(&0i16.to_be_bytes()); // attributes
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes()); // last_offset_delta
    batch[27..35].copy_from_slice(&max_timestamp_ms.to_be_bytes()); // base_timestamp
    batch[35..43].copy_from_slice(&max_timestamp_ms.to_be_bytes()); // max_timestamp
    batch[43..51].copy_from_slice(&(-1i64).to_be_bytes()); // producer_id
    batch[51..53].copy_from_slice(&0i16.to_be_bytes()); // producer_epoch
    batch[53..57].copy_from_slice(&0i32.to_be_bytes()); // base_sequence
    batch[57..61].copy_from_slice(&record_count.to_be_bytes()); // records_count
    let crc = crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(batch)
}

async fn fresh_store() -> PartitionStore {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    PartitionStore::open(object_store, BASE_PATH, TOPIC, PARTITION)
        .await
        .expect("open partition store")
}

#[tokio::test]
async fn batches_inside_retention_window_are_not_deleted() {
    // Two batches: one with timestamp 1_000ms (old), one with 50_000ms
    // (fresh). Retention = 20s, now = 60s ⇒ cutoff = 40s.
    // Batch 0: ts=1_000  < cutoff ⇒ expired.
    // Batch 1: ts=50_000 > cutoff ⇒ FRESH and must NOT be deleted.
    let store = fresh_store().await;
    let _ = store
        .append_batch_durable(&build_batch_at(1, 1_000))
        .await
        .unwrap();
    let _ = store
        .append_batch_durable(&build_batch_at(1, 50_000))
        .await
        .unwrap();
    assert_eq!(store.log_start_offset(), 0);

    let deleted = store
        .apply_retention(/*retention_ms=*/ 20_000, /*now_ms=*/ 60_000)
        .await
        .expect("apply_retention");
    assert_eq!(deleted, 1, "exactly one expired batch must be deleted");
    assert_eq!(
        store.log_start_offset(),
        1,
        "log start must advance past the deleted batch only"
    );

    // The surviving batch must be readable.
    let (next, payload) = store.fetch_from(1).await.expect("fetch surviving");
    assert!(payload.is_some(), "fresh batch must survive retention");
    assert!(next > 1, "fetch must advance past surviving record");
}

#[tokio::test]
async fn retention_stops_at_first_non_expired_batch() {
    // batch 0: expired
    // batch 1: FRESH ← retention must stop here even if...
    // batch 2: expired (out-of-order timestamp; the contiguous-prefix
    //          rule keeps the log free of holes below the new LSO).
    let store = fresh_store().await;
    let _ = store
        .append_batch_durable(&build_batch_at(1, 1_000))
        .await
        .unwrap();
    let _ = store
        .append_batch_durable(&build_batch_at(1, 50_000))
        .await
        .unwrap();
    let _ = store
        .append_batch_durable(&build_batch_at(1, 2_000))
        .await
        .unwrap();

    let deleted = store
        .apply_retention(20_000, 60_000)
        .await
        .expect("apply_retention");
    assert_eq!(
        deleted, 1,
        "retention must stop at the first non-expired batch (no holes below LSO)"
    );
    assert_eq!(
        store.log_start_offset(),
        1,
        "log start must reflect only the contiguous expired prefix"
    );
}

#[tokio::test]
async fn retention_disabled_when_retention_ms_is_zero() {
    let store = fresh_store().await;
    let _ = store
        .append_batch_durable(&build_batch_at(1, 0))
        .await
        .unwrap();
    let deleted = store
        .apply_retention(0, 1_000_000)
        .await
        .expect("retention");
    assert_eq!(deleted, 0, "retention_ms=0 must be a no-op");
    assert_eq!(store.log_start_offset(), 0);
}

#[tokio::test]
async fn retention_on_empty_log_is_a_noop() {
    let store = fresh_store().await;
    let deleted = store
        .apply_retention(60_000, 1_000_000)
        .await
        .expect("retention");
    assert_eq!(deleted, 0, "retention on empty log must do nothing");
    assert_eq!(store.log_start_offset(), 0);
}

#[tokio::test]
async fn retention_in_zombie_mode_is_rejected() {
    // A stale owner that's been zombied must not be allowed to delete
    // records: the new owner may have already acked reads on them. The
    // path errors out before any LSO advance.
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let zombie = Arc::new(ZombieModeState::new());
    let store = PartitionStore::builder()
        .object_store(object_store)
        .base_path(BASE_PATH)
        .topic(TOPIC)
        .partition(PARTITION)
        .zombie_mode(zombie.clone())
        .build()
        .await
        .expect("open with zombie state");

    let _ = store
        .append_batch_durable(&build_batch_at(1, 1_000))
        .await
        .expect("produce");

    zombie.enter();

    let result = store.apply_retention(1_000, 1_000_000).await;
    assert!(
        result.is_err(),
        "zombie-mode broker must not be allowed to retention-delete"
    );
    assert_eq!(
        store.log_start_offset(),
        0,
        "zombie-rejected retention must NOT advance log_start_offset"
    );
}

#[tokio::test]
async fn fetch_below_lso_after_retention_returns_no_data() {
    // Once retention has moved the LSO past offsets 0..N, those records
    // are gone — `fetch_from(below_LSO)` must not surface deleted bytes
    // and must surface the new starting offset.
    let store = fresh_store().await;
    let _ = store
        .append_batch_durable(&build_batch_at(1, 1_000))
        .await
        .unwrap();
    let _ = store
        .append_batch_durable(&build_batch_at(1, 2_000))
        .await
        .unwrap();
    let _ = store
        .append_batch_durable(&build_batch_at(1, 100_000))
        .await
        .unwrap();
    let deleted = store.apply_retention(10_000, 50_000).await.unwrap();
    assert_eq!(deleted, 2);
    assert_eq!(store.log_start_offset(), 2);

    // A fetch starting at offset 0 (below LSO) must not return the
    // deleted records. The exact return shape is implementation-defined
    // (some stores return Ok(None), some advance to LSO) — both are
    // acceptable as long as no deleted data leaks.
    let (next_offset, payload) = store.fetch_from(0).await.expect("fetch from below LSO");
    if let Some(bytes) = payload {
        // If anything is returned, it must come from offset >= LSO.
        assert!(
            next_offset > store.log_start_offset(),
            "fetch must not surface deleted records: next_offset={next_offset}, LSO={}",
            store.log_start_offset()
        );
        assert!(!bytes.is_empty(), "non-None payload must be non-empty");
    }
}
