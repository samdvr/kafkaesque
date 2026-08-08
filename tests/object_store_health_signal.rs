//! Regression test for the object-store health signal.
//!
//! The consecutive-failure gauge behind `/readyz` and zombie-mode entry
//! must only move on errors that mean the store is *unreachable*. SlateDB
//! generates expected non-fatal errors constantly against a healthy store:
//! `NotFound` from WAL/manifest existence probes, and `NotImplemented`
//! from `LocalFileSystem` rejecting the ULID attribute SlateDB attaches to
//! conditional puts (SlateDB then retries the put without it).
//!
//! Counting those as failures made an *idle* partition cross the unhealthy
//! threshold within seconds — spamming "PARTIAL NETWORK PARTITION
//! DETECTED" and bouncing the broker through zombie mode. That is exactly
//! what the object_store 0.14 / slatedb 0.15 upgrade first shipped, and
//! it broke the single-node e2e run.
//!
//! Lives in its own test binary on purpose: the gauge is process-global
//! metrics state, so a sibling test touching the object store would make
//! the assertions meaningless.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafkaesque::cluster::metrics;
use kafkaesque::cluster::{ClusterConfig, ObjectStoreType, PartitionStore, create_object_store};

/// Minimal well-formed v2 RecordBatch, mirroring the lib-test helper.
fn test_batch(record_count: i32) -> Bytes {
    let mut batch = vec![0u8; 100];
    batch[0..8].copy_from_slice(&0i64.to_be_bytes()); // base_offset (patched)
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes()); // batch_length
    batch[12..16].copy_from_slice(&0i32.to_be_bytes()); // partition_leader_epoch
    batch[16] = 2; // magic
    batch[21..23].copy_from_slice(&0i16.to_be_bytes()); // attributes
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes()); // last_offset_delta
    batch[27..35].copy_from_slice(&0i64.to_be_bytes()); // first_timestamp
    batch[35..43].copy_from_slice(&0i64.to_be_bytes()); // max_timestamp
    batch[43..51].copy_from_slice(&1i64.to_be_bytes()); // producer_id
    batch[51..53].copy_from_slice(&0i16.to_be_bytes()); // producer_epoch
    batch[53..57].copy_from_slice(&0i32.to_be_bytes()); // first_sequence
    batch[57..61].copy_from_slice(&record_count.to_be_bytes()); // records_count
    Bytes::from(batch)
}

#[tokio::test]
async fn slatedb_probe_errors_do_not_mark_object_store_unreachable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = ClusterConfig {
        object_store: ObjectStoreType::Local {
            path: dir.path().to_string_lossy().to_string(),
        },
        ..ClusterConfig::default()
    };

    // The full production stack: metrics wrapper over LocalFileSystem,
    // behind the cluster prefix — the health gauge is fed from inside it.
    let store = create_object_store(&config).expect("create object store");

    let ps = PartitionStore::builder()
        .object_store(Arc::clone(&store))
        .base_path(&dir.path().to_string_lossy())
        .topic("health-signal")
        .partition(0)
        .build()
        .await
        .expect("open partition store");

    assert_eq!(
        metrics::object_store_consecutive_failures(),
        0,
        "opening a partition store must not register unreachability"
    );

    // Round-trip real data. This is the path where SlateDB's conditional
    // put gets NotImplemented from LocalFileSystem and retries without the
    // attribute: the append must still succeed and the records must read
    // back, or the fallback isn't working.
    let offset = ps
        .append_batch_durable(&test_batch(4))
        .await
        .expect("durable append must succeed despite attribute rejection");
    let (_hwm, records) = ps.fetch_from(0).await.expect("fetch");
    assert!(
        records.is_some_and(|r| !r.is_empty()),
        "records appended at offset {offset} must read back"
    );

    // Idle long enough for the WAL/manifest pollers to tick several times;
    // each tick probes for objects that do not exist yet. Pre-fix this
    // reached the unhealthy threshold (10) in about five seconds.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let failures = metrics::object_store_consecutive_failures();
    assert_eq!(
        failures, 0,
        "idle SlateDB polling must not accumulate object-store failures, saw {failures}"
    );
    assert!(
        metrics::is_object_store_healthy(),
        "object store must still be considered healthy while idle"
    );

    ps.close().await.expect("close");
    assert_eq!(
        metrics::object_store_consecutive_failures(),
        0,
        "closing a partition store must not register unreachability"
    );
}
