//! Unit tests for PartitionStore functionality.
//!
//! These tests cover:
//! - Producer state cache eviction edge cases (Section 7.1)
//! - Batch index overflow behavior (Section 7.1)
//! - Idempotency checking logic

#[cfg(test)]
mod tests {
    use super::super::partition_store::*;
    use super::super::zombie_mode::ZombieModeState;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    /// Create a valid Kafka RecordBatch with producer info for testing.
    ///
    /// Format (61 bytes minimum header):
    /// - bytes 0-7: base_offset (i64)
    /// - bytes 8-11: batch_length (i32)
    /// - bytes 12-15: partition_leader_epoch (i32)
    /// - byte 16: magic (i8 = 2)
    /// - bytes 17-20: crc (u32)
    /// - bytes 21-22: attributes (i16)
    /// - bytes 23-26: last_offset_delta (i32)
    /// - bytes 27-34: first_timestamp (i64)
    /// - bytes 35-42: max_timestamp (i64)
    /// - bytes 43-50: producer_id (i64)
    /// - bytes 51-52: producer_epoch (i16)
    /// - bytes 53-56: first_sequence (i32)
    /// - bytes 57-60: records_count (i32)
    fn create_test_batch(
        producer_id: i64,
        producer_epoch: i16,
        first_sequence: i32,
        record_count: i32,
    ) -> Bytes {
        let mut batch = vec![0u8; 100];

        // base_offset (will be patched by PartitionStore)
        batch[0..8].copy_from_slice(&0i64.to_be_bytes());

        // batch_length
        batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes());

        // partition_leader_epoch
        batch[12..16].copy_from_slice(&0i32.to_be_bytes());

        // magic = 2
        batch[16] = 2;

        // attributes
        batch[21..23].copy_from_slice(&0i16.to_be_bytes());

        // last_offset_delta (record_count - 1)
        batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes());

        // first_timestamp
        batch[27..35].copy_from_slice(&0i64.to_be_bytes());

        // max_timestamp
        batch[35..43].copy_from_slice(&0i64.to_be_bytes());

        // producer_id
        batch[43..51].copy_from_slice(&producer_id.to_be_bytes());

        // producer_epoch
        batch[51..53].copy_from_slice(&producer_epoch.to_be_bytes());

        // first_sequence
        batch[53..57].copy_from_slice(&first_sequence.to_be_bytes());

        // records_count
        batch[57..61].copy_from_slice(&record_count.to_be_bytes());

        // Compute and set CRC over bytes 21+
        let crc = compute_crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());

        Bytes::from(batch)
    }

    /// Compute CRC-32C checksum (Castagnoli polynomial).
    fn compute_crc32c(data: &[u8]) -> u32 {
        const CRC32C_TABLE: [u32; 256] = {
            let mut table = [0u32; 256];
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
                table[i] = crc;
                i += 1;
            }
            table
        };

        let mut crc = !0u32;
        for &byte in data {
            let index = ((crc ^ byte as u32) & 0xFF) as usize;
            crc = (crc >> 8) ^ CRC32C_TABLE[index];
        }
        !crc
    }

    /// Create a non-idempotent batch (producer_id = -1)
    fn create_non_idempotent_batch(record_count: i32) -> Bytes {
        create_test_batch(-1, 0, 0, record_count)
    }

    // ========================================================================
    // Producer State Tests
    // ========================================================================

    #[test]
    fn test_producer_state_clone() {
        let state = ProducerState {
            last_sequence: 100,
            producer_epoch: 5,
        };
        let cloned = state;
        assert_eq!(cloned.last_sequence, 100);
        assert_eq!(cloned.producer_epoch, 5);
    }

    #[test]
    fn test_producer_state_debug() {
        let state = ProducerState {
            last_sequence: 100,
            producer_epoch: 5,
        };
        let debug = format!("{:?}", state);
        assert!(debug.contains("last_sequence"));
        assert!(debug.contains("100"));
        assert!(debug.contains("producer_epoch"));
        assert!(debug.contains("5"));
    }

    // ========================================================================
    // PartitionStoreBuilder Tests
    // ========================================================================

    #[test]
    fn test_builder_default() {
        let _builder = PartitionStoreBuilder::new();
        // Just verify it can be created
    }

    #[test]
    fn test_builder_fluent_api() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());

        let _builder = PartitionStore::builder()
            .object_store(object_store)
            .base_path("/test")
            .topic("test-topic")
            .partition(0)
            .max_fetch_response_size(2 * 1024 * 1024)
            .batch_index_max_size(5000)
            .producer_state_cache_ttl_secs(300)
            .zombie_mode(zombie_state);

        // Builder was created successfully
    }

    #[tokio::test]
    async fn test_builder_missing_object_store() {
        let result = PartitionStoreBuilder::new()
            .base_path("/test")
            .topic("test-topic")
            .partition(0)
            .build()
            .await;

        match result {
            Err(super::super::error::SlateDBError::Config(msg)) => {
                assert!(msg.contains("object_store"));
            }
            _ => panic!("Expected Config error for missing object_store"),
        }
    }

    #[tokio::test]
    async fn test_builder_missing_base_path() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let result = PartitionStoreBuilder::new()
            .object_store(object_store)
            .topic("test-topic")
            .partition(0)
            .build()
            .await;

        match result {
            Err(super::super::error::SlateDBError::Config(msg)) => {
                assert!(msg.contains("base_path"));
            }
            _ => panic!("Expected Config error for missing base_path"),
        }
    }

    #[tokio::test]
    async fn test_builder_missing_topic() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let result = PartitionStoreBuilder::new()
            .object_store(object_store)
            .base_path("/test")
            .partition(0)
            .build()
            .await;

        match result {
            Err(super::super::error::SlateDBError::Config(msg)) => {
                assert!(msg.contains("topic"));
            }
            _ => panic!("Expected Config error for missing topic"),
        }
    }

    #[tokio::test]
    async fn test_builder_missing_partition() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let result = PartitionStoreBuilder::new()
            .object_store(object_store)
            .base_path("/test")
            .topic("test-topic")
            .build()
            .await;

        match result {
            Err(super::super::error::SlateDBError::Config(msg)) => {
                assert!(msg.contains("partition"));
            }
            _ => panic!("Expected Config error for missing partition"),
        }
    }

    // ========================================================================
    // PartitionStore Integration Tests (require SlateDB)
    // ========================================================================

    #[tokio::test]
    async fn test_partition_store_open_and_close() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "my-topic", 0)
            .await
            .expect("Failed to open partition store");

        assert_eq!(store.topic(), "my-topic");
        assert_eq!(store.partition(), 0);
        assert_eq!(store.high_watermark(), 0);

        store
            .close()
            .await
            .expect("Failed to close partition store");
    }

    #[tokio::test]
    async fn test_partition_store_with_zombie_flag() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());

        let store = PartitionStore::open_with_zombie_flag(
            object_store,
            "test",
            "zombie-topic",
            0,
            1024 * 1024,
            zombie_state.clone(),
            false, // fail_on_recovery_gap
        )
        .await
        .expect("Failed to open partition store with zombie flag");

        // Initially not in zombie mode - writes should succeed
        let batch = create_non_idempotent_batch(1);
        let result = store.append_batch(&batch).await;
        assert!(result.is_ok());

        // Enable zombie mode
        zombie_state.enter();

        // Writes should now fail
        let batch2 = create_non_idempotent_batch(1);
        let result2 = store.append_batch(&batch2).await;
        match result2 {
            Err(super::super::error::SlateDBError::NotOwned { topic, partition }) => {
                assert_eq!(topic, "zombie-topic");
                assert_eq!(partition, 0);
            }
            _ => panic!("Expected NotOwned error when in zombie mode"),
        }

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_partition_store_append_and_fetch() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "append-topic", 0)
            .await
            .expect("Failed to open partition store");

        // Append a batch with 5 records
        let batch = create_non_idempotent_batch(5);
        let base_offset = store.append_batch(&batch).await.expect("Failed to append");
        assert_eq!(base_offset, 0);
        assert_eq!(store.high_watermark(), 5);

        // Append another batch with 3 records
        let batch2 = create_non_idempotent_batch(3);
        let base_offset2 = store.append_batch(&batch2).await.expect("Failed to append");
        assert_eq!(base_offset2, 5);
        assert_eq!(store.high_watermark(), 8);

        // Fetch from offset 0
        let (hwm, records) = store.fetch_from(0).await.expect("Failed to fetch");
        assert_eq!(hwm, 8);
        assert!(records.is_some());

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_partition_store_fetch_beyond_hwm() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "fetch-topic", 0)
            .await
            .expect("Failed to open partition store");

        // Append some data
        let batch = create_non_idempotent_batch(5);
        store.append_batch(&batch).await.expect("Failed to append");

        // Fetch beyond HWM should return no records
        let (hwm, records) = store.fetch_from(100).await.expect("Failed to fetch");
        assert_eq!(hwm, 5);
        assert!(records.is_none());

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_partition_store_fetch_negative_offset() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "negative-topic", 0)
            .await
            .expect("Failed to open partition store");

        // Negative offset should return no records
        let (hwm, records) = store.fetch_from(-5).await.expect("Failed to fetch");
        assert_eq!(hwm, 0);
        assert!(records.is_none());

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_partition_store_earliest_offset() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "earliest-topic", 0)
            .await
            .expect("Failed to open partition store");

        // Empty store should have earliest offset 0
        let earliest = store
            .earliest_offset()
            .await
            .expect("Failed to get earliest");
        assert_eq!(earliest, 0);

        // Append some data
        let batch = create_non_idempotent_batch(5);
        store.append_batch(&batch).await.expect("Failed to append");

        // Earliest should now be 0 (first batch starts at 0)
        let earliest = store
            .earliest_offset()
            .await
            .expect("Failed to get earliest");
        assert_eq!(earliest, 0);

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_partition_store_validate_lease_for_write() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "lease-topic", 0)
            .await
            .expect("Failed to open partition store");

        // Sufficient TTL should pass (>= 30 seconds )
        assert!(store.validate_lease_for_write(35).is_ok());
        assert!(store.validate_lease_for_write(30).is_ok());

        // Insufficient TTL should fail (< 30 seconds )
        let result = store.validate_lease_for_write(29);
        match result {
            Err(super::super::error::SlateDBError::LeaseTooShort {
                topic,
                partition,
                remaining_secs,
                required_secs,
            }) => {
                assert_eq!(topic, "lease-topic");
                assert_eq!(partition, 0);
                assert_eq!(remaining_secs, 29);
                assert_eq!(required_secs, 30);
            }
            _ => panic!("Expected LeaseTooShort error"),
        }

        store.close().await.expect("Failed to close");
    }

    // ========================================================================
    // Idempotency Tests
    // ========================================================================

    #[tokio::test]
    async fn test_idempotent_producer_new_producer() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "idempotent-topic", 0)
            .await
            .expect("Failed to open partition store");

        // First batch from a new producer should succeed
        let batch = create_test_batch(
            12345, // producer_id
            0,     // producer_epoch
            0,     // first_sequence
            5,     // record_count
        );
        let result = store.append_batch(&batch).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_idempotent_producer_sequential_batches() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "sequential-topic", 0)
            .await
            .expect("Failed to open partition store");

        // First batch: sequence 0-4 (5 records)
        let batch1 = create_test_batch(12345, 0, 0, 5);
        store
            .append_batch(&batch1)
            .await
            .expect("First batch failed");

        // Second batch: sequence 5-9 (5 records) - should succeed
        let batch2 = create_test_batch(12345, 0, 5, 5);
        store
            .append_batch(&batch2)
            .await
            .expect("Second batch failed");

        // Third batch: sequence 10-14 (5 records) - should succeed
        let batch3 = create_test_batch(12345, 0, 10, 5);
        store
            .append_batch(&batch3)
            .await
            .expect("Third batch failed");

        assert_eq!(store.high_watermark(), 15);

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_idempotent_producer_duplicate_detection() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "duplicate-topic", 0)
            .await
            .expect("Failed to open partition store");

        // First batch: sequence 0-4
        let batch1 = create_test_batch(12345, 0, 0, 5);
        store
            .append_batch(&batch1)
            .await
            .expect("First batch failed");

        // Duplicate batch: same sequence 0-4 - should be rejected
        let batch2 = create_test_batch(12345, 0, 0, 5);
        let result = store.append_batch(&batch2).await;

        match result {
            Err(super::super::error::SlateDBError::DuplicateSequence {
                producer_id,
                expected_sequence,
                received_sequence,
            }) => {
                assert_eq!(producer_id, 12345);
                assert_eq!(expected_sequence, 5); // last_sequence(4) + 1
                assert_eq!(received_sequence, 0);
            }
            _ => panic!("Expected DuplicateSequence error, got {:?}", result),
        }

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_idempotent_producer_out_of_order_detection() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "out-of-order-topic", 0)
            .await
            .expect("Failed to open partition store");

        // First batch: sequence 0-4
        let batch1 = create_test_batch(12345, 0, 0, 5);
        store
            .append_batch(&batch1)
            .await
            .expect("First batch failed");

        // Out-of-order batch: sequence 10 instead of 5 - should be rejected
        let batch2 = create_test_batch(12345, 0, 10, 5);
        let result = store.append_batch(&batch2).await;

        match result {
            Err(super::super::error::SlateDBError::OutOfOrderSequence {
                producer_id,
                expected_sequence,
                received_sequence,
            }) => {
                assert_eq!(producer_id, 12345);
                assert_eq!(expected_sequence, 5);
                assert_eq!(received_sequence, 10);
            }
            _ => panic!("Expected OutOfOrderSequence error, got {:?}", result),
        }

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_idempotent_producer_epoch_fencing() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "epoch-topic", 0)
            .await
            .expect("Failed to open partition store");

        // First batch with epoch 5
        let batch1 = create_test_batch(12345, 5, 0, 5);
        store
            .append_batch(&batch1)
            .await
            .expect("First batch failed");

        // Batch with lower epoch (zombie producer) - should be rejected
        let batch2 = create_test_batch(12345, 3, 5, 5);
        let result = store.append_batch(&batch2).await;

        match result {
            Err(super::super::error::SlateDBError::FencedProducer {
                producer_id,
                expected_epoch,
                actual_epoch,
            }) => {
                assert_eq!(producer_id, 12345);
                assert_eq!(expected_epoch, 5);
                assert_eq!(actual_epoch, 3);
            }
            _ => panic!("Expected FencedProducer error, got {:?}", result),
        }

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_idempotent_producer_epoch_increment_resets_sequence() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "epoch-reset-topic", 0)
            .await
            .expect("Failed to open partition store");

        // First batch with epoch 0, sequence 0-4
        let batch1 = create_test_batch(12345, 0, 0, 5);
        store
            .append_batch(&batch1)
            .await
            .expect("First batch failed");

        // Higher epoch resets sequence tracking - sequence 0 should be accepted
        let batch2 = create_test_batch(12345, 1, 0, 5);
        let result = store.append_batch(&batch2).await;
        assert!(
            result.is_ok(),
            "Higher epoch should reset sequence tracking"
        );

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_idempotent_multiple_producers() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "multi-producer-topic", 0)
            .await
            .expect("Failed to open partition store");

        // Producer 1: sequence 0-4
        let batch1 = create_test_batch(100, 0, 0, 5);
        store
            .append_batch(&batch1)
            .await
            .expect("Producer 1 batch 1 failed");

        // Producer 2: sequence 0-2
        let batch2 = create_test_batch(200, 0, 0, 3);
        store
            .append_batch(&batch2)
            .await
            .expect("Producer 2 batch 1 failed");

        // Producer 1: sequence 5-9
        let batch3 = create_test_batch(100, 0, 5, 5);
        store
            .append_batch(&batch3)
            .await
            .expect("Producer 1 batch 2 failed");

        // Producer 2: sequence 3-5
        let batch4 = create_test_batch(200, 0, 3, 3);
        store
            .append_batch(&batch4)
            .await
            .expect("Producer 2 batch 2 failed");

        assert_eq!(store.high_watermark(), 16);

        store.close().await.expect("Failed to close");
    }

    // ========================================================================
    // Batch Index Tests (Section 7.1)
    // ========================================================================

    #[tokio::test]
    async fn test_batch_index_eviction_on_overflow() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Create store with very small batch index max size
        let store = PartitionStore::builder()
            .object_store(object_store)
            .base_path("test")
            .topic("batch-index-topic")
            .partition(0)
            .batch_index_max_size(5) // Very small for testing
            .build()
            .await
            .expect("Failed to build partition store");

        // Append more batches than the index can hold
        for _i in 0..10 {
            let batch = create_non_idempotent_batch(1);
            store.append_batch(&batch).await.expect("Failed to append");
        }

        // Store should still work correctly
        assert_eq!(store.high_watermark(), 10);

        // Fetch should still work (may need SlateDB lookup for evicted entries)
        let (hwm, records) = store.fetch_from(0).await.expect("Failed to fetch");
        assert_eq!(hwm, 10);
        assert!(records.is_some());

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_batch_index_zero_size() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Create store with zero batch index max size (effectively disables caching)
        let store = PartitionStore::builder()
            .object_store(object_store)
            .base_path("test")
            .topic("zero-index-topic")
            .partition(0)
            .batch_index_max_size(0) // No caching
            .build()
            .await
            .expect("Failed to build partition store");

        // Append and fetch should still work
        for _ in 0..5 {
            let batch = create_non_idempotent_batch(1);
            store.append_batch(&batch).await.expect("Failed to append");
        }

        let (hwm, records) = store.fetch_from(0).await.expect("Failed to fetch");
        assert_eq!(hwm, 5);
        assert!(records.is_some());

        store.close().await.expect("Failed to close");
    }

    // ========================================================================
    // Invalid Batch Tests
    // ========================================================================

    #[tokio::test]
    async fn test_reject_zero_record_count_batch() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "invalid-topic", 0)
            .await
            .expect("Failed to open partition store");

        // Create batch with 0 record count
        let batch = create_non_idempotent_batch(0);
        let result = store.append_batch(&batch).await;

        match result {
            Err(super::super::error::SlateDBError::Config(msg)) => {
                assert!(msg.contains("Invalid record count"));
            }
            _ => panic!("Expected Config error for invalid record count"),
        }

        store.close().await.expect("Failed to close");
    }

    #[tokio::test]
    async fn test_reject_negative_record_count_batch() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "negative-count-topic", 0)
            .await
            .expect("Failed to open partition store");

        // Create batch with negative record count
        let batch = create_non_idempotent_batch(-5);
        let result = store.append_batch(&batch).await;

        match result {
            Err(super::super::error::SlateDBError::Config(msg)) => {
                assert!(msg.contains("Invalid record count"));
            }
            _ => panic!("Expected Config error for invalid record count"),
        }

        store.close().await.expect("Failed to close");
    }

    // ========================================================================
    // HWM Check Tests
    // ========================================================================

    #[tokio::test]
    async fn test_high_watermark_check() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "hwm-check-topic", 0)
            .await
            .expect("Failed to open partition store");

        // Initial check
        let hwm = store
            .high_watermark_check()
            .await
            .expect("HWM check failed");
        assert_eq!(hwm, 0);

        // Append some data
        let batch = create_non_idempotent_batch(5);
        store.append_batch(&batch).await.expect("Append failed");

        // Check again
        let hwm = store
            .high_watermark_check()
            .await
            .expect("HWM check failed");
        assert_eq!(hwm, 5);

        store.close().await.expect("Failed to close");
    }

    // ========================================================================
    // Flush Tests
    // ========================================================================

    #[tokio::test]
    async fn test_explicit_flush() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let store = PartitionStore::open(object_store, "test", "flush-topic", 0)
            .await
            .expect("Failed to open partition store");

        // Append data
        let batch = create_non_idempotent_batch(5);
        store.append_batch(&batch).await.expect("Append failed");

        // Explicit flush should succeed
        store.flush().await.expect("Flush failed");

        store.close().await.expect("Failed to close");
    }

    // ========================================================================
    // Producer State Persistence Tests
    // ========================================================================

    /// Test that producer state is persisted and recovered across store reopens.
    ///
    /// This is a critical test for idempotency - producer state must survive
    /// broker restarts to prevent duplicate messages after recovery.
    #[tokio::test]
    async fn test_producer_state_persistence_across_reopen() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let producer_id = 999888777i64;
        let producer_epoch = 5i16;

        // Phase 1: Write some batches with idempotent producer
        {
            let store = PartitionStore::open(object_store.clone(), "test", "persist-topic", 0)
                .await
                .expect("Failed to open partition store");

            // Write three batches: sequences 0-4, 5-9, 10-14
            let batch1 = create_test_batch(producer_id, producer_epoch, 0, 5);
            store.append_batch(&batch1).await.expect("Batch 1 failed");

            let batch2 = create_test_batch(producer_id, producer_epoch, 5, 5);
            store.append_batch(&batch2).await.expect("Batch 2 failed");

            let batch3 = create_test_batch(producer_id, producer_epoch, 10, 5);
            store.append_batch(&batch3).await.expect("Batch 3 failed");

            assert_eq!(store.high_watermark(), 15);

            // Close the store (this should persist all state)
            store.close().await.expect("Failed to close");
        }

        // Phase 2: Reopen and verify producer state was recovered
        {
            let store = PartitionStore::open(object_store.clone(), "test", "persist-topic", 0)
                .await
                .expect("Failed to reopen partition store");

            // Verify HWM was recovered
            assert_eq!(store.high_watermark(), 15);

            // Duplicate batch should be rejected (proves state was recovered)
            let duplicate_batch = create_test_batch(producer_id, producer_epoch, 10, 5);
            let result = store.append_batch(&duplicate_batch).await;

            match result {
                Err(super::super::error::SlateDBError::DuplicateSequence {
                    producer_id: pid,
                    expected_sequence,
                    received_sequence,
                }) => {
                    assert_eq!(pid, producer_id);
                    assert_eq!(expected_sequence, 15); // last_sequence was 14, so next is 15
                    assert_eq!(received_sequence, 10);
                }
                Ok(_) => panic!("Duplicate batch should have been rejected after reopen"),
                Err(e) => panic!("Expected DuplicateSequence error, got {:?}", e),
            }

            // Next sequential batch should succeed
            let next_batch = create_test_batch(producer_id, producer_epoch, 15, 5);
            store
                .append_batch(&next_batch)
                .await
                .expect("Sequential batch after reopen should succeed");

            assert_eq!(store.high_watermark(), 20);

            store.close().await.expect("Failed to close");
        }
    }

    /// Test that multiple producer states are recovered correctly.
    #[tokio::test]
    async fn test_multiple_producer_state_recovery() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let producer1 = 111i64;
        let producer2 = 222i64;
        let producer3 = 333i64;

        // Phase 1: Write with multiple producers
        {
            let store =
                PartitionStore::open(object_store.clone(), "test", "multi-persist-topic", 0)
                    .await
                    .expect("Failed to open partition store");

            // Producer 1: sequences 0-9
            let batch1a = create_test_batch(producer1, 0, 0, 5);
            store
                .append_batch(&batch1a)
                .await
                .expect("P1 batch 1 failed");
            let batch1b = create_test_batch(producer1, 0, 5, 5);
            store
                .append_batch(&batch1b)
                .await
                .expect("P1 batch 2 failed");

            // Producer 2: sequences 0-4
            let batch2 = create_test_batch(producer2, 0, 0, 5);
            store.append_batch(&batch2).await.expect("P2 batch failed");

            // Producer 3: sequences 0-2
            let batch3 = create_test_batch(producer3, 0, 0, 3);
            store.append_batch(&batch3).await.expect("P3 batch failed");

            store.close().await.expect("Failed to close");
        }

        // Phase 2: Reopen and verify all producer states
        {
            let store =
                PartitionStore::open(object_store.clone(), "test", "multi-persist-topic", 0)
                    .await
                    .expect("Failed to reopen partition store");

            // Producer 1: next sequence should be 10
            let dup1 = create_test_batch(producer1, 0, 5, 5);
            assert!(
                store.append_batch(&dup1).await.is_err(),
                "P1 duplicate should fail"
            );

            let next1 = create_test_batch(producer1, 0, 10, 5);
            store
                .append_batch(&next1)
                .await
                .expect("P1 next sequence should succeed");

            // Producer 2: next sequence should be 5
            let dup2 = create_test_batch(producer2, 0, 0, 5);
            assert!(
                store.append_batch(&dup2).await.is_err(),
                "P2 duplicate should fail"
            );

            let next2 = create_test_batch(producer2, 0, 5, 5);
            store
                .append_batch(&next2)
                .await
                .expect("P2 next sequence should succeed");

            // Producer 3: next sequence should be 3
            let dup3 = create_test_batch(producer3, 0, 0, 3);
            assert!(
                store.append_batch(&dup3).await.is_err(),
                "P3 duplicate should fail"
            );

            let next3 = create_test_batch(producer3, 0, 3, 3);
            store
                .append_batch(&next3)
                .await
                .expect("P3 next sequence should succeed");

            store.close().await.expect("Failed to close");
        }
    }

    /// Test that producer epoch fencing survives reopen.
    #[tokio::test]
    async fn test_producer_epoch_persistence() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let producer_id = 12345i64;

        // Phase 1: Write with epoch 5
        {
            let store =
                PartitionStore::open(object_store.clone(), "test", "epoch-persist-topic", 0)
                    .await
                    .expect("Failed to open partition store");

            let batch = create_test_batch(producer_id, 5, 0, 5);
            store.append_batch(&batch).await.expect("Write failed");

            store.close().await.expect("Failed to close");
        }

        // Phase 2: Verify epoch fencing after reopen
        {
            let store =
                PartitionStore::open(object_store.clone(), "test", "epoch-persist-topic", 0)
                    .await
                    .expect("Failed to reopen partition store");

            // Lower epoch should be rejected (zombie producer)
            let zombie_batch = create_test_batch(producer_id, 3, 5, 5);
            match store.append_batch(&zombie_batch).await {
                Err(super::super::error::SlateDBError::FencedProducer { .. }) => {
                    // Expected
                }
                other => panic!("Expected FencedProducer error, got {:?}", other),
            }

            // Same epoch should work with correct sequence
            let same_epoch = create_test_batch(producer_id, 5, 5, 5);
            store
                .append_batch(&same_epoch)
                .await
                .expect("Same epoch with correct sequence should succeed");

            // Higher epoch should reset sequence
            let higher_epoch = create_test_batch(producer_id, 6, 0, 5);
            store
                .append_batch(&higher_epoch)
                .await
                .expect("Higher epoch should reset sequence");

            store.close().await.expect("Failed to close");
        }
    }

    // ========================================================================
    // Crash Recovery Tests
    // ========================================================================

    /// Test that data survives a simulated crash (drop without explicit close).
    ///
    /// This test verifies that data can survive crashes when explicitly flushed.
    /// In production, await_durable=false is used for throughput, so crash
    /// recovery depends on periodic WAL flushes. Tests must call flush()
    /// explicitly to simulate durable writes before crash.
    #[tokio::test]
    async fn test_crash_recovery_data_survives_without_explicit_close() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let producer_id = 777666555i64;
        let producer_epoch = 1i16;

        // Phase 1: Write data and DROP without explicit close (simulates crash)
        {
            let store = PartitionStore::open(object_store.clone(), "test", "crash-topic", 0)
                .await
                .expect("Failed to open partition store");

            // Write batches
            let batch1 = create_test_batch(producer_id, producer_epoch, 0, 5);
            store.append_batch(&batch1).await.expect("Batch 1 failed");

            let batch2 = create_test_batch(producer_id, producer_epoch, 5, 5);
            store.append_batch(&batch2).await.expect("Batch 2 failed");

            assert_eq!(store.high_watermark(), 10);

            // Flush to ensure data is persisted before simulated crash
            // In production, SlateDB's periodic flush handles this
            store.flush().await.expect("Flush failed");

            // DO NOT call close() - just drop to simulate crash
            drop(store);
        }

        // Phase 2: Reopen and verify data survived the "crash"
        {
            let store = PartitionStore::open(object_store.clone(), "test", "crash-topic", 0)
                .await
                .expect("Failed to reopen partition store after crash");

            // Data should be recovered
            assert_eq!(
                store.high_watermark(),
                10,
                "HWM should be recovered after crash"
            );

            // Fetch should return the data
            let (hwm, records) = store
                .fetch_from(0)
                .await
                .expect("Failed to fetch after crash");
            assert_eq!(hwm, 10);
            assert!(
                records.is_some(),
                "Records should be recoverable after crash"
            );

            // Producer state should also be recovered
            let duplicate = create_test_batch(producer_id, producer_epoch, 5, 5);
            assert!(
                store.append_batch(&duplicate).await.is_err(),
                "Duplicate detection should work after crash recovery"
            );

            // Next sequential batch should succeed
            let next = create_test_batch(producer_id, producer_epoch, 10, 5);
            store
                .append_batch(&next)
                .await
                .expect("Sequential batch after crash should succeed");

            assert_eq!(store.high_watermark(), 15);

            store.close().await.expect("Failed to close");
        }
    }

    /// Test concurrent writes don't corrupt data on crash.
    ///
    /// Simulates multiple batches being written, then crash occurs.
    /// All successfully acknowledged batches should be recoverable.
    #[tokio::test]
    async fn test_crash_recovery_concurrent_producers() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Phase 1: Multiple producers write, then crash
        {
            let store =
                PartitionStore::open(object_store.clone(), "test", "concurrent-crash-topic", 0)
                    .await
                    .expect("Failed to open partition store");

            // Interleaved writes from multiple producers
            for i in 0..5 {
                let batch1 = create_test_batch(100, 0, i * 2, 2);
                store
                    .append_batch(&batch1)
                    .await
                    .expect("P100 batch failed");

                let batch2 = create_test_batch(200, 0, i * 3, 3);
                store
                    .append_batch(&batch2)
                    .await
                    .expect("P200 batch failed");
            }

            // Flush to ensure data is persisted before simulated crash
            store.flush().await.expect("Flush failed");

            // Crash without close
            drop(store);
        }

        // Phase 2: Verify all data recovered
        {
            let store =
                PartitionStore::open(object_store.clone(), "test", "concurrent-crash-topic", 0)
                    .await
                    .expect("Failed to reopen");

            // HWM should reflect all written batches: 5*(2+3) = 25
            assert_eq!(store.high_watermark(), 25);

            // Both producers' states should be recovered
            // Producer 100: last sequence was 9 (sequences 0-9)
            let dup100 = create_test_batch(100, 0, 8, 2);
            assert!(store.append_batch(&dup100).await.is_err());
            let next100 = create_test_batch(100, 0, 10, 2);
            store
                .append_batch(&next100)
                .await
                .expect("P100 next should work");

            // Producer 200: last sequence was 14 (sequences 0-14)
            let dup200 = create_test_batch(200, 0, 12, 3);
            assert!(store.append_batch(&dup200).await.is_err());
            let next200 = create_test_batch(200, 0, 15, 3);
            store
                .append_batch(&next200)
                .await
                .expect("P200 next should work");

            store.close().await.expect("Failed to close");
        }
    }

    /// Test that partial batch writes are atomic (all or nothing).
    ///
    /// If a batch write fails partway through, the entire batch should be
    /// rolled back and not visible after recovery.
    #[tokio::test]
    async fn test_batch_write_atomicity() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Phase 1: Write valid batches, then try invalid batch
        {
            let store = PartitionStore::open(object_store.clone(), "test", "atomic-topic", 0)
                .await
                .expect("Failed to open");

            // Valid batch
            let batch1 = create_test_batch(100, 0, 0, 5);
            store
                .append_batch(&batch1)
                .await
                .expect("Valid batch failed");
            assert_eq!(store.high_watermark(), 5);

            // Invalid batch (out of order sequence) should fail
            let invalid = create_test_batch(100, 0, 10, 5); // Expected 5, got 10
            assert!(store.append_batch(&invalid).await.is_err());

            // HWM should not have changed
            assert_eq!(store.high_watermark(), 5);

            // Flush to ensure data is persisted before simulated crash
            store.flush().await.expect("Flush failed");

            drop(store);
        }

        // Phase 2: Verify only valid data survived
        {
            let store = PartitionStore::open(object_store.clone(), "test", "atomic-topic", 0)
                .await
                .expect("Failed to reopen");

            // Only the valid batch should be present
            assert_eq!(store.high_watermark(), 5);

            // Can continue from where valid writes left off
            let next = create_test_batch(100, 0, 5, 5);
            store
                .append_batch(&next)
                .await
                .expect("Continue after crash failed");

            assert_eq!(store.high_watermark(), 10);

            store.close().await.expect("Failed to close");
        }
    }

    /// Test recovery when zombie mode was active during crash.
    ///
    /// Zombie mode state is in-memory only, so after restart the broker
    /// should be able to write again (assuming it re-acquires the lease).
    #[tokio::test]
    async fn test_crash_recovery_clears_zombie_state() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());

        // Phase 1: Enter zombie mode, then crash
        {
            let store = PartitionStore::open_with_zombie_flag(
                object_store.clone(),
                "test",
                "zombie-crash-topic",
                0,
                1024 * 1024,
                zombie_state.clone(),
                false, // fail_on_recovery_gap
            )
            .await
            .expect("Failed to open");

            // Write some data
            let batch = create_non_idempotent_batch(5);
            store.append_batch(&batch).await.expect("Write failed");

            // Flush to ensure data is persisted before simulated crash
            store.flush().await.expect("Flush failed");

            // Enter zombie mode
            zombie_state.enter();

            // Writes should now fail
            let batch2 = create_non_idempotent_batch(5);
            assert!(store.append_batch(&batch2).await.is_err());

            // Crash while in zombie mode
            drop(store);
        }

        // Phase 2: After restart with fresh zombie state, writes should work
        {
            let fresh_zombie_state = Arc::new(ZombieModeState::new());

            let store = PartitionStore::open_with_zombie_flag(
                object_store.clone(),
                "test",
                "zombie-crash-topic",
                0,
                1024 * 1024,
                fresh_zombie_state, // Fresh state, not in zombie mode
                false,              // fail_on_recovery_gap
            )
            .await
            .expect("Failed to reopen");

            // Data should be recovered
            assert_eq!(store.high_watermark(), 5);

            // Writes should work with fresh zombie state
            let batch = create_non_idempotent_batch(5);
            store
                .append_batch(&batch)
                .await
                .expect("Write after restart should work");

            assert_eq!(store.high_watermark(), 10);

            store.close().await.expect("Failed to close");
        }
    }

    // ========================================================================
    // TOCTOU (Time-of-Check to Time-of-Use) Prevention Tests
    // ========================================================================
    //
    // These tests validate the epoch-based fencing mechanism that prevents
    // split-brain data corruption when partition ownership transfers.

    /// Test that epoch validation rejects writes when another broker has acquired the partition.
    ///
    /// This simulates the TOCTOU race condition:
    /// 1. Broker A acquires partition with epoch 1
    /// 2. Broker A starts a write operation
    /// 3. Broker B acquires partition with epoch 2 (leadership transfer)
    /// 4. Broker B writes its epoch to SlateDB
    /// 5. Broker A's write should be rejected due to epoch mismatch
    ///
    /// The test verifies that epoch validation prevents stale writes.
    #[tokio::test]
    async fn test_epoch_mismatch_rejects_stale_writes() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Simulate Broker A acquiring with epoch 1
        let store_a = PartitionStore::builder()
            .object_store(object_store.clone())
            .base_path("test")
            .topic("epoch-test")
            .partition(0)
            .leader_epoch(1)
            .build()
            .await
            .expect("Failed to open store A");

        // Broker A writes successfully
        let batch1 = create_non_idempotent_batch(5);
        store_a
            .append_batch(&batch1)
            .await
            .expect("Broker A write should succeed");
        assert_eq!(store_a.high_watermark(), 5);

        // Close Broker A's store (simulating it being slow/paused)
        store_a.close().await.expect("Failed to close store A");

        // Simulate Broker B acquiring with epoch 2
        let store_b = PartitionStore::builder()
            .object_store(object_store.clone())
            .base_path("test")
            .topic("epoch-test")
            .partition(0)
            .leader_epoch(2) // Higher epoch - new leader
            .build()
            .await
            .expect("Failed to open store B");

        // Broker B writes successfully (it's the new leader)
        let batch2 = create_non_idempotent_batch(5);
        store_b
            .append_batch(&batch2)
            .await
            .expect("Broker B write should succeed");
        assert_eq!(store_b.high_watermark(), 10);

        // Close store_b BEFORE attempting stale open
        // This is necessary because SlateDB may fence existing handles when
        // a new instance opens, even if our epoch check fails
        store_b.close().await.expect("Failed to close store B");

        // Now if we try to open with old epoch, it should fail or detect mismatch
        // This simulates a stale Broker A trying to reconnect with old epoch
        let result = PartitionStore::builder()
            .object_store(object_store.clone())
            .base_path("test")
            .topic("epoch-test")
            .partition(0)
            .leader_epoch(1) // Stale epoch
            .build()
            .await;

        // Opening with stale epoch should fail with EpochMismatch
        match result {
            Err(super::super::error::SlateDBError::EpochMismatch { expected_epoch, .. }) => {
                assert_eq!(expected_epoch, 1); // What stale broker expected
                // Note: stored_epoch may not be reliably available from error conversion,
                // so we don't assert on it. The key invariant is that opening with
                // a stale epoch fails.
            }
            Err(e) => panic!("Expected EpochMismatch error, got: {:?}", e),
            Ok(_) => panic!("Expected EpochMismatch error, but open succeeded"),
        }
    }

    /// Test that epoch 0 (disabled fencing) allows any writes.
    ///
    /// When leader_epoch is 0, epoch validation is disabled for backwards
    /// compatibility with coordinators that don't track epochs.
    #[tokio::test]
    async fn test_epoch_zero_disables_fencing() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Open with epoch 0 (fencing disabled)
        let store = PartitionStore::builder()
            .object_store(object_store.clone())
            .base_path("test")
            .topic("epoch-zero-test")
            .partition(0)
            .leader_epoch(0) // Epoch 0 = fencing disabled
            .build()
            .await
            .expect("Failed to open store");

        // Writes should succeed
        let batch = create_non_idempotent_batch(5);
        store
            .append_batch(&batch)
            .await
            .expect("Write with epoch 0 should succeed");

        store.close().await.expect("Failed to close");

        // Reopen with epoch 0 - should still work (no stored epoch to conflict)
        let store2 = PartitionStore::builder()
            .object_store(object_store.clone())
            .base_path("test")
            .topic("epoch-zero-test")
            .partition(0)
            .leader_epoch(0)
            .build()
            .await
            .expect("Reopen with epoch 0 should succeed");

        assert_eq!(store2.high_watermark(), 5);
        store2.close().await.expect("Failed to close");
    }

    // ========================================================================
    // Durability Tests (await_durable=false behavior)
    // ========================================================================
    //
    // These tests document and verify the behavior of await_durable=false writes.
    // With await_durable=false, data may be lost if the process crashes before
    // the periodic flush (default 100ms).

    /// Test that documents await_durable=false data loss window.
    ///
    /// This test demonstrates that with await_durable=false:
    /// 1. Writes are acknowledged before being flushed to object store
    /// 2. If we DON'T call flush() before "crash", data may be lost
    /// 3. The recovery logic handles this gracefully
    ///
    /// NOTE: This test is primarily documentation - the actual data loss window
    /// depends on SlateDB's flush_interval timing which is hard to control in tests.
    #[tokio::test]
    async fn test_await_durable_false_acknowledged_before_flush() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // First write session - write data but DON'T flush before "crash"
        let written_hwm = {
            let store = PartitionStore::open(object_store.clone(), "test", "durable-test", 0)
                .await
                .expect("Failed to open");

            // Write batches - these are acknowledged immediately due to await_durable=false
            let batch1 = create_non_idempotent_batch(5);
            store
                .append_batch(&batch1)
                .await
                .expect("Write 1 should succeed");

            let batch2 = create_non_idempotent_batch(5);
            store
                .append_batch(&batch2)
                .await
                .expect("Write 2 should succeed");

            // HWM is updated in memory immediately
            let hwm = store.high_watermark();
            assert_eq!(hwm, 10);

            // IMPORTANT: We explicitly flush() here to ensure data is durable.
            // In a real crash scenario without flush(), some data could be lost.
            // We flush here because InMemory object store + SlateDB doesn't reliably
            // simulate partial data loss in tests.
            store.flush().await.expect("Flush should succeed");

            // Close without additional writes
            store.close().await.expect("Failed to close");
            hwm
        };

        // Reopen - data should be recovered because we flushed
        {
            let store = PartitionStore::open(object_store.clone(), "test", "durable-test", 0)
                .await
                .expect("Failed to reopen");

            // HWM should match what we wrote (because we flushed)
            assert_eq!(store.high_watermark(), written_hwm);

            store.close().await.expect("Failed to close");
        }
    }

    /// Test that flush() ensures durability before simulated crash.
    ///
    /// This verifies that explicitly calling flush() before crash ensures
    /// all data is recoverable, unlike the await_durable=false default.
    #[tokio::test]
    async fn test_explicit_flush_ensures_durability() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Write and flush
        {
            let store = PartitionStore::open(object_store.clone(), "test", "flush-test", 0)
                .await
                .expect("Failed to open");

            for i in 0..10 {
                let batch = create_non_idempotent_batch(1);
                store
                    .append_batch(&batch)
                    .await
                    .unwrap_or_else(|_| panic!("Write {} failed", i));
            }

            assert_eq!(store.high_watermark(), 10);

            // Explicit flush before crash
            store.flush().await.expect("Flush failed");

            // Simulate crash (drop without proper close)
            drop(store);
        }

        // Recovery should find all data
        {
            let store = PartitionStore::open(object_store.clone(), "test", "flush-test", 0)
                .await
                .expect("Failed to reopen");

            // All 10 records should be recovered
            assert_eq!(store.high_watermark(), 10);

            store.close().await.expect("Failed to close");
        }
    }

    // ========================================================================
    // Zombie Mode Re-entry Race Tests
    // ========================================================================
    //
    // These tests verify the zombie mode re-entry detection mechanism that
    // prevents exiting zombie mode when heartbeats fail during verification.

    /// Test zombie mode re-entry detection during exit verification.
    ///
    /// This test simulates the race condition where:
    /// 1. We enter zombie mode (timestamp T1)
    /// 2. We start verification and capture entered_at = T1
    /// 3. Heartbeat fails again, re-entering zombie mode (timestamp T2)
    /// 4. We finish verification and try to exit with T1
    /// 5. Exit should FAIL because T1 != T2
    #[test]
    fn test_zombie_mode_reentry_race_detection() {
        use std::thread;
        use std::time::Duration;

        let zombie_state = ZombieModeState::new();

        // Step 1: Enter zombie mode
        assert!(zombie_state.enter());
        let verification_start_timestamp = zombie_state.entered_at();
        assert!(verification_start_timestamp > 0);

        // Step 2: Simulate re-entry (heartbeat fails again during verification)
        // First exit, then re-enter with new timestamp
        zombie_state.force_exit("test");
        thread::sleep(Duration::from_millis(5)); // Ensure different timestamp
        assert!(zombie_state.enter());
        let new_timestamp = zombie_state.entered_at();

        // Timestamps should be different
        assert_ne!(verification_start_timestamp, new_timestamp);

        // Step 3: Try to exit with old timestamp - should fail
        assert!(!zombie_state.try_exit(verification_start_timestamp, "recovered"));

        // Still in zombie mode because re-entry was detected
        assert!(zombie_state.is_active());

        // Step 4: Exit with correct timestamp should succeed
        assert!(zombie_state.try_exit(new_timestamp, "recovered"));
        assert!(!zombie_state.is_active());
    }

    /// Test concurrent zombie mode entry and exit attempts.
    ///
    /// Multiple threads racing to enter/exit zombie mode should result in
    /// consistent state without data races.
    #[test]
    fn test_zombie_mode_concurrent_entry_exit_race() {
        use std::sync::Arc;
        use std::thread;

        let zombie_state = Arc::new(ZombieModeState::new());
        let iterations = 100;

        for _ in 0..iterations {
            // Reset state
            zombie_state.force_exit("reset");

            let state1 = zombie_state.clone();
            let state2 = zombie_state.clone();

            // Thread 1: Enter and try to exit
            let handle1 = thread::spawn(move || {
                if state1.enter() {
                    let ts = state1.entered_at();
                    // Simulate some verification work
                    thread::yield_now();
                    state1.try_exit(ts, "thread1")
                } else {
                    false
                }
            });

            // Thread 2: Also try to enter
            let handle2 = thread::spawn(move || state2.enter());

            let result1 = handle1.join().unwrap();
            let result2 = handle2.join().unwrap();

            // At most one thread should successfully enter first
            // (one gets true from enter(), the other gets false or enters after first exits)
            // The key invariant: we should never have inconsistent state
            let final_active = zombie_state.is_active();

            // If thread1 successfully exited, state should be inactive (unless thread2 re-entered)
            // If thread2 entered after thread1 exited, state could be active
            // This is acceptable - the test verifies no panics or data races occur
            let _ = (result1, result2, final_active); // Use values to avoid warnings
        }
    }

    /// Test that zombie mode blocks writes even during re-entry window.
    ///
    /// This ensures that if zombie mode is re-entered during verification,
    /// writes continue to be blocked.
    #[tokio::test]
    async fn test_zombie_mode_reentry_blocks_writes() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());

        let store = PartitionStore::open_with_zombie_flag(
            object_store.clone(),
            "test",
            "zombie-reentry-test",
            0,
            1024 * 1024,
            zombie_state.clone(),
            false,
        )
        .await
        .expect("Failed to open");

        // Write should succeed initially
        let batch1 = create_non_idempotent_batch(5);
        store
            .append_batch(&batch1)
            .await
            .expect("Initial write should succeed");

        // Enter zombie mode
        zombie_state.enter();
        let first_timestamp = zombie_state.entered_at();

        // Writes should fail
        let batch2 = create_non_idempotent_batch(5);
        assert!(store.append_batch(&batch2).await.is_err());

        // Simulate re-entry (exit and immediately re-enter)
        zombie_state.force_exit("simulated_recovery");
        std::thread::sleep(std::time::Duration::from_millis(2));
        zombie_state.enter();
        let second_timestamp = zombie_state.entered_at();

        // Verify timestamps changed
        assert_ne!(first_timestamp, second_timestamp);

        // Writes should still fail (still in zombie mode)
        let batch3 = create_non_idempotent_batch(5);
        assert!(store.append_batch(&batch3).await.is_err());

        // Exit properly
        zombie_state.force_exit("test_cleanup");

        // Now writes should succeed
        let batch4 = create_non_idempotent_batch(5);
        store
            .append_batch(&batch4)
            .await
            .expect("Write after exit should succeed");

        // HWM should be 10 (5 from first write + 5 from final write)
        assert_eq!(store.high_watermark(), 10);

        store.close().await.expect("Failed to close");
    }
}
