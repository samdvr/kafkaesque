//! This test exercises the full consumer group protocol to identify

#![allow(clippy::field_reassign_with_default)]

use bytes::{BufMut, Bytes, BytesMut};
use std::net::SocketAddr;
use tempfile::TempDir;

use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
use kafkaesque::error::KafkaCode;
use kafkaesque::server::request::*;
use kafkaesque::server::{Handler, RequestContext};

/// Create a minimal request context for testing.
fn create_test_context() -> RequestContext {
    RequestContext {
        client_addr: "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
        api_version: 8,
        client_id: Some("debug-test-client".to_string()),
        request_id: uuid::Uuid::new_v4(),
    }
}

/// Create a test handler with local file storage.
async fn create_handler(data_path: &str) -> SlateDBClusterHandler {
    let mut config = ClusterConfig::default();
    config.broker_id = 0;
    config.auto_create_topics = true;
    config.object_store = kafkaesque::cluster::ObjectStoreType::Local {
        path: data_path.to_string(),
    };
    config.object_store_path = data_path.to_string();

    SlateDBClusterHandler::new(config)
        .await
        .expect("Failed to create handler")
}

/// Helper to create a Kafka RecordBatch with actual message content.
fn create_record_batch_with_messages(messages: &[&str]) -> Bytes {
    let mut batch = BytesMut::new();

    // Reserve space for batch header (61 bytes)
    batch.put_i64(0); // base_offset
    let batch_length_pos = batch.len();
    batch.put_i32(0); // batch_length (placeholder)
    batch.put_i32(0); // partition_leader_epoch
    batch.put_u8(2); // magic = 2
    let crc_pos = batch.len();
    batch.put_u32(0); // crc (placeholder)
    batch.put_i16(0); // attributes
    batch.put_i32((messages.len() - 1) as i32); // last_offset_delta
    batch.put_i64(0); // base_timestamp
    batch.put_i64(0); // max_timestamp
    batch.put_i64(-1); // producer_id
    batch.put_i16(-1); // producer_epoch
    batch.put_i32(-1); // base_sequence
    batch.put_i32(messages.len() as i32); // records_count

    // Add records
    for (offset_delta, msg) in messages.iter().enumerate() {
        let record_start = batch.len();

        // Encode record length (varint placeholder)
        let record_length_pos = batch.len();
        batch.put_u8(0); // placeholder for length varint

        // attributes (1 byte varint)
        batch.put_u8(0);

        // timestamp_delta (varint)
        batch.put_u8(0);

        // offset_delta (varint)
        put_varint(&mut batch, offset_delta as i64);

        // key length (varint) - null key
        put_varint(&mut batch, -1);

        // value length (varint)
        put_varint(&mut batch, msg.len() as i64);

        // value
        batch.put_slice(msg.as_bytes());

        // headers count (varint)
        batch.put_u8(0);

        // Calculate and update record length
        let record_len = batch.len() - record_start - 1;
        batch[record_length_pos] = record_len as u8;
    }

    // Update batch length
    let batch_len = (batch.len() - batch_length_pos - 4) as i32;
    batch[batch_length_pos..batch_length_pos + 4].copy_from_slice(&batch_len.to_be_bytes());

    // Compute and update CRC-32C (from attributes onwards)
    let crc = compute_crc32c(&batch[crc_pos + 4..]);
    batch[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

    batch.freeze()
}

fn put_varint(buf: &mut BytesMut, value: i64) {
    let encoded = ((value as u64).wrapping_shl(1)) ^ ((value >> 63) as u64);
    let mut v = encoded;
    while v >= 0x80 {
        buf.put_u8(((v & 0x7F) | 0x80) as u8);
        v >>= 7;
    }
    buf.put_u8((v & 0x7F) as u8);
}

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

    let mut crc = 0xFFFFFFFFu32;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[index];
    }
    !crc
}

/// Create subscription metadata for JoinGroup (Kafka consumer protocol format)
fn create_subscription_metadata(topics: &[&str]) -> Bytes {
    let mut buf = BytesMut::new();

    // Version 0
    buf.put_i16(0);

    // Topics array
    buf.put_i32(topics.len() as i32);
    for topic in topics {
        let topic_bytes = topic.as_bytes();
        buf.put_i16(topic_bytes.len() as i16);
        buf.put_slice(topic_bytes);
    }

    // User data - null
    buf.put_i32(-1);

    buf.freeze()
}

/// Create assignment data for SyncGroup (Kafka consumer protocol format)
fn create_assignment_data(topic: &str, partitions: &[i32]) -> Bytes {
    let mut buf = BytesMut::new();

    // Version 0
    buf.put_i16(0);

    // Topics array - 1 topic
    buf.put_i32(1);

    // Topic name
    let topic_bytes = topic.as_bytes();
    buf.put_i16(topic_bytes.len() as i16);
    buf.put_slice(topic_bytes);

    // Partitions array
    buf.put_i32(partitions.len() as i32);
    for &partition in partitions {
        buf.put_i32(partition);
    }

    // User data - null
    buf.put_i32(-1);

    buf.freeze()
}

/// Parse and print assignment data for debugging
fn parse_assignment(data: &[u8]) -> Option<Vec<(String, Vec<i32>)>> {
    if data.len() < 6 {
        return None;
    }

    let _version = i16::from_be_bytes([data[0], data[1]]);
    let topic_count = i32::from_be_bytes([data[2], data[3], data[4], data[5]]);

    let mut result = Vec::new();
    let mut offset = 6;

    for _ in 0..topic_count {
        if offset + 2 > data.len() {
            return None;
        }

        let name_len = i16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        if offset + name_len > data.len() {
            return None;
        }

        let name = String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
        offset += name_len;

        if offset + 4 > data.len() {
            return None;
        }

        let part_count = i32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        let mut partitions = Vec::new();
        for _ in 0..part_count {
            if offset + 4 > data.len() {
                return None;
            }
            let p = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            partitions.push(p);
            offset += 4;
        }

        result.push((name, partitions));
    }

    Some(result)
}

#[tokio::test]
async fn test_consumer_group_protocol_debug() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp_dir.path().to_string_lossy().to_string();

    let topic = "consumer-group-debug-topic";
    let group_id = "debug-consumer-group";

    let handler = create_handler(&data_path).await;
    let ctx = create_test_context();

    // =========================================================================
    // Step 1: Create topic via CreateTopics
    // =========================================================================
    println!("\n=== STEP 1: CreateTopics ===");
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: topic.to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let create_resp = handler.handle_create_topics(&ctx, create_req).await;
    println!(
        "CreateTopics response: {:?}",
        create_resp.topics[0].error_code
    );
    assert!(
        create_resp.topics[0].error_code == KafkaCode::None
            || create_resp.topics[0].error_code == KafkaCode::TopicAlreadyExists,
        "Unexpected error: {:?}",
        create_resp.topics[0].error_code
    );

    // =========================================================================
    // Step 2: Produce messages
    // =========================================================================
    println!("\n=== STEP 2: Produce messages ===");
    let messages = vec!["msg-1", "msg-2", "msg-3", "msg-4", "msg-5"];
    let records = create_record_batch_with_messages(&messages);

    let produce_req = ProduceRequestData {
        transactional_id: None,
        acks: -1,
        timeout_ms: 5000,
        topics: vec![ProduceTopicData {
            name: topic.to_string(),
            partitions: vec![ProducePartitionData {
                partition_index: 0,
                records,
            }],
        }],
    };

    let produce_resp = handler.handle_produce(&ctx, produce_req).await;
    let produce_result = &produce_resp.responses[0].partitions[0];
    println!(
        "Produce response: error={:?}, base_offset={}",
        produce_result.error_code, produce_result.base_offset
    );
    assert_eq!(produce_result.error_code, KafkaCode::None);

    // =========================================================================
    // Step 3: Metadata - verify topic and broker info
    // =========================================================================
    println!("\n=== STEP 3: Metadata ===");
    let metadata_req = MetadataRequestData {
        topics: Some(vec![topic.to_string()]),
    };
    let metadata_resp = handler.handle_metadata(&ctx, metadata_req).await;
    println!("Brokers:");
    for broker in &metadata_resp.brokers {
        println!(
            "  Broker {}: {}:{}",
            broker.node_id, broker.host, broker.port
        );
    }
    println!("Topics:");
    for topic_meta in &metadata_resp.topics {
        println!(
            "  Topic '{}': error={:?}, partitions={}",
            topic_meta.name,
            topic_meta.error_code,
            topic_meta.partitions.len()
        );
        for part in &topic_meta.partitions {
            println!(
                "    Partition {}: leader={}, error={:?}",
                part.partition_index, part.leader_id, part.error_code
            );
        }
    }

    // =========================================================================
    // Step 4: FindCoordinator for consumer group
    // =========================================================================
    println!("\n=== STEP 4: FindCoordinator ===");
    let find_coord_req = FindCoordinatorRequestData {
        key: group_id.to_string(),
        key_type: 0,
    };
    let find_coord_resp = handler.handle_find_coordinator(&ctx, find_coord_req).await;
    println!(
        "FindCoordinator: error={:?}, node_id={}, host='{}', port={}",
        find_coord_resp.error_code,
        find_coord_resp.node_id,
        find_coord_resp.host,
        find_coord_resp.port
    );
    assert_eq!(find_coord_resp.error_code, KafkaCode::None);

    // =========================================================================
    // Step 5: JoinGroup
    // =========================================================================
    println!("\n=== STEP 5: JoinGroup ===");
    let join_req = JoinGroupRequestData {
        group_id: group_id.to_string(),
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 60000,
        member_id: String::new(), // Empty for initial join
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupProtocolData {
            name: "range".to_string(),
            metadata: create_subscription_metadata(&[topic]),
        }],
    };

    let join_resp = handler.handle_join_group(&ctx, join_req).await;
    println!(
        "JoinGroup: error={:?}, generation={}, leader='{}', member_id='{}'",
        join_resp.error_code, join_resp.generation_id, join_resp.leader, join_resp.member_id
    );
    println!(
        "  Protocol: {:?}, Members: {}",
        join_resp.protocol_name,
        join_resp.members.len()
    );
    assert_eq!(join_resp.error_code, KafkaCode::None);

    let member_id = join_resp.member_id.clone();
    let generation_id = join_resp.generation_id;
    let is_leader = join_resp.leader == member_id;

    // =========================================================================
    // Step 6: SyncGroup
    // =========================================================================
    println!("\n=== STEP 6: SyncGroup (is_leader={}) ===", is_leader);
    let assignments = if is_leader {
        vec![SyncGroupAssignmentData {
            member_id: member_id.clone(),
            assignment: create_assignment_data(topic, &[0]),
        }]
    } else {
        vec![]
    };

    let sync_req = SyncGroupRequestData {
        group_id: group_id.to_string(),
        generation_id,
        member_id: member_id.clone(),
        assignments,
    };

    let sync_resp = handler.handle_sync_group(&ctx, sync_req).await;
    println!(
        "SyncGroup: error={:?}, assignment_len={}",
        sync_resp.error_code,
        sync_resp.assignment.len()
    );

    if !sync_resp.assignment.is_empty() {
        if let Some(parsed) = parse_assignment(&sync_resp.assignment) {
            println!("  Parsed assignment:");
            for (topic_name, partitions) in &parsed {
                println!("    Topic '{}': partitions {:?}", topic_name, partitions);
            }
        } else {
            println!(
                "  Assignment bytes (hex): {:02x?}",
                &sync_resp.assignment[..sync_resp.assignment.len().min(50)]
            );
        }
    } else {
        println!("  WARNING: Empty assignment returned!");
    }
    assert_eq!(sync_resp.error_code, KafkaCode::None);

    // =========================================================================
    // Step 7: ListOffsets - get earliest and latest
    // =========================================================================
    println!("\n=== STEP 7: ListOffsets ===");

    // Earliest (-2)
    let list_offsets_req = ListOffsetsRequestData {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopicData {
            name: topic.to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 0,
                timestamp: -2,
            }],
        }],
    };
    let list_offsets_resp = handler.handle_list_offsets(&ctx, list_offsets_req).await;
    let earliest_result = &list_offsets_resp.topics[0].partitions[0];
    println!(
        "Earliest offset: error={:?}, offset={}",
        earliest_result.error_code, earliest_result.offset
    );

    // Latest (-1)
    let list_offsets_req2 = ListOffsetsRequestData {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopicData {
            name: topic.to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 0,
                timestamp: -1,
            }],
        }],
    };
    let list_offsets_resp2 = handler.handle_list_offsets(&ctx, list_offsets_req2).await;
    let latest_result = &list_offsets_resp2.topics[0].partitions[0];
    println!(
        "Latest offset: error={:?}, offset={}",
        latest_result.error_code, latest_result.offset
    );

    assert_eq!(earliest_result.error_code, KafkaCode::None);
    assert_eq!(latest_result.error_code, KafkaCode::None);

    // =========================================================================
    // Step 8: Fetch from offset 0
    // =========================================================================
    println!("\n=== STEP 8: Fetch ===");
    let fetch_req = FetchRequestData {
        replica_id: -1,
        max_wait_ms: 5000,
        min_bytes: 1,
        max_bytes: 1024 * 1024,
        isolation_level: 0,
        topics: vec![FetchTopicData {
            name: topic.to_string(),
            partitions: vec![FetchPartitionData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024 * 1024,
            }],
        }],
    };

    let fetch_resp = handler.handle_fetch(&ctx, fetch_req).await;
    let fetch_result = &fetch_resp.responses[0].partitions[0];
    let records_len = fetch_result.records.as_ref().map(|r| r.len()).unwrap_or(0);
    println!(
        "Fetch: error={:?}, high_watermark={}, records={} bytes",
        fetch_result.error_code, fetch_result.high_watermark, records_len
    );
    assert_eq!(fetch_result.error_code, KafkaCode::None);
    assert!(records_len > 0, "Expected records but got 0 bytes!");

    // =========================================================================
    // Step 9: OffsetFetch (committed offsets)
    // =========================================================================
    println!("\n=== STEP 9: OffsetFetch ===");
    let offset_fetch_req = OffsetFetchRequestData {
        group_id: group_id.to_string(),
        topics: vec![OffsetFetchTopicData {
            name: topic.to_string(),
            partition_indexes: vec![0],
        }],
    };
    let offset_fetch_resp = handler.handle_offset_fetch(&ctx, offset_fetch_req).await;
    let committed = &offset_fetch_resp.topics[0].partitions[0];
    println!(
        "Committed offset: error={:?}, offset={}",
        committed.error_code, committed.committed_offset
    );

    // =========================================================================
    // Step 10: Heartbeat
    // =========================================================================
    println!("\n=== STEP 10: Heartbeat ===");
    let heartbeat_req = HeartbeatRequestData {
        group_id: group_id.to_string(),
        generation_id,
        member_id: member_id.clone(),
    };
    let heartbeat_resp = handler.handle_heartbeat(&ctx, heartbeat_req).await;
    println!("Heartbeat: error={:?}", heartbeat_resp.error_code);
    assert_eq!(heartbeat_resp.error_code, KafkaCode::None);

    // Cleanup
    handler.shutdown().await.expect("Failed to shutdown");

    println!("\n=== TEST PASSED - All consumer group protocol steps work! ===");
}
