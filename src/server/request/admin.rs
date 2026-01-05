//! Admin request parsing (CreateTopics, DeleteTopics, InitProducerId).

use nom::{
    IResult,
    number::complete::{be_i16, be_i32, be_i64},
};
use nombytes::NomBytes;

use crate::parser::{
    bytes_to_string, bytes_to_string_opt, parse_array, parse_nullable_string, parse_string,
};

// ============================================================================
// CreateTopics
// ============================================================================

/// CreateTopics request data.
#[derive(Debug, Clone)]
pub struct CreateTopicsRequestData {
    pub topics: Vec<CreateTopicData>,
    pub timeout_ms: i32,
}

#[derive(Debug, Clone)]
pub struct CreateTopicData {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
}

pub fn parse_create_topics_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, CreateTopicsRequestData> {
    let (s, topics) = parse_array(parse_create_topic)(s)?;
    let (s, timeout_ms) = be_i32(s)?;

    Ok((s, CreateTopicsRequestData { topics, timeout_ms }))
}

fn parse_create_topic(s: NomBytes) -> IResult<NomBytes, CreateTopicData> {
    let (s, name) = parse_string(s)?;
    let (s, num_partitions) = be_i32(s)?;
    let (s, replication_factor) = be_i16(s)?;
    // Skip replica assignments and configs for now
    let (s, _replica_assignments) = parse_array(|s| {
        let (s, _) = be_i32(s)?;
        let (s, _) = parse_array(be_i32)(s)?;
        Ok((s, ()))
    })(s)?;
    let (s, _configs) = parse_array(|s| {
        let (s, _) = parse_string(s)?;
        let (s, _) = parse_nullable_string(s)?;
        Ok((s, ()))
    })(s)?;

    Ok((
        s,
        CreateTopicData {
            name: bytes_to_string(&name)?,
            num_partitions,
            replication_factor,
        },
    ))
}

// ============================================================================
// DeleteTopics
// ============================================================================

/// DeleteTopics request data.
#[derive(Debug, Clone)]
pub struct DeleteTopicsRequestData {
    pub topic_names: Vec<String>,
    pub timeout_ms: i32,
}

pub fn parse_delete_topics_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, DeleteTopicsRequestData> {
    let (s, topic_names) = parse_array(|s| {
        let (s, name) = parse_string(s)?;
        Ok((s, String::from_utf8_lossy(&name).to_string()))
    })(s)?;
    let (s, timeout_ms) = be_i32(s)?;

    Ok((
        s,
        DeleteTopicsRequestData {
            topic_names,
            timeout_ms,
        },
    ))
}

// ============================================================================
// InitProducerId
// ============================================================================

/// InitProducerId request data.
#[derive(Debug, Clone)]
pub struct InitProducerIdRequestData {
    pub transactional_id: Option<String>,
    pub transaction_timeout_ms: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

pub fn parse_init_producer_id_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, InitProducerIdRequestData> {
    let (s, transactional_id) = parse_nullable_string(s)?;
    let (s, transaction_timeout_ms) = be_i32(s)?;
    // Version 3+ has producer_id and producer_epoch
    let (s, producer_id) = if version >= 3 { be_i64(s)? } else { (s, -1i64) };
    let (s, producer_epoch) = if version >= 3 { be_i16(s)? } else { (s, -1i16) };

    Ok((
        s,
        InitProducerIdRequestData {
            transactional_id: bytes_to_string_opt(transactional_id)?,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_nom_bytes(data: &[u8]) -> NomBytes {
        NomBytes::from(data)
    }

    // ========================================================================
    // CreateTopics Tests
    // ========================================================================

    #[test]
    fn test_parse_create_topics_request_single() {
        // Build a CreateTopics request with 1 topic
        let mut data = Vec::new();
        // Array count: 1 topic
        data.extend_from_slice(&1i32.to_be_bytes());
        // Topic name length: 10
        data.extend_from_slice(&10i16.to_be_bytes());
        // Topic name: "test-topic"
        data.extend_from_slice(b"test-topic");
        // num_partitions: 3
        data.extend_from_slice(&3i32.to_be_bytes());
        // replication_factor: 1
        data.extend_from_slice(&1i16.to_be_bytes());
        // Empty replica assignments array
        data.extend_from_slice(&0i32.to_be_bytes());
        // Empty configs array
        data.extend_from_slice(&0i32.to_be_bytes());
        // timeout_ms: 30000
        data.extend_from_slice(&30000i32.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_create_topics_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].name, "test-topic");
        assert_eq!(request.topics[0].num_partitions, 3);
        assert_eq!(request.topics[0].replication_factor, 1);
        assert_eq!(request.timeout_ms, 30000);
    }

    #[test]
    fn test_parse_create_topics_request_multiple() {
        // Build a CreateTopics request with 2 topics
        let mut data = Vec::new();
        // Array count: 2 topics
        data.extend_from_slice(&2i32.to_be_bytes());

        // Topic 1: "topic-a"
        data.extend_from_slice(&7i16.to_be_bytes());
        data.extend_from_slice(b"topic-a");
        data.extend_from_slice(&5i32.to_be_bytes()); // 5 partitions
        data.extend_from_slice(&2i16.to_be_bytes()); // replication factor 2
        data.extend_from_slice(&0i32.to_be_bytes()); // empty replica assignments
        data.extend_from_slice(&0i32.to_be_bytes()); // empty configs

        // Topic 2: "topic-b"
        data.extend_from_slice(&7i16.to_be_bytes());
        data.extend_from_slice(b"topic-b");
        data.extend_from_slice(&10i32.to_be_bytes()); // 10 partitions
        data.extend_from_slice(&3i16.to_be_bytes()); // replication factor 3
        data.extend_from_slice(&0i32.to_be_bytes()); // empty replica assignments
        data.extend_from_slice(&0i32.to_be_bytes()); // empty configs

        // timeout_ms: 60000
        data.extend_from_slice(&60000i32.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_create_topics_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.topics.len(), 2);
        assert_eq!(request.topics[0].name, "topic-a");
        assert_eq!(request.topics[0].num_partitions, 5);
        assert_eq!(request.topics[0].replication_factor, 2);
        assert_eq!(request.topics[1].name, "topic-b");
        assert_eq!(request.topics[1].num_partitions, 10);
        assert_eq!(request.topics[1].replication_factor, 3);
        assert_eq!(request.timeout_ms, 60000);
    }

    #[test]
    fn test_parse_create_topics_request_empty() {
        // Build a CreateTopics request with 0 topics
        let mut data = Vec::new();
        // Array count: 0 topics
        data.extend_from_slice(&0i32.to_be_bytes());
        // timeout_ms: 5000
        data.extend_from_slice(&5000i32.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_create_topics_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert!(request.topics.is_empty());
        assert_eq!(request.timeout_ms, 5000);
    }

    #[test]
    fn test_parse_create_topics_request_with_default_partitions() {
        // Build a CreateTopics request with -1 partitions (server decides)
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
        data.extend_from_slice(&8i16.to_be_bytes());
        data.extend_from_slice(b"my-topic");
        data.extend_from_slice(&(-1i32).to_be_bytes()); // -1 means server decides
        data.extend_from_slice(&(-1i16).to_be_bytes()); // -1 means server decides
        data.extend_from_slice(&0i32.to_be_bytes()); // empty replica assignments
        data.extend_from_slice(&0i32.to_be_bytes()); // empty configs
        data.extend_from_slice(&30000i32.to_be_bytes()); // timeout

        let input = create_nom_bytes(&data);
        let result = parse_create_topics_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.topics[0].num_partitions, -1);
        assert_eq!(request.topics[0].replication_factor, -1);
    }

    #[test]
    fn test_create_topics_request_data_debug() {
        let data = CreateTopicsRequestData {
            topics: vec![CreateTopicData {
                name: "test".to_string(),
                num_partitions: 1,
                replication_factor: 1,
            }],
            timeout_ms: 5000,
        };
        let debug = format!("{:?}", data);
        assert!(debug.contains("CreateTopicsRequestData"));
        assert!(debug.contains("5000"));
    }

    #[test]
    fn test_create_topics_request_data_clone() {
        let original = CreateTopicsRequestData {
            topics: vec![CreateTopicData {
                name: "test".to_string(),
                num_partitions: 3,
                replication_factor: 1,
            }],
            timeout_ms: 10000,
        };
        let cloned = original.clone();
        assert_eq!(cloned.topics.len(), 1);
        assert_eq!(cloned.topics[0].name, "test");
        assert_eq!(cloned.timeout_ms, 10000);
    }

    #[test]
    fn test_create_topic_data_clone() {
        let original = CreateTopicData {
            name: "my-topic".to_string(),
            num_partitions: 10,
            replication_factor: 3,
        };
        let cloned = original.clone();
        assert_eq!(cloned.name, "my-topic");
        assert_eq!(cloned.num_partitions, 10);
        assert_eq!(cloned.replication_factor, 3);
    }

    // ========================================================================
    // DeleteTopics Tests
    // ========================================================================

    #[test]
    fn test_parse_delete_topics_request() {
        // Build a DeleteTopics request:
        // topics array: 1 element with name "test-topic" (10 chars)
        // timeout_ms: 30000 (0x00007530)
        let mut data = Vec::new();
        // Array count: 1
        data.extend_from_slice(&1i32.to_be_bytes());
        // Topic name length: 10
        data.extend_from_slice(&10i16.to_be_bytes());
        // Topic name: "test-topic"
        data.extend_from_slice(b"test-topic");
        // Timeout: 30000
        data.extend_from_slice(&30000i32.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_delete_topics_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.topic_names.len(), 1);
        assert_eq!(request.topic_names[0], "test-topic");
        assert_eq!(request.timeout_ms, 30000);
    }

    #[test]
    fn test_parse_init_producer_id_request_v0() {
        // Build an InitProducerId request v0:
        // transactional_id: null (-1)
        // transaction_timeout_ms: 60000
        let mut data = Vec::new();
        // Null string: -1
        data.extend_from_slice(&(-1i16).to_be_bytes());
        // Transaction timeout: 60000
        data.extend_from_slice(&60000i32.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_init_producer_id_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert!(request.transactional_id.is_none());
        assert_eq!(request.transaction_timeout_ms, 60000);
        assert_eq!(request.producer_id, -1);
        assert_eq!(request.producer_epoch, -1);
    }

    #[test]
    fn test_parse_init_producer_id_request_v3() {
        // Build an InitProducerId request v3:
        // transactional_id: "my-txn" (6 chars)
        // transaction_timeout_ms: 60000
        // producer_id: 12345
        // producer_epoch: 0
        let mut data = Vec::new();
        // String length: 6
        data.extend_from_slice(&6i16.to_be_bytes());
        // String: "my-txn"
        data.extend_from_slice(b"my-txn");
        // Transaction timeout: 60000
        data.extend_from_slice(&60000i32.to_be_bytes());
        // Producer ID: 12345
        data.extend_from_slice(&12345i64.to_be_bytes());
        // Producer epoch: 0
        data.extend_from_slice(&0i16.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_init_producer_id_request(input, 3);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.transactional_id, Some("my-txn".to_string()));
        assert_eq!(request.transaction_timeout_ms, 60000);
        assert_eq!(request.producer_id, 12345);
        assert_eq!(request.producer_epoch, 0);
    }

    #[test]
    fn test_parse_delete_topics_request_multiple() {
        // Build a DeleteTopics request with multiple topics
        let mut data = Vec::new();
        // Array count: 3 topics
        data.extend_from_slice(&3i32.to_be_bytes());
        // Topic 1: "topic-a"
        data.extend_from_slice(&7i16.to_be_bytes());
        data.extend_from_slice(b"topic-a");
        // Topic 2: "topic-b"
        data.extend_from_slice(&7i16.to_be_bytes());
        data.extend_from_slice(b"topic-b");
        // Topic 3: "topic-c"
        data.extend_from_slice(&7i16.to_be_bytes());
        data.extend_from_slice(b"topic-c");
        // Timeout: 15000
        data.extend_from_slice(&15000i32.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_delete_topics_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.topic_names.len(), 3);
        assert_eq!(request.topic_names[0], "topic-a");
        assert_eq!(request.topic_names[1], "topic-b");
        assert_eq!(request.topic_names[2], "topic-c");
        assert_eq!(request.timeout_ms, 15000);
    }

    #[test]
    fn test_parse_delete_topics_request_empty() {
        // Build a DeleteTopics request with 0 topics
        let mut data = Vec::new();
        data.extend_from_slice(&0i32.to_be_bytes()); // 0 topics
        data.extend_from_slice(&10000i32.to_be_bytes()); // timeout

        let input = create_nom_bytes(&data);
        let result = parse_delete_topics_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert!(request.topic_names.is_empty());
        assert_eq!(request.timeout_ms, 10000);
    }

    #[test]
    fn test_delete_topics_request_data_clone() {
        let original = DeleteTopicsRequestData {
            topic_names: vec!["topic1".to_string(), "topic2".to_string()],
            timeout_ms: 5000,
        };
        let cloned = original.clone();
        assert_eq!(cloned.topic_names.len(), 2);
        assert_eq!(cloned.topic_names[0], "topic1");
        assert_eq!(cloned.timeout_ms, 5000);
    }

    // ========================================================================
    // InitProducerId Tests
    // ========================================================================

    #[test]
    fn test_parse_init_producer_id_request_v1() {
        // Build an InitProducerId request v1 (no producer_id/epoch fields)
        let mut data = Vec::new();
        // Null string: -1
        data.extend_from_slice(&(-1i16).to_be_bytes());
        // Transaction timeout: 30000
        data.extend_from_slice(&30000i32.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_init_producer_id_request(input, 1);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert!(request.transactional_id.is_none());
        assert_eq!(request.transaction_timeout_ms, 30000);
        // v1 doesn't have these fields, defaults to -1
        assert_eq!(request.producer_id, -1);
        assert_eq!(request.producer_epoch, -1);
    }

    #[test]
    fn test_parse_init_producer_id_request_v3_with_existing_producer() {
        // Build an InitProducerId request v3 with existing producer (bump epoch)
        let mut data = Vec::new();
        // Null transactional_id
        data.extend_from_slice(&(-1i16).to_be_bytes());
        // Transaction timeout: 60000
        data.extend_from_slice(&60000i32.to_be_bytes());
        // Existing producer ID: 54321
        data.extend_from_slice(&54321i64.to_be_bytes());
        // Current epoch: 5
        data.extend_from_slice(&5i16.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_init_producer_id_request(input, 3);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert!(request.transactional_id.is_none());
        assert_eq!(request.transaction_timeout_ms, 60000);
        assert_eq!(request.producer_id, 54321);
        assert_eq!(request.producer_epoch, 5);
    }

    #[test]
    fn test_parse_init_producer_id_request_v4() {
        // Build an InitProducerId request v4
        let mut data = Vec::new();
        // transactional_id: "tx-1234" (7 chars)
        data.extend_from_slice(&7i16.to_be_bytes());
        data.extend_from_slice(b"tx-1234");
        // Transaction timeout: 120000
        data.extend_from_slice(&120000i32.to_be_bytes());
        // Producer ID: -1 (new producer)
        data.extend_from_slice(&(-1i64).to_be_bytes());
        // Producer epoch: -1 (new producer)
        data.extend_from_slice(&(-1i16).to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_init_producer_id_request(input, 4);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.transactional_id, Some("tx-1234".to_string()));
        assert_eq!(request.transaction_timeout_ms, 120000);
        assert_eq!(request.producer_id, -1);
        assert_eq!(request.producer_epoch, -1);
    }

    #[test]
    fn test_init_producer_id_request_data_clone() {
        let original = InitProducerIdRequestData {
            transactional_id: Some("my-txn".to_string()),
            transaction_timeout_ms: 60000,
            producer_id: 12345,
            producer_epoch: 2,
        };
        let cloned = original.clone();
        assert_eq!(cloned.transactional_id, Some("my-txn".to_string()));
        assert_eq!(cloned.transaction_timeout_ms, 60000);
        assert_eq!(cloned.producer_id, 12345);
        assert_eq!(cloned.producer_epoch, 2);
    }

    #[test]
    fn test_init_producer_id_request_data_clone_none_txn() {
        let original = InitProducerIdRequestData {
            transactional_id: None,
            transaction_timeout_ms: 30000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let cloned = original.clone();
        assert!(cloned.transactional_id.is_none());
        assert_eq!(cloned.producer_id, -1);
    }

    // ========================================================================
    // Debug/Clone Tests
    // ========================================================================

    #[test]
    fn test_create_topic_data_debug() {
        let data = CreateTopicData {
            name: "test".to_string(),
            num_partitions: 3,
            replication_factor: 1,
        };
        let debug = format!("{:?}", data);
        assert!(debug.contains("CreateTopicData"));
    }

    #[test]
    fn test_delete_topics_request_data_debug() {
        let data = DeleteTopicsRequestData {
            topic_names: vec!["topic1".to_string()],
            timeout_ms: 5000,
        };
        let debug = format!("{:?}", data);
        assert!(debug.contains("DeleteTopicsRequestData"));
    }

    #[test]
    fn test_init_producer_id_request_data_debug() {
        let data = InitProducerIdRequestData {
            transactional_id: Some("txn".to_string()),
            transaction_timeout_ms: 60000,
            producer_id: 1,
            producer_epoch: 0,
        };
        let debug = format!("{:?}", data);
        assert!(debug.contains("InitProducerIdRequestData"));
    }

    #[test]
    fn test_init_producer_id_request_data_debug_no_txn() {
        let data = InitProducerIdRequestData {
            transactional_id: None,
            transaction_timeout_ms: 30000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let debug = format!("{:?}", data);
        assert!(debug.contains("None"));
    }
}
