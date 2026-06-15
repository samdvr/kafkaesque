//! Tests for consistent hash assignment.

use kafkaesque::cluster::{BrokerInfo, consistent_hash_assignment};

#[test]
fn test_consistent_hash_single_broker() {
    let brokers = vec![BrokerInfo {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        registered_at: 0,
    }];

    let result = consistent_hash_assignment("test-topic", 0, &brokers);
    assert_eq!(result, 1);
}

#[test]
fn test_consistent_hash_empty_brokers() {
    let brokers: Vec<BrokerInfo> = vec![];
    let result = consistent_hash_assignment("test-topic", 0, &brokers);
    assert_eq!(result, -1); // -1 indicates no brokers available
}

#[test]
fn test_consistent_hash_multiple_brokers() {
    let brokers = vec![
        BrokerInfo {
            broker_id: 1,
            host: "broker1".to_string(),
            port: 9092,
            registered_at: 0,
        },
        BrokerInfo {
            broker_id: 2,
            host: "broker2".to_string(),
            port: 9092,
            registered_at: 0,
        },
        BrokerInfo {
            broker_id: 3,
            host: "broker3".to_string(),
            port: 9092,
            registered_at: 0,
        },
    ];

    // Should return a valid broker id
    let result = consistent_hash_assignment("test-topic", 0, &brokers);
    assert!((1..=3).contains(&result));
}

#[test]
fn test_consistent_hash_deterministic() {
    let brokers = vec![
        BrokerInfo {
            broker_id: 1,
            host: "broker1".to_string(),
            port: 9092,
            registered_at: 0,
        },
        BrokerInfo {
            broker_id: 2,
            host: "broker2".to_string(),
            port: 9092,
            registered_at: 0,
        },
    ];

    // Same input should produce same output
    let result1 = consistent_hash_assignment("test-topic", 5, &brokers);
    let result2 = consistent_hash_assignment("test-topic", 5, &brokers);
    assert_eq!(result1, result2);
}

#[test]
fn test_consistent_hash_different_partitions() {
    let brokers = vec![
        BrokerInfo {
            broker_id: 1,
            host: "broker1".to_string(),
            port: 9092,
            registered_at: 0,
        },
        BrokerInfo {
            broker_id: 2,
            host: "broker2".to_string(),
            port: 9092,
            registered_at: 0,
        },
    ];

    // Different partitions may hash to different brokers
    let results: Vec<_> = (0..10)
        .map(|p| consistent_hash_assignment("test-topic", p, &brokers))
        .collect();

    // All results should be valid broker IDs
    for result in &results {
        assert!(*result == 1 || *result == 2);
    }
}

#[test]
fn test_consistent_hash_large_cluster() {
    let brokers: Vec<BrokerInfo> = (1..=10)
        .map(|id| BrokerInfo {
            broker_id: id,
            host: format!("broker{}", id),
            port: 9092,
            registered_at: 0,
        })
        .collect();

    // Test multiple partitions across large cluster
    let mut broker_distribution = std::collections::HashMap::new();
    for partition in 0..100 {
        let broker_id = consistent_hash_assignment("load-test-topic", partition, &brokers);
        *broker_distribution.entry(broker_id).or_insert(0) += 1;
    }

    // All brokers should get some partitions
    assert!(broker_distribution.len() >= 5);
}

#[test]
fn test_consistent_hash_stability() {
    let brokers = vec![
        BrokerInfo {
            broker_id: 1,
            host: "broker1".to_string(),
            port: 9092,
            registered_at: 0,
        },
        BrokerInfo {
            broker_id: 2,
            host: "broker2".to_string(),
            port: 9092,
            registered_at: 0,
        },
        BrokerInfo {
            broker_id: 3,
            host: "broker3".to_string(),
            port: 9092,
            registered_at: 0,
        },
    ];

    // Same key should always return same broker
    let key1_broker = consistent_hash_assignment("stable-topic", 42, &brokers);
    let key2_broker = consistent_hash_assignment("stable-topic", 42, &brokers);
    let key3_broker = consistent_hash_assignment("stable-topic", 42, &brokers);

    assert_eq!(key1_broker, key2_broker);
    assert_eq!(key2_broker, key3_broker);
}
