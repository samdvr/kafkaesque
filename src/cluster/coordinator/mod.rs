//! Coordination backends for cluster state.
//!
//! This module provides the coordination layer for cluster state management.
//!
//! ## Raft Coordinator (default)
//! Embedded Raft consensus with no external dependencies:
//! - Broker registration and heartbeat
//! - Partition ownership (acquire/renew/release)
//! - Topic metadata management
//! - Consumer group coordination
//! - Committed offset storage
//! - Producer ID allocation
//!
//! See [`crate::cluster::raft`] module for details.
//!
//! ## Design Philosophy
//!
//! This project uses Raft as the sole coordination mechanism because:
//! 1. **Strong consistency**: Raft provides linearizable reads/writes
//! 2. **No external dependencies**: Embedded consensus, no external services needed
//! 3. **Split-brain prevention**: Leader election prevents multiple writers
//! 4. **Proven correctness**: Raft is a well-studied consensus protocol

pub mod producer;

use conhash::Node;
use serde::{Deserialize, Serialize};

// Re-export validation functions for backwards compatibility
pub use super::validation::{validate_group_id, validate_member_metadata, validate_topic_name};

/// Information about a broker in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub registered_at: i64,
}

/// Implement Node trait for BrokerInfo to use with consistent hashing.
impl Node for BrokerInfo {
    fn name(&self) -> String {
        format!("broker-{}", self.broker_id)
    }
}

/// Compute which broker should own a partition using consistent hashing.
///
/// Uses the conhash crate to implement a proper consistent hash ring with
/// virtual nodes (replicas) for even distribution.
///
/// The algorithm:
/// 1. Build a consistent hash ring with all brokers (each with VIRTUAL_NODES_PER_BROKER replicas)
/// 2. Hash the partition key "{topic}:{partition}" to find the owning broker
///
/// This ensures:
/// - Deterministic assignment across all brokers
/// - Minimal partition movement when brokers join/leave (only ~1/n partitions move)
/// - Even distribution across brokers (due to virtual nodes)
pub fn consistent_hash_assignment(topic: &str, partition: i32, brokers: &[BrokerInfo]) -> i32 {
    use crate::constants::VIRTUAL_NODES_PER_BROKER;
    use conhash::ConsistentHash;

    if brokers.is_empty() {
        return -1; // No brokers available
    }

    if brokers.len() == 1 {
        return brokers[0].broker_id; // Only one broker
    }

    // Build the consistent hash ring
    let mut ring: ConsistentHash<BrokerInfo> = ConsistentHash::new();
    for broker in brokers {
        ring.add(broker, VIRTUAL_NODES_PER_BROKER);
    }

    // Create the partition key and look up the owning broker
    let partition_key = format!("{}:{}", topic, partition);
    match ring.get_str(&partition_key) {
        Some(broker) => broker.broker_id,
        None => brokers[0].broker_id, // Fallback (should never happen)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that consistent hashing with conhash produces consistent results.
    #[test]
    fn test_conhash_consistency() {
        let brokers = vec![
            BrokerInfo {
                broker_id: 0,
                host: "host0".to_string(),
                port: 9092,
                registered_at: 0,
            },
            BrokerInfo {
                broker_id: 1,
                host: "host1".to_string(),
                port: 9092,
                registered_at: 0,
            },
        ];

        // Same inputs should always produce same assignment
        let result1 = consistent_hash_assignment("my-topic", 0, &brokers);
        let result2 = consistent_hash_assignment("my-topic", 0, &brokers);
        assert_eq!(result1, result2);

        // Different partitions may produce different assignments
        let result3 = consistent_hash_assignment("my-topic", 1, &brokers);
        let result4 = consistent_hash_assignment("other-topic", 0, &brokers);

        // Results should be valid broker IDs
        assert!(result1 == 0 || result1 == 1);
        assert!(result3 == 0 || result3 == 1);
        assert!(result4 == 0 || result4 == 1);
    }

    /// Test that consistent hashing assignment is deterministic.
    #[test]
    fn test_consistent_hash_assignment_deterministic() {
        let brokers = vec![
            BrokerInfo {
                broker_id: 0,
                host: "host0".to_string(),
                port: 9092,
                registered_at: 0,
            },
            BrokerInfo {
                broker_id: 1,
                host: "host1".to_string(),
                port: 9092,
                registered_at: 0,
            },
            BrokerInfo {
                broker_id: 2,
                host: "host2".to_string(),
                port: 9092,
                registered_at: 0,
            },
        ];

        // Same topic/partition should always go to the same broker
        let assigned1 = consistent_hash_assignment("test", 0, &brokers);
        let assigned2 = consistent_hash_assignment("test", 0, &brokers);
        assert_eq!(assigned1, assigned2);
    }

    /// Test that consistent hashing distributes partitions evenly.
    #[test]
    fn test_consistent_hash_distribution() {
        let brokers = vec![
            BrokerInfo {
                broker_id: 0,
                host: "host0".to_string(),
                port: 9092,
                registered_at: 0,
            },
            BrokerInfo {
                broker_id: 1,
                host: "host1".to_string(),
                port: 9092,
                registered_at: 0,
            },
            BrokerInfo {
                broker_id: 2,
                host: "host2".to_string(),
                port: 9092,
                registered_at: 0,
            },
        ];

        let mut counts = std::collections::HashMap::new();

        // Assign 100 partitions across 3 brokers
        for partition in 0..100 {
            let broker = consistent_hash_assignment("test-topic", partition, &brokers);
            *counts.entry(broker).or_insert(0) += 1;
        }

        // Each broker should get roughly 1/3 of partitions (allow 20% deviation)
        for broker_id in [0, 1, 2] {
            let count = counts.get(&broker_id).copied().unwrap_or(0);
            assert!(
                (20..=50).contains(&count),
                "Broker {} got {} partitions, expected ~33",
                broker_id,
                count
            );
        }
    }
}
