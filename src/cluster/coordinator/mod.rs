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
use std::sync::RwLock;

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

/// A pre-built consistent-hash ring over a fixed broker set.
///
/// Callers that perform many lookups against the same broker set
/// (e.g. iterating every partition on every topic) should build one
/// ring up-front and reuse it across lookups. Each call to
/// [`consistent_hash_assignment`] otherwise rebuilds the ring with
/// `VIRTUAL_NODES_PER_BROKER` virtual nodes per broker, which is
/// O(P*N*V) work for a single scan.
pub struct BrokerRing {
    ring: conhash::ConsistentHash<BrokerInfo>,
    fallback: i32,
}

impl BrokerRing {
    pub fn from_brokers(brokers: &[BrokerInfo]) -> Option<Self> {
        use crate::constants::VIRTUAL_NODES_PER_BROKER;
        use conhash::ConsistentHash;

        if brokers.is_empty() {
            return None;
        }
        let fallback = brokers[0].broker_id;
        let mut ring: ConsistentHash<BrokerInfo> = ConsistentHash::new();
        for broker in brokers {
            ring.add(broker, VIRTUAL_NODES_PER_BROKER);
        }
        Some(Self { ring, fallback })
    }

    pub fn assign(&self, topic: &str, partition: i32) -> i32 {
        let key = format!("{}:{}", topic, partition);
        match self.ring.get_str(&key) {
            Some(broker) => broker.broker_id,
            None => self.fallback,
        }
    }
}

/// Fingerprint of a broker set used to detect ring-relevant changes
/// without holding references to the brokers themselves. Sorted broker
/// IDs and host:port pairs are the only inputs to ring construction
/// (registered_at is metadata).
#[derive(Default)]
struct CachedRing {
    fingerprint: Vec<u8>,
    ring: Option<std::sync::Arc<BrokerRing>>,
}

static RING_CACHE: once_cell::sync::Lazy<RwLock<CachedRing>> =
    once_cell::sync::Lazy::new(|| RwLock::new(CachedRing::default()));

fn fingerprint_of(brokers: &[BrokerInfo]) -> Vec<u8> {
    let mut ids: Vec<(i32, &str, i32)> = brokers
        .iter()
        .map(|b| (b.broker_id, b.host.as_str(), b.port))
        .collect();
    ids.sort_unstable();
    let mut out = Vec::with_capacity(ids.len() * 16);
    for (id, host, port) in ids {
        out.extend_from_slice(&id.to_be_bytes());
        out.extend_from_slice(host.as_bytes());
        out.push(0);
        out.extend_from_slice(&port.to_be_bytes());
    }
    out
}

fn cached_ring(brokers: &[BrokerInfo]) -> Option<std::sync::Arc<BrokerRing>> {
    let fp = fingerprint_of(brokers);
    {
        let guard = RING_CACHE.read().unwrap_or_else(|e| e.into_inner());
        if guard.fingerprint == fp
            && let Some(ring) = guard.ring.as_ref()
        {
            return Some(std::sync::Arc::clone(ring));
        }
    }
    let new_ring = std::sync::Arc::new(BrokerRing::from_brokers(brokers)?);
    let mut guard = RING_CACHE.write().unwrap_or_else(|e| e.into_inner());
    guard.fingerprint = fp;
    guard.ring = Some(std::sync::Arc::clone(&new_ring));
    Some(new_ring)
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
///
/// The ring is cached behind a fingerprint of the broker set, so repeated
/// calls with an unchanged broker list don't rebuild it. For tight loops
/// (e.g. iterating every partition), prefer [`BrokerRing::from_brokers`]
/// once, then [`BrokerRing::assign`] per lookup, to skip the cache lookup
/// entirely.
pub fn consistent_hash_assignment(topic: &str, partition: i32, brokers: &[BrokerInfo]) -> i32 {
    if brokers.is_empty() {
        return -1; // No brokers available
    }

    if brokers.len() == 1 {
        return brokers[0].broker_id; // Only one broker
    }

    match cached_ring(brokers) {
        Some(ring) => ring.assign(topic, partition),
        None => -1,
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
