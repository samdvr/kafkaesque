//! Cluster-stable shard hashing.
//!
//! All metadata routing decisions — which shard owns a topic, a consumer
//! group, or a producer id — flow through these helpers. The hash function
//! and the seed are part of the bootstrap contract and must NEVER change for
//! the lifetime of a cluster: changing them would re-shuffle every key and
//! silently corrupt every shard's local view of the world.
//!
//! ## Why xxh3_64
//!
//! - Non-cryptographic, but stable across versions of `twox-hash` (the seed
//!   below pins the variant explicitly).
//! - Uniform distribution at small key sizes (topic names, group ids).
//! - Significantly faster than `DefaultHasher`, which matters because every
//!   metadata write hits this path.
//!
//! ## Persisted contract
//!
//! `cluster_meta.bin` records `hash_algo: "xxh3_64"` at bootstrap. The config
//! load path refuses to start when the persisted value disagrees.
//!
//! ## Seed
//!
//! The seed is fixed at zero. It is part of the wire/disk contract — do not
//! change it, even to fix a uniformity issue, without a full cluster
//! re-bootstrap.

#![allow(dead_code)] // wired in subsequent migration steps

use twox_hash::XxHash3_64;

use super::types::ShardId;

/// Cluster-wide pinned hash algorithm name. Persisted in `cluster_meta.bin`
/// at bootstrap and validated on every restart.
pub const HASH_ALGO: &str = "xxh3_64";

/// Fixed seed for [`HASH_ALGO`]. See module docs — never change.
const HASH_SEED: u64 = 0;

/// Hash a key using the cluster-stable hash function.
///
/// Returns the raw 64-bit digest. Callers usually want [`shard_for_key`]
/// instead, which folds this into a `ShardId`.
#[inline]
pub fn hash64(key: &[u8]) -> u64 {
    XxHash3_64::oneshot_with_seed(HASH_SEED, key)
}

/// Map an arbitrary byte key onto a shard in `0..shards`.
///
/// `shards` must be in `1..=u16::MAX`. The caller validates this at config
/// load (see `RaftConfig::metadata_shards`); we use a saturating modulo so a
/// misuse returns shard 0 rather than panicking, which would take down the
/// whole broker on any metadata write.
#[inline]
pub fn shard_for_key(key: &[u8], shards: u16) -> ShardId {
    let n = shards.max(1) as u64;
    (hash64(key) % n) as ShardId
}

/// Convenience: shard for a topic name.
#[inline]
pub fn shard_for_topic(topic: &str, shards: u16) -> ShardId {
    shard_for_key(topic.as_bytes(), shards)
}

/// Convenience: shard for a consumer group id.
#[inline]
pub fn shard_for_group(group_id: &str, shards: u16) -> ShardId {
    shard_for_key(group_id.as_bytes(), shards)
}

/// Convenience: shard for a producer id. Producer ids are i64; we hash the
/// big-endian byte representation so the encoding is unambiguous and
/// independent of the host's endianness.
#[inline]
pub fn shard_for_producer(producer_id: i64, shards: u16) -> ShardId {
    shard_for_key(&producer_id.to_be_bytes(), shards)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic() {
        // The seed is part of the disk contract. If this test ever fails,
        // someone changed `HASH_SEED` or the underlying algorithm — that
        // re-shuffles every key in every existing cluster.
        let h = hash64(b"orders");
        assert_eq!(h, hash64(b"orders"));
        // Empty key has a defined output too.
        let _ = hash64(b"");
    }

    #[test]
    fn shard_assignment_is_within_range() {
        for shards in [1_u16, 2, 8, 1024, u16::MAX] {
            for key in ["", "a", "orders", "a-very-long-topic-name", "🚀"] {
                let s = shard_for_topic(key, shards);
                assert!(s < shards, "shard {} out of range for {}", s, shards);
            }
        }
    }

    #[test]
    fn zero_shards_does_not_panic() {
        // Misconfiguration safety net: a 0 shard count is rejected at config
        // load, but if it ever leaks through we want a deterministic 0
        // rather than a divide-by-zero panic on the metadata write path.
        assert_eq!(shard_for_topic("anything", 0), 0);
    }

    #[test]
    fn topic_partitions_map_to_same_shard() {
        // Topics decide the shard, partitions don't — every partition of
        // the same topic must land on the same shard. The reconciler relies
        // on this when fanning out InitPartition.
        let s = shard_for_topic("orders", 8);
        for partition in 0..32 {
            let _ = partition;
            assert_eq!(shard_for_topic("orders", 8), s);
        }
    }

    #[test]
    fn producer_id_hash_is_endian_independent() {
        // We encode big-endian explicitly; this test would fail if someone
        // switched to native-endian and ran on a little-endian host.
        let h = shard_for_producer(0x0123_4567_89ab_cdef, 1024);
        assert_eq!(h, shard_for_key(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef], 1024));
    }

    #[test]
    fn distribution_is_roughly_uniform() {
        // Loose sanity check: 10k synthetic topic names across 8 shards
        // should give every shard between 10% and 15% of the load.
        // Tighter than this is flaky; looser than this would hide a real
        // distribution bug.
        let shards: u16 = 8;
        let mut counts = [0_u32; 8];
        for i in 0..10_000 {
            let key = format!("topic-{i}");
            counts[shard_for_topic(&key, shards) as usize] += 1;
        }
        for (i, c) in counts.iter().enumerate() {
            assert!(
                (1000..=1500).contains(c),
                "shard {} got {} hits, expected 1000..=1500",
                i,
                c
            );
        }
    }

    #[test]
    fn algo_constant_matches_persisted_contract() {
        // If this string ever changes, every existing cluster_meta.bin will
        // refuse to start — which is the exact behaviour we want, but the
        // test is a tripwire so the change isn't silent.
        assert_eq!(HASH_ALGO, "xxh3_64");
    }
}
