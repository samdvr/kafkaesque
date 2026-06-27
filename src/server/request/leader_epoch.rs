//! OffsetForLeaderEpoch (key 23) request parsing.
//!
//! Wire format (Kafka protocol spec, OffsetForLeaderEpochRequest;
//! flexible from v4 — not yet supported here):
//!
//! - v0 body:
//!   `topics: ARRAY of (topic: STRING, partitions: ARRAY of (partition: INT32, leader_epoch: INT32))`
//! - v1+: same shape (response gains `leader_epoch`).
//! - v2+: `current_leader_epoch: INT32` (default -1) prepended to each
//!   partition; client tells us "I think I'm at this epoch", broker can
//!   return FENCED_LEADER_EPOCH if it disagrees.
//! - v3+: `replica_id: INT32` (default -2) prefixes the topics array.
//! - v4 (flexible): not advertised yet.
//!
//! Modern consumers (KIP-320) send this on every rebalance to validate
//! that the broker hasn't truncated past their fetch offset. Clients
//! that don't get a recognized response degrade to "trust the broker"
//! and log warnings.

use nom::{IResult, number::complete::be_i32};
use nombytes::NomBytes;

use crate::parser::{parse_array, parse_kafka_string};

/// OffsetForLeaderEpoch request data.
#[derive(Debug, Clone, Default)]
pub struct OffsetForLeaderEpochRequestData {
    /// Replica id (v3+). `-2` is the conventional "consumer/admin" sentinel
    /// used by every non-replica client. Defaulted at v0–v2 since the
    /// field is absent on the wire.
    pub replica_id: i32,
    pub topics: Vec<OffsetForLeaderEpochTopicData>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochTopicData {
    pub name: String,
    pub partitions: Vec<OffsetForLeaderEpochPartitionData>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochPartitionData {
    pub partition_index: i32,
    /// Client's view of the current leader epoch (v2+, default `-1`).
    /// `-1` means the client hasn't seen an epoch yet, so don't fence.
    pub current_leader_epoch: i32,
    /// Epoch the client wants the end-offset for.
    pub leader_epoch: i32,
}

pub fn parse_offset_for_leader_epoch_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, OffsetForLeaderEpochRequestData> {
    // replica_id: v3+ only; defaults to -2 (consumer client) so v0-v2
    // requests carry the same "not a replica" semantics through the rest
    // of the broker.
    let (s, replica_id) = if version >= 3 { be_i32(s)? } else { (s, -2i32) };
    let (s, topics) = parse_array(|s| parse_topic(s, version))(s)?;

    Ok((s, OffsetForLeaderEpochRequestData { replica_id, topics }))
}

fn parse_topic(s: NomBytes, version: i16) -> IResult<NomBytes, OffsetForLeaderEpochTopicData> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, partitions) = parse_array(|s| parse_partition(s, version))(s)?;
    Ok((s, OffsetForLeaderEpochTopicData { name, partitions }))
}

fn parse_partition(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, OffsetForLeaderEpochPartitionData> {
    let (s, partition_index) = be_i32(s)?;
    // current_leader_epoch (KIP-320) added in v2; default to `-1` so older
    // requests are read as "client has no epoch yet" rather than fenced
    // against an arbitrary value.
    let (s, current_leader_epoch) = if version >= 2 { be_i32(s)? } else { (s, -1i32) };
    let (s, leader_epoch) = be_i32(s)?;
    Ok((
        s,
        OffsetForLeaderEpochPartitionData {
            partition_index,
            current_leader_epoch,
            leader_epoch,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn nb(data: &[u8]) -> NomBytes {
        NomBytes::new(Bytes::copy_from_slice(data))
    }

    #[test]
    fn v0_one_partition_no_current_epoch_no_replica_id() {
        let mut data = Vec::new();
        // 1 topic
        data.extend_from_slice(&1i32.to_be_bytes());
        // name = "t"
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(b"t");
        // 1 partition
        data.extend_from_slice(&1i32.to_be_bytes());
        // partition = 0
        data.extend_from_slice(&0i32.to_be_bytes());
        // leader_epoch = 7
        data.extend_from_slice(&7i32.to_be_bytes());

        let (rest, parsed) = parse_offset_for_leader_epoch_request(nb(&data), 0).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(parsed.replica_id, -2, "v0 has no replica_id; default -2");
        assert_eq!(parsed.topics.len(), 1);
        assert_eq!(parsed.topics[0].name, "t");
        let p = &parsed.topics[0].partitions[0];
        assert_eq!(p.partition_index, 0);
        assert_eq!(p.current_leader_epoch, -1, "v0 has no current_leader_epoch");
        assert_eq!(p.leader_epoch, 7);
    }

    #[test]
    fn v2_consumes_current_leader_epoch() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(b"t");
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 partition
        data.extend_from_slice(&3i32.to_be_bytes()); // partition = 3
        data.extend_from_slice(&5i32.to_be_bytes()); // current_leader_epoch
        data.extend_from_slice(&4i32.to_be_bytes()); // leader_epoch

        let (rest, parsed) = parse_offset_for_leader_epoch_request(nb(&data), 2).unwrap();
        assert!(rest.into_bytes().is_empty(), "v2 must consume body fully");
        let p = &parsed.topics[0].partitions[0];
        assert_eq!(p.partition_index, 3);
        assert_eq!(p.current_leader_epoch, 5);
        assert_eq!(p.leader_epoch, 4);
    }

    #[test]
    fn v3_consumes_replica_id_prefix() {
        let mut data = Vec::new();
        data.extend_from_slice(&(-2i32).to_be_bytes()); // replica_id (consumer)
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(b"t");
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 partition
        data.extend_from_slice(&0i32.to_be_bytes()); // partition
        data.extend_from_slice(&(-1i32).to_be_bytes()); // current_leader_epoch
        data.extend_from_slice(&0i32.to_be_bytes()); // leader_epoch

        let (rest, parsed) = parse_offset_for_leader_epoch_request(nb(&data), 3).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(parsed.replica_id, -2);
        assert_eq!(parsed.topics.len(), 1);
    }

    #[test]
    fn empty_topics_array() {
        let mut data = Vec::new();
        data.extend_from_slice(&(-2i32).to_be_bytes()); // replica_id
        data.extend_from_slice(&0i32.to_be_bytes()); // 0 topics
        let (_, parsed) = parse_offset_for_leader_epoch_request(nb(&data), 3).unwrap();
        assert!(parsed.topics.is_empty());
    }

    #[test]
    fn multiple_partitions_per_topic() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
        data.extend_from_slice(&6i16.to_be_bytes());
        data.extend_from_slice(b"orders");
        data.extend_from_slice(&3i32.to_be_bytes()); // 3 partitions
        for i in 0i32..3 {
            data.extend_from_slice(&i.to_be_bytes()); // partition_index
            data.extend_from_slice(&(-1i32).to_be_bytes()); // current_leader_epoch
            data.extend_from_slice(&(i + 10).to_be_bytes()); // leader_epoch
        }
        let (_, parsed) = parse_offset_for_leader_epoch_request(nb(&data), 2).unwrap();
        assert_eq!(parsed.topics[0].partitions.len(), 3);
        assert_eq!(parsed.topics[0].partitions[1].partition_index, 1);
        assert_eq!(parsed.topics[0].partitions[1].leader_epoch, 11);
    }
}
