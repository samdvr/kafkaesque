//! Metadata response encoding.
//!
//! Supports v0..=v9. v9+ uses the KIP-482 flexible wire format (compact
//! arrays / strings, trailing tagged fields per record). Per-version
//! deltas:
//!
//! - v1+ adds `controller_id`, broker `rack`, and topic `is_internal`
//! - v2+ adds top-level `cluster_id` (nullable string)
//! - v3+ adds `throttle_time_ms` at the head of the response
//! - v5+ adds per-partition `offline_replicas`
//! - v7+ adds per-partition `leader_epoch`
//! - v8+ adds `cluster_authorized_operations` and per-topic
//!   `topic_authorized_operations`
//! - v9+ uses flexible encoding (response header v1: trailing tagged
//!   fields after the correlation id)

use bytes::BufMut;

use crate::encode::{
    ToByte, encode_array, encode_compact_array, encode_empty_tagged_fields, encode_unsigned_varint,
};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

/// Metadata response data.
#[derive(Debug, Clone, Default)]
pub struct MetadataResponseData {
    /// v3+ only. Throttle window in ms.
    pub throttle_time_ms: i32,
    pub brokers: Vec<BrokerData>,
    /// v2+ only. `None` until v9 in flexible encoding.
    pub cluster_id: Option<String>,
    /// v1+ only. `-1` if no controller is currently elected.
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
    /// v8..=v10 only. Bitmask of cluster-level ACL operations the
    /// principal may perform. `0` if not requested.
    pub cluster_authorized_operations: i32,
}

#[derive(Debug, Clone)]
pub struct BrokerData {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    /// v1+ only.
    pub rack: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TopicMetadata {
    pub error_code: KafkaCode,
    pub name: String,
    /// v1+ only.
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
    /// v8+ only. Bitmask of topic-level ACL operations.
    pub topic_authorized_operations: i32,
}

#[derive(Debug, Clone, Default)]
pub struct PartitionMetadata {
    pub error_code: KafkaCode,
    pub partition_index: i32,
    pub leader_id: i32,
    /// v7+ only.
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    /// v5+ only.
    pub offline_replicas: Vec<i32>,
}

impl MetadataResponseData {
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        let flexible = version >= 9;

        if version >= 3 {
            self.throttle_time_ms.encode(buffer)?;
        }

        // brokers
        if flexible {
            encode_compact_array(buffer, &self.brokers, |buf, b| {
                encode_broker_versioned(buf, b, version, true)
            })?;
        } else {
            crate::encode::encode_array_len_pub(buffer, self.brokers.len())?;
            for b in &self.brokers {
                encode_broker_versioned(buffer, b, version, false)?;
            }
        }

        if version >= 2 {
            if flexible {
                encode_compact_nullable_string(buffer, self.cluster_id.as_deref());
            } else {
                encode_nullable_string(self.cluster_id.as_deref(), buffer)?;
            }
        }
        if version >= 1 {
            self.controller_id.encode(buffer)?;
        }

        // topics
        if flexible {
            encode_compact_array(buffer, &self.topics, |buf, t| {
                encode_topic_versioned(buf, t, version, true)
            })?;
        } else {
            crate::encode::encode_array_len_pub(buffer, self.topics.len())?;
            for t in &self.topics {
                encode_topic_versioned(buffer, t, version, false)?;
            }
        }

        if (8..=10).contains(&version) {
            self.cluster_authorized_operations.encode(buffer)?;
        }

        if flexible {
            encode_empty_tagged_fields(buffer);
        }
        Ok(())
    }
}

fn encode_broker_versioned<W: BufMut>(
    buffer: &mut W,
    b: &BrokerData,
    version: i16,
    flexible: bool,
) -> Result<()> {
    b.node_id.encode(buffer)?;
    if flexible {
        encode_compact_string(buffer, &b.host);
    } else {
        b.host.encode(buffer)?;
    }
    b.port.encode(buffer)?;
    if version >= 1 {
        if flexible {
            encode_compact_nullable_string(buffer, b.rack.as_deref());
        } else {
            encode_nullable_string(b.rack.as_deref(), buffer)?;
        }
    }
    if flexible {
        encode_empty_tagged_fields(buffer);
    }
    Ok(())
}

fn encode_topic_versioned<W: BufMut>(
    buffer: &mut W,
    t: &TopicMetadata,
    version: i16,
    flexible: bool,
) -> Result<()> {
    (t.error_code as i16).encode(buffer)?;
    if flexible {
        // v9 still uses compact_string (non-nullable) for the name; v12+
        // makes it nullable. We target up to v9 so non-nullable is correct.
        encode_compact_string(buffer, &t.name);
    } else {
        t.name.encode(buffer)?;
    }
    if version >= 1 {
        (t.is_internal as i8).encode(buffer)?;
    }

    if flexible {
        encode_compact_array(buffer, &t.partitions, |buf, p| {
            encode_partition_versioned(buf, p, version, true)
        })?;
    } else {
        crate::encode::encode_array_len_pub(buffer, t.partitions.len())?;
        for p in &t.partitions {
            encode_partition_versioned(buffer, p, version, false)?;
        }
    }
    if version >= 8 {
        t.topic_authorized_operations.encode(buffer)?;
    }
    if flexible {
        encode_empty_tagged_fields(buffer);
    }
    Ok(())
}

fn encode_partition_versioned<W: BufMut>(
    buffer: &mut W,
    p: &PartitionMetadata,
    version: i16,
    flexible: bool,
) -> Result<()> {
    (p.error_code as i16).encode(buffer)?;
    p.partition_index.encode(buffer)?;
    p.leader_id.encode(buffer)?;
    if version >= 7 {
        p.leader_epoch.encode(buffer)?;
    }

    if flexible {
        encode_compact_array(buffer, &p.replica_nodes, |buf, n| {
            n.encode(buf)?;
            Ok(())
        })?;
        encode_compact_array(buffer, &p.isr_nodes, |buf, n| {
            n.encode(buf)?;
            Ok(())
        })?;
    } else {
        encode_array(buffer, &p.replica_nodes)?;
        encode_array(buffer, &p.isr_nodes)?;
    }

    if version >= 5 {
        if flexible {
            encode_compact_array(buffer, &p.offline_replicas, |buf, n| {
                n.encode(buf)?;
                Ok(())
            })?;
        } else {
            encode_array(buffer, &p.offline_replicas)?;
        }
    }

    if flexible {
        encode_empty_tagged_fields(buffer);
    }
    Ok(())
}

/// Compact non-nullable string: varint(len + 1) then bytes.
fn encode_compact_string<W: BufMut>(buffer: &mut W, s: &str) {
    encode_unsigned_varint(buffer, (s.len() + 1) as u32);
    buffer.put_slice(s.as_bytes());
}

/// Compact nullable string: varint(0) for null, otherwise varint(len + 1) + bytes.
fn encode_compact_nullable_string<W: BufMut>(buffer: &mut W, s: Option<&str>) {
    match s {
        None => encode_unsigned_varint(buffer, 0),
        Some(value) => {
            encode_unsigned_varint(buffer, (value.len() + 1) as u32);
            buffer.put_slice(value.as_bytes());
        }
    }
}

impl ToByte for MetadataResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v1 framing — preserves prior behavior for callers
        // that haven't been updated to use `encode_versioned`.
        self.encode_versioned(buffer, 1)
    }
}

impl ToByte for BrokerData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Classic v1 default: includes rack as nullable string.
        encode_broker_versioned(buffer, self, 1, false)
    }
}

impl ToByte for TopicMetadata {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        encode_topic_versioned(buffer, self, 1, false)
    }
}

impl ToByte for PartitionMetadata {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        encode_partition_versioned(buffer, self, 1, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_response_encode() {
        let response = MetadataResponseData {
            throttle_time_ms: 0,
            brokers: vec![],
            cluster_id: None,
            controller_id: 0,
            topics: vec![],
            cluster_authorized_operations: 0,
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_broker_data_encode() {
        let broker = BrokerData {
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            rack: Some("rack1".to_string()),
        };

        let mut buffer = Vec::new();
        broker.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_broker_data_encode_no_rack() {
        let broker = BrokerData {
            node_id: 0,
            host: "kafka.local".to_string(),
            port: 9093,
            rack: None,
        };

        let mut buffer = Vec::new();
        broker.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_topic_metadata_encode() {
        let topic = TopicMetadata {
            error_code: KafkaCode::None,
            name: "test-topic".to_string(),
            is_internal: false,
            partitions: vec![],
            topic_authorized_operations: 0,
        };

        let mut buffer = Vec::new();
        topic.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_topic_metadata_encode_internal() {
        let topic = TopicMetadata {
            error_code: KafkaCode::None,
            name: "__consumer_offsets".to_string(),
            is_internal: true,
            partitions: vec![],
            topic_authorized_operations: 0,
        };

        let mut buffer = Vec::new();
        topic.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_partition_metadata_encode() {
        let partition = PartitionMetadata {
            error_code: KafkaCode::None,
            partition_index: 0,
            leader_id: 1,
            leader_epoch: -1,
            replica_nodes: vec![1, 2, 3],
            isr_nodes: vec![1, 2],
            offline_replicas: vec![],
        };

        let mut buffer = Vec::new();
        partition.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    /// v0 omits `controller_id` (added v1+). Header should be brokers
    /// array immediately followed by topics array.
    #[test]
    fn v0_response_omits_controller_id() {
        let response = MetadataResponseData {
            throttle_time_ms: 0,
            brokers: vec![],
            cluster_id: None,
            controller_id: 5,
            topics: vec![],
            cluster_authorized_operations: 0,
        };
        let mut v0 = Vec::new();
        response.encode_versioned(&mut v0, 0).unwrap();
        let mut v1 = Vec::new();
        response.encode_versioned(&mut v1, 1).unwrap();
        // v0: brokers_len (4) + topics_len (4) = 8
        // v1: brokers_len (4) + controller_id (4) + topics_len (4) = 12
        assert_eq!(v0.len(), 8);
        assert_eq!(v1.len(), 12);
    }

    /// v3 prepends `throttle_time_ms`.
    #[test]
    fn v3_adds_throttle_time_ms() {
        let response = MetadataResponseData {
            throttle_time_ms: 0xCAFE_BABEu32 as i32,
            brokers: vec![],
            cluster_id: None,
            controller_id: 0,
            topics: vec![],
            cluster_authorized_operations: 0,
        };
        let mut v2 = Vec::new();
        response.encode_versioned(&mut v2, 2).unwrap();
        let mut v3 = Vec::new();
        response.encode_versioned(&mut v3, 3).unwrap();
        assert_eq!(v2.len() + 4, v3.len());
        assert_eq!(
            i32::from_be_bytes([v3[0], v3[1], v3[2], v3[3]]),
            0xCAFE_BABEu32 as i32
        );
    }

    /// v2 adds `cluster_id` (nullable string) between brokers and
    /// controller_id. Confirm encoded position and value.
    #[test]
    fn v2_emits_cluster_id_after_brokers() {
        let response = MetadataResponseData {
            throttle_time_ms: 0,
            brokers: vec![],
            cluster_id: Some("kafkaesque-cluster".to_string()),
            controller_id: 1,
            topics: vec![],
            cluster_authorized_operations: 0,
        };
        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 2).unwrap();
        // brokers_len (4) + cluster_id_len (2) + "kafkaesque-cluster" (18)
        // + controller_id (4) + topics_len (4) = 32
        assert_eq!(buf.len(), 32);
        // brokers_len at [0..4] = 0
        assert_eq!(&buf[0..4], &[0, 0, 0, 0]);
        // cluster_id length prefix (i16) at [4..6] = 18
        assert_eq!(i16::from_be_bytes([buf[4], buf[5]]), 18);
        // cluster_id payload starts at byte 6
        assert_eq!(&buf[6..24], b"kafkaesque-cluster");
    }

    /// v9 round-trip via flexible encoding: compact arrays + strings,
    /// trailing tagged fields. Covers brokers, topics, partitions, and
    /// the top-level body trailer.
    #[test]
    fn v9_flexible_round_trip_shape() {
        let response = MetadataResponseData {
            throttle_time_ms: 0,
            brokers: vec![BrokerData {
                node_id: 1,
                host: "h".to_string(),
                port: 9092,
                rack: None,
            }],
            cluster_id: Some("c".to_string()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: KafkaCode::None,
                name: "t".to_string(),
                is_internal: false,
                partitions: vec![PartitionMetadata {
                    error_code: KafkaCode::None,
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 7,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    offline_replicas: vec![],
                }],
                topic_authorized_operations: 0,
            }],
            cluster_authorized_operations: 0,
        };
        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 9).unwrap();

        // Manual layout walk-through (one broker with host="h",
        // cluster_id="c", one topic "t" with one partition having
        // 1 replica + 1 ISR):
        //   throttle (4) + brokers_array_len (1) +
        //   { node_id (4) + host_len (1) + host (1) + port (4)
        //     + rack_null (1) + tagged (1) = 12 }
        //   + cluster_id_len (1) + cluster_id (1)
        //   + controller_id (4) + topics_array_len (1)
        //   + { error_code (2) + name_len (1) + name (1) + is_internal (1)
        //       + partitions_array_len (1)
        //       + { error_code (2) + partition_index (4) + leader_id (4)
        //           + leader_epoch (4) + replicas_len (1) + replicas (4)
        //           + isr_len (1) + isr (4) + offline_len (1) + tagged (1) = 26 }
        //       + topic_authorized_operations (4) + tagged (1) = 37 }
        //   + cluster_authorized_operations (4) + body tagged (1)
        // = 4 + 1 + 12 + 1 + 1 + 4 + 1 + 37 + 4 + 1 = 66
        assert_eq!(buf.len(), 66, "v9 layout byte count off");

        // Spot-check: throttle, then 0x02 broker compact array.
        assert_eq!(&buf[0..4], &[0, 0, 0, 0]);
        assert_eq!(buf[4], 0x02);
    }

    /// v9 must terminate with the body's tagged-fields byte.
    #[test]
    fn v9_response_ends_with_tagged_fields_byte() {
        let response = MetadataResponseData::default();
        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 9).unwrap();
        assert_eq!(*buf.last().unwrap(), 0u8);
    }
}
