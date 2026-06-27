//! OffsetForLeaderEpoch (key 23) response encoding.
//!
//! Wire layout (Kafka protocol spec, OffsetForLeaderEpochResponse;
//! flexible from v4 — not yet emitted here):
//!
//! - v0:
//!   `topics: ARRAY of (topic: STRING, partitions: ARRAY of (
//!       error_code: INT16, partition: INT32, end_offset: INT64))`
//! - v1+: each partition adds `leader_epoch: INT32` (default `-1`)
//!   between `partition` and `end_offset`.
//! - v2+: `throttle_time_ms: INT32` prepended at the top.
//! - v3: identical to v2 on the response side (the v3 difference is in
//!   the request, which adds `replica_id`).

use bytes::BufMut;

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

#[derive(Debug, Clone, Default)]
pub struct OffsetForLeaderEpochResponseData {
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetForLeaderEpochTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetForLeaderEpochPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochPartitionResponse {
    pub error_code: KafkaCode,
    pub partition_index: i32,
    /// The leader epoch the broker reports for this partition. Always
    /// emitted at v1+, ignored at v0. `-1` is the conventional "not
    /// known" sentinel.
    pub leader_epoch: i32,
    /// The largest offset for the requested epoch. We always return our
    /// current log-end offset (object-store data plane never truncates
    /// past it), so the consumer's "is my fetch offset still valid?"
    /// check passes whenever the broker still owns the partition.
    pub end_offset: i64,
}

impl OffsetForLeaderEpochPartitionResponse {
    pub fn error(partition_index: i32, error_code: KafkaCode) -> Self {
        Self {
            error_code,
            partition_index,
            leader_epoch: -1,
            end_offset: -1,
        }
    }

    pub fn success(partition_index: i32, leader_epoch: i32, end_offset: i64) -> Self {
        Self {
            error_code: KafkaCode::None,
            partition_index,
            leader_epoch,
            end_offset,
        }
    }
}

impl OffsetForLeaderEpochResponseData {
    /// Encode the body for the given request version.
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        // throttle_time_ms is v2+; v0/v1 omit it.
        if version >= 2 {
            self.throttle_time_ms.encode(buffer)?;
        }
        crate::encode::encode_as_array(buffer, &self.topics, |buf, t| {
            t.name.encode(buf)?;
            crate::encode::encode_as_array(buf, &t.partitions, |buf, p| {
                (p.error_code as i16).encode(buf)?;
                p.partition_index.encode(buf)?;
                if version >= 1 {
                    p.leader_epoch.encode(buf)?;
                }
                p.end_offset.encode(buf)?;
                Ok(())
            })?;
            Ok(())
        })?;
        Ok(())
    }
}

impl ToByte for OffsetForLeaderEpochResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v3 (the highest classic version we advertise).
        self.encode_versioned(buffer, 3)
    }
}

impl ToByte for OffsetForLeaderEpochTopicResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        encode_array(buffer, &self.partitions)?;
        Ok(())
    }
}

impl ToByte for OffsetForLeaderEpochPartitionResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v3 layout. Version-aware call sites use
        // `OffsetForLeaderEpochResponseData::encode_versioned` and never
        // hit this path.
        (self.error_code as i16).encode(buffer)?;
        self.partition_index.encode(buffer)?;
        self.leader_epoch.encode(buffer)?;
        self.end_offset.encode(buffer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BytesMut};

    #[test]
    fn v0_omits_leader_epoch_and_throttle() {
        let resp = OffsetForLeaderEpochResponseData {
            throttle_time_ms: 100,
            topics: vec![OffsetForLeaderEpochTopicResponse {
                name: "t".into(),
                partitions: vec![OffsetForLeaderEpochPartitionResponse::success(0, 7, 42)],
            }],
        };
        let mut buf = BytesMut::new();
        resp.encode_versioned(&mut buf, 0).expect("encode");
        let mut bytes = buf.freeze();
        // v0: NO throttle prefix. Goes straight to topics array.
        assert_eq!(bytes.get_i32(), 1, "topics array len");
        assert_eq!(bytes.get_i16(), 1); // name len
        bytes.advance(1); // "t"
        assert_eq!(bytes.get_i32(), 1, "partitions array len");
        assert_eq!(bytes.get_i16(), 0, "error_code = None");
        assert_eq!(bytes.get_i32(), 0, "partition_index");
        // v0 has NO leader_epoch — next 8 bytes ARE end_offset.
        assert_eq!(bytes.get_i64(), 42);
        assert!(!bytes.has_remaining());
    }

    #[test]
    fn v1_emits_leader_epoch_but_no_throttle() {
        let resp = OffsetForLeaderEpochResponseData {
            throttle_time_ms: 0,
            topics: vec![OffsetForLeaderEpochTopicResponse {
                name: "t".into(),
                partitions: vec![OffsetForLeaderEpochPartitionResponse::success(3, 5, 99)],
            }],
        };
        let mut buf = BytesMut::new();
        resp.encode_versioned(&mut buf, 1).expect("encode");
        let mut bytes = buf.freeze();
        bytes.advance(
            4 /*topics len*/ + 2 + 1 /*name*/ + 4, /*partitions len*/
        );
        assert_eq!(bytes.get_i16(), 0, "error_code");
        assert_eq!(bytes.get_i32(), 3, "partition_index");
        assert_eq!(bytes.get_i32(), 5, "leader_epoch");
        assert_eq!(bytes.get_i64(), 99, "end_offset");
        assert!(!bytes.has_remaining());
    }

    #[test]
    fn v2_prefixes_throttle_time() {
        let resp = OffsetForLeaderEpochResponseData {
            throttle_time_ms: 250,
            topics: vec![],
        };
        let mut buf = BytesMut::new();
        resp.encode_versioned(&mut buf, 2).expect("encode");
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i32(), 250, "v2 leads with throttle_time_ms");
        assert_eq!(bytes.get_i32(), 0, "empty topics array");
        assert!(!bytes.has_remaining());
    }

    #[test]
    fn error_partition_round_trips_sentinels() {
        let resp =
            OffsetForLeaderEpochPartitionResponse::error(5, KafkaCode::NotLeaderForPartition);
        assert_eq!(resp.partition_index, 5);
        assert_eq!(resp.error_code, KafkaCode::NotLeaderForPartition);
        assert_eq!(resp.leader_epoch, -1);
        assert_eq!(resp.end_offset, -1);
    }

    #[test]
    fn v3_matches_v2_on_response_side() {
        // v3 differs only on the REQUEST (replica_id prefix). The
        // response wire shape is identical to v2.
        let resp = OffsetForLeaderEpochResponseData {
            throttle_time_ms: 5,
            topics: vec![OffsetForLeaderEpochTopicResponse {
                name: "x".into(),
                partitions: vec![OffsetForLeaderEpochPartitionResponse::success(0, 1, 2)],
            }],
        };
        let mut v2 = BytesMut::new();
        let mut v3 = BytesMut::new();
        resp.encode_versioned(&mut v2, 2).unwrap();
        resp.encode_versioned(&mut v3, 3).unwrap();
        assert_eq!(v2.as_ref(), v3.as_ref());
    }
}
