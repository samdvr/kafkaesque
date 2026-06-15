//! Fetch request parsing.

use nom::{
    IResult,
    number::complete::{be_i8, be_i32, be_i64},
};
use nombytes::NomBytes;

use crate::parser::{parse_array, parse_kafka_string};

/// Fetch request data.
#[derive(Debug, Clone)]
pub struct FetchRequestData {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub topics: Vec<FetchTopicData>,
}

#[derive(Debug, Clone)]
pub struct FetchTopicData {
    pub name: String,
    pub partitions: Vec<FetchPartitionData>,
}

#[derive(Debug, Clone)]
pub struct FetchPartitionData {
    pub partition_index: i32,
    pub fetch_offset: i64,
    pub partition_max_bytes: i32,
}

pub fn parse_fetch_request(s: NomBytes, version: i16) -> IResult<NomBytes, FetchRequestData> {
    let (s, replica_id) = be_i32(s)?;
    let (s, max_wait_ms) = be_i32(s)?;
    let (s, min_bytes) = be_i32(s)?;
    // `max_bytes` was added in Fetch v3, `isolation_level` in v4. A
    // version-blind parser silently misaligns the byte stream once we ever
    // accept v0..v2, treating partition data as `max_bytes` and corrupting
    // every field downstream.
    let (s, max_bytes) = if version >= 3 {
        be_i32(s)?
    } else {
        (s, i32::MAX)
    };
    let (s, isolation_level) = if version >= 4 { be_i8(s)? } else { (s, 0) };
    let (s, topics) = parse_array(parse_fetch_topic)(s)?;

    Ok((
        s,
        FetchRequestData {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            topics,
        },
    ))
}

fn parse_fetch_topic(s: NomBytes) -> IResult<NomBytes, FetchTopicData> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, partitions) = parse_array(parse_fetch_partition)(s)?;

    Ok((s, FetchTopicData { name, partitions }))
}

fn parse_fetch_partition(s: NomBytes) -> IResult<NomBytes, FetchPartitionData> {
    let (s, partition_index) = be_i32(s)?;
    let (s, fetch_offset) = be_i64(s)?;
    let (s, partition_max_bytes) = be_i32(s)?;

    Ok((
        s,
        FetchPartitionData {
            partition_index,
            fetch_offset,
            partition_max_bytes,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, Bytes, BytesMut};

    type TestPartition = (i32, i64, i32);
    type TestTopic<'a> = (&'a str, &'a [TestPartition]);

    fn put_kafka_string(buf: &mut BytesMut, s: &str) {
        buf.put_i16(s.len() as i16);
        buf.put_slice(s.as_bytes());
    }

    fn build_fetch_v4(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        topics: &[TestTopic<'_>],
    ) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i32(replica_id);
        buf.put_i32(max_wait_ms);
        buf.put_i32(min_bytes);
        buf.put_i32(max_bytes);
        buf.put_i8(isolation_level);
        buf.put_i32(topics.len() as i32);
        for (name, partitions) in topics {
            put_kafka_string(&mut buf, name);
            buf.put_i32(partitions.len() as i32);
            for (idx, off, max) in *partitions {
                buf.put_i32(*idx);
                buf.put_i64(*off);
                buf.put_i32(*max);
            }
        }
        buf.freeze()
    }

    #[test]
    fn valid_fetch_v4_decodes() {
        let bytes = build_fetch_v4(-1, 500, 1, 1_048_576, 0, &[("t", &[(0, 100, 65_536)])]);
        let (rest, req) = parse_fetch_request(NomBytes::new(bytes), 4).expect("valid v4");
        assert!(rest.to_bytes().is_empty());
        assert_eq!(req.replica_id, -1);
        assert_eq!(req.max_wait_ms, 500);
        assert_eq!(req.min_bytes, 1);
        assert_eq!(req.max_bytes, 1_048_576);
        assert_eq!(req.isolation_level, 0);
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].name, "t");
        assert_eq!(req.topics[0].partitions[0].fetch_offset, 100);
    }

    #[test]
    fn v0_omits_max_bytes_and_isolation_level() {
        // v0..v2: only replica_id, max_wait_ms, min_bytes, then topics.
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // replica_id
        buf.put_i32(0); // max_wait_ms
        buf.put_i32(0); // min_bytes
        buf.put_i32(1); // topics
        put_kafka_string(&mut buf, "t");
        buf.put_i32(1); // partitions
        buf.put_i32(0); // partition_index
        buf.put_i64(7); // fetch_offset
        buf.put_i32(64); // partition_max_bytes

        let (rest, req) = parse_fetch_request(NomBytes::new(buf.freeze()), 0).expect("valid v0");
        assert!(
            rest.to_bytes().is_empty(),
            "v0 must consume all bytes (no max_bytes/iso)"
        );
        assert_eq!(
            req.max_bytes,
            i32::MAX,
            "v0 has no max_bytes; default i32::MAX"
        );
        assert_eq!(req.isolation_level, 0);
        assert_eq!(req.topics[0].partitions[0].fetch_offset, 7);
    }

    #[test]
    fn truncated_input_returns_parse_error() {
        let bytes = Bytes::from_static(&[0u8; 6]); // not enough for the v4 header
        assert!(parse_fetch_request(NomBytes::new(bytes), 4).is_err());
    }

    #[test]
    fn topics_array_length_overflow_rejected() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1);
        buf.put_i32(0);
        buf.put_i32(0);
        buf.put_i32(0);
        buf.put_i8(0);
        buf.put_i32(i32::MAX); // claim 2B topics, supply none

        assert!(
            parse_fetch_request(NomBytes::new(buf.freeze()), 4).is_err(),
            "length-prefix overflow on topics must be rejected"
        );
    }
}
