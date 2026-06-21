//! Fetch request parsing.
//!
//! Supports `Fetch` API key v0..=v11 (classic, non-flexible). Per-version
//! wire layout deltas:
//!
//! - v3 added `max_bytes` (top-level)
//! - v4 added `isolation_level` (top-level)
//! - v5 added `log_start_offset` (per-partition)
//! - v7 added `session_id` + `session_epoch` (top-level) and the
//!   `forgotten_topics_data` array (KIP-227 incremental fetch sessions)
//! - v9 added `current_leader_epoch` (per-partition)
//! - v11 added `rack_id` (top-level, KIP-392 rack-aware replica selection)
//!
//! v12+ is flexible and intentionally out of scope.

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
    /// v7+ KIP-227 incremental fetch session id. `0` means "no session"
    /// (full fetch); a non-zero id continues a previously opened session.
    pub session_id: i32,
    /// v7+ session epoch. `0` is the first request in a session, `-1`
    /// signals the final request, otherwise `n` is the next expected
    /// epoch within an open session.
    pub session_epoch: i32,
    pub topics: Vec<FetchTopicData>,
    /// v7+ topics/partitions the client is dropping from its incremental
    /// fetch session. Always empty for v0..=v6.
    pub forgotten_topics: Vec<ForgottenTopicData>,
    /// v11+ KIP-392 client rack id. Empty for older versions or when the
    /// client did not set one.
    pub rack_id: String,
}

#[derive(Debug, Clone)]
pub struct FetchTopicData {
    pub name: String,
    pub partitions: Vec<FetchPartitionData>,
}

#[derive(Debug, Clone)]
pub struct FetchPartitionData {
    pub partition_index: i32,
    /// v9+ client-side leader epoch. `-1` means "unknown".
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    /// v5+ log-start-offset hint from the client. `-1` means "unknown".
    pub log_start_offset: i64,
    pub partition_max_bytes: i32,
}

#[derive(Debug, Clone)]
pub struct ForgottenTopicData {
    pub topic: String,
    pub partitions: Vec<i32>,
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
    // v7 (KIP-227) added incremental fetch session bookkeeping fields.
    let (s, session_id) = if version >= 7 { be_i32(s)? } else { (s, 0) };
    let (s, session_epoch) = if version >= 7 { be_i32(s)? } else { (s, -1) };

    let (s, topics) = parse_array(|s| parse_fetch_topic(s, version))(s)?;
    let (s, forgotten_topics) = if version >= 7 {
        parse_array(parse_forgotten_topic)(s)?
    } else {
        (s, Vec::new())
    };
    let (s, rack_id) = if version >= 11 {
        parse_kafka_string(s)?
    } else {
        (s, String::new())
    };

    Ok((
        s,
        FetchRequestData {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id,
        },
    ))
}

fn parse_fetch_topic(s: NomBytes, version: i16) -> IResult<NomBytes, FetchTopicData> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, partitions) = parse_array(|s| parse_fetch_partition(s, version))(s)?;

    Ok((s, FetchTopicData { name, partitions }))
}

fn parse_fetch_partition(s: NomBytes, version: i16) -> IResult<NomBytes, FetchPartitionData> {
    let (s, partition_index) = be_i32(s)?;
    let (s, current_leader_epoch) = if version >= 9 { be_i32(s)? } else { (s, -1) };
    let (s, fetch_offset) = be_i64(s)?;
    let (s, log_start_offset) = if version >= 5 { be_i64(s)? } else { (s, -1) };
    let (s, partition_max_bytes) = be_i32(s)?;

    Ok((
        s,
        FetchPartitionData {
            partition_index,
            current_leader_epoch,
            fetch_offset,
            log_start_offset,
            partition_max_bytes,
        },
    ))
}

fn parse_forgotten_topic(s: NomBytes) -> IResult<NomBytes, ForgottenTopicData> {
    let (s, topic) = parse_kafka_string(s)?;
    let (s, partitions) = parse_array(be_i32)(s)?;
    Ok((s, ForgottenTopicData { topic, partitions }))
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
        assert_eq!(req.session_id, 0);
        assert_eq!(req.session_epoch, -1);
        assert!(req.forgotten_topics.is_empty());
        assert!(req.rack_id.is_empty());
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].name, "t");
        assert_eq!(req.topics[0].partitions[0].fetch_offset, 100);
        assert_eq!(req.topics[0].partitions[0].log_start_offset, -1);
        assert_eq!(req.topics[0].partitions[0].current_leader_epoch, -1);
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

    /// v5: `log_start_offset` shows up between `fetch_offset` and
    /// `partition_max_bytes`. A version-blind parser would consume the
    /// next 8 bytes from `partition_max_bytes` and the topic list and
    /// silently mis-frame everything downstream.
    #[test]
    fn v5_parses_per_partition_log_start_offset() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // replica_id
        buf.put_i32(0); // max_wait_ms
        buf.put_i32(0); // min_bytes
        buf.put_i32(1_000_000); // max_bytes (v3+)
        buf.put_i8(0); // isolation_level (v4+)
        buf.put_i32(1); // topics
        put_kafka_string(&mut buf, "t");
        buf.put_i32(1); // partitions
        buf.put_i32(3); // partition_index
        buf.put_i64(100); // fetch_offset
        buf.put_i64(50); // log_start_offset (v5+)
        buf.put_i32(2048); // partition_max_bytes

        let (rest, req) = parse_fetch_request(NomBytes::new(buf.freeze()), 5).expect("v5 valid");
        assert!(rest.to_bytes().is_empty());
        let p = &req.topics[0].partitions[0];
        assert_eq!(p.partition_index, 3);
        assert_eq!(p.fetch_offset, 100);
        assert_eq!(p.log_start_offset, 50);
        assert_eq!(p.partition_max_bytes, 2048);
    }

    /// v7: top-level `session_id` + `session_epoch` and the
    /// `forgotten_topics_data` array. Verify they decode and the topic
    /// array still aligns afterwards.
    #[test]
    fn v7_parses_session_fields_and_forgotten_topics() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // replica_id
        buf.put_i32(0); // max_wait_ms
        buf.put_i32(0); // min_bytes
        buf.put_i32(1_000_000); // max_bytes
        buf.put_i8(0); // isolation_level
        buf.put_i32(42); // session_id
        buf.put_i32(7); // session_epoch
        buf.put_i32(1); // topics
        put_kafka_string(&mut buf, "t");
        buf.put_i32(1); // partitions
        buf.put_i32(0); // partition_index
        buf.put_i64(0); // fetch_offset
        buf.put_i64(-1); // log_start_offset
        buf.put_i32(64); // partition_max_bytes
        // forgotten_topics
        buf.put_i32(1);
        put_kafka_string(&mut buf, "old-topic");
        buf.put_i32(2); // 2 forgotten partitions
        buf.put_i32(5);
        buf.put_i32(6);

        let (rest, req) = parse_fetch_request(NomBytes::new(buf.freeze()), 7).expect("v7 valid");
        assert!(rest.to_bytes().is_empty());
        assert_eq!(req.session_id, 42);
        assert_eq!(req.session_epoch, 7);
        assert_eq!(req.forgotten_topics.len(), 1);
        assert_eq!(req.forgotten_topics[0].topic, "old-topic");
        assert_eq!(req.forgotten_topics[0].partitions, vec![5, 6]);
    }

    /// v9: per-partition `current_leader_epoch` slips between
    /// `partition_index` and `fetch_offset`. Mis-parsing this field
    /// would shove every later i64 read 4 bytes off and corrupt
    /// `fetch_offset` for every partition in the request.
    #[test]
    fn v9_parses_current_leader_epoch_per_partition() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // replica_id
        buf.put_i32(0); // max_wait_ms
        buf.put_i32(0); // min_bytes
        buf.put_i32(1_000_000); // max_bytes
        buf.put_i8(0); // isolation_level
        buf.put_i32(0); // session_id
        buf.put_i32(-1); // session_epoch
        buf.put_i32(1); // topics
        put_kafka_string(&mut buf, "t");
        buf.put_i32(1); // partitions
        buf.put_i32(2); // partition_index
        buf.put_i32(99); // current_leader_epoch (v9+)
        buf.put_i64(100); // fetch_offset
        buf.put_i64(0); // log_start_offset
        buf.put_i32(1024); // partition_max_bytes
        buf.put_i32(0); // forgotten_topics (empty)

        let (rest, req) = parse_fetch_request(NomBytes::new(buf.freeze()), 9).expect("v9 valid");
        assert!(rest.to_bytes().is_empty());
        let p = &req.topics[0].partitions[0];
        assert_eq!(p.current_leader_epoch, 99);
        assert_eq!(p.fetch_offset, 100);
    }

    /// v11: trailing `rack_id` is the last field on the wire. A
    /// version-blind parser would treat it as a partition-data tail.
    #[test]
    fn v11_parses_trailing_rack_id() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1);
        buf.put_i32(0);
        buf.put_i32(0);
        buf.put_i32(1_000_000);
        buf.put_i8(0);
        buf.put_i32(0);
        buf.put_i32(-1);
        buf.put_i32(0); // empty topics
        buf.put_i32(0); // empty forgotten_topics
        put_kafka_string(&mut buf, "rack-a");

        let (rest, req) = parse_fetch_request(NomBytes::new(buf.freeze()), 11).expect("v11 valid");
        assert!(rest.to_bytes().is_empty());
        assert_eq!(req.rack_id, "rack-a");
    }
}
