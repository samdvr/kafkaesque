//! CreatePartitions (key 37) request parsing.
//!
//! Wire format (Kafka protocol spec, CreatePartitionsRequest;
//! flexible from v2 — not yet supported here):
//!
//! - v0–v1 body:
//!   `topics: ARRAY of (
//!       name: STRING,
//!       count: INT32,
//!       assignments: NULLABLE_ARRAY of [INT32]
//!    )`,
//!   `timeout_ms: INT32`,
//!   `validate_only: BOOLEAN`
//!
//! The `count` field is the new TOTAL partition count (not a delta — Kafka
//! is "grow to N partitions"). `count <= existing` is rejected by the
//! handler with INVALID_PARTITIONS.
//!
//! `assignments` is the per-new-partition replica assignment. We don't
//! honor explicit replica assignments — leadership is decided by the
//! consistent-hash ring at acquire time — so the field is parsed and
//! dropped at v0–v1 (a non-null array with the wrong shape is still
//! rejected as a parse error).

use nom::{
    IResult,
    number::complete::{be_i8, be_i32},
};
use nombytes::NomBytes;

use crate::parser::{parse_array, parse_kafka_string, parse_nullable_array};

/// CreatePartitions request data.
#[derive(Debug, Clone, Default)]
pub struct CreatePartitionsRequestData {
    pub topics: Vec<CreatePartitionsTopicData>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsTopicData {
    pub name: String,
    /// New TOTAL partition count for the topic — not a delta. Kafka
    /// rejects when `count` is not strictly greater than the current
    /// partition count.
    pub count: i32,
}

pub fn parse_create_partitions_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, CreatePartitionsRequestData> {
    let (s, topics) = parse_array(parse_topic)(s)?;
    let (s, timeout_ms) = be_i32(s)?;
    let (s, validate_only) = be_i8(s)?;
    Ok((
        s,
        CreatePartitionsRequestData {
            topics,
            timeout_ms,
            validate_only: validate_only != 0,
        },
    ))
}

fn parse_topic(s: NomBytes) -> IResult<NomBytes, CreatePartitionsTopicData> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, count) = be_i32(s)?;
    // assignments: NULLABLE_ARRAY of (broker_ids: ARRAY of INT32). We
    // parse — and drop — the values: explicit assignment isn't honored,
    // ownership is computed by the ring on acquire. parse_nullable_array
    // distinguishes null (no assignments) from `[]` and accepts both.
    let (s, _assignments) = parse_nullable_array(|s| {
        let (s, _broker_ids) = parse_array(be_i32)(s)?;
        Ok((s, ()))
    })(s)?;
    Ok((s, CreatePartitionsTopicData { name, count }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn nb(data: &[u8]) -> NomBytes {
        NomBytes::new(Bytes::copy_from_slice(data))
    }

    fn build_topic(name: &str, count: i32, null_assignments: bool) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(name.len() as i16).to_be_bytes());
        data.extend_from_slice(name.as_bytes());
        data.extend_from_slice(&count.to_be_bytes());
        if null_assignments {
            data.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            data.extend_from_slice(&0i32.to_be_bytes());
        }
        data
    }

    #[test]
    fn v0_single_topic_null_assignments() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
        data.extend(build_topic("orders", 12, true));
        data.extend_from_slice(&30000i32.to_be_bytes()); // timeout_ms
        data.push(0); // validate_only = false

        let (rest, parsed) = parse_create_partitions_request(nb(&data), 0).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(parsed.topics.len(), 1);
        assert_eq!(parsed.topics[0].name, "orders");
        assert_eq!(parsed.topics[0].count, 12);
        assert_eq!(parsed.timeout_ms, 30000);
        assert!(!parsed.validate_only);
    }

    #[test]
    fn v1_validate_only_round_trips() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_topic("t", 4, false));
        data.extend_from_slice(&5000i32.to_be_bytes());
        data.push(1); // validate_only = true

        let (_, parsed) = parse_create_partitions_request(nb(&data), 1).unwrap();
        assert!(parsed.validate_only);
    }

    #[test]
    fn empty_topics_array() {
        let mut data = Vec::new();
        data.extend_from_slice(&0i32.to_be_bytes()); // 0 topics
        data.extend_from_slice(&1000i32.to_be_bytes());
        data.push(0);

        let (rest, parsed) = parse_create_partitions_request(nb(&data), 0).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert!(parsed.topics.is_empty());
    }

    #[test]
    fn assignments_array_consumed_when_present() {
        // Non-null but populated assignments array MUST be consumed so the
        // following `timeout_ms`/`validate_only` parse aligned. A bug here
        // shifts every byte after the assignments by 4 and produces a
        // bogus timeout that's still "validly parsed" — exactly the
        // failure mode the test pins.
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(b"t");
        data.extend_from_slice(&3i32.to_be_bytes()); // count
        // assignments: 2 entries, each [broker_id]
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend_from_slice(&1i32.to_be_bytes()); // first inner len
        data.extend_from_slice(&7i32.to_be_bytes());
        data.extend_from_slice(&1i32.to_be_bytes()); // second inner len
        data.extend_from_slice(&8i32.to_be_bytes());
        data.extend_from_slice(&15000i32.to_be_bytes()); // timeout_ms
        data.push(0); // validate_only

        let (rest, parsed) = parse_create_partitions_request(nb(&data), 0).unwrap();
        assert!(
            rest.into_bytes().is_empty(),
            "assignments must be fully consumed"
        );
        assert_eq!(parsed.topics[0].name, "t");
        assert_eq!(parsed.topics[0].count, 3);
        assert_eq!(parsed.timeout_ms, 15000);
    }
}
