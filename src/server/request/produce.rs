//! Produce request parsing.

use bytes::Bytes;
use nom::{
    IResult,
    number::complete::{be_i16, be_i32},
};
use nombytes::NomBytes;

use crate::parser::{parse_array, parse_kafka_string, parse_kafka_string_opt};

/// Produce request data.
#[derive(Debug, Clone)]
pub struct ProduceRequestData {
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicData>,
}

#[derive(Debug, Clone)]
pub struct ProduceTopicData {
    pub name: String,
    pub partitions: Vec<ProducePartitionData>,
}

#[derive(Debug, Clone)]
pub struct ProducePartitionData {
    pub partition_index: i32,
    pub records: Bytes,
}

pub fn parse_produce_request(s: NomBytes, _version: i16) -> IResult<NomBytes, ProduceRequestData> {
    let (s, transactional_id) = parse_kafka_string_opt(s)?;
    let (s, acks) = be_i16(s)?;
    let (s, timeout_ms) = be_i32(s)?;
    let (s, topics) = parse_array(parse_produce_topic)(s)?;

    Ok((
        s,
        ProduceRequestData {
            transactional_id,
            acks,
            timeout_ms,
            topics,
        },
    ))
}

fn parse_produce_topic(s: NomBytes) -> IResult<NomBytes, ProduceTopicData> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, partitions) = parse_array(parse_produce_partition)(s)?;

    Ok((s, ProduceTopicData { name, partitions }))
}

fn parse_produce_partition(s: NomBytes) -> IResult<NomBytes, ProducePartitionData> {
    let (s, partition_index) = be_i32(s)?;
    let (s, record_set_size) = be_i32(s)?;
    // The Kafka protocol types this field as NULLABLE_BYTES: -1 is a legal
    // null encoding meaning "no records this partition". Treating it as a
    // hard parse error breaks any client that sends -1 (the canonical
    // librdkafka encoding for an empty partition slot in a multi-partition
    // batch). Negative values other than -1 are still rejected because they
    // would cast to enormous unsigned takes.
    if record_set_size == -1 {
        return Ok((
            s,
            ProducePartitionData {
                partition_index,
                records: Bytes::new(),
            },
        ));
    }
    if record_set_size < 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            s,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (s, records) = nom::bytes::complete::take(record_set_size as usize)(s)?;

    Ok((
        s,
        ProducePartitionData {
            partition_index,
            records: records.into_bytes(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    type TestPartition<'a> = (i32, &'a [u8]);
    type TestTopic<'a> = (&'a str, &'a [TestPartition<'a>]);

    fn put_kafka_string(buf: &mut BytesMut, s: Option<&str>) {
        match s {
            None => buf.put_i16(-1),
            Some(s) => {
                buf.put_i16(s.len() as i16);
                buf.put_slice(s.as_bytes());
            }
        }
    }

    fn build_produce_request_v3(
        transactional_id: Option<&str>,
        acks: i16,
        timeout_ms: i32,
        topics: &[TestTopic<'_>],
    ) -> Bytes {
        let mut buf = BytesMut::new();
        put_kafka_string(&mut buf, transactional_id);
        buf.put_i16(acks);
        buf.put_i32(timeout_ms);
        buf.put_i32(topics.len() as i32);
        for (name, partitions) in topics {
            put_kafka_string(&mut buf, Some(name));
            buf.put_i32(partitions.len() as i32);
            for (partition_index, records) in *partitions {
                buf.put_i32(*partition_index);
                buf.put_i32(records.len() as i32);
                buf.put_slice(records);
            }
        }
        buf.freeze()
    }

    #[test]
    fn valid_produce_request_decodes() {
        let bytes =
            build_produce_request_v3(None, 1, 5_000, &[("topic-a", &[(0, b"record-bytes-here")])]);
        let (rest, req) =
            parse_produce_request(NomBytes::new(bytes), 3).expect("valid request must decode");
        assert!(
            rest.to_bytes().is_empty(),
            "no bytes should remain after parse"
        );
        assert_eq!(req.transactional_id, None);
        assert_eq!(req.acks, 1);
        assert_eq!(req.timeout_ms, 5_000);
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].name, "topic-a");
        assert_eq!(req.topics[0].partitions.len(), 1);
        assert_eq!(req.topics[0].partitions[0].partition_index, 0);
        assert_eq!(
            req.topics[0].partitions[0].records.as_ref(),
            b"record-bytes-here"
        );
    }

    #[test]
    fn null_record_set_size_is_accepted_as_empty_partition() {
        let mut buf = BytesMut::new();
        put_kafka_string(&mut buf, None);
        buf.put_i16(0); // acks
        buf.put_i32(0); // timeout
        buf.put_i32(1); // topics
        put_kafka_string(&mut buf, Some("t"));
        buf.put_i32(1); // partitions
        buf.put_i32(0); // partition_index
        buf.put_i32(-1); // record_set_size = NULL — must be accepted

        let (_, req) =
            parse_produce_request(NomBytes::new(buf.freeze()), 3).expect("null records OK");
        assert!(req.topics[0].partitions[0].records.is_empty());
    }

    #[test]
    fn truncated_input_returns_parse_error() {
        let bytes = Bytes::from_static(&[0x00, 0x00]); // only 2 bytes, not even a transactional_id
        let result = parse_produce_request(NomBytes::new(bytes), 3);
        assert!(result.is_err(), "truncated input must error, not panic");
    }

    #[test]
    fn negative_non_null_record_set_size_rejected() {
        let mut buf = BytesMut::new();
        put_kafka_string(&mut buf, None);
        buf.put_i16(0);
        buf.put_i32(0);
        buf.put_i32(1);
        put_kafka_string(&mut buf, Some("t"));
        buf.put_i32(1);
        buf.put_i32(0);
        buf.put_i32(-2); // negative but not the legal NULL sentinel

        let result = parse_produce_request(NomBytes::new(buf.freeze()), 3);
        assert!(
            result.is_err(),
            "negative non-(-1) record_set_size must be rejected (would cast to enormous take)"
        );
    }

    #[test]
    fn record_set_size_overflow_rejected_via_take() {
        let mut buf = BytesMut::new();
        put_kafka_string(&mut buf, None);
        buf.put_i16(0);
        buf.put_i32(0);
        buf.put_i32(1);
        put_kafka_string(&mut buf, Some("t"));
        buf.put_i32(1);
        buf.put_i32(0);
        buf.put_i32(i32::MAX); // declares 2 GiB of records but supplies none

        let result = parse_produce_request(NomBytes::new(buf.freeze()), 3);
        assert!(
            result.is_err(),
            "length-prefix overflow must be rejected by the underlying take, not panic"
        );
    }

    #[test]
    fn empty_topics_array_decodes() {
        let bytes = build_produce_request_v3(Some("txn-1"), -1, 30_000, &[]);
        let (rest, req) = parse_produce_request(NomBytes::new(bytes), 3).expect("empty topics OK");
        assert!(rest.to_bytes().is_empty());
        assert_eq!(req.transactional_id.as_deref(), Some("txn-1"));
        assert_eq!(req.acks, -1);
        assert_eq!(req.topics.len(), 0);
    }
}
