//! Produce request parsing.
//!
//! Supports v3..=v9. v9+ uses the KIP-482 flexible wire format. For our
//! target range the body fields are stable across versions; only the
//! length-prefix encoding changes.

use bytes::Bytes;
use nom::{
    IResult,
    number::complete::{be_i16, be_i32},
};
use nombytes::NomBytes;

use crate::parser::{
    parse_array, parse_compact_nullable_string, parse_kafka_string, parse_kafka_string_opt,
    parse_unsigned_varint, skip_tagged_fields,
};

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

pub fn parse_produce_request(s: NomBytes, version: i16) -> IResult<NomBytes, ProduceRequestData> {
    if version >= 9 {
        parse_produce_request_flexible(s)
    } else {
        parse_produce_request_classic(s)
    }
}

fn parse_produce_request_classic(s: NomBytes) -> IResult<NomBytes, ProduceRequestData> {
    let (s, transactional_id) = parse_kafka_string_opt(s)?;
    let (s, acks) = be_i16(s)?;
    let (s, timeout_ms) = be_i32(s)?;
    let (s, topics) = parse_array(parse_produce_topic_classic)(s)?;

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

fn parse_produce_topic_classic(s: NomBytes) -> IResult<NomBytes, ProduceTopicData> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, partitions) = parse_array(parse_produce_partition_classic)(s)?;

    Ok((s, ProduceTopicData { name, partitions }))
}

fn parse_produce_partition_classic(s: NomBytes) -> IResult<NomBytes, ProducePartitionData> {
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

/// v9+ flexible body: transactional_id is `COMPACT_NULLABLE_STRING`, the
/// arrays are compact, records are `COMPACT_NULLABLE_BYTES`, and every
/// record carries a trailing tagged-fields section.
fn parse_produce_request_flexible(s: NomBytes) -> IResult<NomBytes, ProduceRequestData> {
    let (s, txn_bytes) = parse_compact_nullable_string(s)?;
    let transactional_id = match txn_bytes {
        Some(b) => match std::str::from_utf8(&b) {
            Ok(t) => Some(t.to_string()),
            Err(_) => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    s,
                    nom::error::ErrorKind::Verify,
                )));
            }
        },
        None => None,
    };
    let (s, acks) = be_i16(s)?;
    let (s, timeout_ms) = be_i32(s)?;
    let (s, topics) = parse_compact_array(s, parse_produce_topic_flexible)?;
    let (s, _) = skip_tagged_fields(s)?;

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

fn parse_produce_topic_flexible(s: NomBytes) -> IResult<NomBytes, ProduceTopicData> {
    let (s, name_bytes) = crate::parser::parse_compact_nullable_string(s)?;
    let name = match name_bytes {
        Some(b) => match std::str::from_utf8(&b) {
            Ok(t) => t.to_string(),
            Err(_) => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    s,
                    nom::error::ErrorKind::Verify,
                )));
            }
        },
        // Topic name is COMPACT_STRING (non-nullable) in v9; if a client
        // sends 0 (null) we treat it as empty.
        None => String::new(),
    };
    let (s, partitions) = parse_compact_array(s, parse_produce_partition_flexible)?;
    let (s, _) = skip_tagged_fields(s)?;
    Ok((s, ProduceTopicData { name, partitions }))
}

fn parse_produce_partition_flexible(s: NomBytes) -> IResult<NomBytes, ProducePartitionData> {
    let (s, partition_index) = be_i32(s)?;
    // records: COMPACT_NULLABLE_BYTES — varint length, where 0 = null.
    let (s, raw_len) = parse_unsigned_varint(s)?;
    let (s, records) = if raw_len == 0 {
        (s, Bytes::new())
    } else {
        let n = (raw_len - 1) as usize;
        let (s, payload) = nom::bytes::complete::take(n)(s)?;
        (s, payload.into_bytes())
    };
    let (s, _) = skip_tagged_fields(s)?;
    Ok((
        s,
        ProducePartitionData {
            partition_index,
            records,
        },
    ))
}

/// Parse a flexible compact array.
fn parse_compact_array<O, F>(
    s: NomBytes,
    mut item: F,
) -> IResult<NomBytes, Vec<O>>
where
    F: FnMut(NomBytes) -> IResult<NomBytes, O>,
{
    let (s, raw_len) = parse_unsigned_varint(s)?;
    if raw_len == 0 {
        return Ok((s, Vec::new()));
    }
    let n = (raw_len - 1) as usize;
    if n > crate::constants::MAX_PROTOCOL_ARRAY_SIZE as usize {
        return Err(nom::Err::Error(nom::error::Error::new(
            s,
            nom::error::ErrorKind::TooLarge,
        )));
    }
    let mut out = Vec::with_capacity(n);
    let mut remaining = s;
    for _ in 0..n {
        let (next, x) = item(remaining)?;
        out.push(x);
        remaining = next;
    }
    Ok((remaining, out))
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

    /// v9 flexible: transactional_id compact-nullable, compact arrays,
    /// trailing tagged-fields after every record.
    #[test]
    fn v9_flexible_round_trip() {
        let mut buf = BytesMut::new();
        // transactional_id = null (varint 0)
        buf.put_u8(0);
        buf.put_i16(1); // acks
        buf.put_i32(30_000); // timeout_ms
        // topics: compact_array len 1 -> varint 2
        buf.put_u8(0x02);
        // topic.name = "t" -> compact_string (varint 2 + 1 byte)
        buf.put_u8(0x02);
        buf.put_slice(b"t");
        // partitions: compact_array len 1 -> varint 2
        buf.put_u8(0x02);
        // partition_index
        buf.put_i32(0);
        // records: compact_nullable_bytes — len 4 -> varint 5 + 4 payload bytes
        buf.put_u8(0x05);
        buf.put_slice(b"abcd");
        // partition tagged fields
        buf.put_u8(0);
        // topic tagged fields
        buf.put_u8(0);
        // body tagged fields
        buf.put_u8(0);

        let (rest, req) =
            parse_produce_request(NomBytes::new(buf.freeze()), 9).expect("v9 valid");
        assert!(rest.to_bytes().is_empty());
        assert_eq!(req.transactional_id, None);
        assert_eq!(req.acks, 1);
        assert_eq!(req.timeout_ms, 30_000);
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].name, "t");
        assert_eq!(req.topics[0].partitions.len(), 1);
        assert_eq!(req.topics[0].partitions[0].records.as_ref(), b"abcd");
    }

    /// v9 with null records compact (varint 0): produces an empty
    /// payload, not a parse error.
    #[test]
    fn v9_flexible_null_records_decodes_as_empty() {
        let mut buf = BytesMut::new();
        buf.put_u8(0); // transactional_id = null
        buf.put_i16(1);
        buf.put_i32(30_000);
        buf.put_u8(0x02); // 1 topic
        buf.put_u8(0x02); // name "t"
        buf.put_slice(b"t");
        buf.put_u8(0x02); // 1 partition
        buf.put_i32(0);
        buf.put_u8(0); // records: compact null
        buf.put_u8(0); // partition tagged
        buf.put_u8(0); // topic tagged
        buf.put_u8(0); // body tagged

        let (_, req) = parse_produce_request(NomBytes::new(buf.freeze()), 9).expect("null OK");
        assert!(req.topics[0].partitions[0].records.is_empty());
    }
}
