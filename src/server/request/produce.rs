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
