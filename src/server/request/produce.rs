//! Produce request parsing.

use bytes::Bytes;
use nom::{
    IResult,
    number::complete::{be_i16, be_i32},
};
use nombytes::NomBytes;

use crate::parser::{
    bytes_to_string, bytes_to_string_opt, parse_array, parse_nullable_string, parse_string,
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

pub fn parse_produce_request(s: NomBytes, _version: i16) -> IResult<NomBytes, ProduceRequestData> {
    let (s, transactional_id) = parse_nullable_string(s)?;
    let (s, acks) = be_i16(s)?;
    let (s, timeout_ms) = be_i32(s)?;
    let (s, topics) = parse_array(parse_produce_topic)(s)?;

    Ok((
        s,
        ProduceRequestData {
            transactional_id: bytes_to_string_opt(transactional_id)?,
            acks,
            timeout_ms,
            topics,
        },
    ))
}

fn parse_produce_topic(s: NomBytes) -> IResult<NomBytes, ProduceTopicData> {
    let (s, name) = parse_string(s)?;
    let (s, partitions) = parse_array(parse_produce_partition)(s)?;

    Ok((
        s,
        ProduceTopicData {
            name: bytes_to_string(&name)?,
            partitions,
        },
    ))
}

fn parse_produce_partition(s: NomBytes) -> IResult<NomBytes, ProducePartitionData> {
    let (s, partition_index) = be_i32(s)?;
    let (s, record_set_size) = be_i32(s)?;
    let (s, records) = nom::bytes::complete::take(record_set_size as usize)(s)?;

    Ok((
        s,
        ProducePartitionData {
            partition_index,
            records: records.into_bytes(),
        },
    ))
}
