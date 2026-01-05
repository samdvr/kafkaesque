//! Fetch request parsing.

use nom::{
    IResult,
    number::complete::{be_i8, be_i32, be_i64},
};
use nombytes::NomBytes;

use crate::parser::{bytes_to_string, parse_array, parse_string};

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

pub fn parse_fetch_request(s: NomBytes, _version: i16) -> IResult<NomBytes, FetchRequestData> {
    let (s, replica_id) = be_i32(s)?;
    let (s, max_wait_ms) = be_i32(s)?;
    let (s, min_bytes) = be_i32(s)?;
    let (s, max_bytes) = be_i32(s)?;
    let (s, isolation_level) = be_i8(s)?;
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
    let (s, name) = parse_string(s)?;
    let (s, partitions) = parse_array(parse_fetch_partition)(s)?;

    Ok((
        s,
        FetchTopicData {
            name: bytes_to_string(&name)?,
            partitions,
        },
    ))
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
