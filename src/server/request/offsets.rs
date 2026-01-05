//! Offset-related request parsing (ListOffsets, OffsetCommit, OffsetFetch).

use nom::{
    IResult,
    number::complete::{be_i8, be_i32, be_i64},
};
use nombytes::NomBytes;

use crate::parser::{
    bytes_to_string, bytes_to_string_opt, parse_array, parse_nullable_string, parse_string,
};

// ============================================================================
// ListOffsets
// ============================================================================

/// ListOffsets request data.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequestData {
    pub replica_id: i32,
    pub isolation_level: i8,
    pub topics: Vec<ListOffsetsTopicData>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsTopicData {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartitionData>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsPartitionData {
    pub partition_index: i32,
    pub timestamp: i64,
}

pub fn parse_list_offsets_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, ListOffsetsRequestData> {
    let (s, replica_id) = be_i32(s)?;
    let (s, isolation_level) = if version >= 2 { be_i8(s)? } else { (s, 0i8) };
    let (s, topics) = parse_array(|s| parse_list_offsets_topic(s, version))(s)?;

    Ok((
        s,
        ListOffsetsRequestData {
            replica_id,
            isolation_level,
            topics,
        },
    ))
}

fn parse_list_offsets_topic(s: NomBytes, version: i16) -> IResult<NomBytes, ListOffsetsTopicData> {
    let (s, name) = parse_string(s)?;
    let (s, partitions) = parse_array(|s| parse_list_offsets_partition(s, version))(s)?;

    Ok((
        s,
        ListOffsetsTopicData {
            name: bytes_to_string(&name)?,
            partitions,
        },
    ))
}

fn parse_list_offsets_partition(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, ListOffsetsPartitionData> {
    let (s, partition_index) = be_i32(s)?;
    let (s, timestamp) = be_i64(s)?;

    Ok((
        s,
        ListOffsetsPartitionData {
            partition_index,
            timestamp,
        },
    ))
}

// ============================================================================
// OffsetCommit
// ============================================================================

/// OffsetCommit request data.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequestData {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub topics: Vec<OffsetCommitTopicData>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitTopicData {
    pub name: String,
    pub partitions: Vec<OffsetCommitPartitionData>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitPartitionData {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_metadata: Option<String>,
}

pub fn parse_offset_commit_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, OffsetCommitRequestData> {
    let (s, group_id) = parse_string(s)?;
    let (s, generation_id) = be_i32(s)?;
    let (s, member_id) = parse_string(s)?;
    let (s, topics) = parse_array(parse_offset_commit_topic)(s)?;

    Ok((
        s,
        OffsetCommitRequestData {
            group_id: bytes_to_string(&group_id)?,
            generation_id,
            member_id: bytes_to_string(&member_id)?,
            topics,
        },
    ))
}

fn parse_offset_commit_topic(s: NomBytes) -> IResult<NomBytes, OffsetCommitTopicData> {
    let (s, name) = parse_string(s)?;
    let (s, partitions) = parse_array(parse_offset_commit_partition)(s)?;

    Ok((
        s,
        OffsetCommitTopicData {
            name: bytes_to_string(&name)?,
            partitions,
        },
    ))
}

fn parse_offset_commit_partition(s: NomBytes) -> IResult<NomBytes, OffsetCommitPartitionData> {
    let (s, partition_index) = be_i32(s)?;
    let (s, committed_offset) = be_i64(s)?;
    let (s, committed_metadata) = parse_nullable_string(s)?;

    Ok((
        s,
        OffsetCommitPartitionData {
            partition_index,
            committed_offset,
            committed_metadata: bytes_to_string_opt(committed_metadata)?,
        },
    ))
}

// ============================================================================
// OffsetFetch
// ============================================================================

/// OffsetFetch request data.
#[derive(Debug, Clone)]
pub struct OffsetFetchRequestData {
    pub group_id: String,
    pub topics: Vec<OffsetFetchTopicData>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchTopicData {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

pub fn parse_offset_fetch_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, OffsetFetchRequestData> {
    let (s, group_id) = parse_string(s)?;
    let (s, topics) = parse_array(parse_offset_fetch_topic)(s)?;

    Ok((
        s,
        OffsetFetchRequestData {
            group_id: bytes_to_string(&group_id)?,
            topics,
        },
    ))
}

fn parse_offset_fetch_topic(s: NomBytes) -> IResult<NomBytes, OffsetFetchTopicData> {
    let (s, name) = parse_string(s)?;
    let (s, partition_indexes) = parse_array(be_i32)(s)?;

    Ok((
        s,
        OffsetFetchTopicData {
            name: bytes_to_string(&name)?,
            partition_indexes,
        },
    ))
}
