//! Offset-related request parsing (ListOffsets, OffsetCommit, OffsetFetch).

use nom::{
    IResult,
    number::complete::{be_i8, be_i32, be_i64},
};
use nombytes::NomBytes;

use crate::parser::{parse_array, parse_kafka_string, parse_kafka_string_opt};

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
    let (s, name) = parse_kafka_string(s)?;
    let (s, partitions) = parse_array(|s| parse_list_offsets_partition(s, version))(s)?;

    Ok((
        s,
        ListOffsetsTopicData {
            name,
            partitions,
        },
    ))
}

fn parse_list_offsets_partition(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, ListOffsetsPartitionData> {
    let (s, partition_index) = be_i32(s)?;
    let (s, timestamp) = be_i64(s)?;
    // `max_num_offsets` (INT32) exists ONLY in ListOffsets v0; v1 (KIP-79)
    // removed it. Without consuming it, every v0 partition after the first
    // would be parsed misaligned by 4 bytes. The value itself is dropped:
    // the broker always returns a single offset per partition (v1+
    // semantics), which is what every modern client expects.
    let (s, _max_num_offsets) = if version == 0 { be_i32(s)? } else { (s, 1i32) };

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
///
/// Per-version wire layout (Kafka protocol spec, OffsetCommitRequest):
/// - v0: `group_id`, `[topics]`
/// - v1: adds `generation_id` (INT32) and `member_id` (STRING) after
///   `group_id`; each partition gains `commit_timestamp` (INT64, v1 only)
/// - v2: adds `retention_time_ms` (INT64) after `member_id`; partition
///   `commit_timestamp` removed
/// - v5+: `retention_time_ms` removed (KIP-211) — not advertised here
///   (`versions.rs` caps OffsetCommit at v2)
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
    version: i16,
) -> IResult<NomBytes, OffsetCommitRequestData> {
    let (s, group_id) = parse_kafka_string(s)?;
    // `generation_id` and `member_id` were added in OffsetCommit v1; a v0
    // request has neither. Reading them unconditionally consumed the v0
    // topics array as garbage. v0 commits are "simple" (outside a consumer
    // group generation): Kafka models that as generation -1 / empty member.
    let (s, generation_id) = if version >= 1 { be_i32(s)? } else { (s, -1i32) };
    let (s, member_id) = if version >= 1 {
        parse_kafka_string(s)?
    } else {
        (s, String::new())
    };
    // `retention_time_ms` (INT64) exists in v2..=v4 (added v2, removed in
    // v5 by KIP-211). It sits between the group fields and the topics
    // array; skipping it would shift the whole topics array by 8 bytes.
    // The value is parsed for wire correctness and currently dropped: the
    // broker has no per-commit retention override (offsets live in SlateDB
    // with cluster-level retention), matching v5+ semantics where the
    // field no longer exists. Not stored on the struct so existing
    // struct-literal construction sites stay source-compatible.
    let (s, _retention_time_ms) = if (2..=4).contains(&version) {
        be_i64(s)?
    } else {
        (s, -1i64)
    };
    let (s, topics) = parse_array(|s| parse_offset_commit_topic(s, version))(s)?;

    Ok((
        s,
        OffsetCommitRequestData {
            group_id,
            generation_id,
            member_id,
            topics,
        },
    ))
}

fn parse_offset_commit_topic(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, OffsetCommitTopicData> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, partitions) = parse_array(|s| parse_offset_commit_partition(s, version))(s)?;

    Ok((
        s,
        OffsetCommitTopicData {
            name,
            partitions,
        },
    ))
}

fn parse_offset_commit_partition(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, OffsetCommitPartitionData> {
    let (s, partition_index) = be_i32(s)?;
    let (s, committed_offset) = be_i64(s)?;
    // `commit_timestamp` (INT64) exists ONLY in v1 (added v1, removed v2).
    // Parsed and dropped: the broker timestamps commits itself, which is
    // exactly the v0/v2+ behavior.
    let (s, _commit_timestamp) = if version == 1 { be_i64(s)? } else { (s, -1i64) };
    let (s, committed_metadata) = parse_kafka_string_opt(s)?;

    Ok((
        s,
        OffsetCommitPartitionData {
            partition_index,
            committed_offset,
            committed_metadata,
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
    let (s, group_id) = parse_kafka_string(s)?;
    let (s, topics) = parse_array(parse_offset_fetch_topic)(s)?;

    Ok((
        s,
        OffsetFetchRequestData {
            group_id,
            topics,
        },
    ))
}

fn parse_offset_fetch_topic(s: NomBytes) -> IResult<NomBytes, OffsetFetchTopicData> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, partition_indexes) = parse_array(be_i32)(s)?;

    Ok((
        s,
        OffsetFetchTopicData {
            name,
            partition_indexes,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_string(s: &str, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(s.len() as i16).to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
    }

    /// One OffsetCommit partition entry in wire format for the given version.
    fn build_commit_partition(
        version: i16,
        partition: i32,
        offset: i64,
        metadata: Option<&str>,
        buf: &mut Vec<u8>,
    ) {
        buf.extend_from_slice(&partition.to_be_bytes());
        buf.extend_from_slice(&offset.to_be_bytes());
        if version == 1 {
            // commit_timestamp (v1 only)
            buf.extend_from_slice(&1_700_000_000_000i64.to_be_bytes());
        }
        match metadata {
            Some(m) => build_string(m, buf),
            None => buf.extend_from_slice(&(-1i16).to_be_bytes()),
        }
    }

    // ========================================================================
    // OffsetCommit
    // ========================================================================

    #[test]
    fn test_parse_offset_commit_v0_has_no_generation_or_member() {
        // v0 layout: group_id, [topics]. No generation_id / member_id /
        // retention_time_ms anywhere.
        let mut data = Vec::new();
        build_string("grp-v0", &mut data);
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
        build_string("topic-a", &mut data);
        data.extend_from_slice(&2i32.to_be_bytes()); // 2 partitions
        build_commit_partition(0, 0, 100, Some("m0"), &mut data);
        build_commit_partition(0, 1, 200, None, &mut data);

        let (rest, req) = parse_offset_commit_request(NomBytes::from(data.as_slice()), 0).unwrap();
        assert!(rest.into_bytes().is_empty(), "must consume entire body");
        assert_eq!(req.group_id, "grp-v0");
        assert_eq!(req.generation_id, -1, "v0 defaults to generation -1");
        assert_eq!(req.member_id, "", "v0 defaults to empty member id");
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].name, "topic-a");
        assert_eq!(req.topics[0].partitions.len(), 2);
        assert_eq!(req.topics[0].partitions[0].partition_index, 0);
        assert_eq!(req.topics[0].partitions[0].committed_offset, 100);
        assert_eq!(
            req.topics[0].partitions[0].committed_metadata,
            Some("m0".to_string())
        );
        assert_eq!(req.topics[0].partitions[1].partition_index, 1);
        assert_eq!(req.topics[0].partitions[1].committed_offset, 200);
        assert_eq!(req.topics[0].partitions[1].committed_metadata, None);
    }

    #[test]
    fn test_parse_offset_commit_v1_with_commit_timestamp() {
        // v1 layout: group_id, generation_id, member_id, [topics] where each
        // partition carries a commit_timestamp (v1 only).
        let mut data = Vec::new();
        build_string("grp-v1", &mut data);
        data.extend_from_slice(&7i32.to_be_bytes()); // generation_id
        build_string("member-1", &mut data);
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
        build_string("topic-b", &mut data);
        data.extend_from_slice(&2i32.to_be_bytes()); // 2 partitions
        build_commit_partition(1, 3, 42, Some("meta"), &mut data);
        build_commit_partition(1, 4, 43, None, &mut data);

        let (rest, req) = parse_offset_commit_request(NomBytes::from(data.as_slice()), 1).unwrap();
        assert!(rest.into_bytes().is_empty(), "must consume entire body");
        assert_eq!(req.group_id, "grp-v1");
        assert_eq!(req.generation_id, 7);
        assert_eq!(req.member_id, "member-1");
        // The second partition only parses correctly if commit_timestamp
        // was consumed for the first.
        assert_eq!(req.topics[0].partitions[1].partition_index, 4);
        assert_eq!(req.topics[0].partitions[1].committed_offset, 43);
    }

    #[test]
    fn test_parse_offset_commit_v2_with_retention_time() {
        // v2 layout: group_id, generation_id, member_id, retention_time_ms,
        // [topics]; partitions have no commit_timestamp.
        let mut data = Vec::new();
        build_string("grp-v2", &mut data);
        data.extend_from_slice(&9i32.to_be_bytes()); // generation_id
        build_string("member-2", &mut data);
        data.extend_from_slice(&86_400_000i64.to_be_bytes()); // retention_time_ms
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
        build_string("topic-c", &mut data);
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 partition
        build_commit_partition(2, 0, 555, Some("x"), &mut data);

        let (rest, req) = parse_offset_commit_request(NomBytes::from(data.as_slice()), 2).unwrap();
        assert!(rest.into_bytes().is_empty(), "must consume entire body");
        assert_eq!(req.group_id, "grp-v2");
        assert_eq!(req.generation_id, 9);
        assert_eq!(req.member_id, "member-2");
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].partitions[0].committed_offset, 555);
        assert_eq!(
            req.topics[0].partitions[0].committed_metadata,
            Some("x".to_string())
        );
    }

    #[test]
    fn test_parse_offset_commit_v2_without_retention_time_fails() {
        // A v2 body missing retention_time_ms must NOT silently parse: the
        // topics array would be read 8 bytes early.
        let mut data = Vec::new();
        build_string("grp", &mut data);
        data.extend_from_slice(&1i32.to_be_bytes());
        build_string("m", &mut data);
        // retention_time_ms missing — topics array follows immediately.
        data.extend_from_slice(&1i32.to_be_bytes());
        build_string("t", &mut data);
        data.extend_from_slice(&1i32.to_be_bytes());
        build_commit_partition(2, 0, 1, None, &mut data);

        let result = parse_offset_commit_request(NomBytes::from(data.as_slice()), 2);
        // Either an outright parse error or leftover bytes — never a clean
        // full parse of the intended structure.
        match result {
            Err(_) => {}
            Ok((rest, req)) => {
                assert!(
                    !rest.into_bytes().is_empty() || req.topics.len() != 1,
                    "mis-framed v2 body must not parse as if well-formed"
                );
            }
        }
    }

    // ========================================================================
    // ListOffsets
    // ========================================================================

    #[test]
    fn test_parse_list_offsets_v0_consumes_max_num_offsets() {
        // v0 partitions carry max_num_offsets (INT32); with two partitions a
        // parser that skips it reads the second partition misaligned.
        let mut data = Vec::new();
        data.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
        build_string("topic", &mut data);
        data.extend_from_slice(&2i32.to_be_bytes()); // 2 partitions
        // partition 0
        data.extend_from_slice(&0i32.to_be_bytes());
        data.extend_from_slice(&(-1i64).to_be_bytes()); // timestamp = latest
        data.extend_from_slice(&5i32.to_be_bytes()); // max_num_offsets (v0 only)
        // partition 1
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend_from_slice(&(-2i64).to_be_bytes()); // timestamp = earliest
        data.extend_from_slice(&1i32.to_be_bytes()); // max_num_offsets (v0 only)

        let (rest, req) = parse_list_offsets_request(NomBytes::from(data.as_slice()), 0).unwrap();
        assert!(rest.into_bytes().is_empty(), "must consume entire body");
        assert_eq!(req.replica_id, -1);
        assert_eq!(req.isolation_level, 0);
        assert_eq!(req.topics[0].partitions.len(), 2);
        assert_eq!(req.topics[0].partitions[0].partition_index, 0);
        assert_eq!(req.topics[0].partitions[0].timestamp, -1);
        assert_eq!(req.topics[0].partitions[1].partition_index, 1);
        assert_eq!(req.topics[0].partitions[1].timestamp, -2);
    }

    #[test]
    fn test_parse_list_offsets_v1_has_no_max_num_offsets() {
        let mut data = Vec::new();
        data.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
        build_string("t", &mut data);
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 partition
        data.extend_from_slice(&3i32.to_be_bytes());
        data.extend_from_slice(&123i64.to_be_bytes());

        let (rest, req) = parse_list_offsets_request(NomBytes::from(data.as_slice()), 1).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(req.topics[0].partitions[0].partition_index, 3);
        assert_eq!(req.topics[0].partitions[0].timestamp, 123);
    }

    #[test]
    fn test_parse_list_offsets_v2_isolation_level() {
        let mut data = Vec::new();
        data.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
        data.push(1); // isolation_level = read_committed (v2+)
        data.extend_from_slice(&0i32.to_be_bytes()); // 0 topics

        let (rest, req) = parse_list_offsets_request(NomBytes::from(data.as_slice()), 2).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(req.isolation_level, 1);
        assert!(req.topics.is_empty());
    }
}
