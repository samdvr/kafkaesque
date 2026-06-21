//! Metadata request parsing.
//!
//! Supports v0..=v9. v9+ uses the KIP-482 flexible wire format (compact
//! strings + arrays, trailing tagged fields per record). v10+ adds
//! per-topic `topic_id` UUIDs and is intentionally out of scope.

use nom::{
    IResult,
    number::complete::{be_i8, be_i32},
};
use nombytes::NomBytes;

use crate::constants::MAX_PROTOCOL_ARRAY_SIZE;
use crate::parser::{
    parse_compact_nullable_string, parse_kafka_string, parse_unsigned_varint, skip_tagged_fields,
};

/// Metadata request data.
#[derive(Debug, Clone, Default)]
pub struct MetadataRequestData {
    /// The topics to fetch metadata for. None means all topics.
    pub topics: Option<Vec<String>>,
    /// v4+ controls broker-side auto-creation. Default `true` for v0..=v3
    /// (which had no opt-out wire-format) and forwarded as-is for v4+.
    pub allow_auto_topic_creation: bool,
    /// v8..=v10 (removed in v11+). When set, the response carries per-cluster
    /// ACL bits.
    pub include_cluster_authorized_operations: bool,
    /// v8+. When set, the response carries per-topic ACL bits.
    pub include_topic_authorized_operations: bool,
}

/// Parse a Metadata request.
///
/// Per-version wire layout (Kafka protocol spec, MetadataRequest):
/// - v0: `[topics]` (a null/-1 array is not legal; empty means "all")
/// - v1–v3: `[topics]`, where a null (-1) array means "all topics"
/// - v4+: adds `allow_auto_topic_creation` (BOOLEAN) after the topics array
/// - v8+: adds `include_cluster_authorized_operations` and
///   `include_topic_authorized_operations` (BOOLEAN)
/// - v9+: flexible encoding — compact arrays, compact strings, trailing
///   tagged fields per record. Note the request **header** is also
///   flexible at v9+ (handled in `parse_request_header`).
pub fn parse_metadata_request(s: NomBytes, version: i16) -> IResult<NomBytes, MetadataRequestData> {
    if version >= 9 {
        parse_metadata_request_flexible(s, version)
    } else {
        parse_metadata_request_classic(s, version)
    }
}

fn parse_metadata_request_classic(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, MetadataRequestData> {
    let (s, topic_count) = be_i32(s)?;

    let (s, topics) = if topic_count == -1 {
        (s, None)
    } else {
        if !(0..=MAX_PROTOCOL_ARRAY_SIZE).contains(&topic_count) {
            return Err(nom::Err::Error(nom::error::Error::new(
                s,
                nom::error::ErrorKind::TooLarge,
            )));
        }
        let mut topics = Vec::with_capacity(topic_count as usize);
        let mut remaining = s;
        for _ in 0..topic_count {
            let (next, name) = parse_kafka_string(remaining)?;
            topics.push(name);
            remaining = next;
        }
        (remaining, Some(topics))
    };

    let (s, allow_auto_topic_creation) = if version >= 4 {
        let (s, b) = be_i8(s)?;
        (s, b != 0)
    } else {
        (s, true)
    };
    let (s, include_cluster_authorized_operations) = if version >= 8 {
        let (s, b) = be_i8(s)?;
        (s, b != 0)
    } else {
        (s, false)
    };
    let (s, include_topic_authorized_operations) = if version >= 8 {
        let (s, b) = be_i8(s)?;
        (s, b != 0)
    } else {
        (s, false)
    };

    Ok((
        s,
        MetadataRequestData {
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
        },
    ))
}

/// v9+ uses compact arrays / strings throughout the body and a trailing
/// tagged-fields section after every record.
fn parse_metadata_request_flexible(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, MetadataRequestData> {
    // topics: compact_nullable_array. length = 0 -> null/all-topics,
    // length = N+1 -> N topics.
    let (s, raw_len) = parse_unsigned_varint(s)?;
    let (s, topics) = if raw_len == 0 {
        (s, None)
    } else {
        let n = (raw_len - 1) as usize;
        if n > MAX_PROTOCOL_ARRAY_SIZE as usize {
            return Err(nom::Err::Error(nom::error::Error::new(
                s,
                nom::error::ErrorKind::TooLarge,
            )));
        }
        let mut topics = Vec::with_capacity(n);
        let mut remaining = s;
        for _ in 0..n {
            // v9 has no topic_id (that's v10+); each topic is just a
            // compact name + tagged fields.
            let (next, name_opt) = parse_compact_nullable_string(remaining)?;
            let name = match name_opt {
                Some(b) => match std::str::from_utf8(&b) {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        return Err(nom::Err::Error(nom::error::Error::new(
                            next,
                            nom::error::ErrorKind::Verify,
                        )));
                    }
                },
                None => String::new(),
            };
            let (next, _) = skip_tagged_fields(next)?;
            topics.push(name);
            remaining = next;
        }
        (remaining, Some(topics))
    };

    let (s, allow_auto_topic_creation) = be_i8(s)?;
    let (s, include_cluster_authorized_operations) = be_i8(s)?;
    let (s, include_topic_authorized_operations) = be_i8(s)?;
    let (s, _) = skip_tagged_fields(s)?;

    Ok((
        s,
        MetadataRequestData {
            topics,
            allow_auto_topic_creation: allow_auto_topic_creation != 0,
            include_cluster_authorized_operations: include_cluster_authorized_operations != 0,
            include_topic_authorized_operations: include_topic_authorized_operations != 0,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_topics(names: &[&str]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(names.len() as i32).to_be_bytes());
        for name in names {
            data.extend_from_slice(&(name.len() as i16).to_be_bytes());
            data.extend_from_slice(name.as_bytes());
        }
        data
    }

    #[test]
    fn test_parse_metadata_v1_no_auto_create_flag() {
        let data = build_topics(&["a", "bb"]);
        let (rest, req) = parse_metadata_request(NomBytes::from(data.as_slice()), 1).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(req.topics, Some(vec!["a".to_string(), "bb".to_string()]));
        assert!(req.allow_auto_topic_creation);
    }

    #[test]
    fn test_parse_metadata_v4_consumes_allow_auto_topic_creation() {
        // v4 appends a BOOLEAN after the topics array; it must be consumed.
        let mut data = build_topics(&["t"]);
        data.push(0); // allow_auto_topic_creation = false
        let (rest, req) = parse_metadata_request(NomBytes::from(data.as_slice()), 4).unwrap();
        assert!(
            rest.into_bytes().is_empty(),
            "v4 boolean must be consumed, not left as trailing bytes"
        );
        assert_eq!(req.topics, Some(vec!["t".to_string()]));
        assert!(!req.allow_auto_topic_creation);
    }

    #[test]
    fn test_parse_metadata_v4_all_topics_with_flag() {
        let mut data = (-1i32).to_be_bytes().to_vec();
        data.push(1); // allow_auto_topic_creation = true
        let (rest, req) = parse_metadata_request(NomBytes::from(data.as_slice()), 4).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert!(req.topics.is_none());
        assert!(req.allow_auto_topic_creation);
    }

    /// v8 appends two more BOOLEANs (cluster + topic authorized
    /// operations). A version-blind parser would silently swallow them
    /// or, worse, mis-frame whatever follows.
    #[test]
    fn test_parse_metadata_v8_consumes_authz_flags() {
        let mut data = build_topics(&["t"]);
        data.push(1); // allow_auto_topic_creation
        data.push(1); // include_cluster_authorized_operations
        data.push(0); // include_topic_authorized_operations
        let (rest, req) = parse_metadata_request(NomBytes::from(data.as_slice()), 8).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert!(req.allow_auto_topic_creation);
        assert!(req.include_cluster_authorized_operations);
        assert!(!req.include_topic_authorized_operations);
    }

    /// v9 switches to flexible encoding: compact arrays/strings and
    /// trailing tagged-fields after each record.
    #[test]
    fn test_parse_metadata_v9_flexible() {
        // topics: compact_array length 2 (varint = 3): [topic-1, t2]
        let mut data = Vec::new();
        data.push(0x03); // 3 = length 2 + 1
        // topic 1: name "topic-1" + tagged_fields
        data.push(0x08); // 8 = length 7 + 1
        data.extend_from_slice(b"topic-1");
        data.push(0x00); // tagged_fields
        // topic 2: name "t2" + tagged_fields
        data.push(0x03); // 3 = length 2 + 1
        data.extend_from_slice(b"t2");
        data.push(0x00);
        // top-level: allow_auto_topic_creation, includes, tagged
        data.push(1);
        data.push(0);
        data.push(1);
        data.push(0); // tagged_fields

        let (rest, req) = parse_metadata_request(NomBytes::from(data.as_slice()), 9).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(
            req.topics,
            Some(vec!["topic-1".to_string(), "t2".to_string()])
        );
        assert!(req.allow_auto_topic_creation);
        assert!(!req.include_cluster_authorized_operations);
        assert!(req.include_topic_authorized_operations);
    }

    /// v9 null-topics (compact varint 0) means "all topics". This is the
    /// flexible-encoding equivalent of v1+'s `-1` array length.
    #[test]
    fn test_parse_metadata_v9_all_topics_via_null_array() {
        let mut data = Vec::new();
        data.push(0x00); // null compact array
        data.push(1); // allow_auto_topic_creation
        data.push(0);
        data.push(0);
        data.push(0); // tagged_fields
        let (rest, req) = parse_metadata_request(NomBytes::from(data.as_slice()), 9).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert!(req.topics.is_none());
    }
}
