//! Metadata request parsing.

use nom::{
    IResult,
    number::complete::{be_i8, be_i32},
};
use nombytes::NomBytes;

use crate::constants::MAX_PROTOCOL_ARRAY_SIZE;
use crate::parser::{bytes_to_string, parse_string};

/// Metadata request data.
#[derive(Debug, Clone, Default)]
pub struct MetadataRequestData {
    /// The topics to fetch metadata for. None means all topics.
    pub topics: Option<Vec<String>>,
}

/// Parse a Metadata request.
///
/// Per-version wire layout (Kafka protocol spec, MetadataRequest):
/// - v0: `[topics]` (a null/-1 array is not legal; empty means "all")
/// - v1–v3: `[topics]`, where a null (-1) array means "all topics"
/// - v4+: adds `allow_auto_topic_creation` (BOOLEAN) after the topics
///   array (KIP-4 / broker-side auto-create opt-out)
///
/// `versions.rs` advertises Metadata 0..=1 only, so v4+ requests are
/// rejected with UnsupportedVersion before this parser runs. The v4+
/// branch below keeps the parser spec-correct for every version it could
/// ever be handed; the parsed flag is currently dropped because topic
/// auto-creation is governed by cluster config, not per-request opt-in,
/// and `MetadataRequestData` is constructed literally throughout cluster
/// and integration code that this change must not touch.
pub fn parse_metadata_request(s: NomBytes, version: i16) -> IResult<NomBytes, MetadataRequestData> {
    let (s, topic_count) = be_i32(s)?;
    if topic_count == -1 {
        // allow_auto_topic_creation: added in Metadata v4.
        let (s, _allow_auto_topic_creation) = if version >= 4 { be_i8(s)? } else { (s, 1i8) };
        return Ok((s, MetadataRequestData { topics: None }));
    }

    // Enforce the same array-size cap that `parser::parse_array` uses
    // (MAX_PROTOCOL_ARRAY_SIZE = 100k). Without this gate a remote attacker
    // could send `topic_count = i32::MAX` and force a 2 GiB Vec allocation
    // before any data is read.
    if !(0..=MAX_PROTOCOL_ARRAY_SIZE).contains(&topic_count) {
        return Err(nom::Err::Error(nom::error::Error::new(
            s,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    let mut topics = Vec::with_capacity(topic_count as usize);
    let mut remaining = s;
    for _ in 0..topic_count {
        let (s, name) = parse_string(remaining)?;
        topics.push(bytes_to_string(&name)?);
        remaining = s;
    }

    // allow_auto_topic_creation: added in Metadata v4 (see doc comment).
    let (remaining, _allow_auto_topic_creation) = if version >= 4 {
        be_i8(remaining)?
    } else {
        (remaining, 1i8)
    };

    Ok((
        remaining,
        MetadataRequestData {
            topics: Some(topics),
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
    }

    #[test]
    fn test_parse_metadata_v4_all_topics_with_flag() {
        let mut data = (-1i32).to_be_bytes().to_vec();
        data.push(1); // allow_auto_topic_creation = true
        let (rest, req) = parse_metadata_request(NomBytes::from(data.as_slice()), 4).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert!(req.topics.is_none());
    }
}
