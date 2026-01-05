//! Metadata request parsing.

use nom::{IResult, number::complete::be_i32};
use nombytes::NomBytes;

use crate::parser::{bytes_to_string, parse_string};

/// Metadata request data.
#[derive(Debug, Clone, Default)]
pub struct MetadataRequestData {
    /// The topics to fetch metadata for. None means all topics.
    pub topics: Option<Vec<String>>,
}

pub fn parse_metadata_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, MetadataRequestData> {
    let (s, topic_count) = be_i32(s)?;
    if topic_count == -1 {
        return Ok((s, MetadataRequestData { topics: None }));
    }

    let mut topics = Vec::with_capacity(topic_count as usize);
    let mut remaining = s;
    for _ in 0..topic_count {
        let (s, name) = parse_string(remaining)?;
        topics.push(bytes_to_string(&name)?);
        remaining = s;
    }

    Ok((
        remaining,
        MetadataRequestData {
            topics: Some(topics),
        },
    ))
}
