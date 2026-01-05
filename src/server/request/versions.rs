//! API versions request parsing.

use nom::IResult;
use nombytes::NomBytes;

use crate::parser::{parse_compact_nullable_string, skip_tagged_fields};

/// ApiVersions request data.
#[derive(Debug, Clone)]
pub struct ApiVersionsRequestData {
    // We don't need to use these fields, just parse/skip them
    pub client_software_name: Option<String>,
    pub client_software_version: Option<String>,
}

pub fn parse_api_versions_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, ApiVersionsRequestData> {
    if version >= 3 {
        // v3+ uses flexible encoding with optional software info
        let (s, name) = parse_compact_nullable_string(s)?;
        let (s, ver) = parse_compact_nullable_string(s)?;
        let (s, _) = skip_tagged_fields(s)?;

        let name = name.and_then(|b| std::str::from_utf8(&b).ok().map(|s| s.to_string()));
        let ver = ver.and_then(|b| std::str::from_utf8(&b).ok().map(|s| s.to_string()));

        Ok((
            s,
            ApiVersionsRequestData {
                client_software_name: name,
                client_software_version: ver,
            },
        ))
    } else {
        // v0-v2 have no body
        Ok((
            s,
            ApiVersionsRequestData {
                client_software_name: None,
                client_software_version: None,
            },
        ))
    }
}
