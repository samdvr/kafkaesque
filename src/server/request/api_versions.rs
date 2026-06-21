//! API versions request parsing.

use nom::IResult;
use nom::error::ErrorKind;
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

        // Distinguish wire-null (legitimate) from invalid UTF-8 (malformed):
        // collapsing the latter to None is indistinguishable from the former
        // and lets a probing client smuggle non-UTF-8 bytes past the parser.
        let name = match name {
            None => None,
            Some(b) => match std::str::from_utf8(&b) {
                Ok(s) => Some(s.to_string()),
                Err(_) => {
                    return Err(nom::Err::Failure(nom::error::Error::new(
                        s,
                        ErrorKind::Verify,
                    )));
                }
            },
        };
        let ver = match ver {
            None => None,
            Some(b) => match std::str::from_utf8(&b) {
                Ok(s) => Some(s.to_string()),
                Err(_) => {
                    return Err(nom::Err::Failure(nom::error::Error::new(
                        s,
                        ErrorKind::Verify,
                    )));
                }
            },
        };

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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, Bytes, BytesMut};

    fn put_compact_str(buf: &mut BytesMut, s: Option<&str>) {
        match s {
            None => buf.put_u8(0), // unsigned varint 0 = null in compact-nullable
            Some(s) => {
                let len = (s.len() + 1) as u64;
                let mut v = len;
                while v >= 0x80 {
                    buf.put_u8(((v & 0x7F) | 0x80) as u8);
                    v >>= 7;
                }
                buf.put_u8(v as u8);
                buf.put_slice(s.as_bytes());
            }
        }
    }

    #[test]
    fn v0_through_v2_have_empty_body() {
        for v in 0..=2i16 {
            let (rest, data) = parse_api_versions_request(NomBytes::new(Bytes::new()), v)
                .unwrap_or_else(|_| panic!("v{v} must accept empty body"));
            assert!(rest.to_bytes().is_empty());
            assert!(data.client_software_name.is_none());
            assert!(data.client_software_version.is_none());
        }
    }

    #[test]
    fn v3_decodes_software_name_and_version() {
        let mut buf = BytesMut::new();
        put_compact_str(&mut buf, Some("librdkafka"));
        put_compact_str(&mut buf, Some("2.3.0"));
        buf.put_u8(0); // tagged-fields length 0
        let (rest, data) =
            parse_api_versions_request(NomBytes::new(buf.freeze()), 3).expect("valid v3");
        assert!(rest.to_bytes().is_empty());
        assert_eq!(data.client_software_name.as_deref(), Some("librdkafka"));
        assert_eq!(data.client_software_version.as_deref(), Some("2.3.0"));
    }

    #[test]
    fn v3_null_software_name_is_none_not_error() {
        let mut buf = BytesMut::new();
        put_compact_str(&mut buf, None);
        put_compact_str(&mut buf, None);
        buf.put_u8(0);
        let (rest, data) = parse_api_versions_request(NomBytes::new(buf.freeze()), 3)
            .expect("null compact strings legal in v3");
        assert!(rest.to_bytes().is_empty());
        assert!(data.client_software_name.is_none());
        assert!(data.client_software_version.is_none());
    }

    #[test]
    fn v3_invalid_utf8_software_name_rejected() {
        let mut buf = BytesMut::new();
        // length+1 = 3 → 1-byte varint = 3
        buf.put_u8(3);
        buf.put_slice(&[0xFF, 0xFE]); // not valid UTF-8
        put_compact_str(&mut buf, Some("v"));
        buf.put_u8(0);
        assert!(
            parse_api_versions_request(NomBytes::new(buf.freeze()), 3).is_err(),
            "invalid UTF-8 must not collapse to None"
        );
    }

    #[test]
    fn v3_truncated_input_returns_parse_error() {
        let bytes = Bytes::from_static(&[]);
        assert!(parse_api_versions_request(NomBytes::new(bytes), 3).is_err());
    }
}
