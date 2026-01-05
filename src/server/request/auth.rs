//! SASL authentication request parsing.

use bytes::Bytes;
use nom::{IResult, number::complete::be_i32};
use nombytes::NomBytes;

use crate::parser::parse_string;

/// SaslHandshake request data.
#[derive(Debug, Clone)]
pub struct SaslHandshakeRequestData {
    pub mechanism: String,
}

pub fn parse_sasl_handshake_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, SaslHandshakeRequestData> {
    let (s, mechanism) = parse_string(s)?;

    Ok((
        s,
        SaslHandshakeRequestData {
            mechanism: String::from_utf8_lossy(&mechanism).to_string(),
        },
    ))
}

/// SaslAuthenticate request data.
#[derive(Debug, Clone)]
pub struct SaslAuthenticateRequestData {
    pub auth_bytes: Bytes,
}

pub fn parse_sasl_authenticate_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, SaslAuthenticateRequestData> {
    let (s, auth_len) = be_i32(s)?;
    let (s, auth_bytes) = nom::bytes::complete::take(auth_len as usize)(s)?;

    Ok((
        s,
        SaslAuthenticateRequestData {
            auth_bytes: auth_bytes.into_bytes(),
        },
    ))
}
