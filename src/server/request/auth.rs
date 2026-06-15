//! SASL authentication request parsing.

use bytes::Bytes;
use nom::{IResult, number::complete::be_i32};
use nombytes::NomBytes;

use crate::constants::MAX_SASL_AUTH_BYTES_SIZE;
use crate::parser::parse_kafka_string;

/// SaslHandshake request data.
#[derive(Debug, Clone)]
pub struct SaslHandshakeRequestData {
    pub mechanism: String,
}

pub fn parse_sasl_handshake_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, SaslHandshakeRequestData> {
    let (s, mechanism) = parse_kafka_string(s)?;

    Ok((s, SaslHandshakeRequestData { mechanism }))
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
    if !(0..=MAX_SASL_AUTH_BYTES_SIZE).contains(&auth_len) {
        return Err(nom::Err::Error(nom::error::Error::new(
            s,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (s, auth_bytes) = nom::bytes::complete::take(auth_len as usize)(s)?;

    Ok((
        s,
        SaslAuthenticateRequestData {
            auth_bytes: auth_bytes.into_bytes(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn valid_handshake_decodes() {
        let mut buf = BytesMut::new();
        buf.put_i16(5);
        buf.put_slice(b"PLAIN");
        let (rest, data) =
            parse_sasl_handshake_request(NomBytes::new(buf.freeze()), 0).expect("valid mechanism");
        assert!(rest.to_bytes().is_empty());
        assert_eq!(data.mechanism, "PLAIN");
    }

    #[test]
    fn handshake_truncated_string_errors() {
        let mut buf = BytesMut::new();
        buf.put_i16(10);
        buf.put_slice(b"abc"); // declares 10 bytes but supplies 3
        assert!(parse_sasl_handshake_request(NomBytes::new(buf.freeze()), 0).is_err());
    }

    #[test]
    fn authenticate_zero_length_decodes() {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        let (rest, data) = parse_sasl_authenticate_request(NomBytes::new(buf.freeze()), 0)
            .expect("zero-length auth bytes accepted");
        assert!(rest.to_bytes().is_empty());
        assert!(data.auth_bytes.is_empty());
    }

    #[test]
    fn authenticate_negative_length_rejected() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1);
        assert!(
            parse_sasl_authenticate_request(NomBytes::new(buf.freeze()), 0).is_err(),
            "negative SASL auth length must be rejected"
        );
    }

    #[test]
    fn authenticate_oversized_length_rejected() {
        let mut buf = BytesMut::new();
        buf.put_i32(MAX_SASL_AUTH_BYTES_SIZE.saturating_add(1));
        assert!(
            parse_sasl_authenticate_request(NomBytes::new(buf.freeze()), 0).is_err(),
            "auth length above MAX_SASL_AUTH_BYTES_SIZE must be rejected pre-allocation"
        );
    }

    #[test]
    fn authenticate_truncated_payload_errors() {
        let mut buf = BytesMut::new();
        buf.put_i32(8);
        buf.put_slice(b"abc"); // declares 8 bytes but supplies 3
        assert!(parse_sasl_authenticate_request(NomBytes::new(buf.freeze()), 0).is_err());
    }
}
