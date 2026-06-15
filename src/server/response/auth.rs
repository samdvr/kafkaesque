//! SASL authentication response encoding.

use bytes::{BufMut, Bytes};

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

/// SaslHandshake response data.
#[derive(Debug, Clone)]
pub struct SaslHandshakeResponseData {
    pub error_code: KafkaCode,
    pub mechanisms: Vec<String>,
}

impl ToByte for SaslHandshakeResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        encode_array(buffer, &self.mechanisms)?;
        Ok(())
    }
}

/// SaslAuthenticate response data.
#[derive(Debug, Clone)]
pub struct SaslAuthenticateResponseData {
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
    pub auth_bytes: Bytes,
}

impl ToByte for SaslAuthenticateResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        (self.auth_bytes.len() as i32).encode(buffer)?;
        buffer.put(self.auth_bytes.as_ref());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BytesMut};

    #[test]
    fn handshake_response_encodes_error_code_then_mechanisms() {
        let resp = SaslHandshakeResponseData {
            error_code: KafkaCode::None,
            mechanisms: vec!["PLAIN".into(), "SCRAM-SHA-256".into()],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).expect("encode");
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i16(), 0, "error_code first");
        assert_eq!(bytes.get_i32(), 2, "mechanisms array length");
        assert_eq!(bytes.get_i16(), 5, "first mechanism length");
        let mut name = vec![0u8; 5];
        bytes.copy_to_slice(&mut name);
        assert_eq!(name, b"PLAIN");
    }

    #[test]
    fn handshake_response_encodes_unsupported_mechanism_error() {
        let resp = SaslHandshakeResponseData {
            error_code: KafkaCode::UnsupportedSaslMechanism,
            mechanisms: vec!["SCRAM-SHA-256".into()],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).expect("encode");
        let bytes = buf.freeze();
        let code = i16::from_be_bytes([bytes[0], bytes[1]]);
        assert_eq!(code, KafkaCode::UnsupportedSaslMechanism as i16);
    }

    #[test]
    fn authenticate_response_with_empty_auth_bytes_writes_zero_length() {
        let resp = SaslAuthenticateResponseData {
            error_code: KafkaCode::None,
            error_message: None,
            auth_bytes: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).expect("encode");
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i16(), 0, "error_code");
        assert_eq!(bytes.get_i16(), -1, "null error_message");
        assert_eq!(bytes.get_i32(), 0, "auth_bytes length");
        assert!(bytes.is_empty());
    }

    #[test]
    fn authenticate_response_failure_carries_message_and_bytes() {
        let resp = SaslAuthenticateResponseData {
            error_code: KafkaCode::SaslAuthenticationFailed,
            error_message: Some("bad creds".into()),
            auth_bytes: Bytes::from_static(b"resp"),
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).expect("encode");
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i16(), KafkaCode::SaslAuthenticationFailed as i16);
        let msg_len = bytes.get_i16();
        assert_eq!(msg_len, "bad creds".len() as i16);
        let mut msg = vec![0u8; msg_len as usize];
        bytes.copy_to_slice(&mut msg);
        assert_eq!(msg, b"bad creds");
        assert_eq!(bytes.get_i32(), 4, "auth_bytes length = 4");
        assert_eq!(bytes.as_ref(), b"resp");
    }
}
