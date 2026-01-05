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
