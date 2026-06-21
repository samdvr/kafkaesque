//! IncrementalAlterConfigs (key 44) response encoding.
//!
//! Wire layout (Kafka protocol spec, IncrementalAlterConfigsResponse;
//! flexible from v1 — not yet emitted here):
//!
//! - v0:
//!   `throttle_time_ms: INT32`,
//!   `responses: ARRAY of (
//!       error_code: INT16,
//!       error_message: NULLABLE_STRING,
//!       resource_type: INT8,
//!       resource_name: STRING)`
//!
//! Identical body shape to AlterConfigs response v0 — but Kafka treats
//! them as separate APIs and will fall back from one to the other only
//! through the version-negotiation table, never by reusing the response.

use bytes::BufMut;

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

#[derive(Debug, Clone, Default)]
pub struct IncrementalAlterConfigsResponseData {
    pub throttle_time_ms: i32,
    pub responses: Vec<IncrementalAlterConfigsResult>,
}

#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsResult {
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
}

impl ToByte for IncrementalAlterConfigsResult {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        self.resource_type.encode(buffer)?;
        self.resource_name.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for IncrementalAlterConfigsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        encode_array(buffer, &self.responses)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BytesMut};

    #[test]
    fn encodes_per_resource_results() {
        let resp = IncrementalAlterConfigsResponseData {
            throttle_time_ms: 0,
            responses: vec![
                IncrementalAlterConfigsResult {
                    error_code: KafkaCode::None,
                    error_message: None,
                    resource_type: 2,
                    resource_name: "t1".into(),
                },
                IncrementalAlterConfigsResult {
                    error_code: KafkaCode::InvalidConfig,
                    error_message: Some("bad value".into()),
                    resource_type: 2,
                    resource_name: "t2".into(),
                },
            ],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i32(), 0);
        assert_eq!(bytes.get_i32(), 2, "responses array len");
        // Result 1 — happy path with null error_message
        assert_eq!(bytes.get_i16(), KafkaCode::None as i16);
        assert_eq!(bytes.get_i16(), -1);
        assert_eq!(bytes.get_i8(), 2);
        assert_eq!(bytes.get_i16(), 2);
        bytes.advance(2);
        // Result 2 — error path
        assert_eq!(bytes.get_i16(), KafkaCode::InvalidConfig as i16);
        assert_eq!(bytes.get_i16(), "bad value".len() as i16);
        bytes.advance("bad value".len());
        assert_eq!(bytes.get_i8(), 2);
        assert_eq!(bytes.get_i16(), 2);
        bytes.advance(2);
        assert!(!bytes.has_remaining());
    }

    #[test]
    fn empty_responses() {
        let resp = IncrementalAlterConfigsResponseData {
            throttle_time_ms: 0,
            responses: vec![],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).unwrap();
        // 4 (throttle) + 4 (empty array) = 8 bytes
        assert_eq!(buf.len(), 8);
    }
}
