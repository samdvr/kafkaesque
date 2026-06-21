//! CreatePartitions (key 37) response encoding.
//!
//! Wire layout (Kafka protocol spec, CreatePartitionsResponse;
//! flexible from v2 — not yet emitted here):
//!
//! - v0–v1:
//!   `throttle_time_ms: INT32`,
//!   `results: ARRAY of (
//!       name: STRING,
//!       error_code: INT16,
//!       error_message: NULLABLE_STRING)`

use bytes::BufMut;

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

#[derive(Debug, Clone, Default)]
pub struct CreatePartitionsResponseData {
    pub throttle_time_ms: i32,
    pub results: Vec<CreatePartitionsTopicResult>,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsTopicResult {
    pub name: String,
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
}

impl ToByte for CreatePartitionsTopicResult {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        Ok(())
    }
}

impl ToByte for CreatePartitionsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        encode_array(buffer, &self.results)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BytesMut};

    #[test]
    fn encodes_throttle_then_results_array() {
        let resp = CreatePartitionsResponseData {
            throttle_time_ms: 50,
            results: vec![CreatePartitionsTopicResult {
                name: "events".into(),
                error_code: KafkaCode::None,
                error_message: None,
            }],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).expect("encode");
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i32(), 50, "throttle_time_ms");
        assert_eq!(bytes.get_i32(), 1, "results array len");
        assert_eq!(bytes.get_i16(), 6); // name len
        bytes.advance(6);
        assert_eq!(bytes.get_i16(), KafkaCode::None as i16);
        assert_eq!(bytes.get_i16(), -1, "null error_message");
    }

    #[test]
    fn encodes_error_message_when_set() {
        let resp = CreatePartitionsResponseData {
            throttle_time_ms: 0,
            results: vec![CreatePartitionsTopicResult {
                name: "t".into(),
                error_code: KafkaCode::InvalidPartitions,
                error_message: Some("count must be > current".to_string()),
            }],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        bytes.advance(4 + 4 + 2 + 1); // throttle + array len + name len + name
        assert_eq!(bytes.get_i16(), KafkaCode::InvalidPartitions as i16);
        assert_eq!(bytes.get_i16(), "count must be > current".len() as i16);
    }
}
