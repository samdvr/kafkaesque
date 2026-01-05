//! Admin response encoding (CreateTopics, DeleteTopics, InitProducerId).

use bytes::BufMut;

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

// ============================================================================
// CreateTopics
// ============================================================================

/// CreateTopics response data.
#[derive(Debug, Clone, Default)]
pub struct CreateTopicsResponseData {
    pub throttle_time_ms: i32,
    pub topics: Vec<CreateTopicResponseData>,
}

#[derive(Debug, Clone)]
pub struct CreateTopicResponseData {
    pub name: String,
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
}

impl ToByte for CreateTopicResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        Ok(())
    }
}

impl ToByte for CreateTopicsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        encode_array(buffer, &self.topics)?;
        Ok(())
    }
}

// ============================================================================
// DeleteTopics
// ============================================================================

/// DeleteTopics response data.
#[derive(Debug, Clone, Default)]
pub struct DeleteTopicsResponseData {
    pub throttle_time_ms: i32,
    pub responses: Vec<DeleteTopicResponseData>,
}

#[derive(Debug, Clone)]
pub struct DeleteTopicResponseData {
    pub name: String,
    pub error_code: KafkaCode,
}

impl ToByte for DeleteTopicResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        Ok(())
    }
}

impl ToByte for DeleteTopicsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        encode_array(buffer, &self.responses)?;
        Ok(())
    }
}

// ============================================================================
// ErrorResponse
// ============================================================================

/// Generic error response for unsupported API versions.
#[derive(Debug, Clone)]
pub struct ErrorResponseData {
    pub error_code: KafkaCode,
}

impl ToByte for ErrorResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        Ok(())
    }
}

// ============================================================================
// InitProducerId
// ============================================================================

/// InitProducerId response data.
#[derive(Debug, Clone, Default)]
pub struct InitProducerIdResponseData {
    pub throttle_time_ms: i32,
    pub error_code: KafkaCode,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdResponseData {
    /// Encode for flexible version (v3+).
    pub fn encode_flexible<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        self.producer_id.encode(buffer)?;
        self.producer_epoch.encode(buffer)?;
        // Empty tagged fields (flexible version marker)
        buffer.put_u8(0);
        Ok(())
    }
}

impl ToByte for InitProducerIdResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to flexible encoding for modern clients
        self.encode_flexible(buffer)
    }
}
