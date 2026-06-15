//! Offset-related response encoding (ListOffsets, OffsetCommit, OffsetFetch).

use bytes::BufMut;

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

// ============================================================================
// ListOffsets
// ============================================================================

/// ListOffsets response data.
#[derive(Debug, Clone, Default)]
pub struct ListOffsetsResponseData {
    pub throttle_time_ms: i32,
    pub topics: Vec<ListOffsetsTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsTopicResponse {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartitionResponse>,
}

#[derive(Debug, Clone, Default)]
pub struct ListOffsetsPartitionResponse {
    pub partition_index: i32,
    pub error_code: KafkaCode,
    pub timestamp: i64,
    pub offset: i64,
}

impl ListOffsetsPartitionResponse {
    /// Create an error response for a partition.
    ///
    /// Sets timestamp and offset to -1 (invalid).
    pub fn error(partition_index: i32, error_code: KafkaCode) -> Self {
        Self {
            partition_index,
            error_code,
            timestamp: -1,
            offset: -1,
        }
    }

    /// Create a success response for a partition.
    pub fn success(partition_index: i32, offset: i64) -> Self {
        Self {
            partition_index,
            error_code: KafkaCode::None,
            timestamp: -1,
            offset,
        }
    }
}

impl ListOffsetsResponseData {
    /// Encode for a specific API version.
    /// - v0: topics array only
    /// - v1+: throttle_time_ms + topics array
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        if version >= 1 {
            self.throttle_time_ms.encode(buffer)?;
        }
        encode_array(buffer, &self.topics)?;
        Ok(())
    }
}

impl ToByte for ListOffsetsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v1 format
        self.encode_versioned(buffer, 1)
    }
}

impl ToByte for ListOffsetsTopicResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        encode_array(buffer, &self.partitions)?;
        Ok(())
    }
}

impl ToByte for ListOffsetsPartitionResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.partition_index.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        self.timestamp.encode(buffer)?;
        self.offset.encode(buffer)?;
        Ok(())
    }
}

// ============================================================================
// OffsetCommit
// ============================================================================

/// OffsetCommit response data.
#[derive(Debug, Clone, Default)]
pub struct OffsetCommitResponseData {
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetCommitTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetCommitPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitPartitionResponse {
    pub partition_index: i32,
    pub error_code: KafkaCode,
}

impl OffsetCommitPartitionResponse {
    /// Create a new response with the given error code.
    pub fn new(partition_index: i32, error_code: KafkaCode) -> Self {
        Self {
            partition_index,
            error_code,
        }
    }

    /// Create a success response for a partition.
    pub fn success(partition_index: i32) -> Self {
        Self {
            partition_index,
            error_code: KafkaCode::None,
        }
    }
}

impl ToByte for OffsetCommitResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        encode_array(buffer, &self.topics)?;
        Ok(())
    }
}

impl ToByte for OffsetCommitTopicResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        encode_array(buffer, &self.partitions)?;
        Ok(())
    }
}

impl ToByte for OffsetCommitPartitionResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.partition_index.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        Ok(())
    }
}

// ============================================================================
// OffsetFetch
// ============================================================================

/// OffsetFetch response data.
#[derive(Debug, Clone, Default)]
pub struct OffsetFetchResponseData {
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetFetchTopicResponse>,
    pub error_code: KafkaCode,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetFetchPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchPartitionResponse {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub metadata: Option<String>,
    pub error_code: KafkaCode,
}

impl OffsetFetchPartitionResponse {
    /// Create a response for a partition with committed offset.
    pub fn new(partition_index: i32, committed_offset: i64, metadata: Option<String>) -> Self {
        Self {
            partition_index,
            committed_offset,
            metadata,
            error_code: KafkaCode::None,
        }
    }

    /// Create an error response for unknown offset.
    pub fn unknown(partition_index: i32) -> Self {
        Self {
            partition_index,
            committed_offset: -1,
            metadata: None,
            error_code: KafkaCode::None,
        }
    }
}

impl OffsetFetchResponseData {
    /// Encode for a specific API version.
    /// - v0-v1: topics array only
    /// - v2: topics array + error_code
    /// - v3+: throttle_time_ms + topics array + error_code
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        if version >= 3 {
            self.throttle_time_ms.encode(buffer)?;
        }
        encode_array(buffer, &self.topics)?;
        if version >= 2 {
            (self.error_code as i16).encode(buffer)?;
        }
        Ok(())
    }
}

impl ToByte for OffsetFetchResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v1 format (just topics array)
        self.encode_versioned(buffer, 1)
    }
}

impl ToByte for OffsetFetchTopicResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        encode_array(buffer, &self.partitions)?;
        Ok(())
    }
}

impl ToByte for OffsetFetchPartitionResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.partition_index.encode(buffer)?;
        self.committed_offset.encode(buffer)?;
        encode_nullable_string(self.metadata.as_deref(), buffer)?;
        (self.error_code as i16).encode(buffer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // ListOffsets Tests
    // ========================================================================

    #[test]
    fn test_list_offsets_response_encode() {
        let response = ListOffsetsResponseData {
            throttle_time_ms: 100,
            topics: vec![],
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_list_offsets_topic_response_encode() {
        let topic = ListOffsetsTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![],
        };

        let mut buffer = Vec::new();
        topic.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_list_offsets_partition_response_encode() {
        let partition = ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            timestamp: 1234567890,
            offset: 100,
        };

        let mut buffer = Vec::new();
        partition.encode(&mut buffer).unwrap();

        // partition_index (4) + error_code (2) + timestamp (8) + offset (8)
        assert_eq!(buffer.len(), 22);
    }

    #[test]
    fn test_full_list_offsets_response_encode() {
        let response = ListOffsetsResponseData {
            throttle_time_ms: 0,
            topics: vec![ListOffsetsTopicResponse {
                name: "topic1".to_string(),
                partitions: vec![
                    ListOffsetsPartitionResponse::success(0, 100),
                    ListOffsetsPartitionResponse::error(1, KafkaCode::NotLeaderForPartition),
                ],
            }],
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    // ========================================================================
    // OffsetCommit Tests
    // ========================================================================

    #[test]
    fn test_offset_commit_response_encode() {
        let response = OffsetCommitResponseData {
            throttle_time_ms: 50,
            topics: vec![],
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_offset_commit_topic_response_encode() {
        let topic = OffsetCommitTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![],
        };

        let mut buffer = Vec::new();
        topic.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_offset_commit_partition_response_encode() {
        let partition = OffsetCommitPartitionResponse {
            partition_index: 5,
            error_code: KafkaCode::None,
        };

        let mut buffer = Vec::new();
        partition.encode(&mut buffer).unwrap();

        // partition_index (4) + error_code (2)
        assert_eq!(buffer.len(), 6);
    }

    #[test]
    fn test_full_offset_commit_response_encode() {
        let response = OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitTopicResponse {
                name: "topic1".to_string(),
                partitions: vec![
                    OffsetCommitPartitionResponse::success(0),
                    OffsetCommitPartitionResponse::new(1, KafkaCode::IllegalGeneration),
                ],
            }],
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    // ========================================================================
    // OffsetFetch Tests
    // ========================================================================

    #[test]
    fn test_offset_fetch_response_encode() {
        let response = OffsetFetchResponseData {
            throttle_time_ms: 100,
            topics: vec![],
            error_code: KafkaCode::None,
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_offset_fetch_topic_response_encode() {
        let topic = OffsetFetchTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![],
        };

        let mut buffer = Vec::new();
        topic.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_offset_fetch_partition_response_encode() {
        let partition = OffsetFetchPartitionResponse {
            partition_index: 0,
            committed_offset: 12345,
            metadata: Some("test-metadata".to_string()),
            error_code: KafkaCode::None,
        };

        let mut buffer = Vec::new();
        partition.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_offset_fetch_partition_response_encode_no_metadata() {
        let partition = OffsetFetchPartitionResponse::unknown(0);

        let mut buffer = Vec::new();
        partition.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_full_offset_fetch_response_encode() {
        let response = OffsetFetchResponseData {
            throttle_time_ms: 0,
            topics: vec![OffsetFetchTopicResponse {
                name: "topic1".to_string(),
                partitions: vec![
                    OffsetFetchPartitionResponse::new(0, 100, Some("meta".to_string())),
                    OffsetFetchPartitionResponse::unknown(1),
                ],
            }],
            error_code: KafkaCode::None,
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    // ========================================================================
    // Version-specific encoding tests
    // ========================================================================

    #[test]
    fn test_list_offsets_response_v0_no_throttle_time() {
        let response = ListOffsetsResponseData {
            throttle_time_ms: 100, // Should be ignored in v0
            topics: vec![ListOffsetsTopicResponse {
                name: "test-topic".to_string(),
                partitions: vec![ListOffsetsPartitionResponse::success(0, 1000)],
            }],
        };

        let mut buf_v0 = Vec::new();
        response.encode_versioned(&mut buf_v0, 0).unwrap();

        let mut buf_v1 = Vec::new();
        response.encode_versioned(&mut buf_v1, 1).unwrap();

        // v0 should be 4 bytes smaller (no throttle_time_ms)
        assert_eq!(buf_v0.len() + 4, buf_v1.len());

        // v1 should start with throttle_time_ms (100 = 0x00000064)
        assert_eq!(&buf_v1[0..4], &[0, 0, 0, 100]);
    }

    #[test]
    fn test_offset_fetch_response_v0_v1_topics_only() {
        let response = OffsetFetchResponseData {
            throttle_time_ms: 100,
            topics: vec![OffsetFetchTopicResponse {
                name: "test".to_string(),
                partitions: vec![OffsetFetchPartitionResponse::unknown(0)],
            }],
            error_code: KafkaCode::UnknownMemberId, // Should be ignored in v0-v1
        };

        let mut buf_v0 = Vec::new();
        response.encode_versioned(&mut buf_v0, 0).unwrap();

        let mut buf_v1 = Vec::new();
        response.encode_versioned(&mut buf_v1, 1).unwrap();

        let mut buf_v2 = Vec::new();
        response.encode_versioned(&mut buf_v2, 2).unwrap();

        let mut buf_v3 = Vec::new();
        response.encode_versioned(&mut buf_v3, 3).unwrap();

        // v0 and v1 should be the same (just topics array)
        assert_eq!(buf_v0.len(), buf_v1.len());

        // v2 adds error_code (2 bytes)
        assert_eq!(buf_v0.len() + 2, buf_v2.len());

        // v3 adds throttle_time_ms (4 bytes) + error_code (2 bytes)
        assert_eq!(buf_v0.len() + 6, buf_v3.len());
    }

    #[test]
    fn test_offset_fetch_response_v3_includes_throttle_time() {
        let response = OffsetFetchResponseData {
            throttle_time_ms: 100,
            topics: vec![],
            error_code: KafkaCode::None,
        };

        let mut buf_v3 = Vec::new();
        response.encode_versioned(&mut buf_v3, 3).unwrap();

        // v3: throttle_time_ms (4) + topics array (4 for length) + error_code (2) = 10 bytes
        assert_eq!(buf_v3.len(), 10);

        // Should start with throttle_time_ms
        assert_eq!(&buf_v3[0..4], &[0, 0, 0, 100]);
    }
}
