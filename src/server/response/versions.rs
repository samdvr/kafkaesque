//! API versions response encoding.

use bytes::BufMut;

use crate::encode::{ToByte, encode_array, encode_compact_array, encode_empty_tagged_fields};
use crate::error::{KafkaCode, Result};

use super::super::request::ApiKey;

/// ApiVersions response data.
#[derive(Debug, Clone)]
pub struct ApiVersionsResponseData {
    pub error_code: KafkaCode,
    pub api_keys: Vec<ApiVersionData>,
    pub throttle_time_ms: i32,
}

#[derive(Debug, Clone)]
pub struct ApiVersionData {
    pub api_key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiVersionData {
    /// Encode for flexible version (v3+).
    /// Each entry has tagged fields at the end.
    fn encode_flexible<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        let api_key_i16: i16 = self.api_key.into();
        api_key_i16.encode(buffer)?;
        self.min_version.encode(buffer)?;
        self.max_version.encode(buffer)?;
        // Empty tagged fields for this struct
        encode_empty_tagged_fields(buffer);
        Ok(())
    }
}

impl ToByte for ApiVersionData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        let api_key_i16: i16 = self.api_key.into();
        api_key_i16.encode(buffer)?;
        self.min_version.encode(buffer)?;
        self.max_version.encode(buffer)?;
        Ok(())
    }
}

impl ApiVersionsResponseData {
    /// Encode for a specific API version.
    /// - v0: error_code + api_keys array (NO throttle_time_ms)
    /// - v1-v2: error_code + api_keys array + throttle_time_ms
    /// - v3+: use encode_flexible() instead
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, api_version: i16) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        encode_array(buffer, &self.api_keys)?;
        // throttle_time_ms was added in v1
        if api_version >= 1 {
            self.throttle_time_ms.encode(buffer)?;
        }
        Ok(())
    }

    /// Encode for flexible version (v3+).
    /// Uses compact arrays and includes tagged fields.
    ///
    /// ApiVersions v3 response format (per Kafka protocol schema):
    /// - error_code: INT16
    /// - api_keys: COMPACT_ARRAY of ApiVersionData
    /// - throttle_time_ms: INT32
    /// - TAG_BUFFER (containing optional tagged fields)
    ///
    /// Note: SupportedFeatures (tag 0), FinalizedFeaturesEpoch (tag 1),
    /// FinalizedFeatures (tag 2), and ZkMigrationReady (tag 3) are OPTIONAL
    /// tagged fields that go in the TAG_BUFFER section, not in the main body.
    /// We omit them by using an empty TAG_BUFFER (0x00).
    pub fn encode_flexible<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;

        // Compact array of api versions
        encode_compact_array(buffer, &self.api_keys, |buf, item| {
            item.encode_flexible(buf)
        })?;

        self.throttle_time_ms.encode(buffer)?;

        // Empty tagged fields (SupportedFeatures, FinalizedFeaturesEpoch,
        // FinalizedFeatures, ZkMigrationReady are optional and omitted)
        encode_empty_tagged_fields(buffer);
        Ok(())
    }
}

impl ToByte for ApiVersionsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        encode_array(buffer, &self.api_keys)?;
        self.throttle_time_ms.encode(buffer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_versions_response_flexible_encoding() {
        // Create a simple response with 2 APIs
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![
                ApiVersionData {
                    api_key: ApiKey::Produce, // 0
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersionData {
                    api_key: ApiKey::Fetch, // 1
                    min_version: 0,
                    max_version: 4,
                },
            ],
            throttle_time_ms: 0,
        };

        let mut body = Vec::new();
        response.encode_flexible(&mut body).unwrap();

        // Expected format (flexible ApiVersions v3 response body):
        // error_code: INT16 (2 bytes) = 0x00 0x00
        // api_keys: COMPACT_ARRAY
        //   - length+1 as varint: 3 (2 items + 1) = 0x03
        //   - item 0: api_key=0, min=0, max=3, tagged_fields=0
        //   - item 1: api_key=1, min=0, max=4, tagged_fields=0
        // throttle_time_ms: INT32 (4 bytes) = 0x00 0x00 0x00 0x00
        // tagged_fields: varint(0) = 0x00
        //
        // Note: SupportedFeatures, FinalizedFeaturesEpoch, FinalizedFeatures
        // are OPTIONAL tagged fields (tag 0, 1, 2) that we omit.

        let expected: Vec<u8> = vec![
            // error_code
            0x00, 0x00, // compact array length+1 = 3
            0x03, // item 0: Produce (api_key=0, min=0, max=3, tagged_fields=0)
            0x00, 0x00, // api_key = 0
            0x00, 0x00, // min_version = 0
            0x00, 0x03, // max_version = 3
            0x00, // tagged_fields
            // item 1: Fetch (api_key=1, min=0, max=4, tagged_fields=0)
            0x00, 0x01, // api_key = 1
            0x00, 0x00, // min_version = 0
            0x00, 0x04, // max_version = 4
            0x00, // tagged_fields
            // throttle_time_ms
            0x00, 0x00, 0x00, 0x00, // tagged_fields (empty - no SupportedFeatures, etc.)
            0x00,
        ];

        assert_eq!(
            body, expected,
            "Flexible encoding mismatch.\nActual: {:02x?}\nExpected: {:02x?}",
            body, expected
        );
    }

    #[test]
    fn test_api_version_data_encode() {
        let data = ApiVersionData {
            api_key: ApiKey::Metadata, // 3
            min_version: 0,
            max_version: 9,
        };

        let mut buf = Vec::new();
        data.encode(&mut buf).unwrap();

        // Standard encoding: api_key (2) + min_version (2) + max_version (2) = 6 bytes
        assert_eq!(buf.len(), 6);
        assert_eq!(&buf[0..2], &[0x00, 0x03]); // api_key = 3
        assert_eq!(&buf[2..4], &[0x00, 0x00]); // min_version = 0
        assert_eq!(&buf[4..6], &[0x00, 0x09]); // max_version = 9
    }

    #[test]
    fn test_api_version_data_encode_flexible() {
        let data = ApiVersionData {
            api_key: ApiKey::Metadata, // 3
            min_version: 0,
            max_version: 9,
        };

        let mut buf = Vec::new();
        data.encode_flexible(&mut buf).unwrap();

        // Flexible encoding: api_key (2) + min_version (2) + max_version (2) + tagged_fields (1) = 7 bytes
        assert_eq!(buf.len(), 7);
        assert_eq!(&buf[0..2], &[0x00, 0x03]); // api_key = 3
        assert_eq!(&buf[2..4], &[0x00, 0x00]); // min_version = 0
        assert_eq!(&buf[4..6], &[0x00, 0x09]); // max_version = 9
        assert_eq!(buf[6], 0x00); // empty tagged_fields
    }

    #[test]
    fn test_api_versions_response_v0_no_throttle_time() {
        // v0 should NOT include throttle_time_ms
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 3,
            }],
            throttle_time_ms: 100, // This should NOT be included in v0
        };

        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 0).unwrap();

        // v0 format: error_code (2) + array_length (4) + 1 item * 6 bytes = 12 bytes
        // NO throttle_time_ms
        assert_eq!(buf.len(), 12);
        assert_eq!(&buf[0..2], &[0x00, 0x00]); // error_code = 0
        assert_eq!(&buf[2..6], &[0x00, 0x00, 0x00, 0x01]); // array length = 1
        // item: api_key=0, min=0, max=3
        assert_eq!(&buf[6..8], &[0x00, 0x00]); // api_key = 0
        assert_eq!(&buf[8..10], &[0x00, 0x00]); // min_version = 0
        assert_eq!(&buf[10..12], &[0x00, 0x03]); // max_version = 3
    }

    #[test]
    fn test_api_versions_response_v1_with_throttle_time() {
        // v1+ SHOULD include throttle_time_ms
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 3,
            }],
            throttle_time_ms: 100,
        };

        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 1).unwrap();

        // v1+ format: error_code (2) + array_length (4) + 1 item * 6 bytes + throttle_time_ms (4) = 16 bytes
        assert_eq!(buf.len(), 16);
        assert_eq!(&buf[0..2], &[0x00, 0x00]); // error_code = 0
        assert_eq!(&buf[2..6], &[0x00, 0x00, 0x00, 0x01]); // array length = 1
        // item: api_key=0, min=0, max=3
        assert_eq!(&buf[6..8], &[0x00, 0x00]); // api_key = 0
        assert_eq!(&buf[8..10], &[0x00, 0x00]); // min_version = 0
        assert_eq!(&buf[10..12], &[0x00, 0x03]); // max_version = 3
        // throttle_time_ms = 100 = 0x00000064
        assert_eq!(&buf[12..16], &[0x00, 0x00, 0x00, 0x64]);
    }

    #[test]
    fn test_api_versions_response_v2_with_throttle_time() {
        // v2 SHOULD include throttle_time_ms (same as v1)
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Fetch,
                min_version: 0,
                max_version: 11,
            }],
            throttle_time_ms: 250,
        };

        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 2).unwrap();

        // v2 format: error_code (2) + array_length (4) + 1 item * 6 bytes + throttle_time_ms (4) = 16 bytes
        assert_eq!(buf.len(), 16);
        assert_eq!(&buf[0..2], &[0x00, 0x00]); // error_code = 0
        assert_eq!(&buf[2..6], &[0x00, 0x00, 0x00, 0x01]); // array length = 1
        // item: api_key=1, min=0, max=11
        assert_eq!(&buf[6..8], &[0x00, 0x01]); // api_key = 1 (Fetch)
        assert_eq!(&buf[8..10], &[0x00, 0x00]); // min_version = 0
        assert_eq!(&buf[10..12], &[0x00, 0x0B]); // max_version = 11
        // throttle_time_ms = 250 = 0x000000FA
        assert_eq!(&buf[12..16], &[0x00, 0x00, 0x00, 0xFA]);
    }

    #[test]
    fn test_api_versions_response_v0_empty_array() {
        // v0 with empty api_keys array
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 0).unwrap();

        // v0 format: error_code (2) + array_length (4) = 6 bytes
        assert_eq!(buf.len(), 6);
        assert_eq!(&buf[0..2], &[0x00, 0x00]); // error_code = 0
        assert_eq!(&buf[2..6], &[0x00, 0x00, 0x00, 0x00]); // array length = 0
    }

    #[test]
    fn test_api_versions_response_v1_empty_array() {
        // v1 with empty api_keys array should still include throttle_time_ms
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![],
            throttle_time_ms: 500,
        };

        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 1).unwrap();

        // v1 format: error_code (2) + array_length (4) + throttle_time_ms (4) = 10 bytes
        assert_eq!(buf.len(), 10);
        assert_eq!(&buf[0..2], &[0x00, 0x00]); // error_code = 0
        assert_eq!(&buf[2..6], &[0x00, 0x00, 0x00, 0x00]); // array length = 0
        // throttle_time_ms = 500 = 0x000001F4
        assert_eq!(&buf[6..10], &[0x00, 0x00, 0x01, 0xF4]);
    }

    #[test]
    fn test_api_versions_response_with_error_code() {
        // Test with error code (UnsupportedVersion)
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::UnsupportedVersion,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 0).unwrap();

        // error_code = 35 (UnsupportedVersion) = 0x0023
        assert_eq!(&buf[0..2], &[0x00, 0x23]);
    }

    #[test]
    fn test_api_versions_response_v0_multiple_apis() {
        // v0 with multiple APIs
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![
                ApiVersionData {
                    api_key: ApiKey::Produce,
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersionData {
                    api_key: ApiKey::Fetch,
                    min_version: 0,
                    max_version: 13,
                },
                ApiVersionData {
                    api_key: ApiKey::ListOffsets,
                    min_version: 0,
                    max_version: 7,
                },
                ApiVersionData {
                    api_key: ApiKey::Metadata,
                    min_version: 0,
                    max_version: 12,
                },
            ],
            throttle_time_ms: 0, // Ignored in v0
        };

        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 0).unwrap();

        // v0 format: error_code (2) + array_length (4) + 4 items * 6 bytes = 30 bytes
        assert_eq!(buf.len(), 30);
        assert_eq!(&buf[0..2], &[0x00, 0x00]); // error_code = 0
        assert_eq!(&buf[2..6], &[0x00, 0x00, 0x00, 0x04]); // array length = 4

        // Verify each API
        // Item 0: Produce (0, 0, 9)
        assert_eq!(&buf[6..8], &[0x00, 0x00]); // api_key = 0
        assert_eq!(&buf[8..10], &[0x00, 0x00]); // min = 0
        assert_eq!(&buf[10..12], &[0x00, 0x09]); // max = 9

        // Item 1: Fetch (1, 0, 13)
        assert_eq!(&buf[12..14], &[0x00, 0x01]); // api_key = 1
        assert_eq!(&buf[14..16], &[0x00, 0x00]); // min = 0
        assert_eq!(&buf[16..18], &[0x00, 0x0D]); // max = 13

        // Item 2: ListOffsets (2, 0, 7)
        assert_eq!(&buf[18..20], &[0x00, 0x02]); // api_key = 2
        assert_eq!(&buf[20..22], &[0x00, 0x00]); // min = 0
        assert_eq!(&buf[22..24], &[0x00, 0x07]); // max = 7

        // Item 3: Metadata (3, 0, 12)
        assert_eq!(&buf[24..26], &[0x00, 0x03]); // api_key = 3
        assert_eq!(&buf[26..28], &[0x00, 0x00]); // min = 0
        assert_eq!(&buf[28..30], &[0x00, 0x0C]); // max = 12
    }

    #[test]
    fn test_api_versions_response_flexible_with_throttle() {
        // v3 flexible with non-zero throttle_time_ms
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 9,
            }],
            throttle_time_ms: 1000,
        };

        let mut buf = Vec::new();
        response.encode_flexible(&mut buf).unwrap();

        // Verify throttle_time_ms is at correct position
        // error_code (2) + compact_array_len (1) + 1 item (7) = 10 bytes, then throttle_time_ms
        // throttle_time_ms = 1000 = 0x000003E8
        assert_eq!(&buf[10..14], &[0x00, 0x00, 0x03, 0xE8]);
    }

    #[test]
    fn test_api_versions_response_flexible_empty_array() {
        // v3 flexible with empty api_keys array
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let mut buf = Vec::new();
        response.encode_flexible(&mut buf).unwrap();

        // Format:
        // error_code (2) + compact_array_len=1 (1) + throttle_time_ms (4) + tagged_fields (1) = 8 bytes
        let expected: Vec<u8> = vec![
            // error_code
            0x00, 0x00, // compact array length+1 = 1 (0 items)
            0x01, // throttle_time_ms = 0
            0x00, 0x00, 0x00, 0x00, // tagged_fields (empty)
            0x00,
        ];

        assert_eq!(buf, expected);
    }

    #[test]
    fn test_api_versions_response_flexible_with_error() {
        // v3 flexible with error code
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::UnsupportedVersion,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let mut buf = Vec::new();
        response.encode_flexible(&mut buf).unwrap();

        // error_code = 35 (UnsupportedVersion) = 0x0023
        assert_eq!(&buf[0..2], &[0x00, 0x23]);
    }

    #[test]
    fn test_api_versions_response_flexible_many_apis() {
        // v3 flexible with many APIs (test compact array encoding)
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![
                ApiVersionData {
                    api_key: ApiKey::Produce,
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersionData {
                    api_key: ApiKey::Fetch,
                    min_version: 0,
                    max_version: 13,
                },
                ApiVersionData {
                    api_key: ApiKey::ListOffsets,
                    min_version: 0,
                    max_version: 7,
                },
                ApiVersionData {
                    api_key: ApiKey::Metadata,
                    min_version: 0,
                    max_version: 12,
                },
                ApiVersionData {
                    api_key: ApiKey::OffsetCommit,
                    min_version: 0,
                    max_version: 8,
                },
                ApiVersionData {
                    api_key: ApiKey::OffsetFetch,
                    min_version: 0,
                    max_version: 8,
                },
                ApiVersionData {
                    api_key: ApiKey::FindCoordinator,
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersionData {
                    api_key: ApiKey::JoinGroup,
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersionData {
                    api_key: ApiKey::Heartbeat,
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersionData {
                    api_key: ApiKey::LeaveGroup,
                    min_version: 0,
                    max_version: 5,
                },
            ],
            throttle_time_ms: 0,
        };

        let mut buf = Vec::new();
        response.encode_flexible(&mut buf).unwrap();

        // Verify compact array length = 10 + 1 = 11 (0x0B)
        assert_eq!(buf[2], 0x0B);

        // Calculate expected size:
        // error_code (2) + compact_array_len (1) + 10 items * 7 bytes (70) +
        // throttle_time_ms (4) + tagged_fields (1) = 78 bytes
        assert_eq!(buf.len(), 78);
    }

    #[test]
    fn test_api_version_data_with_non_zero_min_version() {
        // Test encoding with non-zero min_version
        let data = ApiVersionData {
            api_key: ApiKey::Fetch,
            min_version: 4,
            max_version: 13,
        };

        let mut buf = Vec::new();
        data.encode(&mut buf).unwrap();

        assert_eq!(&buf[0..2], &[0x00, 0x01]); // api_key = 1 (Fetch)
        assert_eq!(&buf[2..4], &[0x00, 0x04]); // min_version = 4
        assert_eq!(&buf[4..6], &[0x00, 0x0D]); // max_version = 13
    }

    #[test]
    fn test_api_versions_response_tobyte_trait() {
        // Test the ToByte trait implementation (backward compat)
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 3,
            }],
            throttle_time_ms: 50,
        };

        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // ToByte always includes throttle_time_ms (like v1+)
        // error_code (2) + array_length (4) + 1 item * 6 bytes + throttle_time_ms (4) = 16 bytes
        assert_eq!(buf.len(), 16);
        assert_eq!(&buf[12..16], &[0x00, 0x00, 0x00, 0x32]); // throttle_time_ms = 50
    }

    #[test]
    fn test_api_version_data_clone() {
        let data = ApiVersionData {
            api_key: ApiKey::Metadata,
            min_version: 1,
            max_version: 12,
        };

        let cloned = data.clone();
        assert_eq!(cloned.api_key, ApiKey::Metadata);
        assert_eq!(cloned.min_version, 1);
        assert_eq!(cloned.max_version, 12);
    }

    #[test]
    fn test_api_versions_response_data_clone() {
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 9,
            }],
            throttle_time_ms: 100,
        };

        let cloned = response.clone();
        assert_eq!(cloned.error_code, KafkaCode::None);
        assert_eq!(cloned.api_keys.len(), 1);
        assert_eq!(cloned.throttle_time_ms, 100);
    }

    #[test]
    fn test_api_versions_size_consistency_v0_vs_v1() {
        // Verify that v0 is exactly 4 bytes smaller than v1 (no throttle_time_ms)
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![
                ApiVersionData {
                    api_key: ApiKey::Produce,
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersionData {
                    api_key: ApiKey::Fetch,
                    min_version: 0,
                    max_version: 13,
                },
            ],
            throttle_time_ms: 100,
        };

        let mut buf_v0 = Vec::new();
        response.encode_versioned(&mut buf_v0, 0).unwrap();

        let mut buf_v1 = Vec::new();
        response.encode_versioned(&mut buf_v1, 1).unwrap();

        // v1 should be exactly 4 bytes larger (throttle_time_ms)
        assert_eq!(buf_v1.len() - buf_v0.len(), 4);
    }

    #[test]
    fn test_api_versions_v1_v2_identical_format() {
        // v1 and v2 should produce identical output
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Metadata,
                min_version: 0,
                max_version: 12,
            }],
            throttle_time_ms: 42,
        };

        let mut buf_v1 = Vec::new();
        response.encode_versioned(&mut buf_v1, 1).unwrap();

        let mut buf_v2 = Vec::new();
        response.encode_versioned(&mut buf_v2, 2).unwrap();

        assert_eq!(buf_v1, buf_v2, "v1 and v2 should have identical encoding");
    }
}
