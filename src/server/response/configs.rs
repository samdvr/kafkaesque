//! Response encoding for DescribeConfigs (key 32) and AlterConfigs (key 33).
//!
//! Wire layouts (Kafka protocol spec):
//!
//! DescribeConfigs response v0–v2 (classic):
//!   throttle_time_ms: INT32
//!   results: ARRAY of:
//!     error_code: INT16
//!     error_message: NULLABLE_STRING
//!     resource_type: INT8
//!     resource_name: STRING
//!     configs: ARRAY of:
//!       name: STRING
//!       value: NULLABLE_STRING
//!       read_only: BOOLEAN
//!       is_default (v0 only) | config_source (v1+, INT8)
//!       is_sensitive: BOOLEAN
//!       synonyms (v1+): ARRAY of (name: STRING, value: NULLABLE_STRING,
//!                                  source: INT8)
//!       config_type (v3+): INT8                     -- not advertised
//!       documentation (v3+): NULLABLE_STRING        -- not advertised
//!
//! AlterConfigs response v0–v1 (classic):
//!   throttle_time_ms: INT32
//!   responses: ARRAY of:
//!     error_code: INT16
//!     error_message: NULLABLE_STRING
//!     resource_type: INT8
//!     resource_name: STRING
//!
//! Both responses use response header v0 (correlation_id only) at every
//! advertised version.

use bytes::BufMut;

use crate::encode::{ToByte, encode_array_len_pub, encode_as_array};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

// ============================================================================
// Config source values (Kafka `ConfigEntry.ConfigSource`).
//
// We only emit two: TOPIC_CONFIG (per-topic override that the operator
// explicitly set) and DEFAULT_CONFIG (the cluster-wide fallback). Other
// values exist (DYNAMIC_BROKER_CONFIG, STATIC_BROKER_CONFIG, …) but the
// Phase-0 surface doesn't need them.
// ============================================================================

pub const CONFIG_SOURCE_TOPIC_CONFIG: i8 = 1;
pub const CONFIG_SOURCE_DEFAULT_CONFIG: i8 = 5;

// ============================================================================
// DescribeConfigs response
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct DescribeConfigsResponseData {
    pub throttle_time_ms: i32,
    pub results: Vec<DescribeConfigsResult>,
}

#[derive(Debug, Clone)]
pub struct DescribeConfigsResult {
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<DescribeConfigsEntry>,
}

#[derive(Debug, Clone)]
pub struct DescribeConfigsEntry {
    pub name: String,
    pub value: Option<String>,
    /// Per Kafka semantics, "the broker enforces this" — for our knobs,
    /// always `false`: every entry can be altered.
    pub read_only: bool,
    /// Source of the value: was it set on the topic, or inherited from the
    /// cluster default? In v0 this maps onto the `is_default` BOOLEAN.
    pub config_source: i8,
    pub is_sensitive: bool,
}

impl DescribeConfigsEntry {
    /// Mark the entry as inherited from the cluster default.
    pub fn from_default(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
            read_only: false,
            config_source: CONFIG_SOURCE_DEFAULT_CONFIG,
            is_sensitive: false,
        }
    }

    /// Mark the entry as a per-topic override.
    pub fn from_topic(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
            read_only: false,
            config_source: CONFIG_SOURCE_TOPIC_CONFIG,
            is_sensitive: false,
        }
    }
}

impl DescribeConfigsResponseData {
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        encode_as_array(buffer, &self.results, |buf, r| {
            (r.error_code as i16).encode(buf)?;
            encode_nullable_string(r.error_message.as_deref(), buf)?;
            r.resource_type.encode(buf)?;
            r.resource_name.encode(buf)?;
            encode_as_array(buf, &r.configs, |buf, e| encode_entry(buf, e, version))?;
            Ok(())
        })?;
        Ok(())
    }
}

impl ToByte for DescribeConfigsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v0 framing — version-aware call sites should use
        // `encode_versioned`. The ApiVersions advertisement caps us at v2,
        // so out-of-band callers asking for the bare ToByte get the most
        // conservative shape.
        self.encode_versioned(buffer, 0)
    }
}

fn encode_entry<W: BufMut>(
    buffer: &mut W,
    entry: &DescribeConfigsEntry,
    version: i16,
) -> Result<()> {
    entry.name.encode(buffer)?;
    encode_nullable_string(entry.value.as_deref(), buffer)?;
    entry.read_only.encode(buffer)?;

    if version >= 1 {
        // v1+ replaces is_default with the typed config_source enum and
        // adds the synonyms array.
        entry.config_source.encode(buffer)?;
        entry.is_sensitive.encode(buffer)?;
        // We don't emit synonyms — the TOPIC_CONFIG → DEFAULT_CONFIG fall-
        // back already conveys "this came from the default" for the cases
        // admin tools care about, and the synonym list adds wire bytes
        // for no Phase-0 benefit. Empty array is the well-formed answer.
        encode_array_len_pub(buffer, 0)?;
    } else {
        // v0: is_default is `true` iff source == DEFAULT_CONFIG.
        let is_default = entry.config_source == CONFIG_SOURCE_DEFAULT_CONFIG;
        is_default.encode(buffer)?;
        entry.is_sensitive.encode(buffer)?;
    }
    Ok(())
}

// ============================================================================
// AlterConfigs response
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct AlterConfigsResponseData {
    pub throttle_time_ms: i32,
    pub responses: Vec<AlterConfigsResult>,
}

#[derive(Debug, Clone)]
pub struct AlterConfigsResult {
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
}

impl ToByte for AlterConfigsResult {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        self.resource_type.encode(buffer)?;
        self.resource_name.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for AlterConfigsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        crate::encode::encode_array(buffer, &self.responses)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BytesMut};

    // ------------------------------------------------------------------
    // DescribeConfigs encoder
    // ------------------------------------------------------------------

    #[test]
    fn describe_configs_v0_emits_is_default_boolean() {
        let resp = DescribeConfigsResponseData {
            throttle_time_ms: 0,
            results: vec![DescribeConfigsResult {
                error_code: KafkaCode::None,
                error_message: None,
                resource_type: 2, // Topic
                resource_name: "events".into(),
                configs: vec![
                    DescribeConfigsEntry::from_default("retention.ms", "604800000"),
                    DescribeConfigsEntry::from_topic("cleanup.policy", "compact"),
                ],
            }],
        };
        let mut buf = BytesMut::new();
        resp.encode_versioned(&mut buf, 0).expect("encode");
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i32(), 0, "throttle_time_ms");
        assert_eq!(bytes.get_i32(), 1, "results array len");
        assert_eq!(bytes.get_i16(), KafkaCode::None as i16);
        assert_eq!(bytes.get_i16(), -1, "null error_message");
        assert_eq!(bytes.get_i8(), 2, "resource_type=Topic");
        assert_eq!(bytes.get_i16(), 6);
        let mut name = vec![0u8; 6];
        bytes.copy_to_slice(&mut name);
        assert_eq!(name, b"events");
        assert_eq!(bytes.get_i32(), 2, "configs array len");
        // First config: retention.ms (default)
        assert_eq!(bytes.get_i16(), "retention.ms".len() as i16);
        bytes.advance("retention.ms".len());
        assert_eq!(bytes.get_i16(), "604800000".len() as i16);
        bytes.advance("604800000".len());
        assert_eq!(bytes.get_u8(), 0, "read_only=false");
        assert_eq!(bytes.get_u8(), 1, "v0 is_default=true (default source)");
        assert_eq!(bytes.get_u8(), 0, "is_sensitive=false");
        // Second config: cleanup.policy (topic override)
        assert_eq!(bytes.get_i16(), "cleanup.policy".len() as i16);
        bytes.advance("cleanup.policy".len());
        assert_eq!(bytes.get_i16(), "compact".len() as i16);
        bytes.advance("compact".len());
        assert_eq!(bytes.get_u8(), 0, "read_only=false");
        assert_eq!(bytes.get_u8(), 0, "v0 is_default=false (topic source)");
        assert_eq!(bytes.get_u8(), 0, "is_sensitive=false");
        assert!(!bytes.has_remaining(), "v0 must end after is_sensitive");
    }

    #[test]
    fn describe_configs_v1_emits_config_source_and_synonyms_array() {
        let resp = DescribeConfigsResponseData {
            throttle_time_ms: 100,
            results: vec![DescribeConfigsResult {
                error_code: KafkaCode::None,
                error_message: None,
                resource_type: 2,
                resource_name: "t".into(),
                configs: vec![DescribeConfigsEntry::from_topic(
                    "cleanup.policy",
                    "compact",
                )],
            }],
        };
        let mut buf = BytesMut::new();
        resp.encode_versioned(&mut buf, 1).expect("encode");
        let mut bytes = buf.freeze();
        // throttle, results.len, error_code, null error_msg, resource_type, name
        assert_eq!(bytes.get_i32(), 100);
        assert_eq!(bytes.get_i32(), 1);
        bytes.advance(2 + 2 + 1 + 2 + "t".len());
        assert_eq!(bytes.get_i32(), 1, "configs array len");
        // entry name + value
        bytes.advance(2 + "cleanup.policy".len() + 2 + "compact".len());
        assert_eq!(bytes.get_u8(), 0, "read_only");
        assert_eq!(
            bytes.get_i8(),
            CONFIG_SOURCE_TOPIC_CONFIG,
            "v1 emits typed config_source"
        );
        assert_eq!(bytes.get_u8(), 0, "is_sensitive");
        assert_eq!(bytes.get_i32(), 0, "v1 emits empty synonyms array");
        assert!(!bytes.has_remaining());
    }

    #[test]
    fn describe_configs_v0_v1_byte_count_differs_by_synonym_array_minus_isdefault() {
        // v1 replaces is_default (BOOL = 1 byte) with config_source
        // (INT8 = 1 byte) and adds an empty synonyms array (INT32 = 4
        // bytes). Net increase per entry: +4 bytes.
        let resp = DescribeConfigsResponseData {
            throttle_time_ms: 0,
            results: vec![DescribeConfigsResult {
                error_code: KafkaCode::None,
                error_message: None,
                resource_type: 2,
                resource_name: "t".into(),
                configs: vec![DescribeConfigsEntry::from_topic("k", "v")],
            }],
        };
        let mut v0 = BytesMut::new();
        resp.encode_versioned(&mut v0, 0).unwrap();
        let mut v1 = BytesMut::new();
        resp.encode_versioned(&mut v1, 1).unwrap();
        assert_eq!(v1.len(), v0.len() + 4);
    }

    #[test]
    fn describe_configs_handles_null_value() {
        // A config explicitly set to null (e.g. "delete this key" intent
        // surfaced via Describe) must round-trip the wire-level null.
        let entry = DescribeConfigsEntry {
            name: "k".into(),
            value: None,
            read_only: false,
            config_source: CONFIG_SOURCE_DEFAULT_CONFIG,
            is_sensitive: false,
        };
        let resp = DescribeConfigsResponseData {
            throttle_time_ms: 0,
            results: vec![DescribeConfigsResult {
                error_code: KafkaCode::None,
                error_message: None,
                resource_type: 2,
                resource_name: "t".into(),
                configs: vec![entry],
            }],
        };
        let mut buf = BytesMut::new();
        resp.encode_versioned(&mut buf, 0).unwrap();
        let mut bytes = buf.freeze();
        // Skip past throttle, array len, error_code, error_msg null, resource_type, resource_name
        bytes.advance(4 + 4 + 2 + 2 + 1 + 2 + "t".len());
        // configs array len + entry name
        bytes.advance(4 + 2 + "k".len());
        // Value: null (-1 i16)
        assert_eq!(bytes.get_i16(), -1, "value=None must encode as -1");
    }

    #[test]
    fn describe_configs_per_resource_error_round_trips() {
        let resp = DescribeConfigsResponseData {
            throttle_time_ms: 0,
            results: vec![DescribeConfigsResult {
                error_code: KafkaCode::UnknownTopicOrPartition,
                error_message: Some("missing".to_string()),
                resource_type: 2,
                resource_name: "ghost".into(),
                configs: vec![],
            }],
        };
        let mut buf = BytesMut::new();
        resp.encode_versioned(&mut buf, 0).unwrap();
        let mut bytes = buf.freeze();
        bytes.advance(4 + 4); // throttle + array len
        assert_eq!(bytes.get_i16(), KafkaCode::UnknownTopicOrPartition as i16);
        assert_eq!(bytes.get_i16(), "missing".len() as i16);
        bytes.advance("missing".len());
        // resource_type, resource_name, empty configs array
        assert_eq!(bytes.get_i8(), 2);
        bytes.advance(2 + "ghost".len());
        assert_eq!(bytes.get_i32(), 0, "configs array empty when erroring");
    }

    // ------------------------------------------------------------------
    // AlterConfigs encoder
    // ------------------------------------------------------------------

    #[test]
    fn alter_configs_response_encodes_throttle_then_results() {
        let resp = AlterConfigsResponseData {
            throttle_time_ms: 0,
            responses: vec![
                AlterConfigsResult {
                    error_code: KafkaCode::None,
                    error_message: None,
                    resource_type: 2,
                    resource_name: "t1".into(),
                },
                AlterConfigsResult {
                    error_code: KafkaCode::InvalidConfig,
                    error_message: Some("bad value".to_string()),
                    resource_type: 2,
                    resource_name: "t2".into(),
                },
            ],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i32(), 0, "throttle");
        assert_eq!(bytes.get_i32(), 2, "responses array");
        // Result 1
        assert_eq!(bytes.get_i16(), KafkaCode::None as i16);
        assert_eq!(bytes.get_i16(), -1, "null error_message");
        assert_eq!(bytes.get_i8(), 2);
        assert_eq!(bytes.get_i16(), 2);
        bytes.advance(2);
        // Result 2
        assert_eq!(bytes.get_i16(), KafkaCode::InvalidConfig as i16);
        assert_eq!(bytes.get_i16(), "bad value".len() as i16);
        bytes.advance("bad value".len());
        assert_eq!(bytes.get_i8(), 2);
        assert_eq!(bytes.get_i16(), 2);
        bytes.advance(2);
        assert!(!bytes.has_remaining());
    }

    #[test]
    fn alter_configs_response_empty_results() {
        let resp = AlterConfigsResponseData {
            throttle_time_ms: 0,
            responses: vec![],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).unwrap();
        // 4 (throttle) + 4 (empty array) = 8 bytes
        assert_eq!(buf.len(), 8);
    }
}
