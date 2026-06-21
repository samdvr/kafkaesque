//! Request parsing for DescribeConfigs (key 32) and AlterConfigs (key 33).
//!
//! Both APIs support v0–v2 in the classic (non-flexible) wire format.
//! v3+ adds `IncrementalAlterConfigs`-style operations which we do not
//! yet implement — see `SUPPORTED_VERSIONS` in `versions.rs`.
//!
//! # Wire format
//!
//! DescribeConfigs (per Kafka `DescribeConfigsRequest.json`):
//! - v0 body: `resources: ARRAY of (resource_type: INT8, resource_name:
//!   STRING, configuration_keys: NULLABLE_ARRAY of STRING)`
//! - v1 appends `include_synonyms: BOOLEAN` after the array.
//! - v2 appends `include_documentation: BOOLEAN` after `include_synonyms`.
//!
//! AlterConfigs (`AlterConfigsRequest.json`):
//! - v0–v1 body: `resources: ARRAY of (resource_type: INT8, resource_name:
//!   STRING, configs: ARRAY of (name: STRING, value: NULLABLE_STRING))`,
//!   followed by `validate_only: BOOLEAN`.
//!
//! Both APIs use request header v1 (no tagged fields) at every supported
//! version — they do not transition to flexible until v4 / v2 respectively,
//! which we don't advertise.

use nom::{
    IResult,
    number::complete::{be_i8, be_i32},
};
use nombytes::NomBytes;

use crate::parser::{parse_array, parse_kafka_string, parse_kafka_string_opt};

/// Kafka [`ConfigResource.Type`] enum value carried over the wire as INT8.
///
/// We carry the raw byte through the parser so the handler can map it to
/// its own enum (or reject unknown types with a typed error). Modeling it
/// as `i8` rather than a Rust enum keeps unknown values addressable —
/// admin tools occasionally send unsupported types and Kafka's contract is
/// to error per-resource, not at the wire layer.
pub const RESOURCE_TYPE_TOPIC: i8 = 2;
pub const RESOURCE_TYPE_BROKER: i8 = 4;

// ============================================================================
// DescribeConfigs
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct DescribeConfigsRequestData {
    pub resources: Vec<DescribeConfigsResource>,
    /// v1+. Carried for round-trip but currently ignored — we don't
    /// advertise alternate config keys yet.
    pub include_synonyms: bool,
    /// v2+. Likewise carried but ignored.
    pub include_documentation: bool,
}

#[derive(Debug, Clone)]
pub struct DescribeConfigsResource {
    pub resource_type: i8,
    pub resource_name: String,
    /// `None` here is the v0+ wire-level NULL (no filter — return every
    /// recognized config). An explicit empty array `Some(vec![])` requests
    /// no keys at all and is honored as such.
    pub configuration_keys: Option<Vec<String>>,
}

pub fn parse_describe_configs_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, DescribeConfigsRequestData> {
    let (s, resources) = parse_array(parse_describe_configs_resource)(s)?;
    let (s, include_synonyms) = if version >= 1 {
        let (s, v) = be_i8(s)?;
        (s, v != 0)
    } else {
        (s, false)
    };
    let (s, include_documentation) = if version >= 2 {
        let (s, v) = be_i8(s)?;
        (s, v != 0)
    } else {
        (s, false)
    };
    Ok((
        s,
        DescribeConfigsRequestData {
            resources,
            include_synonyms,
            include_documentation,
        },
    ))
}

fn parse_describe_configs_resource(s: NomBytes) -> IResult<NomBytes, DescribeConfigsResource> {
    let (s, resource_type) = be_i8(s)?;
    let (s, resource_name) = parse_kafka_string(s)?;
    let (s, configuration_keys) = parse_nullable_string_array(s)?;
    Ok((
        s,
        DescribeConfigsResource {
            resource_type,
            resource_name,
            configuration_keys,
        },
    ))
}

/// Parse a NULLABLE_ARRAY of STRING preserving the null/empty distinction.
///
/// `parse_nullable_array` in the protocol crate collapses `length == -1`
/// into an empty `Vec`, but the DescribeConfigs schema treats those as
/// different requests:
/// - `null` → return every recognized config (filter omitted)
/// - `[]`   → return zero configs (an explicit, unusual choice)
/// We need both, so this is a local re-implementation.
fn parse_nullable_string_array(s: NomBytes) -> IResult<NomBytes, Option<Vec<String>>> {
    let (mut rest, length) = be_i32(s)?;
    if length == -1 {
        return Ok((rest, None));
    }
    if length < 0 {
        // Any other negative value is a protocol error.
        return Err(nom::Err::Failure(nom::error::Error::new(
            rest,
            nom::error::ErrorKind::Verify,
        )));
    }
    let mut out = Vec::with_capacity(length as usize);
    for _ in 0..length {
        let (r, item) = parse_kafka_string(rest)?;
        out.push(item);
        rest = r;
    }
    Ok((rest, Some(out)))
}

// ============================================================================
// AlterConfigs
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct AlterConfigsRequestData {
    pub resources: Vec<AlterConfigsResource>,
    /// When true, the broker must validate the request without applying
    /// it. Real Kafka returns the same per-resource shape either way.
    pub validate_only: bool,
}

#[derive(Debug, Clone)]
pub struct AlterConfigsResource {
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<AlterConfigsEntry>,
}

#[derive(Debug, Clone)]
pub struct AlterConfigsEntry {
    pub name: String,
    /// `None` is the wire-level NULL value, which Kafka treats as "delete
    /// this key from the config map". Distinct from `Some("")`, which
    /// stores the literal empty string.
    pub value: Option<String>,
}

pub fn parse_alter_configs_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, AlterConfigsRequestData> {
    let (s, resources) = parse_array(parse_alter_configs_resource)(s)?;
    let (s, validate_only) = be_i8(s)?;
    Ok((
        s,
        AlterConfigsRequestData {
            resources,
            validate_only: validate_only != 0,
        },
    ))
}

fn parse_alter_configs_resource(s: NomBytes) -> IResult<NomBytes, AlterConfigsResource> {
    let (s, resource_type) = be_i8(s)?;
    let (s, resource_name) = parse_kafka_string(s)?;
    let (s, configs) = parse_array(parse_alter_configs_entry)(s)?;
    Ok((
        s,
        AlterConfigsResource {
            resource_type,
            resource_name,
            configs,
        },
    ))
}

fn parse_alter_configs_entry(s: NomBytes) -> IResult<NomBytes, AlterConfigsEntry> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, value) = parse_kafka_string_opt(s)?;
    Ok((s, AlterConfigsEntry { name, value }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nb(data: &[u8]) -> NomBytes {
        NomBytes::from(data)
    }

    // ------------------------------------------------------------------
    // DescribeConfigs
    // ------------------------------------------------------------------

    fn build_describe_resource(
        resource_type: i8,
        name: &str,
        keys: Option<&[&str]>,
    ) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(resource_type as u8);
        data.extend_from_slice(&(name.len() as i16).to_be_bytes());
        data.extend_from_slice(name.as_bytes());
        match keys {
            None => data.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(arr) => {
                data.extend_from_slice(&(arr.len() as i32).to_be_bytes());
                for k in arr {
                    data.extend_from_slice(&(k.len() as i16).to_be_bytes());
                    data.extend_from_slice(k.as_bytes());
                }
            }
        }
        data
    }

    #[test]
    fn describe_configs_v0_minimal_request() {
        // Single topic, configuration_keys = null (return every key)
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // resources array length
        data.extend(build_describe_resource(RESOURCE_TYPE_TOPIC, "events", None));

        let (rest, parsed) = parse_describe_configs_request(nb(&data), 0).unwrap();
        assert!(rest.into_bytes().is_empty(), "v0 must consume entire body");
        assert_eq!(parsed.resources.len(), 1);
        assert_eq!(parsed.resources[0].resource_type, RESOURCE_TYPE_TOPIC);
        assert_eq!(parsed.resources[0].resource_name, "events");
        assert!(parsed.resources[0].configuration_keys.is_none());
        assert!(!parsed.include_synonyms, "v0 default must be false");
        assert!(!parsed.include_documentation, "v0 default must be false");
    }

    #[test]
    fn describe_configs_v0_with_specific_keys() {
        // configuration_keys = ["retention.ms", "cleanup.policy"]
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_describe_resource(
            RESOURCE_TYPE_TOPIC,
            "t",
            Some(&["retention.ms", "cleanup.policy"]),
        ));

        let (_, parsed) = parse_describe_configs_request(nb(&data), 0).unwrap();
        let keys = parsed.resources[0].configuration_keys.as_ref().unwrap();
        assert_eq!(keys, &vec!["retention.ms".to_string(), "cleanup.policy".to_string()]);
    }

    #[test]
    fn describe_configs_v0_empty_key_filter_distinguished_from_null() {
        // configuration_keys = [] (request NO keys) is distinct from
        // configuration_keys = null (request ALL keys). The wire format
        // must round-trip both, so the parser must NOT collapse them.
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_describe_resource(RESOURCE_TYPE_TOPIC, "t", Some(&[])));
        let (_, parsed) = parse_describe_configs_request(nb(&data), 0).unwrap();
        let keys = parsed.resources[0].configuration_keys.as_ref().unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn describe_configs_v1_consumes_include_synonyms_byte() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_describe_resource(RESOURCE_TYPE_TOPIC, "t", None));
        data.push(1); // include_synonyms = true

        let (rest, parsed) = parse_describe_configs_request(nb(&data), 1).unwrap();
        assert!(rest.into_bytes().is_empty(), "v1 must consume the trailing byte");
        assert!(parsed.include_synonyms);
        assert!(!parsed.include_documentation);
    }

    #[test]
    fn describe_configs_v2_consumes_both_trailing_bytes() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_describe_resource(RESOURCE_TYPE_TOPIC, "t", None));
        data.push(0); // include_synonyms = false
        data.push(1); // include_documentation = true

        let (rest, parsed) = parse_describe_configs_request(nb(&data), 2).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert!(!parsed.include_synonyms);
        assert!(parsed.include_documentation);
    }

    #[test]
    fn describe_configs_multiple_resources() {
        // Mixed-type batch: a topic + a broker.
        let mut data = Vec::new();
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend(build_describe_resource(RESOURCE_TYPE_TOPIC, "topic-a", None));
        data.extend(build_describe_resource(RESOURCE_TYPE_BROKER, "0", None));

        let (_, parsed) = parse_describe_configs_request(nb(&data), 0).unwrap();
        assert_eq!(parsed.resources.len(), 2);
        assert_eq!(parsed.resources[0].resource_type, RESOURCE_TYPE_TOPIC);
        assert_eq!(parsed.resources[0].resource_name, "topic-a");
        assert_eq!(parsed.resources[1].resource_type, RESOURCE_TYPE_BROKER);
        assert_eq!(parsed.resources[1].resource_name, "0");
    }

    #[test]
    fn describe_configs_unknown_resource_type_is_carried_through() {
        // Real Kafka clients occasionally send resource types we don't
        // implement (broker_logger=8, group=16). The parser must not
        // error — the handler returns INVALID_CONFIG per-resource.
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_describe_resource(99, "what", None));
        let (_, parsed) = parse_describe_configs_request(nb(&data), 0).unwrap();
        assert_eq!(parsed.resources[0].resource_type, 99);
    }

    // ------------------------------------------------------------------
    // AlterConfigs
    // ------------------------------------------------------------------

    fn build_alter_resource(
        resource_type: i8,
        name: &str,
        configs: &[(&str, Option<&str>)],
    ) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(resource_type as u8);
        data.extend_from_slice(&(name.len() as i16).to_be_bytes());
        data.extend_from_slice(name.as_bytes());
        data.extend_from_slice(&(configs.len() as i32).to_be_bytes());
        for (k, v) in configs {
            data.extend_from_slice(&(k.len() as i16).to_be_bytes());
            data.extend_from_slice(k.as_bytes());
            match v {
                None => data.extend_from_slice(&(-1i16).to_be_bytes()),
                Some(s) => {
                    data.extend_from_slice(&(s.len() as i16).to_be_bytes());
                    data.extend_from_slice(s.as_bytes());
                }
            }
        }
        data
    }

    #[test]
    fn alter_configs_v0_set_a_value() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_alter_resource(
            RESOURCE_TYPE_TOPIC,
            "events",
            &[("cleanup.policy", Some("compact"))],
        ));
        data.push(0); // validate_only = false

        let (rest, parsed) = parse_alter_configs_request(nb(&data), 0).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert!(!parsed.validate_only);
        assert_eq!(parsed.resources.len(), 1);
        assert_eq!(parsed.resources[0].resource_name, "events");
        assert_eq!(parsed.resources[0].configs.len(), 1);
        assert_eq!(parsed.resources[0].configs[0].name, "cleanup.policy");
        assert_eq!(
            parsed.resources[0].configs[0].value,
            Some("compact".to_string())
        );
    }

    #[test]
    fn alter_configs_validate_only_round_trips() {
        let mut data = Vec::new();
        data.extend_from_slice(&0i32.to_be_bytes()); // empty resources
        data.push(1); // validate_only = true
        let (_, parsed) = parse_alter_configs_request(nb(&data), 0).unwrap();
        assert!(parsed.validate_only);
        assert!(parsed.resources.is_empty());
    }

    #[test]
    fn alter_configs_null_value_is_a_delete_signal_distinct_from_empty() {
        // value = null and value = "" are different operations:
        //   - null  → delete the key
        //   - ""    → set the key to the empty string
        // The parser MUST preserve the distinction.
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_alter_resource(
            RESOURCE_TYPE_TOPIC,
            "t",
            &[("a", None), ("b", Some(""))],
        ));
        data.push(0);

        let (_, parsed) = parse_alter_configs_request(nb(&data), 0).unwrap();
        let configs = &parsed.resources[0].configs;
        assert_eq!(configs[0].name, "a");
        assert_eq!(configs[0].value, None, "null must decode as None");
        assert_eq!(configs[1].name, "b");
        assert_eq!(
            configs[1].value,
            Some(String::new()),
            "empty string must decode as Some(\"\")"
        );
    }

    #[test]
    fn alter_configs_v1_uses_same_layout_as_v0() {
        // v1 differs only in synonym semantics (handled at the response
        // side); the request body shape is unchanged.
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_alter_resource(
            RESOURCE_TYPE_TOPIC,
            "t",
            &[("retention.ms", Some("60000"))],
        ));
        data.push(0);

        let (rest, parsed) = parse_alter_configs_request(nb(&data), 1).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(parsed.resources[0].configs[0].value, Some("60000".to_string()));
    }

    #[test]
    fn alter_configs_multiple_resources_and_keys() {
        let mut data = Vec::new();
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend(build_alter_resource(
            RESOURCE_TYPE_TOPIC,
            "t1",
            &[
                ("cleanup.policy", Some("compact")),
                ("retention.ms", Some("1000")),
            ],
        ));
        data.extend(build_alter_resource(
            RESOURCE_TYPE_TOPIC,
            "t2",
            &[("cleanup.policy", Some("delete"))],
        ));
        data.push(0);

        let (_, parsed) = parse_alter_configs_request(nb(&data), 0).unwrap();
        assert_eq!(parsed.resources.len(), 2);
        assert_eq!(parsed.resources[0].configs.len(), 2);
        assert_eq!(parsed.resources[1].configs.len(), 1);
    }

    #[test]
    fn alter_configs_empty_resource_array() {
        let mut data = Vec::new();
        data.extend_from_slice(&0i32.to_be_bytes());
        data.push(0);
        let (rest, parsed) = parse_alter_configs_request(nb(&data), 0).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert!(parsed.resources.is_empty());
    }
}
