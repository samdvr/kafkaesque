//! IncrementalAlterConfigs (key 44) request parsing.
//!
//! Wire format (Kafka protocol spec, IncrementalAlterConfigsRequest;
//! flexible from v1 — not yet supported here):
//!
//! - v0 body:
//!   `resources: ARRAY of (
//!       resource_type: INT8,
//!       resource_name: STRING,
//!       configs: ARRAY of (
//!           name: STRING,
//!           op: INT8,           // 0=Set, 1=Delete, 2=Append, 3=Subtract
//!           value: NULLABLE_STRING
//!       )
//!    )`,
//!   `validate_only: BOOLEAN`
//!
//! Op semantics (KIP-339):
//! - **Set**: replace the existing value with `value`. NULL means "set to
//!   null", which is functionally equivalent to Delete.
//! - **Delete**: remove the key from the config map. `value` is ignored.
//! - **Append**: add `value` to a list-valued config (e.g. cleanup.policy).
//! - **Subtract**: remove `value` from a list-valued config.
//!
//! Append/Subtract only make sense for list-valued configs; we surface
//! them through to the handler so it can validate by key (none of our
//! current keys are list-valued, so the handler rejects them).

use nom::{IResult, number::complete::be_i8};
use nombytes::NomBytes;

use crate::parser::{parse_array, parse_kafka_string, parse_kafka_string_opt};

/// Op type for an incremental config alter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalAlterOp {
    Set = 0,
    Delete = 1,
    Append = 2,
    Subtract = 3,
}

impl IncrementalAlterOp {
    /// Decode the wire byte. Unknown ops are rejected at parse time so the
    /// handler doesn't have to special-case every "what does op=99 mean?"
    /// edge.
    pub fn from_i8(value: i8) -> Option<Self> {
        match value {
            0 => Some(IncrementalAlterOp::Set),
            1 => Some(IncrementalAlterOp::Delete),
            2 => Some(IncrementalAlterOp::Append),
            3 => Some(IncrementalAlterOp::Subtract),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct IncrementalAlterConfigsRequestData {
    pub resources: Vec<IncrementalAlterConfigsResource>,
    pub validate_only: bool,
}

#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsResource {
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<IncrementalAlterConfigsEntry>,
}

#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsEntry {
    pub name: String,
    pub op: IncrementalAlterOp,
    /// `None` is the wire-level null. For Set this means "delete via
    /// null"; for Delete the value is irrelevant; for Append/Subtract a
    /// null is a protocol-level error which the handler rejects.
    pub value: Option<String>,
}

pub fn parse_incremental_alter_configs_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, IncrementalAlterConfigsRequestData> {
    let (s, resources) = parse_array(parse_resource)(s)?;
    let (s, validate_only) = be_i8(s)?;
    Ok((
        s,
        IncrementalAlterConfigsRequestData {
            resources,
            validate_only: validate_only != 0,
        },
    ))
}

fn parse_resource(s: NomBytes) -> IResult<NomBytes, IncrementalAlterConfigsResource> {
    let (s, resource_type) = be_i8(s)?;
    let (s, resource_name) = parse_kafka_string(s)?;
    let (s, configs) = parse_array(parse_entry)(s)?;
    Ok((
        s,
        IncrementalAlterConfigsResource {
            resource_type,
            resource_name,
            configs,
        },
    ))
}

fn parse_entry(s: NomBytes) -> IResult<NomBytes, IncrementalAlterConfigsEntry> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, op_raw) = be_i8(s)?;
    let (s, value) = parse_kafka_string_opt(s)?;
    let op = IncrementalAlterOp::from_i8(op_raw).ok_or_else(|| {
        nom::Err::Failure(nom::error::Error::new(
            s.clone(),
            nom::error::ErrorKind::Verify,
        ))
    })?;
    Ok((s, IncrementalAlterConfigsEntry { name, op, value }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn nb(data: &[u8]) -> NomBytes {
        NomBytes::new(Bytes::copy_from_slice(data))
    }

    fn build_entry(name: &str, op: i8, value: Option<&str>) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(name.len() as i16).to_be_bytes());
        data.extend_from_slice(name.as_bytes());
        data.push(op as u8);
        match value {
            None => data.extend_from_slice(&(-1i16).to_be_bytes()),
            Some(s) => {
                data.extend_from_slice(&(s.len() as i16).to_be_bytes());
                data.extend_from_slice(s.as_bytes());
            }
        }
        data
    }

    fn build_resource(resource_type: i8, name: &str, entries: &[Vec<u8>]) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(resource_type as u8);
        data.extend_from_slice(&(name.len() as i16).to_be_bytes());
        data.extend_from_slice(name.as_bytes());
        data.extend_from_slice(&(entries.len() as i32).to_be_bytes());
        for e in entries {
            data.extend(e);
        }
        data
    }

    #[test]
    fn v0_set_then_delete_round_trip() {
        let entries = vec![
            build_entry("cleanup.policy", 0, Some("compact")),
            build_entry("retention.ms", 1, None),
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // 1 resource
        data.extend(build_resource(2, "orders", &entries));
        data.push(0); // validate_only = false

        let (rest, parsed) = parse_incremental_alter_configs_request(nb(&data), 0).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(parsed.resources.len(), 1);
        let r = &parsed.resources[0];
        assert_eq!(r.resource_type, 2);
        assert_eq!(r.resource_name, "orders");
        assert_eq!(r.configs.len(), 2);
        assert_eq!(r.configs[0].name, "cleanup.policy");
        assert_eq!(r.configs[0].op, IncrementalAlterOp::Set);
        assert_eq!(r.configs[0].value.as_deref(), Some("compact"));
        assert_eq!(r.configs[1].name, "retention.ms");
        assert_eq!(r.configs[1].op, IncrementalAlterOp::Delete);
        assert!(r.configs[1].value.is_none());
        assert!(!parsed.validate_only);
    }

    #[test]
    fn append_and_subtract_decoded() {
        let entries = vec![
            build_entry("compression.types", 2, Some("zstd")),
            build_entry("compression.types", 3, Some("snappy")),
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_resource(2, "t", &entries));
        data.push(0);

        let (_, parsed) = parse_incremental_alter_configs_request(nb(&data), 0).unwrap();
        let cfgs = &parsed.resources[0].configs;
        assert_eq!(cfgs[0].op, IncrementalAlterOp::Append);
        assert_eq!(cfgs[1].op, IncrementalAlterOp::Subtract);
    }

    #[test]
    fn unknown_op_byte_is_rejected() {
        // Unknown op (5) must NOT silently parse — otherwise the handler
        // would have to defend against every i8 instead of trusting the
        // typed enum.
        let entries = vec![build_entry("k", 5, Some("v"))];
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_resource(2, "t", &entries));
        data.push(0);

        let result = parse_incremental_alter_configs_request(nb(&data), 0);
        assert!(result.is_err(), "unknown op must fail");
    }

    #[test]
    fn validate_only_true_round_trips() {
        let mut data = Vec::new();
        data.extend_from_slice(&0i32.to_be_bytes()); // 0 resources
        data.push(1); // validate_only = true
        let (_, parsed) = parse_incremental_alter_configs_request(nb(&data), 0).unwrap();
        assert!(parsed.validate_only);
        assert!(parsed.resources.is_empty());
    }

    #[test]
    fn null_set_value_distinct_from_empty() {
        // Set with NULL value is functionally a delete; Set with "" is
        // setting to literal empty string. Parser must surface both.
        let entries = vec![build_entry("a", 0, None), build_entry("b", 0, Some(""))];
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend(build_resource(2, "t", &entries));
        data.push(0);

        let (_, parsed) = parse_incremental_alter_configs_request(nb(&data), 0).unwrap();
        let cfgs = &parsed.resources[0].configs;
        assert_eq!(cfgs[0].value, None);
        assert_eq!(cfgs[1].value, Some(String::new()));
    }
}
