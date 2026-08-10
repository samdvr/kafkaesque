//! DescribeAcls / CreateAcls / DeleteAcls (keys 29–31) request parsing.
//!
//! Non-flexible wire layouts (v0 / v1). v1 inserts `resource_pattern_type`
//! (Create) or `pattern_type_filter` (Describe / Delete) after the resource
//! name. Flexible v2+ is not advertised.

use nom::{IResult, number::complete::be_i8};
use nombytes::NomBytes;

use crate::parser::{parse_array, parse_kafka_string, parse_kafka_string_opt};

/// DescribeAcls request body.
#[derive(Debug, Clone, Default)]
pub struct DescribeAclsRequestData {
    pub resource_type: i8,
    pub resource_name: Option<String>,
    /// v1+; v0 parsers force this to Literal (3).
    pub pattern_type: i8,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: i8,
    pub permission_type: i8,
}

/// CreateAcls request body.
#[derive(Debug, Clone, Default)]
pub struct CreateAclsRequestData {
    pub creations: Vec<AclCreation>,
}

#[derive(Debug, Clone)]
pub struct AclCreation {
    pub resource_type: i8,
    pub resource_name: String,
    /// v1+; v0 parsers force this to Literal (3).
    pub pattern_type: i8,
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission_type: i8,
}

/// DeleteAcls request body.
#[derive(Debug, Clone, Default)]
pub struct DeleteAclsRequestData {
    pub filters: Vec<AclDeletionFilter>,
}

#[derive(Debug, Clone)]
pub struct AclDeletionFilter {
    pub resource_type: i8,
    pub resource_name: Option<String>,
    /// v1+; v0 parsers force this to Literal (3) for name filters, or Any (1)
    /// when the name is null — callers map wire values into domain filters.
    pub pattern_type: i8,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: i8,
    pub permission_type: i8,
}

pub fn parse_describe_acls_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, DescribeAclsRequestData> {
    let (s, resource_type) = be_i8(s)?;
    let (s, resource_name) = parse_kafka_string_opt(s)?;
    let (s, pattern_type) = if version >= 1 {
        be_i8(s)?
    } else {
        // Kafka v0 DescribeAcls has no pattern field; Literal is the only
        // pattern that existed then.
        (s, 3i8)
    };
    let (s, principal) = parse_kafka_string_opt(s)?;
    let (s, host) = parse_kafka_string_opt(s)?;
    let (s, operation) = be_i8(s)?;
    let (s, permission_type) = be_i8(s)?;
    Ok((
        s,
        DescribeAclsRequestData {
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        },
    ))
}

pub fn parse_create_acls_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, CreateAclsRequestData> {
    let (s, creations) = parse_array(|input| parse_creation(input, version))(s)?;
    Ok((s, CreateAclsRequestData { creations }))
}

fn parse_creation(s: NomBytes, version: i16) -> IResult<NomBytes, AclCreation> {
    let (s, resource_type) = be_i8(s)?;
    let (s, resource_name) = parse_kafka_string(s)?;
    let (s, pattern_type) = if version >= 1 { be_i8(s)? } else { (s, 3i8) };
    let (s, principal) = parse_kafka_string(s)?;
    let (s, host) = parse_kafka_string(s)?;
    let (s, operation) = be_i8(s)?;
    let (s, permission_type) = be_i8(s)?;
    Ok((
        s,
        AclCreation {
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        },
    ))
}

pub fn parse_delete_acls_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, DeleteAclsRequestData> {
    let (s, filters) = parse_array(|input| parse_deletion_filter(input, version))(s)?;
    Ok((s, DeleteAclsRequestData { filters }))
}

fn parse_deletion_filter(s: NomBytes, version: i16) -> IResult<NomBytes, AclDeletionFilter> {
    let (s, resource_type) = be_i8(s)?;
    let (s, resource_name) = parse_kafka_string_opt(s)?;
    let (s, pattern_type) = if version >= 1 { be_i8(s)? } else { (s, 3i8) };
    let (s, principal) = parse_kafka_string_opt(s)?;
    let (s, host) = parse_kafka_string_opt(s)?;
    let (s, operation) = be_i8(s)?;
    let (s, permission_type) = be_i8(s)?;
    Ok((
        s,
        AclDeletionFilter {
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn nb(data: &[u8]) -> NomBytes {
        NomBytes::new(Bytes::copy_from_slice(data))
    }

    #[test]
    fn describe_v0_defaults_pattern_to_literal() {
        let mut data = Vec::new();
        data.push(2); // Topic
        data.extend_from_slice(&(-1i16).to_be_bytes()); // null name
        data.extend_from_slice(&(-1i16).to_be_bytes()); // null principal
        data.extend_from_slice(&(-1i16).to_be_bytes()); // null host
        data.push(3); // Read
        data.push(3); // Allow
        let (rest, parsed) = parse_describe_acls_request(nb(&data), 0).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(parsed.resource_type, 2);
        assert_eq!(parsed.pattern_type, 3);
        assert_eq!(parsed.operation, 3);
        assert_eq!(parsed.permission_type, 3);
    }

    #[test]
    fn create_v1_reads_pattern_type() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.push(2); // Topic
        data.extend_from_slice(&(b"orders".len() as i16).to_be_bytes());
        data.extend_from_slice(b"orders");
        data.push(4); // Prefixed
        data.extend_from_slice(&(b"User:alice".len() as i16).to_be_bytes());
        data.extend_from_slice(b"User:alice");
        data.extend_from_slice(&(b"*".len() as i16).to_be_bytes());
        data.extend_from_slice(b"*");
        data.push(4); // Write
        data.push(3); // Allow
        let (rest, parsed) = parse_create_acls_request(nb(&data), 1).unwrap();
        assert!(rest.into_bytes().is_empty());
        assert_eq!(parsed.creations.len(), 1);
        let c = &parsed.creations[0];
        assert_eq!(c.resource_name, "orders");
        assert_eq!(c.pattern_type, 4);
        assert_eq!(c.principal, "User:alice");
    }
}
