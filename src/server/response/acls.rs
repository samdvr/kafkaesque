//! DescribeAcls / CreateAcls / DeleteAcls (keys 29–31) response encoding.
//!
//! Non-flexible v0/v1. v1 inserts `pattern_type` after the resource name on
//! Describe resources and Delete matching ACLs. CreateAcls results are
//! identical across v0/v1.

use bytes::BufMut;

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

// ---------------------------------------------------------------------------
// CreateAcls
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct CreateAclsResponseData {
    pub throttle_time_ms: i32,
    pub results: Vec<AclCreationResult>,
}

#[derive(Debug, Clone)]
pub struct AclCreationResult {
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
}

impl ToByte for AclCreationResult {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        Ok(())
    }
}

impl ToByte for CreateAclsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        encode_array(buffer, &self.results)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DescribeAcls
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DescribeAclsResponseData {
    pub throttle_time_ms: i32,
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
    pub resources: Vec<DescribeAclsResource>,
}

#[derive(Debug, Clone)]
pub struct DescribeAclsResource {
    pub resource_type: i8,
    pub resource_name: String,
    pub pattern_type: i8,
    pub acls: Vec<DescribeAclEntry>,
}

#[derive(Debug, Clone)]
pub struct DescribeAclEntry {
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission_type: i8,
}

impl DescribeAclsResponseData {
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        (self.resources.len() as i32).encode(buffer)?;
        for r in &self.resources {
            r.encode_versioned(buffer, version)?;
        }
        Ok(())
    }
}

impl DescribeAclsResource {
    fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        self.resource_type.encode(buffer)?;
        self.resource_name.encode(buffer)?;
        if version >= 1 {
            self.pattern_type.encode(buffer)?;
        }
        (self.acls.len() as i32).encode(buffer)?;
        for a in &self.acls {
            a.encode(buffer)?;
        }
        Ok(())
    }
}

impl ToByte for DescribeAclEntry {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.principal.encode(buffer)?;
        self.host.encode(buffer)?;
        self.operation.encode(buffer)?;
        self.permission_type.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for DescribeAclsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v1 (includes pattern_type) for callers that don't
        // pass a version through `encode_versioned`.
        self.encode_versioned(buffer, 1)
    }
}

// ---------------------------------------------------------------------------
// DeleteAcls
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DeleteAclsResponseData {
    pub throttle_time_ms: i32,
    pub filter_results: Vec<DeleteAclsFilterResult>,
}

#[derive(Debug, Clone)]
pub struct DeleteAclsFilterResult {
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
    pub matching_acls: Vec<DeleteAclsMatchingAcl>,
}

#[derive(Debug, Clone)]
pub struct DeleteAclsMatchingAcl {
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
    pub pattern_type: i8,
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission_type: i8,
}

impl DeleteAclsResponseData {
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        (self.filter_results.len() as i32).encode(buffer)?;
        for f in &self.filter_results {
            f.encode_versioned(buffer, version)?;
        }
        Ok(())
    }
}

impl DeleteAclsFilterResult {
    fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        (self.matching_acls.len() as i32).encode(buffer)?;
        for a in &self.matching_acls {
            a.encode_versioned(buffer, version)?;
        }
        Ok(())
    }
}

impl DeleteAclsMatchingAcl {
    fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        self.resource_type.encode(buffer)?;
        self.resource_name.encode(buffer)?;
        if version >= 1 {
            self.pattern_type.encode(buffer)?;
        }
        self.principal.encode(buffer)?;
        self.host.encode(buffer)?;
        self.operation.encode(buffer)?;
        self.permission_type.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for DeleteAclsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.encode_versioned(buffer, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BytesMut};

    #[test]
    fn create_acls_encodes_per_creation_results() {
        let resp = CreateAclsResponseData {
            throttle_time_ms: 0,
            results: vec![
                AclCreationResult {
                    error_code: KafkaCode::None,
                    error_message: None,
                },
                AclCreationResult {
                    error_code: KafkaCode::InvalidRequest,
                    error_message: Some("bad".into()),
                },
            ],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i32(), 0);
        assert_eq!(bytes.get_i32(), 2);
        assert_eq!(bytes.get_i16(), 0);
        assert_eq!(bytes.get_i16(), -1);
        assert_eq!(bytes.get_i16(), KafkaCode::InvalidRequest as i16);
        assert_eq!(bytes.get_i16(), 3);
        bytes.advance(3);
        assert!(!bytes.has_remaining());
    }

    #[test]
    fn describe_v0_omits_pattern_type() {
        let resp = DescribeAclsResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            error_message: None,
            resources: vec![DescribeAclsResource {
                resource_type: 2,
                resource_name: "t".into(),
                pattern_type: 3,
                acls: vec![],
            }],
        };
        let mut v0 = BytesMut::new();
        let mut v1 = BytesMut::new();
        resp.encode_versioned(&mut v0, 0).unwrap();
        resp.encode_versioned(&mut v1, 1).unwrap();
        assert_eq!(v1.len(), v0.len() + 1, "v1 adds one pattern_type byte");
    }
}
