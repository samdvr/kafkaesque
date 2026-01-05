//! Validation utilities for Kafka identifiers.
//!
//! This module centralizes validation logic for topic names, group IDs, and other
//! identifiers used throughout the cluster. All validation follows Apache Kafka's
//! naming conventions while being slightly more restrictive for cross-platform
//! compatibility with object stores.
//!
//! # Usage
//!
//! ```
//! use kafkaesque::cluster::{validate_topic_name, validate_group_id};
//!
//! // Validate before using identifiers
//! assert!(validate_topic_name("my-topic").is_ok());
//! assert!(validate_group_id("my-consumer-group").is_ok());
//!
//! // Invalid examples
//! assert!(validate_topic_name("").is_err());
//! assert!(validate_group_id("invalid/name").is_err());
//! ```
//!
//! # Validation Rules
//!
//! All identifiers must:
//! - Not be empty
//! - Not exceed their maximum length (topic: 249, group: 255 chars)
//! - Contain only ASCII alphanumeric characters, dots (`.`), underscores (`_`), and hyphens (`-`)
//! - Not contain ASCII control characters
//! - Not be "." or ".." (reserved filesystem names)
//! - Not start with a hyphen (could be interpreted as CLI flags)

use super::error::{SlateDBError, SlateDBResult};
use crate::constants::MAX_MEMBER_METADATA_SIZE;

/// Maximum length for topic names (Kafka's MAX_NAME_LENGTH).
pub const MAX_TOPIC_NAME_LENGTH: usize = 249;

/// Maximum length for consumer group IDs.
pub const MAX_GROUP_ID_LENGTH: usize = 255;

/// Validate a topic name following Apache Kafka's naming conventions.
///
/// Topic names must:
/// - Not be empty
/// - Be at most 249 characters (Kafka's MAX_NAME_LENGTH)
/// - Contain only ASCII alphanumeric characters, dots, underscores, and hyphens
/// - Not contain ASCII control characters
/// - Not be "." or ".." (reserved filesystem names that could cause path traversal)
///
/// # Reserved Names
///
/// Topics starting with "__" (double underscore) are reserved for internal use
/// in Apache Kafka (e.g., __consumer_offsets, __transaction_state). This
/// implementation allows creating such topics but users should avoid them.
///
/// This validation should be called at the handler layer for all incoming
/// topic names to ensure consistent validation across all operations.
///
/// # Examples
///
/// ```
/// use kafkaesque::cluster::validate_topic_name;
///
/// assert!(validate_topic_name("my-topic").is_ok());
/// assert!(validate_topic_name("MyTopic_v1").is_ok());
/// assert!(validate_topic_name("topic.prod").is_ok());
///
/// // Invalid examples
/// assert!(validate_topic_name("").is_err());        // empty
/// assert!(validate_topic_name(".").is_err());       // reserved
/// assert!(validate_topic_name("topic/name").is_err()); // invalid char
/// ```
pub fn validate_topic_name(topic: &str) -> SlateDBResult<()> {
    validate_identifier(topic, MAX_TOPIC_NAME_LENGTH, "Topic name")
}

/// Validate a consumer group ID.
///
/// Group IDs must:
/// - Not be empty
/// - Be at most 255 characters
/// - Contain only alphanumeric characters, dots, underscores, and hyphens
///
/// This validation should be called at the handler layer for all incoming
/// group IDs to ensure consistent validation across all operations.
///
/// # Examples
///
/// ```
/// use kafkaesque::cluster::validate_group_id;
///
/// assert!(validate_group_id("my-consumer-group").is_ok());
/// assert!(validate_group_id("group_v1").is_ok());
///
/// // Invalid examples
/// assert!(validate_group_id("").is_err());           // empty
/// assert!(validate_group_id("group name").is_err()); // space
/// ```
pub fn validate_group_id(group_id: &str) -> SlateDBResult<()> {
    validate_identifier(group_id, MAX_GROUP_ID_LENGTH, "Group ID")
}

/// Validate consumer group member metadata size.
///
/// Member metadata is limited to prevent excessive memory usage during
/// consumer group rebalancing.
///
/// # Arguments
///
/// * `metadata` - The raw metadata bytes from the client
///
/// # Returns
///
/// * `Ok(())` if metadata size is within limits
/// * `Err(SlateDBError::Config)` if metadata exceeds MAX_MEMBER_METADATA_SIZE
pub fn validate_member_metadata(metadata: &[u8]) -> SlateDBResult<()> {
    if metadata.len() > MAX_MEMBER_METADATA_SIZE {
        return Err(SlateDBError::Config(format!(
            "Member metadata too large ({} bytes, max {} bytes)",
            metadata.len(),
            MAX_MEMBER_METADATA_SIZE
        )));
    }
    Ok(())
}

/// Core identifier validation logic.
///
/// This function validates identifiers (topic names, group IDs, etc.) against
/// common rules that apply to all Kafka identifiers.
///
/// # Arguments
///
/// * `value` - The identifier to validate
/// * `max_len` - Maximum allowed length
/// * `field_name` - Name of the field for error messages (e.g., "Topic name")
fn validate_identifier(value: &str, max_len: usize, field_name: &str) -> SlateDBResult<()> {
    // Check for empty
    if value.is_empty() {
        return Err(SlateDBError::Config(format!(
            "{} cannot be empty",
            field_name
        )));
    }

    // Check length limit
    if value.len() > max_len {
        return Err(SlateDBError::Config(format!(
            "{} '{}' is too long ({} chars, max {} chars)",
            field_name,
            truncate_for_display(value, 50),
            value.len(),
            max_len
        )));
    }

    // Reject reserved filesystem names that could cause path issues
    if value == "." || value == ".." {
        return Err(SlateDBError::Config(format!(
            "{} cannot be '.' or '..' (reserved names)",
            field_name
        )));
    }

    // Reject names starting with hyphen (could be interpreted as CLI flags)
    if value.starts_with('-') {
        return Err(SlateDBError::Config(format!(
            "{} '{}' cannot start with a hyphen",
            field_name,
            truncate_for_display(value, 50)
        )));
    }

    // Check each character
    for (i, c) in value.chars().enumerate() {
        // Reject ASCII control characters (0x00-0x1F and 0x7F)
        if c.is_ascii_control() {
            return Err(SlateDBError::Config(format!(
                "{} contains control character at position {} (byte value 0x{:02X})",
                field_name, i, c as u8
            )));
        }

        // Only allow ASCII alphanumeric, dots, underscores, and hyphens
        // This is more restrictive than Kafka (which allows Unicode) but safer
        // for cross-platform compatibility with object stores
        let is_valid = c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-';
        if !is_valid {
            return Err(SlateDBError::Config(format!(
                "Invalid character '{}' (U+{:04X}) in {} at position {}. \
                 Only ASCII letters, digits, '.', '_', and '-' are allowed.",
                c.escape_default(),
                c as u32,
                field_name,
                i
            )));
        }
    }

    Ok(())
}

/// Truncate a string for display in error messages.
///
/// This prevents overly long identifiers from cluttering error output.
fn truncate_for_display(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_topic_name_valid_cases() {
        // Standard valid names
        assert!(validate_topic_name("my-topic").is_ok());
        assert!(validate_topic_name("MyTopic").is_ok());
        assert!(validate_topic_name("my_topic").is_ok());
        assert!(validate_topic_name("my.topic").is_ok());
        assert!(validate_topic_name("topic123").is_ok());
        assert!(validate_topic_name("123topic").is_ok());

        // Mixed characters
        assert!(validate_topic_name("my-topic_v1.0").is_ok());
        assert!(validate_topic_name("Production.Events-v2_final").is_ok());
        assert!(validate_topic_name("a").is_ok()); // Single char
        assert!(validate_topic_name("A").is_ok());
        assert!(validate_topic_name("0").is_ok());

        // Kafka internal topic patterns (allowed but discouraged)
        assert!(validate_topic_name("__consumer_offsets").is_ok());
        assert!(validate_topic_name("__transaction_state").is_ok());

        // Maximum length (249 chars)
        let max_len_name = "a".repeat(249);
        assert!(validate_topic_name(&max_len_name).is_ok());
    }

    #[test]
    fn test_validate_topic_name_empty() {
        let result = validate_topic_name("");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cannot be empty"));
    }

    #[test]
    fn test_validate_topic_name_too_long() {
        // 250 chars - one over the limit
        let too_long = "a".repeat(250);
        let result = validate_topic_name(&too_long);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("too long"));
        assert!(err_msg.contains("250"));
        assert!(err_msg.contains("249"));
    }

    #[test]
    fn test_validate_topic_name_reserved_names() {
        // Single dot
        let result = validate_topic_name(".");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("reserved"));

        // Double dot
        let result = validate_topic_name("..");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("reserved"));

        // Triple dot is allowed (not a filesystem reserved name)
        assert!(validate_topic_name("...").is_ok());
    }

    #[test]
    fn test_validate_topic_name_leading_hyphen() {
        let result = validate_topic_name("-my-topic");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cannot start with a hyphen"));

        // Hyphen in the middle or end is fine
        assert!(validate_topic_name("my-topic").is_ok());
        assert!(validate_topic_name("topic-").is_ok());
    }

    #[test]
    fn test_validate_topic_name_invalid_characters() {
        // Path separators
        let result = validate_topic_name("path/to/topic");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid character"));
        assert!(err_msg.contains("'/'"));

        let result = validate_topic_name("topic\\name");
        assert!(result.is_err());

        // Spaces
        let result = validate_topic_name("my topic");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid character"));

        // Colons (could conflict with key patterns)
        let result = validate_topic_name("topic:name");
        assert!(result.is_err());

        // At signs
        let result = validate_topic_name("topic@name");
        assert!(result.is_err());

        // Other special characters
        assert!(validate_topic_name("topic#name").is_err());
        assert!(validate_topic_name("topic$name").is_err());
        assert!(validate_topic_name("topic%name").is_err());
        assert!(validate_topic_name("topic&name").is_err());
        assert!(validate_topic_name("topic*name").is_err());
        assert!(validate_topic_name("topic!name").is_err());
        assert!(validate_topic_name("topic?name").is_err());
    }

    #[test]
    fn test_validate_topic_name_control_characters() {
        // Null byte
        let result = validate_topic_name("topic\0name");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("control character"));

        // Tab
        let result = validate_topic_name("topic\tname");
        assert!(result.is_err());

        // Newline
        let result = validate_topic_name("topic\nname");
        assert!(result.is_err());

        // Carriage return
        let result = validate_topic_name("topic\rname");
        assert!(result.is_err());

        // Bell
        let result = validate_topic_name("topic\x07name");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_topic_name_unicode() {
        // Non-ASCII characters are rejected for cross-platform safety
        let result = validate_topic_name("topic_日本語");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid character"));

        // Emoji
        let result = validate_topic_name("topic_🎉");
        assert!(result.is_err());

        // Accented characters
        let result = validate_topic_name("café");
        assert!(result.is_err());

        // Cyrillic
        let result = validate_topic_name("топик");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_topic_name_error_position() {
        // Check that error message includes position
        let result = validate_topic_name("valid!invalid");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("position 5")); // '!' is at index 5
    }

    #[test]
    fn test_validate_group_id_valid_cases() {
        assert!(validate_group_id("my-consumer-group").is_ok());
        assert!(validate_group_id("group_v1").is_ok());
        assert!(validate_group_id("group.prod").is_ok());
        assert!(validate_group_id("GroupName123").is_ok());

        // Maximum length (255 chars)
        let max_len_group = "g".repeat(255);
        assert!(validate_group_id(&max_len_group).is_ok());
    }

    #[test]
    fn test_validate_group_id_too_long() {
        let too_long = "g".repeat(256);
        let result = validate_group_id(&too_long);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("too long"));
        assert!(err_msg.contains("256"));
        assert!(err_msg.contains("255"));
    }

    #[test]
    fn test_validate_group_id_invalid_characters() {
        // Same rules as topic names
        assert!(validate_group_id("group/name").is_err());
        assert!(validate_group_id("group name").is_err());
        assert!(validate_group_id("group:name").is_err());
    }

    #[test]
    fn test_validate_member_metadata_valid() {
        // Empty metadata is valid
        assert!(validate_member_metadata(&[]).is_ok());

        // Small metadata is valid
        let small = vec![0u8; 100];
        assert!(validate_member_metadata(&small).is_ok());

        // Exactly at limit
        let at_limit = vec![0u8; MAX_MEMBER_METADATA_SIZE];
        assert!(validate_member_metadata(&at_limit).is_ok());
    }

    #[test]
    fn test_validate_member_metadata_too_large() {
        let too_large = vec![0u8; MAX_MEMBER_METADATA_SIZE + 1];
        let result = validate_member_metadata(&too_large);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("too large"));
    }

    #[test]
    fn test_truncate_for_display() {
        // Short strings are not truncated
        assert_eq!(truncate_for_display("hello", 10), "hello");

        // Exact length is not truncated
        assert_eq!(truncate_for_display("hello", 5), "hello");

        // Long strings are truncated with ellipsis
        assert_eq!(truncate_for_display("hello world", 5), "hello...");
    }
}
