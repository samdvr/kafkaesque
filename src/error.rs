//! Crate & protocol level errors.
//!
//! This module provides the top-level error types for the Kafkaesque Kafka broker.
//!
//! # Error Hierarchy
//!
//! The crate uses a two-layer error hierarchy:
//!
//! ## Protocol Layer (`crate::error`)
//!
//! - [`Error`]: Connection and protocol parsing errors
//! - [`KafkaCode`]: Wire protocol error codes for Kafka protocol responses
//!
//! ## Storage/Cluster Layer (`crate::cluster::error`)
//!
//! - [`SlateDBError`]: Comprehensive storage and coordination errors
//! - Includes Raft, SlateDB, object store, fencing, ownership errors
//! - Has `to_kafka_code()` for mapping to [`KafkaCode`]
//!
//! ## Conversion
//!
//! [`SlateDBError`] can be converted to [`Error`] via `From` impl,
//! allowing storage errors to propagate through the protocol layer.
//!
//! [`SlateDBError`]: crate::cluster::SlateDBError

use bytes::Bytes;
use num_derive::FromPrimitive;
use std::{io, result};
use thiserror::Error as ThisError;

pub type Result<T> = result::Result<T, Error>;

/// Protocol and connection level errors.
///
/// These are low-level errors that occur during:
/// - Network I/O operations
/// - Kafka protocol parsing
/// - Connection management
///
/// For storage and coordination errors, see [`crate::cluster::SlateDBError`].
#[derive(Clone, Debug, ThisError)]
pub enum Error {
    /// An error in the network.
    #[error("IO error: {0:?}")]
    IoError(io::ErrorKind),

    /// Could not parse the data.
    #[error("Parsing error: invalid data ({} bytes)", .0.len())]
    ParsingError(Bytes),

    /// Missing data or connection closed.
    #[error("Missing data: {0}")]
    MissingData(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::IoError(a), Error::IoError(b)) => a == b,
            (Error::ParsingError(a), Error::ParsingError(b)) => a == b,
            (Error::MissingData(a), Error::MissingData(b)) => a == b,
            (Error::Config(a), Error::Config(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IoError(e.kind())
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Error::Config(e.to_string())
    }
}

impl From<crate::cluster::SlateDBError> for Error {
    fn from(e: crate::cluster::SlateDBError) -> Self {
        use crate::cluster::SlateDBError;
        match e {
            SlateDBError::Io(io_err) => Error::IoError(io_err.kind()),
            SlateDBError::Config(msg) => Error::Config(msg),
            other => Error::Config(other.to_string()),
        }
    }
}

/// Various errors reported by a remote Kafka server.
/// See also [Kafka Errors](http://kafka.apache.org/protocol.html)
#[derive(Debug, Copy, Clone, PartialEq, Eq, FromPrimitive, Default)]
pub enum KafkaCode {
    /// An unexpected server error
    Unknown = -1,
    #[default]
    None = 0,
    /// The requested offset is outside the range of offsets
    /// maintained by the server for the given topic/partition
    OffsetOutOfRange = 1,
    /// This indicates that a message contents does not match its CRC
    CorruptMessage = 2,
    /// This request is for a topic or partition that does not exist
    /// on this broker.
    UnknownTopicOrPartition = 3,
    /// The message has a negative size
    InvalidMessageSize = 4,
    /// This error is thrown if we are in the middle of a leadership
    /// election and there is currently no leader for this partition
    /// and hence it is unavailable for writes.
    LeaderNotAvailable = 5,
    /// This error is thrown if the client attempts to send messages
    /// to a replica that is not the leader for some partition. It
    /// indicates that the clients metadata is out of date.
    NotLeaderForPartition = 6,
    /// This error is thrown if the request exceeds the user-specified
    /// time limit in the request.
    RequestTimedOut = 7,
    /// This is not a client facing error and is used mostly by tools
    /// when a broker is not alive.
    BrokerNotAvailable = 8,
    /// If replica is expected on a broker, but is not (this can be
    /// safely ignored).
    ReplicaNotAvailable = 9,
    /// The server has a configurable maximum message size to avoid
    /// unbounded memory allocation. This error is thrown if the
    /// client attempt to produce a message larger than this maximum.
    MessageSizeTooLarge = 10,
    /// Internal error code for broker-to-broker communication.
    StaleControllerEpoch = 11,
    /// If you specify a string larger than configured maximum for
    /// offset metadata
    OffsetMetadataTooLarge = 12,
    /// The server disconnected before a response was received.
    NetworkException = 13,
    /// The broker returns this error code for an offset fetch request
    /// if it is still loading offsets (after a leader change for that
    /// offsets topic partition), or in response to group membership
    /// requests (such as heartbeats) when group metadata is being
    /// loaded by the coordinator.
    GroupLoadInProgress = 14,
    /// The broker returns this error code for group coordinator
    /// requests, offset commits, and most group management requests
    /// if the offsets topic has not yet been created, or if the group
    /// coordinator is not active.
    GroupCoordinatorNotAvailable = 15,
    /// The broker returns this error code if it receives an offset
    /// fetch or commit request for a group that it is not a
    /// coordinator for.
    NotCoordinatorForGroup = 16,
    /// For a request which attempts to access an invalid topic
    /// (e.g. one which has an illegal name), or if an attempt is made
    /// to write to an internal topic (such as the consumer offsets
    /// topic).
    InvalidTopic = 17,
    /// If a message batch in a produce request exceeds the maximum
    /// configured segment size.
    RecordListTooLarge = 18,
    /// Returned from a produce request when the number of in-sync
    /// replicas is lower than the configured minimum and requiredAcks is
    /// -1.
    NotEnoughReplicas = 19,
    /// Returned from a produce request when the message was written
    /// to the log, but with fewer in-sync replicas than required.
    NotEnoughReplicasAfterAppend = 20,
    /// Returned from a produce request if the requested requiredAcks is
    /// invalid (anything other than -1, 1, or 0).
    InvalidRequiredAcks = 21,
    /// Returned from group membership requests (such as heartbeats) when
    /// the generation id provided in the request is not the current
    /// generation.
    IllegalGeneration = 22,
    /// Returned in join group when the member provides a protocol type or
    /// set of protocols which is not compatible with the current group.
    InconsistentGroupProtocol = 23,
    /// Returned in join group when the groupId is empty or null.
    InvalidGroupId = 24,
    /// Returned from group requests (offset commits/fetches, heartbeats,
    /// etc) when the memberId is not in the current generation.
    UnknownMemberId = 25,
    /// Return in join group when the requested session timeout is outside
    /// of the allowed range on the broker
    InvalidSessionTimeout = 26,
    /// Returned in heartbeat requests when the coordinator has begun
    /// rebalancing the group. This indicates to the client that it
    /// should rejoin the group.
    RebalanceInProgress = 27,
    /// This error indicates that an offset commit was rejected because of
    /// oversize metadata.
    InvalidCommitOffsetSize = 28,
    /// Returned by the broker when the client is not authorized to access
    /// the requested topic.
    TopicAuthorizationFailed = 29,
    /// Returned by the broker when the client is not authorized to access
    /// a particular groupId.
    GroupAuthorizationFailed = 30,
    /// Returned by the broker when the client is not authorized to use an
    /// inter-broker or administrative API.
    ClusterAuthorizationFailed = 31,
    /// The timestamp of the message is out of acceptable range.
    InvalidTimestamp = 32,
    /// The broker does not support the requested SASL mechanism.
    UnsupportedSaslMechanism = 33,
    /// Request is not valid given the current SASL state.
    IllegalSaslState = 34,
    /// The version of API is not supported.
    UnsupportedVersion = 35,
    /// Topic with this name already exists.
    TopicAlreadyExists = 36,
    /// The message format version on the broker does not support the request.
    UnsupportedForMessageFormat = 43,
    /// This is not the correct controller for this cluster.
    NotController = 41,
    /// The producer attempted to use a sequence number outside the valid range.
    OutOfOrderSequenceNumber = 45,
    /// The producer attempted to assign a sequence number that was already used.
    DuplicateSequenceNumber = 46,
    /// SASL Authentication failed.
    SaslAuthenticationFailed = 58,
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_traits::FromPrimitive;

    #[test]
    fn test_error_io_error() {
        let err = Error::IoError(io::ErrorKind::ConnectionRefused);
        assert_eq!(err, Error::IoError(io::ErrorKind::ConnectionRefused));
    }

    #[test]
    fn test_error_parsing_error() {
        let data = Bytes::from("bad data");
        let err = Error::ParsingError(data.clone());
        assert_eq!(err, Error::ParsingError(data));
    }

    #[test]
    fn test_error_missing_data() {
        let err = Error::MissingData("connection closed".to_string());
        assert_eq!(err, Error::MissingData("connection closed".to_string()));
    }

    #[test]
    fn test_error_display() {
        let err = Error::MissingData("test".to_string());
        let display = format!("{}", err);
        assert!(display.contains("Missing data"));
        assert!(display.contains("test"));
    }

    #[test]
    fn test_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(Error::MissingData("test".to_string()));
        assert!(err.to_string().contains("Missing data"));
    }

    #[test]
    fn test_kafka_code_from_primitive() {
        assert_eq!(KafkaCode::from_i16(-1), Some(KafkaCode::Unknown));
        assert_eq!(KafkaCode::from_i16(0), Some(KafkaCode::None));
        assert_eq!(KafkaCode::from_i16(1), Some(KafkaCode::OffsetOutOfRange));
        assert_eq!(
            KafkaCode::from_i16(3),
            Some(KafkaCode::UnknownTopicOrPartition)
        );
        assert_eq!(KafkaCode::from_i16(7), Some(KafkaCode::RequestTimedOut));
        assert_eq!(KafkaCode::from_i16(25), Some(KafkaCode::UnknownMemberId));
        assert_eq!(KafkaCode::from_i16(35), Some(KafkaCode::UnsupportedVersion));
        assert_eq!(KafkaCode::from_i16(36), Some(KafkaCode::TopicAlreadyExists));
        assert_eq!(
            KafkaCode::from_i16(58),
            Some(KafkaCode::SaslAuthenticationFailed)
        );
    }

    #[test]
    fn test_kafka_code_unknown_value() {
        // Values not in the enum should return None
        assert_eq!(KafkaCode::from_i16(999), None);
        assert_eq!(KafkaCode::from_i16(-100), None);
    }

    #[test]
    fn test_kafka_code_values() {
        assert_eq!(KafkaCode::Unknown as i16, -1);
        assert_eq!(KafkaCode::None as i16, 0);
        assert_eq!(KafkaCode::OffsetOutOfRange as i16, 1);
        assert_eq!(KafkaCode::CorruptMessage as i16, 2);
        assert_eq!(KafkaCode::UnknownTopicOrPartition as i16, 3);
        assert_eq!(KafkaCode::LeaderNotAvailable as i16, 5);
        assert_eq!(KafkaCode::NotLeaderForPartition as i16, 6);
        assert_eq!(KafkaCode::RequestTimedOut as i16, 7);
        assert_eq!(KafkaCode::RebalanceInProgress as i16, 27);
        assert_eq!(KafkaCode::UnsupportedSaslMechanism as i16, 33);
        assert_eq!(KafkaCode::NotController as i16, 41);
    }

    #[test]
    fn test_kafka_code_clone_and_copy() {
        let code = KafkaCode::OffsetOutOfRange;
        let cloned = code;
        let copied = code;
        assert_eq!(code, cloned);
        assert_eq!(code, copied);
    }

    #[test]
    fn test_kafka_code_debug() {
        let code = KafkaCode::UnknownTopicOrPartition;
        let debug = format!("{:?}", code);
        assert_eq!(debug, "UnknownTopicOrPartition");
    }

    #[test]
    fn test_error_clone() {
        let err = Error::MissingData("test".to_string());
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }
}
