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
use strum::IntoStaticStr;
use thiserror::Error as ThisError;

pub type Result<T> = result::Result<T, Error>;

/// Preserved I/O error detail for protocol-layer diagnostics.
///
/// Storing only [`io::ErrorKind`] (the old shape) discarded the underlying
/// message from `last_os_error()` / `source()` chains, which made connection
/// failures hard to triage from logs alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreservedIoError {
    pub kind: io::ErrorKind,
    pub message: String,
}

impl std::fmt::Display for PreservedIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl From<io::Error> for PreservedIoError {
    fn from(e: io::Error) -> Self {
        Self {
            kind: e.kind(),
            message: e.to_string(),
        }
    }
}

/// Why the frame reader refused an inbound frame before dispatch.
///
/// These are observable capacity / DoS signals — distinct from a benign peer
/// disconnect — so the connection loop can meter each cause via
/// `frame_rejected_total{reason}` and log it at `warn` instead of treating it
/// as a normal close. The `as_str` value is the stable, low-cardinality metric
/// label; keep it `snake_case` and never derive it from attacker input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameRejectReason {
    /// Declared frame size exceeds the configured maximum.
    FrameTooLarge,
    /// The process-global inflight-byte budget is exhausted; the frame was
    /// shed to bound broker memory.
    InflightBudgetExhausted,
    /// Body bytes arrived too slowly (slow-loris / trickle) and tripped the
    /// per-read deadline.
    BodyReadTimeout,
    /// The declared size is negative or below the protocol minimum.
    MalformedSize,
}

impl FrameRejectReason {
    /// Stable metric-label string (low cardinality, `snake_case`).
    pub fn as_str(self) -> &'static str {
        match self {
            FrameRejectReason::FrameTooLarge => "frame_too_large",
            FrameRejectReason::InflightBudgetExhausted => "inflight_budget_exhausted",
            FrameRejectReason::BodyReadTimeout => "body_read_timeout",
            FrameRejectReason::MalformedSize => "malformed_size",
        }
    }
}

impl std::fmt::Display for FrameRejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

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
    #[error("IO error: {0}")]
    IoError(PreservedIoError),

    /// Could not parse the data.
    #[error("Parsing error: invalid data ({} bytes)", .0.len())]
    ParsingError(Bytes),

    /// Request body parsed successfully but left trailing bytes on the wire.
    #[error("Trailing bytes after request body")]
    TrailingBytes,

    /// Missing data or connection closed.
    #[error("Missing data: {0}")]
    MissingData(String),

    /// The peer closed the connection cleanly — EOF on the size prefix, or the
    /// socket closing between frames / mid-frame. This is normal client
    /// behavior; the connection loop treats it as a quiet, debug-level close.
    /// Split out from [`Error::MissingData`] so an actively *rejected* frame
    /// (see [`Error::FrameRejected`]) is no longer indistinguishable from a
    /// benign disconnect in logs and metrics.
    #[error("Peer closed connection: {0}")]
    PeerClosed(String),

    /// The frame reader refused an inbound frame for a specific, observable
    /// reason. Unlike [`Error::PeerClosed`], these are capacity / DoS signals:
    /// the connection loop meters them via `frame_rejected_total{reason}` and
    /// logs at `warn`, then closes the connection (framing can no longer be
    /// trusted).
    #[error("Frame rejected ({reason}): {detail}")]
    FrameRejected {
        /// Stable, low-cardinality reason tag used as the metric label.
        reason: FrameRejectReason,
        /// Human-readable detail for logs (sizes, byte counts, deadlines).
        detail: String,
    },

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Authentication required or rejected at the connection level.
    /// Surfaced when SASL is required and the client tries to use a
    /// non-handshake API before completing SaslAuthenticate.
    #[error("Authentication required: {0}")]
    Authentication(String),
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::IoError(a), Error::IoError(b)) => a.kind == b.kind && a.message == b.message,
            (Error::ParsingError(a), Error::ParsingError(b)) => a == b,
            (Error::TrailingBytes, Error::TrailingBytes) => true,
            (Error::MissingData(a), Error::MissingData(b)) => a == b,
            (Error::PeerClosed(a), Error::PeerClosed(b)) => a == b,
            (
                Error::FrameRejected {
                    reason: ra,
                    detail: da,
                },
                Error::FrameRejected {
                    reason: rb,
                    detail: db,
                },
            ) => ra == rb && da == db,
            (Error::Config(a), Error::Config(b)) => a == b,
            (Error::Authentication(a), Error::Authentication(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IoError(e.into())
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Error::Config(e.to_string())
    }
}

// `From<kafkaesque::cluster::SlateDBError> for Error` lives in the
// umbrella crate (see `kafkaesque::cluster::error`) — keeping the
// conversion there preserves the runtime-independence of this crate.

/// Various errors reported by a remote Kafka server.
/// See also [Kafka Errors](http://kafka.apache.org/protocol.html)
#[derive(Debug, Copy, Clone, PartialEq, Eq, FromPrimitive, Default, IntoStaticStr)]
pub enum KafkaCode {
    /// An unexpected server error
    Unknown = -1,
    #[default]
    #[strum(serialize = "NONE")]
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
    /// Number of partitions is invalid.
    InvalidPartitions = 37,
    /// Replication-factor is invalid.
    InvalidReplicationFactor = 38,
    /// Replica assignment is invalid.
    InvalidReplicaAssignment = 39,
    /// Configuration is invalid.
    InvalidConfig = 40,
    /// This is not the correct controller for this cluster.
    NotController = 41,
    /// This most likely occurs because of a request being malformed by the
    /// client library or the message was sent to an incompatible broker.
    /// Also used to reject features this broker does not implement
    /// (e.g. transactional produce).
    InvalidRequest = 42,
    /// The message format version on the broker does not support the request.
    UnsupportedForMessageFormat = 43,
    /// Request parameters do not satisfy the configured policy.
    PolicyViolation = 44,
    /// The producer attempted to use a sequence number outside the valid range.
    OutOfOrderSequenceNumber = 45,
    /// The producer attempted to assign a sequence number that was already used.
    DuplicateSequenceNumber = 46,
    /// Producer attempted an operation with an old epoch — its transactional/idempotent
    /// state has been fenced by a newer instance. Clients abort the producer and
    /// re-init on this code; mapping a fence to OutOfOrderSequenceNumber instead
    /// would have caused a tight retry loop until `delivery.timeout.ms`.
    InvalidProducerEpoch = 47,
    /// The producer attempted a transactional operation in an invalid state.
    InvalidTxnState = 48,
    /// The producer attempted to use a producer id which is not currently assigned to its transactional id.
    InvalidProducerIdMapping = 49,
    /// The transaction timeout is larger than the maximum value allowed by the broker.
    InvalidTransactionTimeout = 50,
    /// The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.
    ConcurrentTransactions = 51,
    /// Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.
    TransactionCoordinatorFenced = 52,
    /// Transactional Id authorization failed.
    TransactionalIdAuthorizationFailed = 53,
    /// Security features are disabled.
    SecurityDisabled = 54,
    /// The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.
    OperationNotAttempted = 55,
    /// Disk error when trying to access log file on the disk.
    KafkaStorageError = 56,
    /// The user-specified log directory is not found in the broker config.
    LogDirNotFound = 57,
    /// SASL Authentication failed.
    SaslAuthenticationFailed = 58,
    /// The broker does not know the producer ID — usually because the producer's
    /// state was evicted from the broker's idempotent-producer cache. Clients
    /// re-init the producer on this code.
    UnknownProducerId = 59,
    /// A partition reassignment is in progress.
    ReassignmentInProgress = 60,
    /// Delegation Token feature is not enabled.
    DelegationTokenAuthDisabled = 61,
    /// Delegation Token is not found on server.
    DelegationTokenNotFound = 62,
    /// Specified Principal is not valid Owner/Renewer.
    DelegationTokenOwnerMismatch = 63,
    /// Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.
    DelegationTokenRequestNotAllowed = 64,
    /// Delegation Token authorization failed.
    DelegationTokenAuthorizationFailed = 65,
    /// Delegation Token is expired.
    DelegationTokenExpired = 66,
    /// Supplied principalType is not supported.
    InvalidPrincipalType = 67,
    /// The group is not empty.
    NonEmptyGroup = 68,
    /// The group id does not exist.
    GroupIdNotFound = 69,
    /// The fetch session ID was not found.
    FetchSessionIdNotFound = 70,
    /// The fetch session epoch is invalid.
    InvalidFetchSessionEpoch = 71,
    /// There is no listener on the leader broker that matches the listener on which metadata request was processed.
    ListenerNotFound = 72,
    /// Topic deletion is disabled.
    TopicDeletionDisabled = 73,
    /// The leader epoch in the request is older than the epoch on the broker.
    FencedLeaderEpoch = 74,
    /// The leader epoch in the request is newer than the epoch on the broker.
    UnknownLeaderEpoch = 75,
    /// The requesting client does not support the compression type of given partition.
    UnsupportedCompressionType = 76,
    /// Broker epoch has changed.
    StaleBrokerEpoch = 77,
    /// The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.
    OffsetNotAvailable = 78,
    /// The member id required for joining or accessing the consumer group is
    /// missing. Per KIP-394, brokers return this for JoinGroup v4+ when the
    /// client supplied an empty `member_id`, along with a freshly generated id.
    MemberIdRequired = 79,
}

impl KafkaCode {
    /// Stable, low-cardinality metric label for this error code. Used by
    /// `record_request_with_code` to populate the `error_code` label on
    /// `kafkaesque_requests_total` so SREs can graph per-error-code rates
    /// during incidents.
    pub fn metric_label(&self) -> &'static str {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_traits::FromPrimitive;

    #[test]
    fn test_error_io_error_preserves_message() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "connection reset by peer");
        let err = Error::from(io_err);
        match err {
            Error::IoError(detail) => {
                assert_eq!(detail.kind, io::ErrorKind::ConnectionRefused);
                assert!(detail.message.contains("connection reset by peer"));
            }
            other => panic!("expected IoError, got {:?}", other),
        }
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
