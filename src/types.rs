//! Type-safe wrappers for Kafka protocol primitives.
//!
//! These newtypes provide type safety to prevent mixing up different
//! integer types that have the same underlying representation but
//! different semantic meanings.

use bytes::BufMut;
use std::fmt;

use crate::encode::ToByte;
use crate::error::Result;

/// A Kafka message offset within a partition.
///
/// Offsets are 64-bit signed integers that represent the position
/// of a message within a partition's log.
///
/// # Special Values
///
/// Kafka uses negative values for special offset semantics:
/// - `-1` (`LATEST`/`INVALID`): In fetch requests, means "get from end of log".
///   In other contexts, indicates an invalid or unset offset.
/// - `-2` (`EARLIEST`): Means "get from beginning of log".
///
/// Note: `INVALID` and `LATEST` both have value `-1`. This is intentional
/// and matches the Kafka protocol specification. The meaning depends on context:
/// - In `ListOffsets` requests: `-1` means "latest offset"
/// - In offset storage/fetch: `-1` means "no committed offset"
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Offset(pub i64);

impl Offset {
    /// Invalid offset, typically used to indicate an error or unset value.
    ///
    /// Note: This has the same value as `LATEST` (-1). The meaning depends
    /// on context - see type documentation for details.
    pub const INVALID: Self = Offset(-1);

    /// Special offset meaning "earliest available message".
    pub const EARLIEST: Self = Offset(-2);

    /// Special offset meaning "latest available message" (end of log).
    ///
    /// Note: This has the same value as `INVALID` (-1). The meaning depends
    /// on context - see type documentation for details.
    pub const LATEST: Self = Offset(-1);

    /// Create a new offset from a raw value.
    #[inline]
    pub const fn new(value: i64) -> Self {
        Offset(value)
    }

    /// Get the raw i64 value.
    #[inline]
    pub const fn value(self) -> i64 {
        self.0
    }

    /// Check if this is a valid (non-negative) offset.
    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 >= 0
    }

    /// Check if this is a special offset (negative value like EARLIEST or LATEST).
    #[inline]
    pub const fn is_special(self) -> bool {
        self.0 < 0
    }
}

impl From<i64> for Offset {
    fn from(value: i64) -> Self {
        Offset(value)
    }
}

impl From<Offset> for i64 {
    fn from(offset: Offset) -> Self {
        offset.0
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ToByte for Offset {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        self.0.encode(buffer)
    }
}

/// A Kafka broker identifier.
///
/// Broker IDs are 32-bit signed integers that uniquely identify
/// brokers within a Kafka cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BrokerId(pub i32);

impl BrokerId {
    /// Invalid broker ID, typically used to indicate no leader.
    pub const INVALID: Self = BrokerId(-1);

    /// Create a new broker ID from a raw value.
    #[inline]
    pub const fn new(value: i32) -> Self {
        BrokerId(value)
    }

    /// Get the raw i32 value.
    #[inline]
    pub const fn value(self) -> i32 {
        self.0
    }

    /// Check if this is a valid (non-negative) broker ID.
    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 >= 0
    }
}

impl From<i32> for BrokerId {
    fn from(value: i32) -> Self {
        BrokerId(value)
    }
}

impl From<BrokerId> for i32 {
    fn from(id: BrokerId) -> Self {
        id.0
    }
}

impl fmt::Display for BrokerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ToByte for BrokerId {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        self.0.encode(buffer)
    }
}

/// A Kafka request correlation ID.
///
/// Correlation IDs are 32-bit signed integers that clients use to
/// match responses to their corresponding requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct CorrelationId(pub i32);

impl CorrelationId {
    /// Create a new correlation ID from a raw value.
    #[inline]
    pub const fn new(value: i32) -> Self {
        CorrelationId(value)
    }

    /// Get the raw i32 value.
    #[inline]
    pub const fn value(self) -> i32 {
        self.0
    }
}

impl From<i32> for CorrelationId {
    fn from(value: i32) -> Self {
        CorrelationId(value)
    }
}

impl From<CorrelationId> for i32 {
    fn from(id: CorrelationId) -> Self {
        id.0
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ToByte for CorrelationId {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        self.0.encode(buffer)
    }
}

/// A Kafka partition index within a topic.
///
/// Partition indices are 32-bit signed integers that identify
/// a specific partition within a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PartitionIndex(pub i32);

impl PartitionIndex {
    /// Invalid partition index.
    pub const INVALID: Self = PartitionIndex(-1);

    /// Create a new partition index from a raw value.
    #[inline]
    pub const fn new(value: i32) -> Self {
        PartitionIndex(value)
    }

    /// Get the raw i32 value.
    #[inline]
    pub const fn value(self) -> i32 {
        self.0
    }

    /// Check if this is a valid (non-negative) partition index.
    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 >= 0
    }
}

impl From<i32> for PartitionIndex {
    fn from(value: i32) -> Self {
        PartitionIndex(value)
    }
}

impl From<PartitionIndex> for i32 {
    fn from(idx: PartitionIndex) -> Self {
        idx.0
    }
}

impl fmt::Display for PartitionIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ToByte for PartitionIndex {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        self.0.encode(buffer)
    }
}

/// A Kafka consumer group generation ID.
///
/// Generation IDs are 32-bit signed integers that increment
/// each time a consumer group rebalances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct GenerationId(pub i32);

impl GenerationId {
    /// Invalid/unknown generation ID.
    pub const INVALID: Self = GenerationId(-1);

    /// Create a new generation ID from a raw value.
    #[inline]
    pub const fn new(value: i32) -> Self {
        GenerationId(value)
    }

    /// Get the raw i32 value.
    #[inline]
    pub const fn value(self) -> i32 {
        self.0
    }

    /// Increment the generation ID.
    #[inline]
    pub fn next(self) -> Self {
        GenerationId(self.0.wrapping_add(1))
    }
}

impl From<i32> for GenerationId {
    fn from(value: i32) -> Self {
        GenerationId(value)
    }
}

impl From<GenerationId> for i32 {
    fn from(id: GenerationId) -> Self {
        id.0
    }
}

impl fmt::Display for GenerationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ToByte for GenerationId {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        self.0.encode(buffer)
    }
}

/// A Kafka producer ID for idempotent/transactional producers.
///
/// Producer IDs are 64-bit signed integers assigned by the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ProducerId(pub i64);

impl ProducerId {
    /// Invalid/unknown producer ID.
    pub const INVALID: Self = ProducerId(-1);

    /// Create a new producer ID from a raw value.
    #[inline]
    pub const fn new(value: i64) -> Self {
        ProducerId(value)
    }

    /// Get the raw i64 value.
    #[inline]
    pub const fn value(self) -> i64 {
        self.0
    }

    /// Check if this is a valid (non-negative) producer ID.
    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 >= 0
    }
}

impl From<i64> for ProducerId {
    fn from(value: i64) -> Self {
        ProducerId(value)
    }
}

impl From<ProducerId> for i64 {
    fn from(id: ProducerId) -> Self {
        id.0
    }
}

impl fmt::Display for ProducerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ToByte for ProducerId {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        self.0.encode(buffer)
    }
}

/// A Kafka producer epoch for idempotent/transactional producers.
///
/// Producer epochs are 16-bit signed integers that increment when
/// a producer is fenced or recovers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ProducerEpoch(pub i16);

impl ProducerEpoch {
    /// Invalid/unknown producer epoch.
    pub const INVALID: Self = ProducerEpoch(-1);

    /// Create a new producer epoch from a raw value.
    #[inline]
    pub const fn new(value: i16) -> Self {
        ProducerEpoch(value)
    }

    /// Get the raw i16 value.
    #[inline]
    pub const fn value(self) -> i16 {
        self.0
    }
}

impl From<i16> for ProducerEpoch {
    fn from(value: i16) -> Self {
        ProducerEpoch(value)
    }
}

impl From<ProducerEpoch> for i16 {
    fn from(epoch: ProducerEpoch) -> Self {
        epoch.0
    }
}

impl fmt::Display for ProducerEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ToByte for ProducerEpoch {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        self.0.encode(buffer)
    }
}

// ============================================================================
// PartitionId
// ============================================================================

/// A topic-partition identifier.
///
/// This type provides a more ergonomic and type-safe way to identify
/// a specific partition within a topic, replacing the common pattern
/// of using `(String, i32)` tuples throughout the codebase.
///
/// # Usage
///
/// ```
/// use kafkaesque::types::PartitionId;
///
/// let partition = PartitionId::new("my-topic", 0);
/// assert_eq!(partition.to_string(), "my-topic-0");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionId {
    /// The topic name.
    topic: String,
    /// The partition index.
    partition: i32,
}

impl PartitionId {
    /// Create a new partition identifier.
    #[inline]
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }

    /// Get the topic name.
    #[inline]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the partition index.
    #[inline]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Convert to a tuple (for backward compatibility).
    #[inline]
    pub fn as_tuple(&self) -> (&str, i32) {
        (&self.topic, self.partition)
    }

    /// Convert to an owned tuple (for backward compatibility).
    #[inline]
    pub fn into_tuple(self) -> (String, i32) {
        (self.topic, self.partition)
    }
}

impl From<(String, i32)> for PartitionId {
    fn from((topic, partition): (String, i32)) -> Self {
        Self { topic, partition }
    }
}

impl From<(&str, i32)> for PartitionId {
    fn from((topic, partition): (&str, i32)) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
        }
    }
}

impl From<PartitionId> for (String, i32) {
    fn from(id: PartitionId) -> Self {
        (id.topic, id.partition)
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Offset tests
    #[test]
    fn test_offset_new_and_value() {
        let offset = Offset::new(42);
        assert_eq!(offset.value(), 42);
    }

    #[test]
    fn test_offset_constants() {
        assert_eq!(Offset::INVALID.value(), -1);
        assert_eq!(Offset::EARLIEST.value(), -2);
        assert_eq!(Offset::LATEST.value(), -1);
    }

    #[test]
    fn test_offset_is_valid() {
        assert!(Offset::new(0).is_valid());
        assert!(Offset::new(100).is_valid());
        assert!(!Offset::INVALID.is_valid());
        assert!(!Offset::EARLIEST.is_valid());
    }

    #[test]
    fn test_offset_is_special() {
        assert!(Offset::INVALID.is_special());
        assert!(Offset::EARLIEST.is_special());
        assert!(!Offset::new(0).is_special());
    }

    #[test]
    fn test_offset_from_i64() {
        let offset: Offset = 123i64.into();
        assert_eq!(offset.value(), 123);
    }

    #[test]
    fn test_offset_into_i64() {
        let value: i64 = Offset::new(456).into();
        assert_eq!(value, 456);
    }

    #[test]
    fn test_offset_display() {
        assert_eq!(format!("{}", Offset::new(789)), "789");
    }

    #[test]
    fn test_offset_encode() {
        let mut buf = Vec::new();
        Offset::new(0x0102030405060708).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn test_offset_ordering() {
        assert!(Offset::new(1) < Offset::new(2));
        assert!(Offset::new(10) > Offset::new(5));
        assert_eq!(Offset::new(3), Offset::new(3));
    }

    // BrokerId tests
    #[test]
    fn test_broker_id_new_and_value() {
        let id = BrokerId::new(5);
        assert_eq!(id.value(), 5);
    }

    #[test]
    fn test_broker_id_invalid() {
        assert_eq!(BrokerId::INVALID.value(), -1);
        assert!(!BrokerId::INVALID.is_valid());
    }

    #[test]
    fn test_broker_id_is_valid() {
        assert!(BrokerId::new(0).is_valid());
        assert!(BrokerId::new(100).is_valid());
        assert!(!BrokerId::new(-1).is_valid());
    }

    #[test]
    fn test_broker_id_from_i32() {
        let id: BrokerId = 42i32.into();
        assert_eq!(id.value(), 42);
    }

    #[test]
    fn test_broker_id_into_i32() {
        let value: i32 = BrokerId::new(99).into();
        assert_eq!(value, 99);
    }

    #[test]
    fn test_broker_id_encode() {
        let mut buf = Vec::new();
        BrokerId::new(0x01020304).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x01, 0x02, 0x03, 0x04]);
    }

    // CorrelationId tests
    #[test]
    fn test_correlation_id_new_and_value() {
        let id = CorrelationId::new(12345);
        assert_eq!(id.value(), 12345);
    }

    #[test]
    fn test_correlation_id_from_i32() {
        let id: CorrelationId = 999i32.into();
        assert_eq!(id.value(), 999);
    }

    #[test]
    fn test_correlation_id_encode() {
        let mut buf = Vec::new();
        CorrelationId::new(42).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x00, 0x00, 0x00, 0x2A]);
    }

    // PartitionIndex tests
    #[test]
    fn test_partition_index_new_and_value() {
        let idx = PartitionIndex::new(3);
        assert_eq!(idx.value(), 3);
    }

    #[test]
    fn test_partition_index_invalid() {
        assert_eq!(PartitionIndex::INVALID.value(), -1);
        assert!(!PartitionIndex::INVALID.is_valid());
    }

    #[test]
    fn test_partition_index_is_valid() {
        assert!(PartitionIndex::new(0).is_valid());
        assert!(PartitionIndex::new(15).is_valid());
        assert!(!PartitionIndex::new(-1).is_valid());
    }

    #[test]
    fn test_partition_index_encode() {
        let mut buf = Vec::new();
        PartitionIndex::new(7).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x00, 0x00, 0x00, 0x07]);
    }

    // GenerationId tests
    #[test]
    fn test_generation_id_new_and_value() {
        let id = GenerationId::new(5);
        assert_eq!(id.value(), 5);
    }

    #[test]
    fn test_generation_id_invalid() {
        assert_eq!(GenerationId::INVALID.value(), -1);
    }

    #[test]
    fn test_generation_id_next() {
        let id = GenerationId::new(10);
        assert_eq!(id.next().value(), 11);
    }

    #[test]
    fn test_generation_id_next_wraps() {
        let id = GenerationId::new(i32::MAX);
        assert_eq!(id.next().value(), i32::MIN);
    }

    #[test]
    fn test_generation_id_encode() {
        let mut buf = Vec::new();
        GenerationId::new(100).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x00, 0x00, 0x00, 0x64]);
    }

    // ProducerId tests
    #[test]
    fn test_producer_id_new_and_value() {
        let id = ProducerId::new(1000);
        assert_eq!(id.value(), 1000);
    }

    #[test]
    fn test_producer_id_invalid() {
        assert_eq!(ProducerId::INVALID.value(), -1);
        assert!(!ProducerId::INVALID.is_valid());
    }

    #[test]
    fn test_producer_id_is_valid() {
        assert!(ProducerId::new(0).is_valid());
        assert!(ProducerId::new(12345).is_valid());
        assert!(!ProducerId::new(-1).is_valid());
    }

    #[test]
    fn test_producer_id_encode() {
        let mut buf = Vec::new();
        ProducerId::new(12345).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x39]);
    }

    // ProducerEpoch tests
    #[test]
    fn test_producer_epoch_new_and_value() {
        let epoch = ProducerEpoch::new(5);
        assert_eq!(epoch.value(), 5);
    }

    #[test]
    fn test_producer_epoch_invalid() {
        assert_eq!(ProducerEpoch::INVALID.value(), -1);
    }

    #[test]
    fn test_producer_epoch_encode() {
        let mut buf = Vec::new();
        ProducerEpoch::new(42).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x00, 0x2A]);
    }

    // Default tests
    #[test]
    fn test_defaults() {
        assert_eq!(Offset::default().value(), 0);
        assert_eq!(BrokerId::default().value(), 0);
        assert_eq!(CorrelationId::default().value(), 0);
        assert_eq!(PartitionIndex::default().value(), 0);
        assert_eq!(GenerationId::default().value(), 0);
        assert_eq!(ProducerId::default().value(), 0);
        assert_eq!(ProducerEpoch::default().value(), 0);
    }

    // Clone and Copy tests
    #[test]
    fn test_copy_semantics() {
        let offset = Offset::new(100);
        let copied = offset;
        assert_eq!(offset, copied); // Both should still be valid (Copy)
    }

    // Hash tests
    #[test]
    fn test_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Offset::new(1));
        set.insert(Offset::new(2));
        set.insert(Offset::new(1)); // Duplicate
        assert_eq!(set.len(), 2);
    }

    // Debug tests
    #[test]
    fn test_debug() {
        let offset = Offset::new(42);
        let debug_str = format!("{:?}", offset);
        assert!(debug_str.contains("42"));
        assert!(debug_str.contains("Offset"));
    }
}
