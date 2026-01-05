//! Integration tests for the types module.
//!
//! These tests verify the type-safe wrappers for Kafka protocol primitives.

use kafkaesque::types::{
    BrokerId, CorrelationId, GenerationId, Offset, PartitionId, PartitionIndex, ProducerEpoch,
    ProducerId,
};

// ============================================================================
// Offset Tests
// ============================================================================

#[test]
fn test_offset_new_and_value() {
    let offset = Offset::new(12345);
    assert_eq!(offset.value(), 12345);
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
    assert!(Offset::new(i64::MAX).is_valid());
    assert!(!Offset::INVALID.is_valid());
    assert!(!Offset::EARLIEST.is_valid());
    assert!(!Offset::new(-5).is_valid());
}

#[test]
fn test_offset_is_special() {
    assert!(Offset::INVALID.is_special());
    assert!(Offset::EARLIEST.is_special());
    assert!(Offset::LATEST.is_special());
    assert!(!Offset::new(0).is_special());
    assert!(!Offset::new(100).is_special());
}

#[test]
fn test_offset_from_i64() {
    let offset: Offset = 999i64.into();
    assert_eq!(offset.value(), 999);
}

#[test]
fn test_offset_into_i64() {
    let value: i64 = Offset::new(456).into();
    assert_eq!(value, 456);
}

#[test]
fn test_offset_display() {
    assert_eq!(format!("{}", Offset::new(789)), "789");
    assert_eq!(format!("{}", Offset::INVALID), "-1");
}

#[test]
fn test_offset_ordering() {
    assert!(Offset::new(1) < Offset::new(2));
    assert!(Offset::new(10) > Offset::new(5));
    assert_eq!(Offset::new(3), Offset::new(3));
    assert!(Offset::EARLIEST < Offset::INVALID);
}

#[test]
fn test_offset_default() {
    let offset = Offset::default();
    assert_eq!(offset.value(), 0);
}

#[test]
fn test_offset_debug() {
    let offset = Offset::new(42);
    let debug_str = format!("{:?}", offset);
    assert!(debug_str.contains("42"));
}

#[test]
fn test_offset_hash() {
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(Offset::new(1));
    set.insert(Offset::new(2));
    set.insert(Offset::new(1)); // Duplicate
    assert_eq!(set.len(), 2);
}

// ============================================================================
// BrokerId Tests
// ============================================================================

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
    assert!(!BrokerId::new(-100).is_valid());
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
fn test_broker_id_display() {
    assert_eq!(format!("{}", BrokerId::new(7)), "7");
}

#[test]
fn test_broker_id_default() {
    assert_eq!(BrokerId::default().value(), 0);
}

#[test]
fn test_broker_id_ordering() {
    assert!(BrokerId::new(1) < BrokerId::new(2));
}

// ============================================================================
// CorrelationId Tests
// ============================================================================

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
fn test_correlation_id_into_i32() {
    let value: i32 = CorrelationId::new(123).into();
    assert_eq!(value, 123);
}

#[test]
fn test_correlation_id_display() {
    assert_eq!(format!("{}", CorrelationId::new(456)), "456");
}

#[test]
fn test_correlation_id_default() {
    assert_eq!(CorrelationId::default().value(), 0);
}

#[test]
fn test_correlation_id_hash() {
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(CorrelationId::new(1));
    set.insert(CorrelationId::new(2));
    assert_eq!(set.len(), 2);
}

// ============================================================================
// PartitionIndex Tests
// ============================================================================

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
fn test_partition_index_from_i32() {
    let idx: PartitionIndex = 7i32.into();
    assert_eq!(idx.value(), 7);
}

#[test]
fn test_partition_index_into_i32() {
    let value: i32 = PartitionIndex::new(5).into();
    assert_eq!(value, 5);
}

#[test]
fn test_partition_index_display() {
    assert_eq!(format!("{}", PartitionIndex::new(9)), "9");
}

#[test]
fn test_partition_index_default() {
    assert_eq!(PartitionIndex::default().value(), 0);
}

#[test]
fn test_partition_index_ordering() {
    assert!(PartitionIndex::new(0) < PartitionIndex::new(1));
    assert!(PartitionIndex::new(10) > PartitionIndex::new(5));
}

// ============================================================================
// GenerationId Tests
// ============================================================================

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
    let next = id.next();
    assert_eq!(next.value(), 11);
}

#[test]
fn test_generation_id_next_wraps() {
    let id = GenerationId::new(i32::MAX);
    let next = id.next();
    assert_eq!(next.value(), i32::MIN);
}

#[test]
fn test_generation_id_from_i32() {
    let id: GenerationId = 42i32.into();
    assert_eq!(id.value(), 42);
}

#[test]
fn test_generation_id_into_i32() {
    let value: i32 = GenerationId::new(100).into();
    assert_eq!(value, 100);
}

#[test]
fn test_generation_id_display() {
    assert_eq!(format!("{}", GenerationId::new(25)), "25");
}

#[test]
fn test_generation_id_default() {
    assert_eq!(GenerationId::default().value(), 0);
}

#[test]
fn test_generation_id_ordering() {
    assert!(GenerationId::new(1) < GenerationId::new(2));
}

// ============================================================================
// ProducerId Tests
// ============================================================================

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
fn test_producer_id_from_i64() {
    let id: ProducerId = 999i64.into();
    assert_eq!(id.value(), 999);
}

#[test]
fn test_producer_id_into_i64() {
    let value: i64 = ProducerId::new(888).into();
    assert_eq!(value, 888);
}

#[test]
fn test_producer_id_display() {
    assert_eq!(format!("{}", ProducerId::new(777)), "777");
}

#[test]
fn test_producer_id_default() {
    assert_eq!(ProducerId::default().value(), 0);
}

// ============================================================================
// ProducerEpoch Tests
// ============================================================================

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
fn test_producer_epoch_from_i16() {
    let epoch: ProducerEpoch = 10i16.into();
    assert_eq!(epoch.value(), 10);
}

#[test]
fn test_producer_epoch_into_i16() {
    let value: i16 = ProducerEpoch::new(15).into();
    assert_eq!(value, 15);
}

#[test]
fn test_producer_epoch_display() {
    assert_eq!(format!("{}", ProducerEpoch::new(3)), "3");
}

#[test]
fn test_producer_epoch_default() {
    assert_eq!(ProducerEpoch::default().value(), 0);
}

#[test]
fn test_producer_epoch_ordering() {
    assert!(ProducerEpoch::new(1) < ProducerEpoch::new(2));
}

// ============================================================================
// PartitionId Tests
// ============================================================================

#[test]
fn test_partition_id_new() {
    let id = PartitionId::new("my-topic", 5);
    assert_eq!(id.topic(), "my-topic");
    assert_eq!(id.partition(), 5);
}

#[test]
fn test_partition_id_display() {
    let id = PartitionId::new("topic", 3);
    assert_eq!(format!("{}", id), "topic-3");
}

#[test]
fn test_partition_id_as_tuple() {
    let id = PartitionId::new("test", 7);
    let (topic, partition) = id.as_tuple();
    assert_eq!(topic, "test");
    assert_eq!(partition, 7);
}

#[test]
fn test_partition_id_into_tuple() {
    let id = PartitionId::new("test", 7);
    let (topic, partition) = id.into_tuple();
    assert_eq!(topic, "test");
    assert_eq!(partition, 7);
}

#[test]
fn test_partition_id_from_string_tuple() {
    let id: PartitionId = ("my-topic".to_string(), 5).into();
    assert_eq!(id.topic(), "my-topic");
    assert_eq!(id.partition(), 5);
}

#[test]
fn test_partition_id_from_str_tuple() {
    let id: PartitionId = ("my-topic", 5).into();
    assert_eq!(id.topic(), "my-topic");
    assert_eq!(id.partition(), 5);
}

#[test]
fn test_partition_id_into_string_tuple() {
    let id = PartitionId::new("topic", 3);
    let tuple: (String, i32) = id.into();
    assert_eq!(tuple, ("topic".to_string(), 3));
}

#[test]
fn test_partition_id_debug() {
    let id = PartitionId::new("test", 0);
    let debug_str = format!("{:?}", id);
    assert!(debug_str.contains("PartitionId"));
    assert!(debug_str.contains("test"));
}

#[test]
fn test_partition_id_clone() {
    let id = PartitionId::new("topic", 1);
    let cloned = id.clone();
    assert_eq!(id, cloned);
}

#[test]
fn test_partition_id_hash() {
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(PartitionId::new("topic", 0));
    set.insert(PartitionId::new("topic", 1));
    set.insert(PartitionId::new("topic", 0)); // Duplicate
    assert_eq!(set.len(), 2);
}

#[test]
fn test_partition_id_equality() {
    let id1 = PartitionId::new("topic", 0);
    let id2 = PartitionId::new("topic", 0);
    let id3 = PartitionId::new("topic", 1);
    let id4 = PartitionId::new("other", 0);

    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
    assert_ne!(id1, id4);
}
