//! Tests for PartitionState state machine.

use kafkaesque::cluster::PartitionState;

#[test]
fn test_partition_state_unowned() {
    let state = PartitionState::unowned();
    assert!(!state.is_owned());
    assert!(!state.is_transitioning());
    assert!(!state.is_fenced());
    assert_eq!(state.state_name(), "unowned");
}

#[test]
fn test_partition_state_acquiring() {
    let state = PartitionState::start_acquiring();
    assert!(state.is_transitioning());
    assert!(!state.is_owned());
    assert!(!state.is_fenced());
    assert_eq!(state.state_name(), "acquiring");
}

#[test]
fn test_partition_state_releasing() {
    let state = PartitionState::start_releasing();
    assert!(state.is_transitioning());
    assert!(!state.is_owned());
    assert_eq!(state.state_name(), "releasing");
}

#[test]
fn test_partition_state_fenced() {
    let state = PartitionState::fenced();
    assert!(state.is_fenced());
    assert!(!state.is_owned());
    assert_eq!(state.state_name(), "fenced");
}

#[test]
fn test_partition_state_default() {
    let state = PartitionState::default();
    assert!(!state.is_owned());
    assert!(!state.is_transitioning());
    assert_eq!(state.state_name(), "unowned");
}

#[test]
fn test_partition_state_store_none() {
    let state = PartitionState::unowned();
    assert!(state.store().is_none());
}

#[test]
fn test_partition_state_debug() {
    let state = PartitionState::unowned();
    let debug = format!("{:?}", state);
    assert!(debug.contains("Unowned"));

    let acquiring = PartitionState::start_acquiring();
    let debug = format!("{:?}", acquiring);
    assert!(debug.contains("Acquiring"));
}
