//! Tests for ZombieModeState lifecycle.

use kafkaesque::cluster::ZombieModeState;

#[test]
fn test_zombie_mode_state_default() {
    let state = ZombieModeState::default();
    assert!(!state.is_active());
}

#[test]
fn test_zombie_mode_state_new() {
    let state = ZombieModeState::new();
    assert!(!state.is_active());
    assert_eq!(state.entered_at(), 0);
}

#[test]
fn test_zombie_mode_state_enter() {
    let state = ZombieModeState::new();
    assert!(!state.is_active());

    // First entry should return true
    let entered = state.enter();
    assert!(entered);
    assert!(state.is_active());
    assert!(state.entered_at() > 0);

    // Second entry should return false (already in zombie mode)
    let entered_again = state.enter();
    assert!(!entered_again);
    assert!(state.is_active());
}

#[test]
fn test_zombie_mode_state_debug() {
    let state = ZombieModeState::new();
    let debug = format!("{:?}", state);
    assert!(debug.contains("ZombieModeState"));
}
