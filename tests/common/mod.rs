//! Shared helpers used across integration tests.
//!
//! Cargo treats files in `tests/common/` as a sub-module rather than a
//! standalone test binary, so each test file pulls these in with
//! `mod common;` followed by `use common::*;`.

#![allow(dead_code)]
#![allow(unused_imports)]

pub mod broker_handle;
pub mod raft_helper;

use std::net::TcpListener;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;

pub use broker_handle::{BrokerHandle, ClusterHandle, bytes_to_vec};
pub use raft_helper::{
    build_single_node_raft, build_single_node_raft_with_id, raft_test_config, wait_for_raft_leader,
};

/// Allocate a free TCP port on `127.0.0.1` by binding ephemeral and reading
/// back the assigned port. The listener is dropped before returning, so the
/// caller can bind the port immediately. There is a small TOCTOU window —
/// acceptable in tests, where collisions just retry.
pub fn next_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral test port");
    listener.local_addr().expect("local_addr after bind").port()
}

/// Create a fresh in-memory object store for a test cluster.
pub fn create_object_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}
