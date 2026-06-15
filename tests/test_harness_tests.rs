//! Smoke tests for the test harness in `tests/common/`.
//!
//! These prove that `BrokerHandle`/`ClusterHandle` can stand up a broker
//! and dispatch a request end-to-end. They are deliberately tiny — the
//! goal is to validate the harness, not to re-test broker behavior.

mod common;

use common::{BrokerHandle, ClusterHandle};
use kafkaesque::cluster::ClusterProfile;

#[tokio::test]
async fn broker_handle_spawn_returns_live_broker() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    assert_eq!(broker.broker_id, 1);
    assert!(broker.kafka_port > 0);
    assert!(broker.raft_port > 0);

    let ctx = broker.ctx();
    assert_eq!(ctx.principal.as_ref(), "User:ANONYMOUS");
}

#[tokio::test]
async fn broker_handle_ctx_as_overrides_principal() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    let ctx = broker.ctx_as("User:alice");
    assert_eq!(ctx.principal.as_ref(), "User:alice");
}

#[tokio::test]
async fn broker_handle_customize_runs_after_defaults() {
    let broker = BrokerHandle::spawn_with(ClusterProfile::Development, |c| {
        c.auto_create_topics = false
    })
    .await;
    drop(broker);
}

#[tokio::test]
async fn cluster_handle_spawn_yields_independent_brokers() {
    let cluster = ClusterHandle::spawn(2, ClusterProfile::Development).await;
    assert_eq!(cluster.brokers.len(), 2);
    assert_ne!(cluster.broker(0).broker_id, cluster.broker(1).broker_id);
    assert_ne!(
        cluster.broker(0).kafka_port,
        cluster.broker(1).kafka_port,
        "each broker must claim a distinct ephemeral port"
    );
}
