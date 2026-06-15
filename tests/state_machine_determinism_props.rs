//! Raft state-machine determinism property tests.
//!
//! `CoordinationStateMachine::apply_command` is the single point of state
//! mutation across the cluster. Any non-determinism here causes replica
//! divergence, so we exercise it under proptest with two angles:
//!
//! 1. **Same-sequence determinism.** Two fresh state machines that apply the
//!    same `Vec<CoordinationCommand>` must end up with content-equal state.
//!    We compare via a sorted fingerprint (not raw postcard bytes) because
//!    the underlying `HashMap`s use a per-instance random seed for hashing,
//!    so iteration order — and therefore serialized byte order — is not
//!    deterministic across two fresh maps even given identical content.
//!
//! 2. **Reorder invariance for independent commands.** Registering distinct
//!    brokers and creating distinct topics is order-insensitive; broker count
//!    and topic count must be the same after applying the same set in any
//!    order.

use kafkaesque::cluster::raft::{
    AclBinding, AclCommand, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    BrokerCommand, CoordinationCommand, CoordinationStateMachine, PartitionCommand,
};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

fn arb_broker_id() -> impl Strategy<Value = i32> {
    0i32..8
}

fn arb_partition_id() -> impl Strategy<Value = i32> {
    0i32..8
}

fn arb_topic_name() -> impl Strategy<Value = String> {
    prop::sample::select(vec!["a", "b", "c"]).prop_map(String::from)
}

fn arb_timestamp() -> impl Strategy<Value = u64> {
    0u64..1_000_000
}

fn arb_broker_command() -> impl Strategy<Value = BrokerCommand> {
    prop_oneof![
        (arb_broker_id(), arb_timestamp()).prop_map(|(id, ts)| BrokerCommand::Register {
            broker_id: id,
            host: format!("h{}", id),
            port: 9092,
            timestamp_ms: ts,
        }),
        (arb_broker_id(), arb_timestamp()).prop_map(|(id, ts)| BrokerCommand::Heartbeat {
            broker_id: id,
            timestamp_ms: ts,
            reported_local_timestamp_ms: ts,
        }),
        arb_broker_id().prop_map(|id| BrokerCommand::Unregister { broker_id: id }),
        arb_broker_id().prop_map(|id| BrokerCommand::Fence {
            broker_id: id,
            reason: "test".to_string(),
        }),
    ]
}

fn arb_partition_command() -> impl Strategy<Value = PartitionCommand> {
    prop_oneof![
        (arb_topic_name(), 1i32..4, arb_timestamp()).prop_map(|(name, n, ts)| {
            PartitionCommand::CreateTopic {
                name,
                partitions: n,
                config: Default::default(),
                timestamp_ms: ts,
            }
        }),
        arb_topic_name().prop_map(|name| PartitionCommand::DeleteTopic { name }),
        (
            arb_topic_name(),
            arb_partition_id(),
            arb_broker_id(),
            arb_timestamp()
        )
            .prop_map(|(t, p, b, ts)| PartitionCommand::AcquirePartition {
                topic: t,
                partition: p,
                broker_id: b,
                lease_duration_ms: 30_000,
                timestamp_ms: ts,
            }),
        (
            arb_topic_name(),
            arb_partition_id(),
            arb_broker_id(),
            arb_timestamp()
        )
            .prop_map(|(t, p, b, ts)| PartitionCommand::RenewLease {
                topic: t,
                partition: p,
                broker_id: b,
                lease_duration_ms: 30_000,
                timestamp_ms: ts,
            }),
        (arb_topic_name(), arb_partition_id(), arb_broker_id()).prop_map(|(t, p, b)| {
            PartitionCommand::ReleasePartition {
                topic: t,
                partition: p,
                broker_id: b,
            }
        }),
        arb_timestamp().prop_map(|ts| PartitionCommand::ExpireLeases {
            current_time_ms: ts
        }),
    ]
}

fn arb_acl_binding() -> impl Strategy<Value = AclBinding> {
    (
        prop::sample::select(vec![
            AclResourceType::Cluster,
            AclResourceType::Topic,
            AclResourceType::Group,
        ]),
        prop::sample::select(vec!["x".to_string(), "y".to_string()]),
        prop::sample::select(vec![AclPatternType::Literal, AclPatternType::Prefixed]),
        prop::sample::select(vec![
            "User:alice".to_string(),
            "User:bob".to_string(),
            "User:*".to_string(),
        ]),
        prop::sample::select(vec![
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::All,
            AclOperation::Describe,
        ]),
        prop::sample::select(vec![AclPermissionType::Allow, AclPermissionType::Deny]),
    )
        .prop_map(|(rt, name, pt, principal, op, perm)| AclBinding {
            resource_type: rt,
            resource_name: name,
            pattern_type: pt,
            principal,
            host: "*".to_string(),
            operation: op,
            permission: perm,
        })
}

fn arb_acl_command() -> impl Strategy<Value = AclCommand> {
    prop::collection::vec(arb_acl_binding(), 0..3)
        .prop_map(|bindings| AclCommand::CreateAcls { bindings })
}

fn arb_command() -> impl Strategy<Value = CoordinationCommand> {
    prop_oneof![
        Just(CoordinationCommand::Noop),
        arb_broker_command().prop_map(CoordinationCommand::BrokerDomain),
        arb_partition_command().prop_map(CoordinationCommand::PartitionDomain),
        arb_acl_command().prop_map(CoordinationCommand::AclDomain),
    ]
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn run_blocking<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime")
        .block_on(fut)
}

fn apply_all(cmds: &[CoordinationCommand]) -> CoordinationStateMachine {
    let sm = CoordinationStateMachine::new();
    run_blocking(async {
        for cmd in cmds {
            sm.apply_command(cmd.clone()).await;
        }
    });
    sm
}

/// Sorted, content-based fingerprint of the state machine. Two state machines
/// with the same logical content always produce the same fingerprint, even
/// though their internal `HashMap`s may iterate in different orders.
fn fingerprint(sm: &CoordinationStateMachine) -> String {
    run_blocking(async {
        let state = sm.state().await;
        let mut out = String::new();
        out.push_str(&format!(
            "v={};clock={};",
            state.version, state.lease_clock_ms
        ));

        let mut brokers: Vec<_> = state.broker_domain.brokers.iter().collect();
        brokers.sort_by_key(|(id, _)| **id);
        for (id, info) in brokers {
            out.push_str(&format!(
                "B{}:{}:{}:{}:{:?}:{}:{};",
                id,
                info.host,
                info.port,
                info.registered_at_ms,
                info.status,
                info.last_heartbeat_ms,
                info.reported_timestamp_ms,
            ));
        }
        let mut skews: Vec<_> = state.broker_domain.observed_clock_skew_ms.iter().collect();
        skews.sort_by_key(|(id, _)| **id);
        for (id, skew) in skews {
            out.push_str(&format!("S{}:{};", id, skew));
        }

        let mut topics: Vec<_> = state.partition_domain.topics.iter().collect();
        topics.sort_by(|a, b| a.0.cmp(b.0));
        for (name, info) in topics {
            let mut cfg: Vec<_> = info.config.iter().collect();
            cfg.sort_by(|a, b| a.0.cmp(b.0));
            out.push_str(&format!(
                "T{}:p={}:c={}:{:?};",
                name, info.partition_count, info.created_at_ms, cfg
            ));
        }
        let mut parts: Vec<_> = state.partition_domain.partitions.iter().collect();
        parts.sort_by(|a, b| (&a.0.0, a.0.1).cmp(&(&b.0.0, b.0.1)));
        for ((t, p), info) in parts {
            out.push_str(&format!(
                "P{}/{}:o={:?}:ep={}:le={};",
                t, p, info.owner_broker_id, info.leader_epoch, info.lease_expires_at_ms
            ));
        }

        let mut acls: Vec<_> = state
            .acl_domain
            .bindings
            .iter()
            .map(|b| format!("{:?}", b))
            .collect();
        acls.sort();
        for a in acls {
            out.push_str(&format!("A:{};", a));
        }

        out
    })
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    /// Same sequence ⇒ content-equal state across two fresh state machines.
    #[test]
    fn same_sequence_is_deterministic(
        cmds in prop::collection::vec(arb_command(), 0..32),
    ) {
        let a = apply_all(&cmds);
        let b = apply_all(&cmds);
        prop_assert_eq!(fingerprint(&a), fingerprint(&b));
    }

    /// Reordering pairwise-independent commands (registers of distinct
    /// broker IDs, creates of distinct topics) preserves the high-level
    /// invariants (broker count, topic count). Strict equality wouldn't
    /// hold because `version` is sequence-position-dependent.
    #[test]
    fn independent_commands_preserve_invariants_under_reorder(
        order_seed in 0u64..1_000_000,
        broker_ids in prop::collection::btree_set(0i32..16, 0..6),
        topic_names in prop::collection::btree_set(
            prop::sample::select(vec!["t1", "t2", "t3", "t4", "t5", "t6"]).prop_map(String::from),
            0..6,
        ),
    ) {
        let broker_ids: Vec<i32> = broker_ids.into_iter().collect();
        let topic_names: Vec<String> = topic_names.into_iter().collect();

        let mut cmds_a: Vec<CoordinationCommand> = Vec::new();
        for (i, id) in broker_ids.iter().enumerate() {
            cmds_a.push(CoordinationCommand::BrokerDomain(BrokerCommand::Register {
                broker_id: *id,
                host: format!("h{}", i),
                port: 9092,
                timestamp_ms: 1_000,
            }));
        }
        for n in topic_names.iter() {
            cmds_a.push(CoordinationCommand::PartitionDomain(PartitionCommand::CreateTopic {
                name: n.clone(),
                partitions: 3,
                config: Default::default(),
                timestamp_ms: 1_000,
            }));
        }

        // Deterministic Fisher–Yates shuffle from the seed.
        let mut cmds_b = cmds_a.clone();
        let mut s = order_seed.wrapping_add(1);
        for i in (1..cmds_b.len()).rev() {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = (s >> 33) as usize % (i + 1);
            cmds_b.swap(i, j);
        }

        let a = apply_all(&cmds_a);
        let b = apply_all(&cmds_b);

        let summary_a = run_blocking(async {
            let s = a.state().await;
            (s.broker_domain.brokers.len(), s.partition_domain.topics.len())
        });
        let summary_b = run_blocking(async {
            let s = b.state().await;
            (s.broker_domain.brokers.len(), s.partition_domain.topics.len())
        });

        prop_assert_eq!(summary_a, summary_b);
    }
}
