//! P2-2: ACL evaluator decision property tests.
//!
//! `AclDomainState::is_authorized` mixes Allow/Deny precedence with literal,
//! prefix, and wildcard matchers. A misordered check is a silent auth bypass,
//! so we exercise the evaluator's monotonicity properties under proptest:
//!
//! 1. **Deny is monotone-decreasing** on the allowed set: adding a Deny
//!    binding never makes a previously-denied/unmatched tuple Allowed.
//! 2. **Allow is monotone-increasing** on the allowed set: adding an Allow
//!    binding never makes a previously-allowed tuple non-Allowed.
//! 3. **`describe` is exactly the filter**: the bindings returned by
//!    `AclDomainState::describe(filter)` equal the set of bindings for which
//!    every `Some(_)` field of the filter equals the binding's field.

use std::collections::HashSet;

use kafkaesque::cluster::raft::{
    AclBinding, AclCommand, AclDecision, AclDomainState, AclFilter, AclOperation, AclPatternType,
    AclPermissionType, AclResourceType,
};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

const PRINCIPALS: &[&str] = &["User:alice", "User:bob", "User:carol", "User:*"];
const HOSTS: &[&str] = &["10.0.0.1", "10.0.0.2", "*"];
const RESOURCE_NAMES: &[&str] = &["orders", "billing", "payments", "ord", "bill"];
const QUERY_RESOURCE_NAMES: &[&str] = &[
    "orders",
    "billing",
    "payments",
    "orders-2026",
    "billing-q4",
    "unrelated",
];

fn arb_resource_type() -> impl Strategy<Value = AclResourceType> {
    prop::sample::select(vec![
        AclResourceType::Cluster,
        AclResourceType::Topic,
        AclResourceType::Group,
    ])
}

fn arb_pattern_type() -> impl Strategy<Value = AclPatternType> {
    prop::sample::select(vec![AclPatternType::Literal, AclPatternType::Prefixed])
}

fn arb_operation() -> impl Strategy<Value = AclOperation> {
    prop::sample::select(vec![
        AclOperation::All,
        AclOperation::Read,
        AclOperation::Write,
        AclOperation::Create,
        AclOperation::Delete,
        AclOperation::Alter,
        AclOperation::Describe,
        AclOperation::ClusterAction,
        AclOperation::IdempotentWrite,
    ])
}

fn arb_permission() -> impl Strategy<Value = AclPermissionType> {
    prop::sample::select(vec![AclPermissionType::Allow, AclPermissionType::Deny])
}

fn arb_principal() -> impl Strategy<Value = String> {
    prop::sample::select(PRINCIPALS.to_vec()).prop_map(String::from)
}

fn arb_host() -> impl Strategy<Value = String> {
    prop::sample::select(HOSTS.to_vec()).prop_map(String::from)
}

fn arb_resource_name() -> impl Strategy<Value = String> {
    prop::sample::select(RESOURCE_NAMES.to_vec()).prop_map(String::from)
}

fn arb_binding() -> impl Strategy<Value = AclBinding> {
    (
        arb_resource_type(),
        arb_resource_name(),
        arb_pattern_type(),
        arb_principal(),
        arb_host(),
        arb_operation(),
        arb_permission(),
    )
        .prop_map(
            |(rt, name, pt, principal, host, op, perm)| AclBinding {
                resource_type: rt,
                resource_name: name,
                pattern_type: pt,
                principal,
                host,
                operation: op,
                permission: perm,
            },
        )
}

fn arb_filter() -> impl Strategy<Value = AclFilter> {
    (
        proptest::option::of(arb_resource_type()),
        proptest::option::of(arb_resource_name()),
        proptest::option::of(arb_pattern_type()),
        proptest::option::of(arb_principal()),
        proptest::option::of(arb_host()),
        proptest::option::of(arb_operation()),
        proptest::option::of(arb_permission()),
    )
        .prop_map(
            |(rt, name, pt, principal, host, op, perm)| AclFilter {
                resource_type: rt,
                resource_name: name,
                pattern_type: pt,
                principal,
                host,
                operation: op,
                permission: perm,
            },
        )
}

// ---------------------------------------------------------------------------
// Oracle for the filter logic — duplicated from `AclFilter::matches`, which
// is private to its module. Keeping the oracle separate is intentional: a
// bug in `matches` should be visible as a property failure, not silently
// shared with the test.
// ---------------------------------------------------------------------------

fn oracle_filter_matches(filter: &AclFilter, b: &AclBinding) -> bool {
    if let Some(t) = filter.resource_type
        && t != b.resource_type
    {
        return false;
    }
    if let Some(n) = &filter.resource_name
        && n != &b.resource_name
    {
        return false;
    }
    if let Some(p) = filter.pattern_type
        && p != b.pattern_type
    {
        return false;
    }
    if let Some(p) = &filter.principal
        && p != &b.principal
    {
        return false;
    }
    if let Some(h) = &filter.host
        && h != &b.host
    {
        return false;
    }
    if let Some(o) = filter.operation
        && o != b.operation
    {
        return false;
    }
    if let Some(p) = filter.permission
        && p != b.permission
    {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// Query universe — the (principal, host, op, resource_type, name) points we
// evaluate `is_authorized` at when comparing allowed-sets before/after a
// rule change.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Query {
    principal: String,
    host: String,
    op: AclOperation,
    resource_type: AclResourceType,
    resource_name: String,
}

fn query_universe() -> Vec<Query> {
    let mut out = Vec::new();
    let ops = [
        AclOperation::Read,
        AclOperation::Write,
        AclOperation::Describe,
        AclOperation::Delete,
        AclOperation::Alter,
        AclOperation::ClusterAction,
        AclOperation::IdempotentWrite,
        AclOperation::Create,
    ];
    let principals = ["User:alice", "User:bob", "User:carol"];
    let hosts = ["10.0.0.1", "10.0.0.2"];
    let rtypes = [
        AclResourceType::Cluster,
        AclResourceType::Topic,
        AclResourceType::Group,
    ];
    for p in principals {
        for h in hosts {
            for rt in rtypes {
                for name in QUERY_RESOURCE_NAMES {
                    for op in ops {
                        out.push(Query {
                            principal: p.to_string(),
                            host: h.to_string(),
                            op,
                            resource_type: rt,
                            resource_name: name.to_string(),
                        });
                    }
                }
            }
        }
    }
    out
}

fn allowed_set(state: &AclDomainState, queries: &[Query]) -> HashSet<usize> {
    queries
        .iter()
        .enumerate()
        .filter(|(_, q)| {
            state.is_authorized(&q.principal, &q.host, q.op, q.resource_type, &q.resource_name)
                == AclDecision::Allowed
        })
        .map(|(i, _)| i)
        .collect()
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    /// Adding a Deny binding never adds a tuple to the allowed set.
    #[test]
    fn deny_never_increases_allowed_set(
        seed in prop::collection::vec(arb_binding(), 0..16),
        new_binding in arb_binding(),
    ) {
        let queries = query_universe();
        let mut state = AclDomainState::new();
        state.apply(AclCommand::CreateAcls { bindings: seed });

        let before = allowed_set(&state, &queries);

        let deny = AclBinding {
            permission: AclPermissionType::Deny,
            ..new_binding
        };
        state.apply(AclCommand::CreateAcls { bindings: vec![deny] });

        let after = allowed_set(&state, &queries);
        prop_assert!(
            after.is_subset(&before),
            "Deny binding added queries to the allowed set: gained {:?}",
            after.difference(&before).collect::<Vec<_>>()
        );
    }

    /// Adding an Allow binding never removes a tuple from the allowed set.
    #[test]
    fn allow_never_decreases_allowed_set(
        seed in prop::collection::vec(arb_binding(), 0..16),
        new_binding in arb_binding(),
    ) {
        let queries = query_universe();
        let mut state = AclDomainState::new();
        state.apply(AclCommand::CreateAcls { bindings: seed });

        let before = allowed_set(&state, &queries);

        let allow = AclBinding {
            permission: AclPermissionType::Allow,
            ..new_binding
        };
        state.apply(AclCommand::CreateAcls { bindings: vec![allow] });

        let after = allowed_set(&state, &queries);
        prop_assert!(
            before.is_subset(&after),
            "Allow binding removed queries from the allowed set: lost {:?}",
            before.difference(&after).collect::<Vec<_>>()
        );
    }

    /// `describe(filter)` returns exactly the bindings the oracle accepts.
    #[test]
    fn describe_matches_filter_exactly(
        seed in prop::collection::vec(arb_binding(), 0..32),
        filter in arb_filter(),
    ) {
        let mut state = AclDomainState::new();
        state.apply(AclCommand::CreateAcls { bindings: seed });

        // The state's distinct bindings are whatever survived dedup.
        let all: HashSet<AclBinding> = state.bindings.iter().cloned().collect();
        let oracle_matches: HashSet<AclBinding> = all
            .iter()
            .filter(|b| oracle_filter_matches(&filter, b))
            .cloned()
            .collect();

        let described: HashSet<AclBinding> = state.describe(&filter).into_iter().collect();
        prop_assert_eq!(described, oracle_matches);
    }
}
