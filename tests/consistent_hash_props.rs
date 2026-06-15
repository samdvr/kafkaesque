//! Consistent-hash assignment stability property tests.
//!
//! `consistent_hash_assignment` (in `src/cluster/coordinator/mod.rs`) decides
//! which broker owns each `(topic, partition)` key. Reassignment moves data,
//! so two properties matter:
//!
//! 1. **Determinism.** The function is pure — two calls with the same inputs
//!    return the same broker.
//! 2. **Stability.** When the broker set changes by one (add or remove), the
//!    fraction of keys that change owner is bounded by `2 / min(n_before,
//!    n_after)`. This is the loose ceiling consistent hashing guarantees in
//!    expectation; the tight bound is `1 / min(n_before, n_after)` (the
//!    fraction owned by the affected broker), but variance with virtual nodes
//!    can drive an empirical measurement above the tight bound, so the plan
//!    uses `2/min` to keep the property robust.

use std::collections::BTreeSet;

use conhash::ConsistentHash;
use kafkaesque::cluster::{BrokerInfo, consistent_hash_assignment};
use kafkaesque::constants::VIRTUAL_NODES_PER_BROKER;
use proptest::prelude::*;

const TOPICS: &[&str] = &["orders", "billing", "payments", "events", "logs"];
/// Number of `(topic, partition)` keys evaluated per case. Big enough that
/// the empirical fraction of moved keys converges close to the theoretical
/// expectation; small enough to keep proptest cases fast.
const KEYS_PER_CASE: i32 = 256;

fn broker(id: i32) -> BrokerInfo {
    BrokerInfo {
        broker_id: id,
        host: format!("h{id}"),
        port: 9092,
        registered_at: 0,
    }
}

fn assignment_map(brokers: &[BrokerInfo]) -> Vec<((String, i32), i32)> {
    let mut out = Vec::with_capacity((TOPICS.len() as i32 * KEYS_PER_CASE) as usize);

    if brokers.is_empty() {
        return out;
    }

    // Build the ring once per broker set. `consistent_hash_assignment` rebuilds
    // it on every key lookup, which makes proptest (especially shrinking) crawl.
    let mut ring: Option<ConsistentHash<BrokerInfo>> = None;
    if brokers.len() > 1 {
        let mut r = ConsistentHash::new();
        for broker in brokers {
            r.add(broker, VIRTUAL_NODES_PER_BROKER);
        }
        ring = Some(r);
    }

    for t in TOPICS {
        for p in 0..KEYS_PER_CASE {
            let owner = if brokers.len() == 1 {
                brokers[0].broker_id
            } else {
                let partition_key = format!("{t}:{p}");
                ring.as_ref()
                    .and_then(|r| r.get_str(&partition_key))
                    .map(|b| b.broker_id)
                    .unwrap_or(brokers[0].broker_id)
            };
            out.push((((*t).to_string(), p), owner));
        }
    }
    out
}

fn changed_fraction(before: &[((String, i32), i32)], after: &[((String, i32), i32)]) -> f64 {
    assert_eq!(before.len(), after.len());
    let total = before.len() as f64;
    let changed = before
        .iter()
        .zip(after.iter())
        .filter(|(b, a)| b.1 != a.1)
        .count() as f64;
    changed / total
}

/// Generate two non-empty broker ID sets that differ by exactly one element
/// (one is the other plus a single broker).
fn arb_one_broker_change() -> impl Strategy<Value = (Vec<i32>, Vec<i32>)> {
    (
        prop::collection::btree_set(0i32..20, 2..6),
        0i32..20,
        any::<bool>(),
    )
        .prop_filter_map(
            "changed broker must not already be in the smaller set",
            |(set, extra, add)| {
                let smaller: BTreeSet<i32> = set.clone();
                if smaller.contains(&extra) {
                    return None;
                }
                let mut larger = smaller.clone();
                larger.insert(extra);

                let smaller_v: Vec<i32> = smaller.into_iter().collect();
                let larger_v: Vec<i32> = larger.into_iter().collect();

                // `add == true` means the change is "smaller -> larger" (broker
                // joins); `false` means "larger -> smaller" (broker leaves).
                if add {
                    Some((smaller_v, larger_v))
                } else {
                    Some((larger_v, smaller_v))
                }
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, .. ProptestConfig::default() })]

    /// Determinism: same inputs ⇒ same owner.
    #[test]
    fn assignment_is_deterministic(
        broker_ids in prop::collection::btree_set(0i32..32, 1..8),
        topic in prop::sample::select(TOPICS.to_vec()),
        partition in 0i32..1024,
    ) {
        let brokers: Vec<BrokerInfo> = broker_ids.into_iter().map(broker).collect();
        let a = consistent_hash_assignment(topic, partition, &brokers);
        let b = consistent_hash_assignment(topic, partition, &brokers);
        prop_assert_eq!(a, b);
    }

    /// Stability: changing the broker set by one moves at most `2 /
    /// min(n_before, n_after)` of the keys.
    #[test]
    fn one_broker_change_moves_few_keys(
        (before_ids, after_ids) in arb_one_broker_change(),
    ) {
        let brokers_before: Vec<BrokerInfo> = before_ids.iter().copied().map(broker).collect();
        let brokers_after: Vec<BrokerInfo> = after_ids.iter().copied().map(broker).collect();

        let assigned_before = assignment_map(&brokers_before);
        let assigned_after = assignment_map(&brokers_after);

        let n_min = brokers_before.len().min(brokers_after.len()) as f64;
        let bound = 2.0 / n_min;
        let observed = changed_fraction(&assigned_before, &assigned_after);

        prop_assert!(
            observed <= bound,
            "broker change before={:?} after={:?}: moved {:.3} of keys, bound {:.3}",
            before_ids, after_ids, observed, bound
        );
    }

    /// Sanity: every assignment falls on a broker that's actually in the
    /// supplied set.
    #[test]
    fn assignment_picks_a_real_broker(
        broker_ids in prop::collection::btree_set(0i32..32, 1..8),
        topic in prop::sample::select(TOPICS.to_vec()),
        partition in 0i32..1024,
    ) {
        let brokers: Vec<BrokerInfo> = broker_ids.iter().copied().map(broker).collect();
        let owner = consistent_hash_assignment(topic, partition, &brokers);
        prop_assert!(broker_ids.contains(&owner), "owner {owner} not in {:?}", broker_ids);
    }
}
