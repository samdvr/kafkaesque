//! Shared serde helpers for the Raft state-machine domains.
//!
//! Every type that ends up in `CoordinationState` is serialized with
//! `postcard` for snapshot persistence and `InstallSnapshot` RPC payloads.
//! `postcard` is a positional codec — it writes maps as a length-prefixed
//! sequence of (k, v) pairs in the order the serializer emits them, and a
//! `HashMap` iterates in randomized order. Two replicas (or the same
//! replica before and after a process restart) can therefore encode the
//! same logical state to *different* byte sequences, breaking:
//!
//! - Snapshot canonicality — an `InstallSnapshot` payload that decodes
//!   into state X and re-encodes to bytes B' is supposed to satisfy
//!   `decode(B') == X`. The fuzz target `raft_snapshot` asserts this.
//! - State-machine determinism — replicas that hash log entries / state
//!   for replication-integrity checks must agree byte-for-byte.
//!
//! [`serialize_sorted_map`] sorts a `HashMap<K, V>` by key before
//! serializing so the output is deterministic. The wire format is
//! identical to the default `Serialize` impl for `HashMap`, so existing
//! on-disk snapshots still round-trip — only the *encoding* path changes.
//!
//! Apply with `#[serde(serialize_with = "serialize_sorted_map")]` on any
//! `HashMap` field that participates in `CoordinationState` serialization.
//! New `HashMap` fields without this attribute are caught by the
//! `raft_snapshot` fuzz target.

use std::collections::{HashMap, HashSet};

use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeMap;
use serde::ser::SerializeSeq;

/// Serialize a `HashMap<K, V>` with entries sorted by key.
///
/// `K` only needs `Ord + Serialize`; `V` only needs `Serialize`. The
/// allocation here (one `Vec` of borrowed pairs) is bounded by the map
/// size — same memory profile a default `Serialize` would have, just with
/// a `sort_by` between the iter and the writer.
pub(crate) fn serialize_sorted_map<K, V, S>(map: &HashMap<K, V>, ser: S) -> Result<S::Ok, S::Error>
where
    K: Ord + Serialize,
    V: Serialize,
    S: Serializer,
{
    let mut entries: Vec<(&K, &V)> = map.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let mut m = ser.serialize_map(Some(entries.len()))?;
    for (k, v) in entries {
        m.serialize_entry(k, v)?;
    }
    m.end()
}

/// Serialize a `HashSet<T>` as a sorted sequence.
///
/// Postcard encodes sets the same as sequences (length-prefixed, then
/// each element in iteration order). `HashSet` iteration is randomized,
/// so the same logical set can produce different byte sequences across
/// processes. Sorting at the serializer is the same fix as
/// [`serialize_sorted_map`], one type up.
pub(crate) fn serialize_sorted_set<T, S>(set: &HashSet<T>, ser: S) -> Result<S::Ok, S::Error>
where
    T: Ord + Serialize,
    S: Serializer,
{
    let mut entries: Vec<&T> = set.iter().collect();
    entries.sort();
    let mut s = ser.serialize_seq(Some(entries.len()))?;
    for t in entries {
        s.serialize_element(t)?;
    }
    s.end()
}
