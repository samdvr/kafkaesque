#![no_main]

//! P3-2: Config-parsing fuzzers.
//!
//! Covers three parser surfaces used at broker startup:
//!
//! 1. `ClusterProfile::from_str` — direct fuzz, arbitrary string in.
//! 2. `ObjectStoreType` selection — mirrors the `OBJECT_STORE_TYPE` switch
//!    in `ClusterConfig::from_env`. The enum has no public `from_str`, but
//!    its construction-from-string is the same hand-rolled match the env
//!    parser uses, so we drive it directly with arbitrary strings.
//! 3. `ClusterConfig::from_env` — stub driver that sets a small set of
//!    known env vars from arbitrary `(key_idx, value)` pairs, calls
//!    `from_env`, and asserts no panic. libFuzzer is single-threaded per
//!    iteration; a global mutex guards the env-var mutation against the
//!    test harness reading from env in parallel test threads (we're not
//!    in a test, but the lock keeps it safe if anyone changes that).
//!
//! Acceptance: no panics, ever.

use std::str::FromStr;
use std::sync::Mutex;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::{ClusterConfig, ClusterProfile, ObjectStoreType};

const MAX_INPUT: usize = 8 * 1024;

// Keys read by `ClusterConfig::from_env`. Order matters: `KEYS[i]` is what
// `(idx % KEYS.len())` selects in [`Input::env`]. Picked subset focuses on
// the integer-parse and fallback branches where panics are most plausible.
const KEYS: &[&str] = &[
    "BROKER_ID",
    "HOST",
    "ADVERTISED_HOST",
    "HOSTNAME",
    "PORT",
    "HEALTH_PORT",
    "METRICS_AUTH_TOKEN",
    "CLUSTER_ID",
    "RAFT_LISTEN_ADDR",
    "RAFT_PEERS",
    "DATA_PATH",
    "AUTO_CREATE_TOPICS",
    "DEFAULT_NUM_PARTITIONS",
    "MAX_MESSAGE_SIZE",
    "GLOBAL_INFLIGHT_BYTE_BUDGET",
    "MAX_FETCH_RESPONSE_SIZE",
    "MAX_CONCURRENT_PARTITION_WRITES",
    "MAX_CONCURRENT_PARTITION_READS",
    "BROKER_HEARTBEAT_TTL_SECS",
    "GROUP_MEMBER_TTL_SECS",
    "OWNERSHIP_CACHE_TTL_SECS",
    "PRODUCER_STATE_CACHE_TTL_SECS",
    "OBJECT_STORE_TYPE",
    "AWS_S3_BUCKET",
    "S3_BUCKET",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT",
    "S3_ENDPOINT",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "GCS_BUCKET",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "AZURE_CONTAINER",
    "AZURE_STORAGE_ACCOUNT",
    "AZURE_STORAGE_ACCESS_KEY",
    "SASL_ENABLED",
    "SASL_REQUIRED",
    "SASL_USERS_FILE",
    "SASL_PLAIN_REQUIRE_TLS",
    "ACL_ENABLED",
    "ACL_DENY_BY_DEFAULT",
    "KAFKAESQUE_SUPER_USERS",
    "KAFKAESQUE_ACL_BOOTSTRAP_FILE",
    "TLS_ENABLED",
    "TLS_CERT_PATH",
    "TLS_KEY_PATH",
    "FAIL_ON_RECOVERY_GAP",
    "RAFT_SNAPSHOT_THRESHOLD",
    "CLOCK_SKEW_TOLERANCE_MS",
    "SLATEDB_MAX_UNFLUSHED_BYTES",
    "SLATEDB_L0_SST_SIZE_BYTES",
    "SLATEDB_FLUSH_INTERVAL_MS",
    "LOG_RETENTION_MS",
    "LOG_RETENTION_CHECK_INTERVAL_SECS",
    "CONTROL_PLANE_THREADS",
    "DATA_PLANE_THREADS",
    "CLUSTER_PROFILE",
];

// Env mutation is process-global. Even though libFuzzer drives one input at
// a time per process, hold a mutex anyway so anyone wiring in `--jobs > 1`
// (separate processes) or background threads doesn't hit racy set_var.
static ENV_LOCK: Mutex<()> = Mutex::new(());

#[derive(Arbitrary, Debug)]
struct Input<'a> {
    profile_str: &'a str,
    object_store_type: &'a str,
    /// Sparse env-var bindings: each entry sets `KEYS[idx % KEYS.len()]` to
    /// `value` (lossy UTF-8). Entries can collide; later ones win.
    env: Vec<(u8, &'a str)>,
}

fuzz_target!(|raw: &[u8]| {
    if raw.len() > MAX_INPUT {
        return;
    }
    let Ok(input) = Input::arbitrary(&mut arbitrary::Unstructured::new(raw)) else {
        return;
    };

    // 1) ClusterProfile::from_str — must never panic.
    let _ = ClusterProfile::from_str(input.profile_str);

    // 2) ObjectStoreType selection — mirror the from_env switch on its own
    //    so libFuzzer gets coverage on this branch independent of from_env.
    let _ = drive_object_store_type(input.object_store_type);

    // 3) ClusterConfig::from_env stub driver.
    drive_from_env(&input.env);
});

/// Mirror the `OBJECT_STORE_TYPE` switch in `ClusterConfig::from_env`. We
/// can't call any `from_str` here (none is exposed), so this duplicates the
/// case-folded match. If this drifts from `from_env`, the from_env fuzz
/// branch will still cover the real path.
fn drive_object_store_type(s: &str) -> ObjectStoreType {
    match s.to_lowercase().as_str() {
        "s3" => ObjectStoreType::S3 {
            bucket: "fuzz-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        },
        "gcs" => ObjectStoreType::Gcs {
            bucket: "fuzz-bucket".to_string(),
            service_account_key: None,
        },
        "azure" => ObjectStoreType::Azure {
            container: "fuzz-container".to_string(),
            account: "fuzz-account".to_string(),
            access_key: None,
        },
        _ => ObjectStoreType::default(),
    }
}

fn drive_from_env(bindings: &[(u8, &str)]) {
    // SAFETY: env mutation is unsafe in edition 2024. We serialize with
    // ENV_LOCK and clear all KEYS both before and after the call so no
    // state leaks between iterations or to other tests in the binary.
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());

    unsafe {
        clear_all_keys();
        for (idx, value) in bindings {
            let key = KEYS[(*idx as usize) % KEYS.len()];
            // env vars must not contain interior NULs; reject those.
            if value.as_bytes().contains(&0) {
                continue;
            }
            std::env::set_var(key, value);
        }

        // Acceptance: no panics. We deliberately ignore Ok/Err — both
        // outcomes are valid; the criterion is the absence of a panic.
        let _ = ClusterConfig::from_env();

        clear_all_keys();
    }
}

/// SAFETY: caller holds `ENV_LOCK`. Edition 2024 marks `remove_var` unsafe;
/// the lock plus libFuzzer's per-process single-thread guarantee is enough
/// to make this race-free.
unsafe fn clear_all_keys() {
    for &k in KEYS {
        unsafe {
            std::env::remove_var(k);
        }
    }
}
