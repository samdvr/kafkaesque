//! Typed substruct *views* over the flat fields on [`super::ClusterConfig`].
//!
//! `ClusterConfig` keeps every knob as a top-level field for backwards
//! compatibility — every existing call site and test reads `config.foo`.
//! New code should prefer these grouped views: pulling the SASL knobs out
//! of an 80-field struct makes intent obvious and lets a function ask for
//! exactly the slice of configuration it needs.
//!
//! These structs do not own state; they're cheap copies of the relevant
//! `ClusterConfig` fields and are intended to be re-derived rather than
//! cached. Mutating a substruct does not write back to `ClusterConfig`.

use std::time::Duration;

use super::ClusterConfig;

/// SASL authentication knobs.
#[derive(Debug, Clone)]
pub struct SaslConfig {
    pub enabled: bool,
    pub required: bool,
    pub users_file: Option<String>,
    pub plain_require_tls: bool,
    pub require_tls: bool,
}

impl SaslConfig {
    /// Pull the SASL view from the parent config.
    pub fn from_cluster(cfg: &ClusterConfig) -> Self {
        Self {
            enabled: cfg.sasl_enabled,
            required: cfg.sasl_required,
            users_file: cfg.sasl_users_file.clone(),
            plain_require_tls: cfg.sasl_plain_require_tls,
            require_tls: cfg.sasl_require_tls,
        }
    }
}

/// TLS knobs for the Kafka client port. Distinct from
/// [`crate::server::tls::TlsConfig`], which is the *runtime* type built
/// from these paths — this struct is just a `&ClusterConfig` view.
#[derive(Debug, Clone)]
pub struct KafkaTlsConfig {
    pub enabled: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl KafkaTlsConfig {
    pub fn from_cluster(cfg: &ClusterConfig) -> Self {
        Self {
            enabled: cfg.tls_enabled,
            cert_path: cfg.tls_cert_path.clone(),
            key_path: cfg.tls_key_path.clone(),
        }
    }
}

/// Lease and ownership knobs.
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    pub duration: Duration,
    pub renewal_interval: Duration,
    pub heartbeat_interval: Duration,
    pub ownership_check_interval: Duration,
    pub min_lease_ttl_for_write_secs: u64,
}

impl LeaseConfig {
    pub fn from_cluster(cfg: &ClusterConfig) -> Self {
        Self {
            duration: cfg.lease_duration,
            renewal_interval: cfg.lease_renewal_interval,
            heartbeat_interval: cfg.heartbeat_interval,
            ownership_check_interval: cfg.ownership_check_interval,
            min_lease_ttl_for_write_secs: cfg.min_lease_ttl_for_write_secs,
        }
    }
}

/// ACL / authorization knobs.
#[derive(Debug, Clone)]
pub struct AclConfig {
    pub enabled: bool,
    pub deny_by_default: bool,
    pub super_users: Vec<String>,
    pub bootstrap_file: Option<String>,
}

impl AclConfig {
    pub fn from_cluster(cfg: &ClusterConfig) -> Self {
        Self {
            enabled: cfg.acl_enabled,
            deny_by_default: cfg.acl_deny_by_default,
            super_users: cfg.super_users.clone(),
            bootstrap_file: cfg.acl_bootstrap_file.clone(),
        }
    }
}

/// Auto-balancer knobs.
#[derive(Debug, Clone)]
pub struct AutoBalancerConfig {
    pub enabled: bool,
    pub evaluation_interval_secs: u64,
    pub deviation_threshold: f64,
    pub max_partitions_per_cycle: usize,
    pub cooldown_secs: u64,
    pub throughput_weight: f64,
}

impl AutoBalancerConfig {
    pub fn from_cluster(cfg: &ClusterConfig) -> Self {
        Self {
            enabled: cfg.auto_balancer_enabled,
            evaluation_interval_secs: cfg.auto_balancer_evaluation_interval_secs,
            deviation_threshold: cfg.auto_balancer_deviation_threshold,
            max_partitions_per_cycle: cfg.auto_balancer_max_partitions_per_cycle,
            cooldown_secs: cfg.auto_balancer_cooldown_secs,
            throughput_weight: cfg.auto_balancer_throughput_weight,
        }
    }
}

/// SlateDB tuning knobs.
#[derive(Debug, Clone)]
pub struct SlateDbTuningConfig {
    pub max_unflushed_bytes: usize,
    pub l0_sst_size_bytes: usize,
    pub flush_interval_ms: u64,
}

impl SlateDbTuningConfig {
    pub fn from_cluster(cfg: &ClusterConfig) -> Self {
        Self {
            max_unflushed_bytes: cfg.slatedb_max_unflushed_bytes,
            l0_sst_size_bytes: cfg.slatedb_l0_sst_size_bytes,
            flush_interval_ms: cfg.slatedb_flush_interval_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substructs_clone_from_default_config() {
        let cfg = ClusterConfig::default();
        let _ = SaslConfig::from_cluster(&cfg);
        let _ = KafkaTlsConfig::from_cluster(&cfg);
        let _ = LeaseConfig::from_cluster(&cfg);
        let _ = AclConfig::from_cluster(&cfg);
        let _ = AutoBalancerConfig::from_cluster(&cfg);
        let _ = SlateDbTuningConfig::from_cluster(&cfg);
    }

    #[test]
    fn sasl_view_reflects_field_changes() {
        let cfg = ClusterConfig {
            sasl_enabled: true,
            sasl_require_tls: false,
            ..Default::default()
        };
        let view = SaslConfig::from_cluster(&cfg);
        assert!(view.enabled);
        assert!(!view.require_tls);
    }
}
