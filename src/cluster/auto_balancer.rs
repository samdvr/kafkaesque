//! Auto-balancing algorithm for partition distribution.
//!
//! This module implements a throughput-weighted load balancing algorithm that
//! redistributes partitions across brokers to maintain even load distribution.
//!
//! Broker load is scored using a weighted combination of partition count and
//! throughput (default: 30% count, 70% throughput). When deviation exceeds
//! the configured threshold, partitions move from overloaded to underloaded
//! brokers. Cooldown periods and move limits prevent thrashing.

use dashmap::DashMap;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use super::load_metrics::BrokerLoadSummary;

/// Configuration for the auto-balancer.
#[derive(Debug, Clone)]
pub struct AutoBalancerConfig {
    /// Whether auto-balancing is enabled.
    pub enabled: bool,

    /// How often to evaluate load balance (seconds).
    pub evaluation_interval_secs: u64,

    /// Load deviation threshold that triggers rebalancing.
    /// Default 0.3 means 30% deviation from mean triggers rebalance.
    pub deviation_threshold: f64,

    /// Maximum partitions to move per evaluation cycle.
    /// Limits cascading failures from aggressive rebalancing.
    pub max_partitions_per_cycle: usize,

    /// Cooldown period (seconds) before a partition can be moved again.
    /// Prevents thrashing from repeated moves.
    pub cooldown_per_partition_secs: u64,

    /// Weight for throughput in broker score calculation.
    /// Default 0.7 means 70% throughput, 30% partition count.
    pub throughput_weight: f64,

    /// Minimum load difference (bytes/sec) required to trigger a move.
    /// Prevents unnecessary moves for small imbalances.
    pub min_load_diff_threshold: f64,

    /// Threshold multiplier for identifying overloaded brokers.
    /// Default 1.15 means 15% above average is overloaded.
    pub overload_threshold: f64,

    /// Threshold multiplier for identifying underloaded brokers.
    /// Default 0.85 means 15% below average is underloaded.
    pub underload_threshold: f64,

    /// Maximum staleness for load data (milliseconds).
    ///
    /// If broker_loads or partition_loads have timestamps older than this
    /// relative to the newest data, a warning is logged and the evaluation
    /// may be skipped or use stale data with caution.
    ///
    /// Default: 5000ms (5 seconds)
    pub max_data_staleness_ms: u64,
}

impl Default for AutoBalancerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            evaluation_interval_secs: 60,
            deviation_threshold: 0.3,
            max_partitions_per_cycle: 5,
            cooldown_per_partition_secs: 300,
            throughput_weight: 0.7,
            min_load_diff_threshold: 1_000_000.0, // 1 MB/s
            overload_threshold: 1.15,
            underload_threshold: 0.85,
            max_data_staleness_ms: 5000, // 5 seconds default
        }
    }
}

impl AutoBalancerConfig {
    /// Create a config with custom throughput weight.
    pub fn with_throughput_weight(mut self, weight: f64) -> Self {
        self.throughput_weight = weight.clamp(0.0, 1.0);
        self
    }

    /// Create a config with custom deviation threshold.
    pub fn with_deviation_threshold(mut self, threshold: f64) -> Self {
        self.deviation_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    pub fn evaluation_interval(&self) -> Duration {
        Duration::from_secs(self.evaluation_interval_secs)
    }

    pub fn cooldown_duration(&self) -> Duration {
        Duration::from_secs(self.cooldown_per_partition_secs)
    }
}

/// A proposed partition move.
#[derive(Debug, Clone)]
pub struct PartitionMove {
    /// Topic of the partition to move.
    pub topic: String,
    /// Partition index to move.
    pub partition: i32,
    /// Source broker ID.
    pub from_broker_id: i32,
    /// Destination broker ID.
    pub to_broker_id: i32,
    /// Estimated load of this partition (bytes/sec).
    pub estimated_load: f64,
    /// Priority score for ordering moves (higher = more urgent).
    pub priority: f64,
}

/// Reason for rebalancing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceReason {
    /// Load deviation exceeds threshold.
    HighDeviation,
    /// Single broker is significantly overloaded.
    BrokerOverload,
    /// Manual trigger.
    ManualTrigger,
}

impl std::fmt::Display for RebalanceReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RebalanceReason::HighDeviation => write!(f, "high_deviation"),
            RebalanceReason::BrokerOverload => write!(f, "broker_overload"),
            RebalanceReason::ManualTrigger => write!(f, "manual_trigger"),
        }
    }
}

/// Decision from the auto-balancer evaluation.
#[derive(Debug, Clone)]
pub struct RebalanceDecision {
    /// Whether rebalancing should occur.
    pub should_rebalance: bool,
    /// Current load deviation (0.0 to 1.0+).
    pub current_deviation: f64,
    /// Proposed partition moves (ordered by priority).
    pub moves: Vec<PartitionMove>,
    /// Reason for rebalancing (if any).
    pub reason: Option<RebalanceReason>,
    /// Broker scores used in this decision.
    pub broker_scores: HashMap<i32, f64>,
}

impl RebalanceDecision {
    /// Create a no-rebalance decision.
    pub fn no_rebalance(current_deviation: f64, broker_scores: HashMap<i32, f64>) -> Self {
        Self {
            should_rebalance: false,
            current_deviation,
            moves: Vec::new(),
            reason: None,
            broker_scores,
        }
    }
}

/// A consistent snapshot of load data for rebalancing decisions.
///
/// This struct ensures that all load data used for a rebalancing decision
/// comes from the same observation window, preventing race conditions where
/// stale ownership data combined with fresh load data causes incorrect moves.
#[derive(Debug, Clone)]
pub struct LoadSnapshot {
    /// Broker load summaries.
    pub broker_loads: HashMap<i32, BrokerLoadSummary>,
    /// Partition ownership: (topic, partition) -> broker_id.
    pub partition_owners: HashMap<(String, i32), i32>,
    /// Partition loads: (topic, partition) -> bytes/sec.
    pub partition_loads: HashMap<(String, i32), f64>,
    /// When this snapshot was taken (milliseconds since epoch).
    pub timestamp_ms: u64,
    /// Optional: Raft log index this data corresponds to.
    /// If set, all data should be from the same committed state.
    pub raft_index: Option<u64>,
}

impl LoadSnapshot {
    /// Create a new load snapshot with the current timestamp.
    pub fn new(
        broker_loads: HashMap<i32, BrokerLoadSummary>,
        partition_owners: HashMap<(String, i32), i32>,
        partition_loads: HashMap<(String, i32), f64>,
    ) -> Self {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            broker_loads,
            partition_owners,
            partition_loads,
            timestamp_ms,
            raft_index: None,
        }
    }

    /// Create a snapshot with explicit timestamp.
    pub fn with_timestamp(
        broker_loads: HashMap<i32, BrokerLoadSummary>,
        partition_owners: HashMap<(String, i32), i32>,
        partition_loads: HashMap<(String, i32), f64>,
        timestamp_ms: u64,
    ) -> Self {
        Self {
            broker_loads,
            partition_owners,
            partition_loads,
            timestamp_ms,
            raft_index: None,
        }
    }

    /// Create a snapshot with Raft index for consistency tracking.
    pub fn with_raft_index(
        broker_loads: HashMap<i32, BrokerLoadSummary>,
        partition_owners: HashMap<(String, i32), i32>,
        partition_loads: HashMap<(String, i32), f64>,
        raft_index: u64,
    ) -> Self {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            broker_loads,
            partition_owners,
            partition_loads,
            timestamp_ms,
            raft_index: Some(raft_index),
        }
    }

    /// Check if this snapshot is stale compared to current time.
    pub fn is_stale(&self, max_staleness_ms: u64) -> bool {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        now_ms.saturating_sub(self.timestamp_ms) > max_staleness_ms
    }

    pub fn age_ms(&self) -> u64 {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        now_ms.saturating_sub(self.timestamp_ms)
    }
}

/// Auto-balancer for partition distribution.
///
/// Evaluates cluster load and generates partition moves to maintain
/// even distribution across brokers.
pub struct AutoBalancer {
    config: AutoBalancerConfig,

    /// Cooldown tracking: (topic, partition) -> last_move_time.
    cooldowns: DashMap<(String, i32), Instant>,

    /// Last evaluation time.
    last_evaluation: Option<Instant>,

    /// Total moves executed (for metrics).
    total_moves: u64,

    /// Total evaluations performed.
    total_evaluations: u64,
}

impl AutoBalancer {
    /// Create a new auto-balancer with the given configuration.
    pub fn new(config: AutoBalancerConfig) -> Self {
        info!(
            enabled = config.enabled,
            deviation_threshold = config.deviation_threshold,
            max_partitions_per_cycle = config.max_partitions_per_cycle,
            throughput_weight = config.throughput_weight,
            "Creating auto-balancer"
        );

        Self {
            config,
            cooldowns: DashMap::new(),
            last_evaluation: None,
            total_moves: 0,
            total_evaluations: 0,
        }
    }

    /// Create a new auto-balancer with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(AutoBalancerConfig::default())
    }

    pub fn config(&self) -> &AutoBalancerConfig {
        &self.config
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn should_evaluate(&self) -> bool {
        if !self.config.enabled {
            return false;
        }

        match self.last_evaluation {
            Some(last) => last.elapsed() >= self.config.evaluation_interval(),
            None => true, // First evaluation
        }
    }

    /// Evaluate current load and decide if rebalancing is needed.
    ///
    /// # Arguments
    /// * `broker_loads` - Current load summary per broker
    /// * `partition_owners` - Current partition ownership: (topic, partition) -> broker_id
    /// * `partition_loads` - Current partition loads: (topic, partition) -> bytes/sec
    ///
    /// # Note
    /// Prefer using `evaluate_snapshot` which provides staleness checking.
    pub fn evaluate(
        &mut self,
        broker_loads: &HashMap<i32, BrokerLoadSummary>,
        partition_owners: &HashMap<(String, i32), i32>,
        partition_loads: &HashMap<(String, i32), f64>,
    ) -> RebalanceDecision {
        // Create a snapshot and delegate to evaluate_snapshot
        let snapshot = LoadSnapshot::new(
            broker_loads.clone(),
            partition_owners.clone(),
            partition_loads.clone(),
        );
        self.evaluate_snapshot(&snapshot)
    }

    /// Evaluate load using a consistent snapshot.
    ///
    /// This method validates that the snapshot is not stale before making
    /// rebalancing decisions. Using a snapshot ensures all load data comes
    /// from the same observation window.
    ///
    /// # Arguments
    /// * `snapshot` - A LoadSnapshot containing all load data from a single point in time
    ///
    /// # Returns
    /// A RebalanceDecision, potentially with `should_rebalance = false` if data is stale.
    pub fn evaluate_snapshot(&mut self, snapshot: &LoadSnapshot) -> RebalanceDecision {
        self.total_evaluations += 1;
        self.last_evaluation = Some(Instant::now());

        // Check for stale data
        if snapshot.is_stale(self.config.max_data_staleness_ms) {
            let age_ms = snapshot.age_ms();
            warn!(
                snapshot_age_ms = age_ms,
                max_staleness_ms = self.config.max_data_staleness_ms,
                "Load snapshot is stale - skipping rebalance to prevent \
                 incorrect decisions based on outdated data"
            );
            return RebalanceDecision::no_rebalance(0.0, HashMap::new());
        }

        debug!(
            snapshot_age_ms = snapshot.age_ms(),
            raft_index = ?snapshot.raft_index,
            broker_count = snapshot.broker_loads.len(),
            partition_count = snapshot.partition_owners.len(),
            "Evaluating load snapshot"
        );

        if snapshot.broker_loads.len() < 2 {
            debug!("Skipping rebalance: less than 2 brokers");
            return RebalanceDecision::no_rebalance(0.0, HashMap::new());
        }

        let broker_scores = self.calculate_broker_scores(&snapshot.broker_loads);
        let deviation = self.calculate_deviation(&broker_scores);

        debug!(
            broker_count = snapshot.broker_loads.len(),
            partition_count = snapshot.partition_owners.len(),
            deviation = format!("{:.2}%", deviation * 100.0),
            "Evaluating cluster balance"
        );

        if deviation < self.config.deviation_threshold {
            debug!(
                deviation = format!("{:.2}%", deviation * 100.0),
                threshold = format!("{:.2}%", self.config.deviation_threshold * 100.0),
                "Cluster is balanced, no rebalance needed"
            );
            return RebalanceDecision::no_rebalance(deviation, broker_scores);
        }

        let moves = self.generate_moves(
            &broker_scores,
            &snapshot.broker_loads,
            &snapshot.partition_owners,
            &snapshot.partition_loads,
        );

        if moves.is_empty() {
            debug!(
                "No valid moves available (all partitions in cooldown or insufficient load diff)"
            );
            return RebalanceDecision::no_rebalance(deviation, broker_scores);
        }

        info!(
            deviation = format!("{:.2}%", deviation * 100.0),
            move_count = moves.len(),
            snapshot_age_ms = snapshot.age_ms(),
            "Rebalancing needed"
        );

        RebalanceDecision {
            should_rebalance: true,
            current_deviation: deviation,
            moves,
            reason: Some(RebalanceReason::HighDeviation),
            broker_scores,
        }
    }

    /// Calculate weighted scores for each broker.
    ///
    /// Score = partition_count * (1 - throughput_weight) + normalized_throughput * throughput_weight
    fn calculate_broker_scores(
        &self,
        broker_loads: &HashMap<i32, BrokerLoadSummary>,
    ) -> HashMap<i32, f64> {
        if broker_loads.is_empty() {
            return HashMap::new();
        }

        // Find max values for normalization
        let max_partition_count = broker_loads
            .values()
            .map(|b| b.partition_count)
            .max()
            .unwrap_or(1)
            .max(1) as f64;

        let max_throughput = broker_loads
            .values()
            .map(|b| b.total_bytes_per_sec)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(1.0)
            .max(1.0);

        let count_weight = 1.0 - self.config.throughput_weight;
        let throughput_weight = self.config.throughput_weight;

        broker_loads
            .iter()
            .map(|(&broker_id, load)| {
                let normalized_count = load.partition_count as f64 / max_partition_count;
                let normalized_throughput = load.total_bytes_per_sec / max_throughput;

                let score =
                    normalized_count * count_weight + normalized_throughput * throughput_weight;

                (broker_id, score)
            })
            .collect()
    }

    /// Calculate load deviation across brokers.
    ///
    /// Returns (max - min) / avg, or 0.0 if insufficient data.
    fn calculate_deviation(&self, broker_scores: &HashMap<i32, f64>) -> f64 {
        if broker_scores.len() < 2 {
            return 0.0;
        }

        let scores: Vec<f64> = broker_scores.values().copied().collect();
        let min_score = scores
            .iter()
            .copied()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_score = scores
            .iter()
            .copied()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let avg_score: f64 = scores.iter().sum::<f64>() / scores.len() as f64;

        if avg_score <= 0.0 {
            return 0.0;
        }

        (max_score - min_score) / avg_score
    }

    /// Generate partition moves from overloaded to underloaded brokers.
    fn generate_moves(
        &self,
        broker_scores: &HashMap<i32, f64>,
        broker_loads: &HashMap<i32, BrokerLoadSummary>,
        _partition_owners: &HashMap<(String, i32), i32>,
        partition_loads: &HashMap<(String, i32), f64>,
    ) -> Vec<PartitionMove> {
        let scores: Vec<f64> = broker_scores.values().copied().collect();
        let avg_score: f64 = scores.iter().sum::<f64>() / scores.len() as f64;

        let overload_threshold = avg_score * self.config.overload_threshold;
        let underload_threshold = avg_score * self.config.underload_threshold;

        // Find overloaded and underloaded brokers
        let overloaded: Vec<i32> = broker_scores
            .iter()
            .filter(|(_, score)| **score > overload_threshold)
            .map(|(&id, _)| id)
            .collect();

        let underloaded: Vec<i32> = broker_scores
            .iter()
            .filter(|(_, score)| **score < underload_threshold)
            .map(|(&id, _)| id)
            .collect();

        if overloaded.is_empty() || underloaded.is_empty() {
            debug!(
                overloaded_count = overloaded.len(),
                underloaded_count = underloaded.len(),
                "No overloaded/underloaded broker pairs"
            );
            return Vec::new();
        }

        debug!(
            overloaded = ?overloaded,
            underloaded = ?underloaded,
            avg_score = format!("{:.3}", avg_score),
            "Identified load imbalance"
        );

        // Generate candidate moves
        let mut candidates: Vec<PartitionMove> = Vec::new();

        for &from_broker in &overloaded {
            // Get partitions on this broker
            if let Some(broker_load) = broker_loads.get(&from_broker) {
                for partition_info in &broker_load.partitions {
                    let key = (partition_info.topic.clone(), partition_info.partition);

                    if self.is_in_cooldown(&key) {
                        debug!(
                            topic = %partition_info.topic,
                            partition = partition_info.partition,
                            "Partition in cooldown, skipping"
                        );
                        continue;
                    }

                    let partition_load = partition_loads
                        .get(&key)
                        .copied()
                        .unwrap_or(partition_info.bytes_per_sec);

                    if partition_load < self.config.min_load_diff_threshold {
                        continue;
                    }

                    if let Some(&to_broker) = underloaded.first() {
                        if to_broker == from_broker {
                            continue;
                        }

                        let from_score = broker_scores.get(&from_broker).copied().unwrap_or(0.0);
                        let to_score = broker_scores.get(&to_broker).copied().unwrap_or(0.0);
                        let priority = (from_score - to_score) * partition_load;

                        candidates.push(PartitionMove {
                            topic: partition_info.topic.clone(),
                            partition: partition_info.partition,
                            from_broker_id: from_broker,
                            to_broker_id: to_broker,
                            estimated_load: partition_load,
                            priority,
                        });
                    }
                }
            }
        }

        candidates.sort_by(|a, b| b.priority.partial_cmp(&a.priority).unwrap());
        candidates.truncate(self.config.max_partitions_per_cycle);

        candidates
    }

    /// Check if a partition is in cooldown period.
    fn is_in_cooldown(&self, key: &(String, i32)) -> bool {
        if let Some(entry) = self.cooldowns.get(key) {
            let elapsed = entry.value().elapsed();
            elapsed < self.config.cooldown_duration()
        } else {
            false
        }
    }

    /// Record that a partition was moved (start cooldown).
    pub fn record_move(&mut self, topic: &str, partition: i32) {
        self.cooldowns
            .insert((topic.to_string(), partition), Instant::now());
        self.total_moves += 1;

        debug!(
            topic,
            partition,
            cooldown_secs = self.config.cooldown_per_partition_secs,
            "Recorded partition move, cooldown started"
        );
    }

    /// Clean up expired cooldown entries.
    pub fn cleanup_cooldowns(&self) {
        let cooldown_duration = self.config.cooldown_duration();
        let before = self.cooldowns.len();

        self.cooldowns
            .retain(|_, instant| instant.elapsed() < cooldown_duration);

        let removed = before - self.cooldowns.len();
        if removed > 0 {
            debug!(removed, "Cleaned up expired cooldown entries");
        }
    }

    /// Get the number of active cooldowns.
    pub fn active_cooldown_count(&self) -> usize {
        self.cooldowns.len()
    }

    pub fn total_moves(&self) -> u64 {
        self.total_moves
    }

    pub fn total_evaluations(&self) -> u64 {
        self.total_evaluations
    }

    /// Force a rebalance evaluation regardless of timing.
    pub fn force_evaluation(
        &mut self,
        broker_loads: &HashMap<i32, BrokerLoadSummary>,
        partition_owners: &HashMap<(String, i32), i32>,
        partition_loads: &HashMap<(String, i32), f64>,
    ) -> RebalanceDecision {
        let mut decision = self.evaluate(broker_loads, partition_owners, partition_loads);
        if !decision.should_rebalance && !decision.moves.is_empty() {
            decision.reason = Some(RebalanceReason::ManualTrigger);
            decision.should_rebalance = true;
        }
        decision
    }
}

impl Default for AutoBalancer {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::load_metrics::PartitionLoadInfo;

    fn make_broker_load(
        broker_id: i32,
        partition_count: usize,
        bytes_per_sec: f64,
    ) -> BrokerLoadSummary {
        BrokerLoadSummary {
            broker_id,
            partition_count,
            total_bytes_per_sec: bytes_per_sec,
            total_messages_per_sec: 0.0,
            load_score: 0.0,
            partitions: (0..partition_count as i32)
                .map(|p| PartitionLoadInfo {
                    topic: "test-topic".to_string(),
                    partition: p,
                    bytes_per_sec: bytes_per_sec / partition_count as f64,
                    messages_per_sec: 0.0,
                })
                .collect(),
        }
    }

    #[test]
    fn test_config_defaults() {
        let config = AutoBalancerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.deviation_threshold, 0.3);
        assert_eq!(config.throughput_weight, 0.7);
        assert_eq!(config.max_partitions_per_cycle, 5);
    }

    #[test]
    fn test_no_rebalance_single_broker() {
        let mut balancer = AutoBalancer::with_defaults();

        let mut broker_loads = HashMap::new();
        broker_loads.insert(1, make_broker_load(1, 10, 1_000_000.0));

        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();

        let decision = balancer.evaluate(&broker_loads, &partition_owners, &partition_loads);
        assert!(!decision.should_rebalance);
    }

    #[test]
    fn test_no_rebalance_balanced_cluster() {
        let mut balancer = AutoBalancer::with_defaults();

        let mut broker_loads = HashMap::new();
        broker_loads.insert(1, make_broker_load(1, 10, 1_000_000.0));
        broker_loads.insert(2, make_broker_load(2, 10, 1_000_000.0));
        broker_loads.insert(3, make_broker_load(3, 10, 1_000_000.0));

        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();

        let decision = balancer.evaluate(&broker_loads, &partition_owners, &partition_loads);
        assert!(!decision.should_rebalance);
        assert!(decision.current_deviation < 0.1);
    }

    #[test]
    fn test_rebalance_imbalanced_cluster() {
        let mut balancer = AutoBalancer::new(AutoBalancerConfig {
            min_load_diff_threshold: 0.0, // Disable for test
            ..Default::default()
        });

        let mut broker_loads = HashMap::new();
        // Broker 1 is heavily loaded
        broker_loads.insert(1, make_broker_load(1, 20, 10_000_000.0));
        // Broker 2 is lightly loaded
        broker_loads.insert(2, make_broker_load(2, 5, 1_000_000.0));

        let mut partition_owners = HashMap::new();
        for i in 0..20 {
            partition_owners.insert(("test-topic".to_string(), i), 1);
        }
        for i in 0..5 {
            partition_owners.insert(("test-topic".to_string(), 20 + i), 2);
        }

        let partition_loads = HashMap::new();

        let decision = balancer.evaluate(&broker_loads, &partition_owners, &partition_loads);
        assert!(decision.should_rebalance);
        assert!(!decision.moves.is_empty());
        assert!(decision.current_deviation > 0.3);
    }

    #[test]
    fn test_cooldown_prevents_move() {
        let mut balancer = AutoBalancer::with_defaults();

        // Record a move
        balancer.record_move("test-topic", 0);

        // Check cooldown
        assert!(balancer.is_in_cooldown(&("test-topic".to_string(), 0)));
        assert!(!balancer.is_in_cooldown(&("test-topic".to_string(), 1)));
    }

    #[test]
    fn test_max_partitions_per_cycle() {
        let mut balancer = AutoBalancer::new(AutoBalancerConfig {
            max_partitions_per_cycle: 2,
            min_load_diff_threshold: 0.0,
            ..Default::default()
        });

        let mut broker_loads = HashMap::new();
        broker_loads.insert(1, make_broker_load(1, 10, 10_000_000.0));
        broker_loads.insert(2, make_broker_load(2, 2, 500_000.0));

        let mut partition_owners = HashMap::new();
        for i in 0..10 {
            partition_owners.insert(("test-topic".to_string(), i), 1);
        }

        let partition_loads = HashMap::new();

        let decision = balancer.evaluate(&broker_loads, &partition_owners, &partition_loads);
        assert!(decision.moves.len() <= 2);
    }

    #[test]
    fn test_broker_score_calculation() {
        let balancer = AutoBalancer::new(AutoBalancerConfig {
            throughput_weight: 0.7,
            ..Default::default()
        });

        let mut broker_loads = HashMap::new();
        broker_loads.insert(1, make_broker_load(1, 10, 1_000_000.0));
        broker_loads.insert(2, make_broker_load(2, 5, 2_000_000.0));

        let scores = balancer.calculate_broker_scores(&broker_loads);

        // Broker 1: count = 10/10 = 1.0, throughput = 1M/2M = 0.5
        // Score = 1.0 * 0.3 + 0.5 * 0.7 = 0.3 + 0.35 = 0.65
        // Broker 2: count = 5/10 = 0.5, throughput = 2M/2M = 1.0
        // Score = 0.5 * 0.3 + 1.0 * 0.7 = 0.15 + 0.7 = 0.85

        assert!(scores.contains_key(&1));
        assert!(scores.contains_key(&2));
        assert!(scores[&2] > scores[&1]); // Higher throughput = higher score
    }

    #[test]
    fn test_cleanup_cooldowns() {
        let balancer = AutoBalancer::new(AutoBalancerConfig {
            cooldown_per_partition_secs: 0, // Instant expiry for test
            ..Default::default()
        });

        balancer
            .cooldowns
            .insert(("topic".to_string(), 0), Instant::now());
        balancer
            .cooldowns
            .insert(("topic".to_string(), 1), Instant::now());

        assert_eq!(balancer.active_cooldown_count(), 2);

        // Sleep briefly to ensure cooldown expires
        std::thread::sleep(Duration::from_millis(10));

        balancer.cleanup_cooldowns();
        assert_eq!(balancer.active_cooldown_count(), 0);
    }

    #[test]
    fn test_deviation_calculation() {
        let balancer = AutoBalancer::with_defaults();

        let mut scores = HashMap::new();
        scores.insert(1, 0.5);
        scores.insert(2, 0.5);
        scores.insert(3, 0.5);

        let deviation = balancer.calculate_deviation(&scores);
        assert_eq!(deviation, 0.0); // All equal = no deviation

        scores.insert(1, 0.2);
        scores.insert(2, 0.5);
        scores.insert(3, 0.8);

        let deviation = balancer.calculate_deviation(&scores);
        // (0.8 - 0.2) / 0.5 = 1.2
        assert!((deviation - 1.2).abs() < 0.01);
    }

    #[test]
    fn test_config_builders() {
        // Test with_throughput_weight
        let config = AutoBalancerConfig::default().with_throughput_weight(0.5);
        assert_eq!(config.throughput_weight, 0.5);

        // Test clamping
        let config = AutoBalancerConfig::default().with_throughput_weight(1.5);
        assert_eq!(config.throughput_weight, 1.0);

        let config = AutoBalancerConfig::default().with_throughput_weight(-0.5);
        assert_eq!(config.throughput_weight, 0.0);

        // Test with_deviation_threshold
        let config = AutoBalancerConfig::default().with_deviation_threshold(0.5);
        assert_eq!(config.deviation_threshold, 0.5);

        // Test clamping
        let config = AutoBalancerConfig::default().with_deviation_threshold(1.5);
        assert_eq!(config.deviation_threshold, 1.0);
    }

    #[test]
    fn test_config_duration_methods() {
        let config = AutoBalancerConfig {
            evaluation_interval_secs: 120,
            cooldown_per_partition_secs: 600,
            ..Default::default()
        };

        assert_eq!(config.evaluation_interval(), Duration::from_secs(120));
        assert_eq!(config.cooldown_duration(), Duration::from_secs(600));
    }

    #[test]
    fn test_should_evaluate_disabled() {
        let balancer = AutoBalancer::new(AutoBalancerConfig {
            enabled: false,
            ..Default::default()
        });

        assert!(!balancer.should_evaluate());
    }

    #[test]
    fn test_should_evaluate_first_time() {
        let balancer = AutoBalancer::with_defaults();
        assert!(balancer.should_evaluate()); // First time should return true
    }

    #[test]
    fn test_should_evaluate_timing() {
        let config = AutoBalancerConfig {
            evaluation_interval_secs: 0, // Very short for testing
            ..Default::default()
        };
        let mut balancer = AutoBalancer::new(config);

        // Trigger evaluation to set last_evaluation
        let broker_loads = HashMap::new();
        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();
        balancer.evaluate(&broker_loads, &partition_owners, &partition_loads);

        // Should immediately be ready for next evaluation with 0 interval
        assert!(balancer.should_evaluate());
    }

    #[test]
    fn test_rebalance_reason_display() {
        assert_eq!(
            format!("{}", RebalanceReason::HighDeviation),
            "high_deviation"
        );
        assert_eq!(
            format!("{}", RebalanceReason::BrokerOverload),
            "broker_overload"
        );
        assert_eq!(
            format!("{}", RebalanceReason::ManualTrigger),
            "manual_trigger"
        );
    }

    #[test]
    fn test_rebalance_decision_no_rebalance() {
        let scores: HashMap<i32, f64> = [(1, 0.5), (2, 0.5)].into_iter().collect();
        let decision = RebalanceDecision::no_rebalance(0.1, scores.clone());

        assert!(!decision.should_rebalance);
        assert_eq!(decision.current_deviation, 0.1);
        assert!(decision.moves.is_empty());
        assert!(decision.reason.is_none());
        assert_eq!(decision.broker_scores.len(), 2);
    }

    #[test]
    fn test_force_evaluation() {
        let mut balancer = AutoBalancer::with_defaults();

        let mut broker_loads = HashMap::new();
        broker_loads.insert(1, make_broker_load(1, 10, 1_000_000.0));
        broker_loads.insert(2, make_broker_load(2, 10, 1_000_000.0));

        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();

        let decision =
            balancer.force_evaluation(&broker_loads, &partition_owners, &partition_loads);
        // Balanced cluster - no rebalance needed even with force
        assert!(!decision.should_rebalance);
    }

    #[test]
    fn test_metrics_tracking() {
        let mut balancer = AutoBalancer::with_defaults();

        assert_eq!(balancer.total_evaluations(), 0);
        assert_eq!(balancer.total_moves(), 0);

        let broker_loads = HashMap::new();
        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();
        balancer.evaluate(&broker_loads, &partition_owners, &partition_loads);

        assert_eq!(balancer.total_evaluations(), 1);
        assert_eq!(balancer.total_moves(), 0);

        balancer.record_move("topic", 0);
        assert_eq!(balancer.total_moves(), 1);
    }

    #[test]
    fn test_is_enabled() {
        let enabled = AutoBalancer::new(AutoBalancerConfig {
            enabled: true,
            ..Default::default()
        });
        assert!(enabled.is_enabled());

        let disabled = AutoBalancer::new(AutoBalancerConfig {
            enabled: false,
            ..Default::default()
        });
        assert!(!disabled.is_enabled());
    }

    #[test]
    fn test_config_accessor() {
        let config = AutoBalancerConfig {
            deviation_threshold: 0.5,
            ..Default::default()
        };
        let balancer = AutoBalancer::new(config);
        assert_eq!(balancer.config().deviation_threshold, 0.5);
    }

    #[test]
    fn test_default_impl() {
        let balancer = AutoBalancer::default();
        assert!(balancer.is_enabled());
    }

    #[test]
    fn test_empty_broker_scores() {
        let balancer = AutoBalancer::with_defaults();
        let broker_loads: HashMap<i32, BrokerLoadSummary> = HashMap::new();
        let scores = balancer.calculate_broker_scores(&broker_loads);
        assert!(scores.is_empty());
    }

    #[test]
    fn test_deviation_single_broker() {
        let balancer = AutoBalancer::with_defaults();
        let mut scores = HashMap::new();
        scores.insert(1, 0.5);

        let deviation = balancer.calculate_deviation(&scores);
        assert_eq!(deviation, 0.0); // Single broker = no deviation
    }

    #[test]
    fn test_deviation_zero_average() {
        let balancer = AutoBalancer::with_defaults();
        let mut scores = HashMap::new();
        scores.insert(1, 0.0);
        scores.insert(2, 0.0);

        let deviation = balancer.calculate_deviation(&scores);
        assert_eq!(deviation, 0.0); // Zero average handled gracefully
    }

    #[test]
    fn test_generate_moves_no_imbalance() {
        let balancer = AutoBalancer::with_defaults();

        let mut broker_scores = HashMap::new();
        broker_scores.insert(1, 0.5);
        broker_scores.insert(2, 0.5);

        let mut broker_loads = HashMap::new();
        broker_loads.insert(1, make_broker_load(1, 5, 500_000.0));
        broker_loads.insert(2, make_broker_load(2, 5, 500_000.0));

        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();

        let moves = balancer.generate_moves(
            &broker_scores,
            &broker_loads,
            &partition_owners,
            &partition_loads,
        );
        assert!(moves.is_empty()); // No imbalance = no moves
    }

    // ==========================================================================
    // LoadSnapshot Tests
    // ==========================================================================

    #[test]
    fn test_load_snapshot_new() {
        let broker_loads = HashMap::new();
        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();

        let snapshot = LoadSnapshot::new(broker_loads, partition_owners, partition_loads);

        assert!(snapshot.timestamp_ms > 0);
        assert!(snapshot.raft_index.is_none());
        assert!(snapshot.broker_loads.is_empty());
    }

    #[test]
    fn test_load_snapshot_with_timestamp() {
        let broker_loads = HashMap::new();
        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();
        let timestamp = 1234567890u64;

        let snapshot = LoadSnapshot::with_timestamp(
            broker_loads,
            partition_owners,
            partition_loads,
            timestamp,
        );

        assert_eq!(snapshot.timestamp_ms, timestamp);
        assert!(snapshot.raft_index.is_none());
    }

    #[test]
    fn test_load_snapshot_with_raft_index() {
        let broker_loads = HashMap::new();
        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();
        let raft_index = 100u64;

        let snapshot = LoadSnapshot::with_raft_index(
            broker_loads,
            partition_owners,
            partition_loads,
            raft_index,
        );

        assert!(snapshot.timestamp_ms > 0);
        assert_eq!(snapshot.raft_index, Some(raft_index));
    }

    #[test]
    fn test_load_snapshot_is_stale() {
        let broker_loads = HashMap::new();
        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();

        // Fresh snapshot should not be stale
        let fresh_snapshot = LoadSnapshot::new(
            broker_loads.clone(),
            partition_owners.clone(),
            partition_loads.clone(),
        );
        assert!(!fresh_snapshot.is_stale(5000)); // 5 second threshold

        // Old snapshot should be stale
        let old_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
            .saturating_sub(10000); // 10 seconds ago

        let old_snapshot = LoadSnapshot::with_timestamp(
            broker_loads,
            partition_owners,
            partition_loads,
            old_timestamp,
        );
        assert!(old_snapshot.is_stale(5000)); // 5 second threshold
    }

    #[test]
    fn test_load_snapshot_age_ms() {
        let broker_loads = HashMap::new();
        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();

        let snapshot = LoadSnapshot::new(broker_loads, partition_owners, partition_loads);

        // Fresh snapshot should have very low age
        let age = snapshot.age_ms();
        assert!(age < 1000); // Less than 1 second
    }

    #[test]
    fn test_evaluate_snapshot_stale_data() {
        let mut balancer = AutoBalancer::with_defaults();

        let broker_loads = HashMap::new();
        let partition_owners = HashMap::new();
        let partition_loads = HashMap::new();

        // Create a very old snapshot
        let old_timestamp = 0u64; // Very old
        let snapshot = LoadSnapshot::with_timestamp(
            broker_loads,
            partition_owners,
            partition_loads,
            old_timestamp,
        );

        let decision = balancer.evaluate_snapshot(&snapshot);
        assert!(!decision.should_rebalance);
        assert!(decision.broker_scores.is_empty());
    }

    // ==========================================================================
    // PartitionMove Tests
    // ==========================================================================

    #[test]
    fn test_partition_move_clone() {
        let partition_move = PartitionMove {
            topic: "test-topic".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            estimated_load: 1_000_000.0,
            priority: 0.5,
        };

        let cloned = partition_move.clone();
        assert_eq!(cloned.topic, "test-topic");
        assert_eq!(cloned.partition, 0);
        assert_eq!(cloned.from_broker_id, 1);
        assert_eq!(cloned.to_broker_id, 2);
    }

    #[test]
    fn test_partition_move_debug() {
        let partition_move = PartitionMove {
            topic: "test-topic".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            estimated_load: 1_000_000.0,
            priority: 0.5,
        };

        let debug = format!("{:?}", partition_move);
        assert!(debug.contains("test-topic"));
        assert!(debug.contains("from_broker_id"));
    }

    // ==========================================================================
    // RebalanceReason Tests
    // ==========================================================================

    #[test]
    fn test_rebalance_reason_eq() {
        assert_eq!(
            RebalanceReason::HighDeviation,
            RebalanceReason::HighDeviation
        );
        assert_eq!(
            RebalanceReason::BrokerOverload,
            RebalanceReason::BrokerOverload
        );
        assert_eq!(
            RebalanceReason::ManualTrigger,
            RebalanceReason::ManualTrigger
        );
        assert_ne!(
            RebalanceReason::HighDeviation,
            RebalanceReason::BrokerOverload
        );
    }

    #[test]
    fn test_rebalance_reason_clone() {
        let reason = RebalanceReason::HighDeviation;
        let cloned = reason.clone();
        assert_eq!(reason, cloned);
    }

    #[test]
    fn test_rebalance_reason_debug() {
        assert_eq!(
            format!("{:?}", RebalanceReason::HighDeviation),
            "HighDeviation"
        );
        assert_eq!(
            format!("{:?}", RebalanceReason::BrokerOverload),
            "BrokerOverload"
        );
        assert_eq!(
            format!("{:?}", RebalanceReason::ManualTrigger),
            "ManualTrigger"
        );
    }

    // ==========================================================================
    // RebalanceDecision Tests
    // ==========================================================================

    #[test]
    fn test_rebalance_decision_clone() {
        let scores: HashMap<i32, f64> = [(1, 0.5), (2, 0.6)].into_iter().collect();
        let decision = RebalanceDecision::no_rebalance(0.1, scores);

        let cloned = decision.clone();
        assert_eq!(cloned.current_deviation, 0.1);
        assert!(!cloned.should_rebalance);
    }

    #[test]
    fn test_rebalance_decision_debug() {
        let scores: HashMap<i32, f64> = [(1, 0.5)].into_iter().collect();
        let decision = RebalanceDecision::no_rebalance(0.1, scores);

        let debug = format!("{:?}", decision);
        assert!(debug.contains("should_rebalance"));
        assert!(debug.contains("current_deviation"));
    }

    // ==========================================================================
    // AutoBalancerConfig Tests
    // ==========================================================================

    #[test]
    fn test_config_debug() {
        let config = AutoBalancerConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("enabled"));
        assert!(debug.contains("deviation_threshold"));
    }

    #[test]
    fn test_config_clone() {
        let config = AutoBalancerConfig {
            enabled: false,
            deviation_threshold: 0.5,
            ..Default::default()
        };

        let cloned = config.clone();
        assert!(!cloned.enabled);
        assert_eq!(cloned.deviation_threshold, 0.5);
    }

    #[test]
    fn test_config_max_staleness() {
        let config = AutoBalancerConfig::default();
        assert_eq!(config.max_data_staleness_ms, 5000);
    }

    // ==========================================================================
    // LoadSnapshot Debug and Clone Tests
    // ==========================================================================

    #[test]
    fn test_load_snapshot_debug() {
        let snapshot = LoadSnapshot::new(HashMap::new(), HashMap::new(), HashMap::new());
        let debug = format!("{:?}", snapshot);
        assert!(debug.contains("broker_loads"));
        assert!(debug.contains("timestamp_ms"));
    }

    #[test]
    fn test_load_snapshot_clone() {
        let mut broker_loads = HashMap::new();
        broker_loads.insert(1, make_broker_load(1, 5, 1_000_000.0));

        let snapshot = LoadSnapshot::new(broker_loads, HashMap::new(), HashMap::new());
        let cloned = snapshot.clone();

        assert_eq!(cloned.broker_loads.len(), 1);
        assert_eq!(cloned.timestamp_ms, snapshot.timestamp_ms);
    }
}
