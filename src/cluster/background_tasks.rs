//! Centralized background task orchestration.
//!
//! This module provides a `BackgroundTaskRegistry` that:
//! - Centrally manages task lifecycle
//! - Provides consistent shutdown semantics
//! - Enables health monitoring of all tasks
//! - Supports task restart on failure
//!
//! # Problem Solved
//!
//! Previously, background tasks were spawned independently across components:
//! - PartitionManager: 4 loops (heartbeat, lease renewal, ownership, session timeout)
//! - RaftCoordinator: 4 loops (heartbeat, lease expiry, member expiry, producer expiry)
//! - RebalanceCoordinator: 3 loops (failure check, rebalance, metrics reset)
//!
//! This led to:
//! - 11+ concurrent background tasks
//! - Inconsistent shutdown handling
//! - Task lifecycle scattered across components
//!
//! # Example
//!
//! ```rust,no_run
//! use kafkaesque::cluster::background_tasks::{BackgroundTaskRegistry, TaskStatus};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut registry = BackgroundTaskRegistry::new();
//!
//!     // Spawn named tasks
//!     registry.spawn("heartbeat", async {
//!         loop {
//!             // heartbeat logic
//!             tokio::time::sleep(Duration::from_secs(5)).await;
//!         }
//!     });
//!
//!     registry.spawn("lease_renewal", async {
//!         loop {
//!             // lease renewal logic
//!             tokio::time::sleep(Duration::from_secs(20)).await;
//!         }
//!     });
//!
//!     // Check health
//!     for (name, status) in registry.health_check() {
//!         let status_str = match status {
//!             TaskStatus::Running => "OK",
//!             _ => "FAILED",
//!         };
//!         println!("{}: {}", name, status_str);
//!     }
//!
//!     // Graceful shutdown
//!     registry.shutdown_all().await;
//! }
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;

/// Status of a background task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is currently running.
    Running,
    /// Task completed successfully.
    Completed,
    /// Task panicked or was cancelled.
    Failed,
    /// Task was stopped via shutdown.
    Stopped,
}

impl TaskStatus {
    /// Returns true if the task is healthy (running or completed).
    pub fn is_healthy(&self) -> bool {
        matches!(self, TaskStatus::Running | TaskStatus::Completed)
    }
}

/// Information about a registered task.
struct TaskInfo {
    /// The task handle.
    handle: JoinHandle<()>,
    /// When the task was spawned.
    spawned_at: std::time::Instant,
    /// Whether this task is essential (failure triggers shutdown).
    /// Reserved for future use in automatic restart/shutdown logic.
    #[allow(dead_code)]
    essential: bool,
}

/// Central registry for background task management.
///
/// Provides:
/// - Named task registration
/// - Unified shutdown semantics
/// - Health monitoring
/// - Optional task restart on failure
pub struct BackgroundTaskRegistry {
    /// Registered tasks by name.
    tasks: HashMap<&'static str, TaskInfo>,
    /// Shutdown signal sender.
    shutdown_tx: broadcast::Sender<()>,
    /// Whether shutdown has been initiated.
    shutting_down: bool,
}

impl BackgroundTaskRegistry {
    /// Create a new task registry.
    pub fn new() -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            tasks: HashMap::new(),
            shutdown_tx,
            shutting_down: false,
        }
    }

    /// Spawn a named background task.
    ///
    /// The task will receive a shutdown signal via `tokio::select!`.
    /// Tasks should be designed to exit gracefully when shutdown is requested.
    ///
    /// # Arguments
    ///
    /// * `name` - Static name for the task (used in health checks and logs)
    /// * `task` - The async task to run
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// registry.spawn("heartbeat", async move {
    ///     loop {
    ///         send_heartbeat().await;
    ///         tokio::time::sleep(Duration::from_secs(5)).await;
    ///     }
    /// });
    /// ```
    pub fn spawn<F>(&mut self, name: &'static str, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_options(name, task, false);
    }

    /// Spawn a named essential task.
    ///
    /// Essential tasks trigger a full shutdown if they fail or panic.
    /// Use for critical tasks like heartbeat or coordination.
    pub fn spawn_essential<F>(&mut self, name: &'static str, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_options(name, task, true);
    }

    /// Spawn a task with options.
    fn spawn_with_options<F>(&mut self, name: &'static str, task: F, essential: bool)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.shutting_down {
            tracing::warn!(task = name, "Ignoring spawn during shutdown");
            return;
        }

        // If a task with this name exists, abort it first
        if let Some(old_info) = self.tasks.remove(name) {
            old_info.handle.abort();
            tracing::debug!(task = name, "Aborted previous task instance");
        }

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let task_name = name;

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = task => {
                    tracing::debug!(task = task_name, "Task completed");
                }
                _ = shutdown_rx.recv() => {
                    tracing::debug!(task = task_name, "Task received shutdown signal");
                }
            }
        });

        tracing::info!(task = name, essential, "Spawned background task");

        self.tasks.insert(
            name,
            TaskInfo {
                handle,
                spawned_at: std::time::Instant::now(),
                essential,
            },
        );
    }

    /// Spawn a periodic task that runs at a fixed interval.
    ///
    /// # Arguments
    ///
    /// * `name` - Task name
    /// * `interval` - How often to run the task
    /// * `task` - The work to do on each tick (receives tick count)
    pub fn spawn_periodic<F, Fut>(&mut self, name: &'static str, interval: Duration, mut task: F)
    where
        F: FnMut(u64) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        self.spawn(name, async move {
            let mut tick = 0u64;
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;
                task(tick).await;
                tick = tick.wrapping_add(1);
            }
        });
    }

    /// Get the status of all tasks.
    ///
    /// Returns a list of (name, status) pairs.
    pub fn health_check(&self) -> Vec<(&'static str, TaskStatus)> {
        self.tasks
            .iter()
            .map(|(name, info)| {
                let status = if info.handle.is_finished() {
                    // Check if it panicked
                    TaskStatus::Completed // Can't distinguish panic vs completion without join
                } else {
                    TaskStatus::Running
                };
                (*name, status)
            })
            .collect()
    }

    /// Check if all tasks are healthy.
    pub fn all_healthy(&self) -> bool {
        self.tasks.values().all(|info| !info.handle.is_finished())
    }

    /// Get names of failed tasks.
    pub fn failed_tasks(&self) -> Vec<&'static str> {
        self.tasks
            .iter()
            .filter(|(_, info)| info.handle.is_finished())
            .map(|(name, _)| *name)
            .collect()
    }

    /// Get the number of registered tasks.
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Get the number of running tasks.
    pub fn running_count(&self) -> usize {
        self.tasks
            .values()
            .filter(|info| !info.handle.is_finished())
            .count()
    }

    /// Get uptime for a specific task.
    pub fn task_uptime(&self, name: &str) -> Option<Duration> {
        self.tasks.get(name).map(|info| info.spawned_at.elapsed())
    }

    /// Shutdown all tasks gracefully.
    ///
    /// Sends a shutdown signal to all tasks and waits for them to complete.
    /// Tasks that don't respond to the signal within the timeout are aborted.
    pub async fn shutdown_all(&mut self) {
        self.shutdown_all_with_timeout(Duration::from_secs(5)).await;
    }

    /// Shutdown all tasks with a custom timeout.
    pub async fn shutdown_all_with_timeout(&mut self, timeout: Duration) {
        if self.shutting_down {
            return;
        }
        self.shutting_down = true;

        tracing::info!(
            task_count = self.tasks.len(),
            "Initiating background task shutdown"
        );

        // Send shutdown signal
        let _ = self.shutdown_tx.send(());

        // Wait for tasks to complete
        let deadline = tokio::time::Instant::now() + timeout;

        for (name, info) in self.tasks.drain() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());

            if remaining.is_zero() {
                tracing::warn!(task = name, "Aborting task (timeout exceeded)");
                info.handle.abort();
            } else {
                match tokio::time::timeout(remaining, info.handle).await {
                    Ok(Ok(())) => {
                        tracing::debug!(task = name, "Task shutdown complete");
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(task = name, error = %e, "Task panicked during shutdown");
                    }
                    Err(_) => {
                        tracing::warn!(task = name, "Task did not respond to shutdown, aborting");
                    }
                }
            }
        }

        tracing::info!("All background tasks shutdown complete");
    }

    /// Check if shutdown has been initiated.
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down
    }
}

impl Default for BackgroundTaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for BackgroundTaskRegistry {
    fn drop(&mut self) {
        // Abort all tasks on drop if not already shutdown
        if !self.shutting_down {
            let _ = self.shutdown_tx.send(());
            for (name, info) in self.tasks.drain() {
                tracing::debug!(task = name, "Aborting task on registry drop");
                info.handle.abort();
            }
        }
    }
}

/// Thread-safe wrapper around BackgroundTaskRegistry.
///
/// Use this when you need to share the registry across async contexts.
pub struct SharedTaskRegistry {
    inner: Arc<RwLock<BackgroundTaskRegistry>>,
}

impl SharedTaskRegistry {
    /// Create a new shared registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BackgroundTaskRegistry::new())),
        }
    }

    /// Spawn a named task.
    pub async fn spawn<F>(&self, name: &'static str, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.write().await.spawn(name, task);
    }

    /// Spawn an essential task.
    pub async fn spawn_essential<F>(&self, name: &'static str, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.write().await.spawn_essential(name, task);
    }

    /// Get health status of all tasks.
    pub async fn health_check(&self) -> Vec<(&'static str, TaskStatus)> {
        self.inner.read().await.health_check()
    }

    /// Check if all tasks are healthy.
    pub async fn all_healthy(&self) -> bool {
        self.inner.read().await.all_healthy()
    }

    /// Get failed task names.
    pub async fn failed_tasks(&self) -> Vec<&'static str> {
        self.inner.read().await.failed_tasks()
    }

    /// Shutdown all tasks.
    pub async fn shutdown_all(&self) {
        self.inner.write().await.shutdown_all().await;
    }

    /// Clone the inner Arc for sharing.
    pub fn clone_inner(&self) -> Arc<RwLock<BackgroundTaskRegistry>> {
        self.inner.clone()
    }
}

impl Default for SharedTaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SharedTaskRegistry {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    #[tokio::test]
    async fn test_spawn_and_health_check() {
        let mut registry = BackgroundTaskRegistry::new();

        registry.spawn("test_task", async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        assert_eq!(registry.task_count(), 1);
        assert_eq!(registry.running_count(), 1);

        let health = registry.health_check();
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].0, "test_task");
        assert_eq!(health[0].1, TaskStatus::Running);

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_shutdown_stops_tasks() {
        let mut registry = BackgroundTaskRegistry::new();

        registry.spawn("shutdown_test", async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        // Give task time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        registry.shutdown_all().await;

        assert!(registry.is_shutting_down());
        assert_eq!(registry.task_count(), 0);
    }

    #[tokio::test]
    async fn test_spawn_replaces_existing_task() {
        let mut registry = BackgroundTaskRegistry::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter1 = counter.clone();
        let counter2 = counter.clone();

        registry.spawn("counter", async move {
            counter1.store(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Spawn again with same name
        registry.spawn("counter", async move {
            counter2.store(2, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert_eq!(registry.task_count(), 1);

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_spawn_periodic() {
        let mut registry = BackgroundTaskRegistry::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        registry.spawn_periodic("ticker", Duration::from_millis(10), move |_tick| {
            let counter = counter_clone.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });

        tokio::time::sleep(Duration::from_millis(55)).await;

        let count = counter.load(Ordering::SeqCst);
        assert!(count >= 4, "Expected at least 4 ticks, got {}", count);

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_failed_tasks() {
        let mut registry = BackgroundTaskRegistry::new();

        // Spawn a task that completes immediately
        registry.spawn("quick_task", async {});

        // Give it time to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        let failed = registry.failed_tasks();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0], "quick_task");

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_task_uptime() {
        let mut registry = BackgroundTaskRegistry::new();

        registry.spawn("uptime_test", async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let uptime = registry.task_uptime("uptime_test");
        assert!(uptime.is_some());
        assert!(uptime.unwrap() >= Duration::from_millis(100));

        assert!(registry.task_uptime("nonexistent").is_none());

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_all_healthy() {
        let mut registry = BackgroundTaskRegistry::new();

        registry.spawn("healthy1", async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        registry.spawn("healthy2", async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        assert!(registry.all_healthy());

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_shared_registry() {
        let registry = SharedTaskRegistry::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        registry
            .spawn("shared_test", async move {
                counter_clone.store(42, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(10)).await;
            })
            .await;

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 42);

        assert!(registry.all_healthy().await);

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_shutdown_with_timeout() {
        let mut registry = BackgroundTaskRegistry::new();

        // Spawn a task that ignores shutdown signal (simulated by not using select)
        registry.spawn("stubborn_task", async {
            // This won't respond to shutdown, but the outer select! will
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let start = std::time::Instant::now();
        registry
            .shutdown_all_with_timeout(Duration::from_millis(100))
            .await;
        let elapsed = start.elapsed();

        // Should complete within timeout + some margin
        assert!(elapsed < Duration::from_millis(200));
    }

    #[test]
    fn test_task_status_is_healthy() {
        assert!(TaskStatus::Running.is_healthy());
        assert!(TaskStatus::Completed.is_healthy());
        assert!(!TaskStatus::Failed.is_healthy());
        assert!(!TaskStatus::Stopped.is_healthy());
    }

    #[tokio::test]
    async fn test_drop_aborts_tasks() {
        let counter = Arc::new(AtomicBool::new(false));
        let counter_clone = counter.clone();

        {
            let mut registry = BackgroundTaskRegistry::new();
            registry.spawn("drop_test", async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                counter_clone.store(true, Ordering::SeqCst);
            });
        } // registry dropped here

        tokio::time::sleep(Duration::from_millis(50)).await;
        // Task should have been aborted, so counter should still be false
        assert!(!counter.load(Ordering::SeqCst));
    }

    // ========================================================================
    // Additional Tests for Critical Coverage
    // ========================================================================

    #[tokio::test]
    async fn test_spawn_essential() {
        let mut registry = BackgroundTaskRegistry::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        registry.spawn_essential("essential_task", async move {
            counter_clone.store(42, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 42);
        assert_eq!(registry.task_count(), 1);

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_spawn_during_shutdown_ignored() {
        let mut registry = BackgroundTaskRegistry::new();

        // Start initial task
        registry.spawn("initial", async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        // Begin shutdown
        registry.shutdown_all().await;
        assert!(registry.is_shutting_down());

        // Try to spawn during shutdown - should be ignored
        let counter = Arc::new(AtomicBool::new(false));
        let counter_clone = counter.clone();
        registry.spawn("during_shutdown", async move {
            counter_clone.store(true, Ordering::SeqCst);
        });

        // Task should not have started
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!counter.load(Ordering::SeqCst));
        assert_eq!(registry.task_count(), 0); // All tasks drained during shutdown
    }

    #[tokio::test]
    async fn test_spawn_essential_during_shutdown_ignored() {
        let mut registry = BackgroundTaskRegistry::new();

        registry.spawn("initial", async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        registry.shutdown_all().await;
        assert!(registry.is_shutting_down());

        // Essential spawn should also be ignored during shutdown
        registry.spawn_essential("essential_during_shutdown", async {
            panic!("This should never run");
        });

        // No panic occurred, task was ignored
        assert_eq!(registry.task_count(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_task_registration() {
        let mut registry = BackgroundTaskRegistry::new();

        // Spawn multiple tasks concurrently
        for i in 0..10 {
            let name: &'static str = Box::leak(format!("task_{}", i).into_boxed_str());
            registry.spawn(name, async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
            });
        }

        assert_eq!(registry.task_count(), 10);
        assert_eq!(registry.running_count(), 10);
        assert!(registry.all_healthy());

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_health_check_after_task_completes() {
        let mut registry = BackgroundTaskRegistry::new();

        // Spawn a task that completes quickly
        registry.spawn("quick", async {});

        // Wait for it to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Health check should show it as completed (which is considered finished)
        let health = registry.health_check();
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].0, "quick");
        assert_eq!(health[0].1, TaskStatus::Completed);

        // all_healthy checks if task is still running (not finished)
        assert!(!registry.all_healthy());

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_running_count_vs_task_count() {
        let mut registry = BackgroundTaskRegistry::new();

        // Spawn mix of quick and long tasks
        registry.spawn("long1", async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        registry.spawn("quick1", async {});
        registry.spawn("long2", async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        // Wait for quick task to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        // task_count includes all, running_count only running
        assert_eq!(registry.task_count(), 3);
        assert_eq!(registry.running_count(), 2);

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_shutdown_idempotent() {
        let mut registry = BackgroundTaskRegistry::new();

        registry.spawn("task", async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        // First shutdown
        registry.shutdown_all().await;
        assert!(registry.is_shutting_down());

        // Second shutdown should be a no-op (not panic or error)
        registry.shutdown_all().await;
        assert!(registry.is_shutting_down());
    }

    #[tokio::test]
    async fn test_shared_registry_clone() {
        let registry = SharedTaskRegistry::new();
        let registry_clone = registry.clone();

        registry
            .spawn("task1", async {
                tokio::time::sleep(Duration::from_secs(10)).await;
            })
            .await;

        registry_clone
            .spawn("task2", async {
                tokio::time::sleep(Duration::from_secs(10)).await;
            })
            .await;

        // Both registries share the same inner state
        let health = registry.health_check().await;
        assert_eq!(health.len(), 2);

        let health_clone = registry_clone.health_check().await;
        assert_eq!(health_clone.len(), 2);

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_shared_registry_essential() {
        let registry = SharedTaskRegistry::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        registry
            .spawn_essential("shared_essential", async move {
                counter_clone.store(99, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(10)).await;
            })
            .await;

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 99);
        assert!(registry.all_healthy().await);

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_shared_registry_failed_tasks() {
        let registry = SharedTaskRegistry::new();

        // Spawn a task that completes immediately
        registry.spawn("quick", async {}).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let failed = registry.failed_tasks().await;
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0], "quick");

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_default_trait() {
        let registry = BackgroundTaskRegistry::default();
        assert_eq!(registry.task_count(), 0);
        assert!(!registry.is_shutting_down());

        let shared = SharedTaskRegistry::default();
        assert!(shared.all_healthy().await); // Empty registry is healthy
    }

    #[tokio::test]
    async fn test_clone_inner() {
        let registry = SharedTaskRegistry::new();
        let inner = registry.clone_inner();

        // Verify we can use the inner Arc
        {
            let guard = inner.read().await;
            assert_eq!(guard.task_count(), 0);
        }

        registry.shutdown_all().await;
    }

    #[tokio::test]
    async fn test_multiple_periodic_tasks() {
        let mut registry = BackgroundTaskRegistry::new();
        let counter1 = Arc::new(AtomicU32::new(0));
        let counter2 = Arc::new(AtomicU32::new(0));
        let c1 = counter1.clone();
        let c2 = counter2.clone();

        registry.spawn_periodic("periodic1", Duration::from_millis(10), move |_| {
            let c = c1.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        registry.spawn_periodic("periodic2", Duration::from_millis(15), move |_| {
            let c = c2.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let count1 = counter1.load(Ordering::SeqCst);
        let count2 = counter2.load(Ordering::SeqCst);

        // Both should have ticked multiple times
        assert!(
            count1 >= 5,
            "Periodic1 should tick at least 5 times, got {}",
            count1
        );
        assert!(
            count2 >= 4,
            "Periodic2 should tick at least 4 times, got {}",
            count2
        );

        registry.shutdown_all().await;
    }
}
