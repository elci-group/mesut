use async_trait::async_trait;
use mesut_core::{Route, TaskResult, Work};
use mesut_executor::ArcExecutor;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub use policy::*;

mod policy;

/// Scheduling policy trait.
#[async_trait]
pub trait SchedulingPolicy: Send + Sync {
    /// Select executor for the given work based on current runtime state.
    async fn select_executor(&self, work: &Work, state: &RuntimeState) -> TaskResult<Route>;
}

/// Current runtime state for scheduling decisions.
pub struct RuntimeState {
    pub total_submitted: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_cancelled: u64,
    pub executor_stats: HashMap<String, ExecutorStats>,
}

/// Statistics for an executor.
#[derive(Clone, Debug)]
pub struct ExecutorStats {
    pub active_tasks: usize,
    pub queue_depth: usize,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub total_cpu_ms: u64,
    pub total_wall_ms: u64,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self {
            total_submitted: 0,
            total_completed: 0,
            total_failed: 0,
            total_cancelled: 0,
            executor_stats: HashMap::new(),
        }
    }

    pub fn update_executor_stats(&mut self, executor_id: String, stats: ExecutorStats) {
        self.executor_stats.insert(executor_id, stats);
    }

    pub fn add_submission(&mut self) {
        self.total_submitted += 1;
    }

    pub fn add_completion(&mut self) {
        self.total_completed += 1;
    }

    pub fn add_failure(&mut self) {
        self.total_failed += 1;
    }

    pub fn add_cancellation(&mut self) {
        self.total_cancelled += 1;
    }
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_state_creation() {
        let state = RuntimeState::new();
        assert_eq!(state.total_submitted, 0);
        assert_eq!(state.total_completed, 0);
    }

    #[test]
    fn runtime_state_stats_update() {
        let mut state = RuntimeState::new();
        state.add_submission();
        state.add_completion();
        assert_eq!(state.total_submitted, 1);
        assert_eq!(state.total_completed, 1);
    }
}
