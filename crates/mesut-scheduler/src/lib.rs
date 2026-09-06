use mesut_core::{Priority, TaskId};
use std::time::Duration;

pub use adaptive::*;
pub use policy::*;
pub use queue::*;

mod adaptive;
mod policy;
mod queue;

/// Scheduling policy trait.
pub trait SchedulingPolicy: Send + Sync {
    /// Choose among already-routed tasks whose executors have capacity.
    fn select(&self, candidates: &[SchedulingCandidate]) -> Option<TaskId>;
}

#[derive(Clone, Copy, Debug)]
pub struct SchedulingCandidate {
    pub task_id: TaskId,
    pub priority: Priority,
    pub sequence: u64,
    pub executor_active: usize,
    /// Historical mean duration for this task's label, if any has been
    /// observed. `None` for unlabeled work or a label seen for the first time.
    pub expected_duration: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum SchedulingMode {
    Fifo,
    #[default]
    Priority,
    LoadAware,
    /// Like `Priority`, but breaks ties within a priority tier in favor of
    /// the candidate with the shorter historical duration for its label.
    Adaptive,
}

/// Aggregate runtime counters and per-executor statistics.
///
/// `active_tasks`/`queue_depth` in `executor_stats` are point-in-time and
/// only meaningful in a freshly built snapshot (see `MesuT::runtime_state`);
/// the cumulative counters (`completed_tasks`, `failed_tasks`,
/// `total_wall_ms`) accumulate for the life of the runtime.
#[derive(Clone, Debug, Default)]
pub struct RuntimeState {
    pub total_submitted: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_cancelled: u64,
    pub executor_stats: std::collections::HashMap<String, ExecutorStats>,
}

/// Statistics for an executor. There is no CPU-time instrumentation in the
/// runtime, so only wall-clock and count-based fields are tracked.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutorStats {
    pub active_tasks: usize,
    pub queue_depth: usize,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub total_wall_ms: u64,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_executor_stats(&mut self, executor_id: String, stats: ExecutorStats) {
        self.executor_stats.insert(executor_id, stats);
    }

    /// Fold one finished task's wall-clock duration and outcome into
    /// `executor_id`'s cumulative statistics.
    pub fn record_executor_completion(&mut self, executor_id: String, wall_ms: u64, failed: bool) {
        let stats = self.executor_stats.entry(executor_id).or_default();
        stats.total_wall_ms += wall_ms;
        if failed {
            stats.failed_tasks += 1;
        } else {
            stats.completed_tasks += 1;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_state_creation() {
        let state = RuntimeState::new();
        assert_eq!(state.total_submitted, 0);
        assert_eq!(state.total_completed, 0);
    }
}
