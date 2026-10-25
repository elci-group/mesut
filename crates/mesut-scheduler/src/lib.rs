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
    /// Historical mean execution (service) time for this task's label, if any
    /// has been observed. `None` for unlabeled work or a label seen for the
    /// first time. Never includes queue or dispatch wait.
    pub expected_service_time: Option<Duration>,
    /// How long the task has been waiting since admission. Adaptive policies
    /// use this to age starved candidates past the configured threshold.
    pub waited: Duration,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum SchedulingMode {
    Fifo,
    #[default]
    Priority,
    LoadAware,
    /// Like `Priority`, but breaks ties within a priority tier in favor of the
    /// candidate with the shorter historical *execution* time for its label,
    /// and promotes candidates whose queue wait exceeds the configured aging
    /// threshold back to FIFO order so long work cannot starve.
    Adaptive,
}

/// Lifecycle of runtime shutdown.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ShutdownState {
    /// Accepting work normally.
    #[default]
    Running,
    /// Admission is closed; cooperative work is being drained within the
    /// configured grace period.
    Draining,
    /// All work drained and executors are quiescent; the runtime closed
    /// cleanly.
    Terminated,
    /// The grace period expired with work still active: queued work was
    /// cancelled, dispatchable handles were cancelled, and runtime
    /// bookkeeping was cleared. Uncooperative closures may still be running
    /// on executor threads; the runtime no longer tracks them.
    ForceTerminated,
}

/// Aggregate runtime counters and per-executor statistics.
///
/// `active_tasks`/`queue_depth` in `executor_stats` are point-in-time and
/// only meaningful in a freshly built snapshot (see `MesuT::runtime_state`);
/// `queued_tasks`/`in_flight_tasks` are the same kind of point-in-time
/// runtime-level reading. The cumulative counters (`completed_tasks`,
/// `failed_tasks`, `total_wall_ms`, `busy_ms`) accumulate for the life of the
/// runtime.
#[derive(Clone, Debug, Default)]
pub struct RuntimeState {
    pub total_submitted: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_cancelled: u64,
    pub shutdown: ShutdownState,
    /// Admitted but not yet dispatched, at snapshot time.
    pub queued_tasks: usize,
    /// Dispatched and not yet finished, at snapshot time.
    pub in_flight_tasks: usize,
    pub executor_stats: std::collections::HashMap<String, ExecutorStats>,
}

/// Statistics for an executor. CPU-time instrumentation is per-task only
/// (see `mesut_core::TaskTiming`); executor-level `busy_ms` is the sum of
/// measured execution time of its finished tasks.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutorStats {
    pub active_tasks: usize,
    pub queue_depth: usize,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub total_wall_ms: u64,
    /// Sum of measured execution time (job run only, no queue wait) across
    /// finished tasks. Against a known worker count this yields utilisation:
    /// `busy_ms / (elapsed_ms * workers)`.
    pub busy_ms: u64,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_executor_stats(&mut self, executor_id: String, stats: ExecutorStats) {
        self.executor_stats.insert(executor_id, stats);
    }

    /// Fold one finished task's wall-clock duration, measured execution time,
    /// and outcome into `executor_id`'s cumulative statistics.
    pub fn record_executor_completion(
        &mut self,
        executor_id: String,
        wall_ms: u64,
        execution_ms: u64,
        failed: bool,
    ) {
        let stats = self.executor_stats.entry(executor_id).or_default();
        stats.total_wall_ms += wall_ms;
        stats.busy_ms += execution_ms;
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
        assert_eq!(state.shutdown, ShutdownState::Running);
    }

    #[test]
    fn executor_stats_separate_wall_from_busy_time() {
        let mut state = RuntimeState::new();
        state.record_executor_completion("compute".into(), 100, 40, false);
        state.record_executor_completion("compute".into(), 50, 50, true);
        let stats = &state.executor_stats["compute"];
        assert_eq!(stats.total_wall_ms, 150);
        assert_eq!(stats.busy_ms, 90);
        assert_eq!(stats.completed_tasks, 1);
        assert_eq!(stats.failed_tasks, 1);
    }
}
