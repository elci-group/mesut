use async_trait::async_trait;
use mesut_core::{Priority, Route, TaskResult, Work};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;

pub use policy::*;
pub use queue::*;

mod policy;
mod queue;

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
    pub executor_stats: std::collections::HashMap<String, ExecutorStats>,
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
            executor_stats: std::collections::HashMap::new(),
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

/// Priority-based work queue.
pub struct PriorityQueue {
    queues: Arc<RwLock<[VecDeque<Work>; 4]>>,
    max_size: Option<usize>,
}

impl PriorityQueue {
    pub fn new(max_size: Option<usize>) -> Self {
        Self {
            queues: Arc::new(RwLock::new([
                VecDeque::new(),
                VecDeque::new(),
                VecDeque::new(),
                VecDeque::new(),
            ])),
            max_size,
        }
    }

    pub fn enqueue(&self, work: Work) -> TaskResult<()> {
        let mut queues = self.queues.write();
        let size: usize = queues.iter().map(|q| q.len()).sum();

        if let Some(max) = self.max_size {
            if size >= max {
                return Err(mesut_core::TaskError::QueueFull);
            }
        }

        let priority_idx = match work.priority {
            Priority::Critical => 3,
            Priority::High => 2,
            Priority::Normal => 1,
            Priority::Low => 0,
        };

        queues[priority_idx].push_back(work);
        Ok(())
    }

    pub fn dequeue(&self) -> Option<Work> {
        let mut queues = self.queues.write();
        for i in (0..4).rev() {
            if let Some(work) = queues[i].pop_front() {
                return Some(work);
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.queues.read().iter().map(|q| q.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
    fn priority_queue_ordering() {
        let queue = PriorityQueue::new(None);
        let work_low = Work::new(mesut_core::WorkKind::Io).with_priority(Priority::Low);
        let work_high = Work::new(mesut_core::WorkKind::Io).with_priority(Priority::High);

        queue.enqueue(work_low).unwrap();
        queue.enqueue(work_high).unwrap();

        let first = queue.dequeue().unwrap();
        assert_eq!(first.priority, Priority::High);
    }
}
