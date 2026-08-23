use async_trait::async_trait;
use mesut_core::{ExecutorId, TaskId, TaskResult, Work};
use std::sync::Arc;

pub use submission::*;

mod submission;

/// Trait that all executor implementations must implement.
#[async_trait]
pub trait Executor: Send + Sync {
    /// Get the executor's unique identifier.
    fn id(&self) -> ExecutorId;

    /// Check if this executor is currently available.
    fn is_available(&self) -> bool;

    /// Submit work to this executor.
    async fn submit(&self, work: Work) -> TaskResult<Submission>;

    /// Cancel an in-flight or queued task.
    async fn cancel(&self, task_id: TaskId) -> TaskResult<()>;

    /// Get the current queue depth.
    fn queue_depth(&self) -> usize;

    /// Get the number of currently executing tasks.
    fn active_tasks(&self) -> usize;

    /// Gracefully shutdown this executor.
    async fn shutdown(&self) -> TaskResult<()>;

    /// Check if a task is still tracked by this executor.
    async fn is_task_alive(&self, task_id: TaskId) -> bool;
}

/// Type-erased executor for use in collections.
pub type ArcExecutor = Arc<dyn Executor>;

/// Result of submitting work to an executor.
pub struct Submission {
    pub task_id: TaskId,
    pub executor_id: ExecutorId,
}

impl Submission {
    pub fn new(task_id: TaskId, executor_id: ExecutorId) -> Self {
        Self {
            task_id,
            executor_id,
        }
    }
}

/// Executor pool that manages multiple executors.
pub struct ExecutorPool {
    executors: Vec<ArcExecutor>,
}

impl ExecutorPool {
    pub fn new(executors: Vec<ArcExecutor>) -> Self {
        Self { executors }
    }

    pub fn add(&mut self, executor: ArcExecutor) {
        self.executors.push(executor);
    }

    pub fn get(&self, id: ExecutorId) -> Option<ArcExecutor> {
        self.executors
            .iter()
            .find(|e| e.id() == id)
            .cloned()
    }

    pub fn available(&self) -> Vec<ArcExecutor> {
        self.executors
            .iter()
            .filter(|e| e.is_available())
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.executors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.executors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submission_creation() {
        let task_id = TaskId::new();
        let executor_id = ExecutorId::new();
        let submission = Submission::new(task_id, executor_id);
        assert_eq!(submission.task_id, task_id);
        assert_eq!(submission.executor_id, executor_id);
    }

    #[test]
    fn executor_pool_operations() {
        let pool = ExecutorPool::new(vec![]);
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }
}
