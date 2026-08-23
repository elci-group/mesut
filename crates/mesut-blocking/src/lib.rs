use async_trait::async_trait;
use crossbeam::queue::SegQueue;
use mesut_core::{ExecutorId, TaskError, TaskId, TaskResult, Work};
use mesut_executor::{Executor, Submission};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

/// Blocking executor for CPU-bound blocking operations.
pub struct BlockingExecutor {
    id: ExecutorId,
    thread_pool: Arc<BlockingThreadPool>,
    active_tasks: Arc<RwLock<HashMap<TaskId, bool>>>,
    config: BlockingExecutorConfig,
}

/// Configuration for blocking executor.
#[derive(Clone, Debug)]
pub struct BlockingExecutorConfig {
    pub num_threads: usize,
    pub max_queue_size: Option<usize>,
}

impl BlockingExecutorConfig {
    pub fn new() -> Self {
        Self {
            num_threads: num_cpus::get().max(2),
            max_queue_size: Some(10000),
        }
    }

    pub fn with_num_threads(mut self, num: usize) -> Self {
        self.num_threads = num;
        self
    }

    pub fn with_max_queue_size(mut self, size: usize) -> Self {
        self.max_queue_size = Some(size);
        self
    }
}

impl Default for BlockingExecutorConfig {
    fn default() -> Self {
        Self::new()
    }
}

struct BlockingThreadPool {
    work_queue: Arc<SegQueue<Option<Work>>>,
    num_threads: usize,
}

impl BlockingThreadPool {
    fn new(num_threads: usize) -> Arc<Self> {
        let pool = Arc::new(Self {
            work_queue: Arc::new(SegQueue::new()),
            num_threads,
        });

        for _ in 0..num_threads {
            let work_queue = pool.work_queue.clone();
            thread::spawn(move || loop {
                if let Some(work) = work_queue.pop() {
                    let _work = work;
                    // Simulate blocking work
                    thread::sleep(std::time::Duration::from_millis(10));
                } else {
                    thread::sleep(std::time::Duration::from_millis(1));
                }
            });
        }

        pool
    }

    fn submit(&self, work: Work) -> TaskResult<()> {
        self.work_queue.push(Some(work));
        Ok(())
    }

    fn shutdown(&self) {
        for _ in 0..self.num_threads {
            self.work_queue.push(None);
        }
    }

    fn queue_depth(&self) -> usize {
        // Note: SegQueue doesn't provide a queue depth method
        // This is a limitation we'd address in a production implementation
        0
    }
}

impl BlockingExecutor {
    pub fn new(config: BlockingExecutorConfig) -> Self {
        Self {
            id: ExecutorId::new(),
            thread_pool: BlockingThreadPool::new(config.num_threads),
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub fn with_id(mut self, id: ExecutorId) -> Self {
        self.id = id;
        self
    }
}

#[async_trait]
impl Executor for BlockingExecutor {
    fn id(&self) -> ExecutorId {
        self.id
    }

    fn is_available(&self) -> bool {
        if let Some(max_size) = self.config.max_queue_size {
            self.active_tasks.read().len() < max_size
        } else {
            true
        }
    }

    async fn submit(&self, work: Work) -> TaskResult<Submission> {
        if !self.is_available() {
            return Err(TaskError::QueueFull);
        }

        let task_id = work.id;
        let executor_id = self.id;

        self.active_tasks.write().insert(task_id, true);
        let active_tasks = self.active_tasks.clone();
        let task_id_clone = task_id;

        self.thread_pool.submit(work)?;

        // Simulate work completion
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            active_tasks.write().remove(&task_id_clone);
        });

        Ok(Submission::new(task_id, executor_id))
    }

    async fn cancel(&self, task_id: TaskId) -> TaskResult<()> {
        self.active_tasks
            .write()
            .remove(&task_id)
            .ok_or(TaskError::ExecutionFailed(format!("Task {} not found", task_id)))?;
        Ok(())
    }

    fn queue_depth(&self) -> usize {
        self.active_tasks.read().len()
    }

    fn active_tasks(&self) -> usize {
        self.active_tasks.read().len()
    }

    async fn shutdown(&self) -> TaskResult<()> {
        self.thread_pool.shutdown();
        self.active_tasks.write().clear();
        Ok(())
    }

    async fn is_task_alive(&self, task_id: TaskId) -> bool {
        self.active_tasks.read().contains_key(&task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn blocking_executor_creation() {
        let executor = BlockingExecutor::new(BlockingExecutorConfig::default());
        assert!(executor.is_available());
    }

    #[tokio::test]
    async fn blocking_executor_submit() {
        let executor = Arc::new(BlockingExecutor::new(BlockingExecutorConfig::default()));
        let work = Work::new(mesut_core::WorkKind::Blocking);
        let result = executor.submit(work).await;
        assert!(result.is_ok());
    }
}
