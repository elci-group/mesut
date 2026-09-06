use async_trait::async_trait;
use mesut_core::{CancellationToken, ExecutorId, TaskError, TaskId, TaskResult, Work};
use mesut_executor::{Executor, Submission};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Rayon executor adapter for Mesut.
pub struct RayonExecutor {
    id: ExecutorId,
    thread_pool: rayon::ThreadPool,
    active_tasks: Arc<RwLock<HashMap<TaskId, CancellationToken>>>,
    config: RayonExecutorConfig,
    closed: AtomicBool,
    activity: mesut_executor::Activity,
}

/// Configuration for Rayon executor.
#[derive(Clone, Debug)]
pub struct RayonExecutorConfig {
    pub num_threads: Option<usize>,
    pub max_queue_size: Option<usize>,
}

impl RayonExecutorConfig {
    pub fn new() -> Self {
        Self {
            num_threads: Some(num_cpus::get()),
            max_queue_size: Some(10000),
        }
    }

    pub fn with_num_threads(mut self, num: usize) -> Self {
        self.num_threads = Some(num);
        self
    }

    pub fn with_max_queue_size(mut self, size: usize) -> Self {
        self.max_queue_size = Some(size);
        self
    }
}

impl Default for RayonExecutorConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl RayonExecutor {
    pub fn new(config: RayonExecutorConfig) -> Result<Self, TaskError> {
        let num_threads = config.num_threads.unwrap_or_else(num_cpus::get);
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .map_err(|e| TaskError::Internal(e.to_string()))?;

        Ok(Self {
            id: ExecutorId::new(),
            thread_pool,
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            config,
            closed: AtomicBool::new(false),
            activity: Default::default(),
        })
    }

    pub fn with_id(mut self, id: ExecutorId) -> Self {
        self.id = id;
        self
    }
}

#[async_trait]
impl Executor for RayonExecutor {
    fn is_idle(&self) -> bool {
        self.activity.is_idle() && self.active_tasks.read().is_empty()
    }
    fn is_shutdown(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    fn id(&self) -> ExecutorId {
        self.id
    }

    fn is_available(&self) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        if let Some(max_size) = self.config.max_queue_size {
            self.active_tasks.read().len() < max_size
        } else {
            true
        }
    }

    async fn submit(&self, work: Work) -> TaskResult<Submission> {
        if work.is_async() {
            work.finish(Err(TaskError::InvalidConfiguration(
                "async work requires Tokio".into(),
            )));
            return Err(TaskError::InvalidConfiguration(
                "async work requires Tokio".into(),
            ));
        }
        let mut tasks = self.active_tasks.write();
        if self.closed.load(Ordering::Acquire) {
            drop(tasks);
            work.finish(Err(TaskError::ExecutorUnavailable));
            return Err(TaskError::ExecutorUnavailable);
        }
        if tasks.contains_key(&work.id) {
            drop(tasks);
            work.finish(Err(TaskError::InvalidConfiguration(
                "duplicate active task ID".into(),
            )));
            return Err(TaskError::InvalidConfiguration(
                "duplicate active task ID".into(),
            ));
        }
        if self
            .config
            .max_queue_size
            .is_some_and(|limit| tasks.len() >= limit)
        {
            drop(tasks);
            work.finish(Err(TaskError::QueueFull));
            return Err(TaskError::QueueFull);
        }

        let task_id = work.id;
        let executor_id = self.id;
        let active_tasks = self.active_tasks.clone();
        let task_id_clone = task_id;

        tasks.insert(task_id, work.cancellation.clone());
        drop(tasks);

        let thread_pool = &self.thread_pool;
        let work = work.on_complete(move |_| {
            active_tasks.write().remove(&task_id_clone);
        });
        let guard = self.activity.enter();
        thread_pool.spawn(move || {
            let _guard = guard;
            work.execute();
        });

        Ok(Submission::new(task_id, executor_id))
    }

    async fn cancel(&self, task_id: TaskId) -> TaskResult<()> {
        self.active_tasks
            .read()
            .get(&task_id)
            .ok_or(TaskError::ExecutionFailed(format!(
                "Task {} not found",
                task_id
            )))?
            .cancel();
        Ok(())
    }

    fn queue_depth(&self) -> usize {
        self.active_tasks.read().len()
    }

    fn active_tasks(&self) -> usize {
        self.active_tasks.read().len()
    }

    async fn shutdown(&self) -> TaskResult<()> {
        let tasks = self.active_tasks.write();
        self.closed.store(true, Ordering::Release);
        for token in tasks.values() {
            token.cancel();
        }
        Ok(())
    }

    async fn is_task_alive(&self, task_id: TaskId) -> bool {
        self.active_tasks.read().contains_key(&task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rayon_executor_creation() {
        let executor = RayonExecutor::new(RayonExecutorConfig::default()).unwrap();
        assert!(executor.is_available());
        assert_eq!(executor.queue_depth(), 0);
    }

    #[test]
    fn rayon_executor_config() {
        let config = RayonExecutorConfig::default();
        assert!(config.num_threads.is_some());
        assert!(config.max_queue_size.is_some());
    }
}
