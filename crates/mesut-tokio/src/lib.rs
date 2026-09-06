use async_trait::async_trait;
use mesut_core::{ExecutorId, TaskError, TaskId, TaskResult, Work};
use mesut_executor::{Executor, Submission};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Tokio executor adapter for Mesut.
pub struct TokioExecutor {
    id: ExecutorId,
    active_tasks: Arc<RwLock<HashMap<TaskId, JoinHandle<()>>>>,
    config: TokioExecutorConfig,
    closed: AtomicBool,
    activity: mesut_executor::Activity,
}

/// Configuration for Tokio executor.
#[derive(Clone, Debug)]
pub struct TokioExecutorConfig {
    pub max_queue_size: Option<usize>,
    pub worker_threads: Option<usize>,
}

impl TokioExecutorConfig {
    pub fn new() -> Self {
        Self {
            max_queue_size: Some(10000),
            worker_threads: None,
        }
    }

    pub fn with_max_queue_size(mut self, size: usize) -> Self {
        self.max_queue_size = Some(size);
        self
    }

    pub fn with_worker_threads(mut self, threads: usize) -> Self {
        self.worker_threads = Some(threads);
        self
    }
}

impl Default for TokioExecutorConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl TokioExecutor {
    pub fn new(config: TokioExecutorConfig) -> Self {
        Self {
            id: ExecutorId::new(),
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            config,
            closed: AtomicBool::new(false),
            activity: Default::default(),
        }
    }

    pub fn with_id(mut self, id: ExecutorId) -> Self {
        self.id = id;
        self
    }
}

#[async_trait]
impl Executor for TokioExecutor {
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
        if work.is_sync() {
            work.finish(Err(TaskError::InvalidConfiguration(
                "closures require a compute or blocking executor".into(),
            )));
            return Err(TaskError::InvalidConfiguration(
                "closures require a compute or blocking executor".into(),
            ));
        }
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                work.finish(Err(TaskError::ExecutorUnavailable));
                return Err(TaskError::ExecutorUnavailable);
            }
        };
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

        let work = work.on_complete(move |_| {
            active_tasks.write().remove(&task_id_clone);
        });
        let guard = self.activity.enter();
        let handle = runtime.spawn(async move {
            let _guard = guard;
            work.execute_async().await;
        });
        tasks.insert(task_id, handle);

        Ok(Submission::new(task_id, executor_id))
    }

    async fn cancel(&self, task_id: TaskId) -> TaskResult<()> {
        if let Some(handle) = self.active_tasks.read().get(&task_id) {
            handle.abort();
            Ok(())
        } else {
            Err(TaskError::ExecutionFailed(format!(
                "Task {} not found",
                task_id
            )))
        }
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
        for handle in tasks.values() {
            handle.abort();
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

    #[tokio::test]
    async fn tokio_executor_creation() {
        let executor = TokioExecutor::new(TokioExecutorConfig::default());
        assert!(executor.is_available());
        assert_eq!(executor.queue_depth(), 0);
    }

    #[tokio::test]
    async fn tokio_executor_submit() {
        let executor = Arc::new(TokioExecutor::new(TokioExecutorConfig::default()));
        let work = Work::new(mesut_core::WorkKind::Io);
        let result = executor.submit(work).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn tokio_executor_cancel() {
        let executor = Arc::new(TokioExecutor::new(TokioExecutorConfig::default()));
        let work = Work::new(mesut_core::WorkKind::Io);
        let submission = executor.submit(work).await.unwrap();
        let result = executor.cancel(submission.task_id).await;
        assert!(result.is_ok());
    }
}
