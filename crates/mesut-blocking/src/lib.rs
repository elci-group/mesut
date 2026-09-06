use async_trait::async_trait;
use mesut_core::{CancellationToken, ExecutorId, TaskError, TaskId, TaskResult, Work};
use mesut_executor::{Executor, Submission};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

/// Blocking executor for CPU-bound blocking operations.
pub struct BlockingExecutor {
    id: ExecutorId,
    thread_pool: Arc<BlockingThreadPool>,
    active_tasks: Arc<RwLock<HashMap<TaskId, CancellationToken>>>,
    config: BlockingExecutorConfig,
    closed: AtomicBool,
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
    sender: Mutex<Option<mpsc::Sender<PendingWork>>>,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
    activity: mesut_executor::Activity,
}

struct PendingWork {
    work: Work,
    _activity: mesut_executor::ActivityGuard,
}

impl BlockingThreadPool {
    fn new(num_threads: usize) -> Arc<Self> {
        let (sender, receiver) = mpsc::channel::<PendingWork>();
        let receiver = Arc::new(Mutex::new(receiver));
        let pool = Arc::new(Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(Vec::new()),
            activity: Default::default(),
        });

        for _ in 0..num_threads.max(1) {
            let receiver = receiver.clone();
            let worker = thread::spawn(move || loop {
                let next = match receiver.lock() {
                    Ok(receiver) => receiver.recv(),
                    Err(_) => break,
                };
                match next {
                    Ok(pending) => {
                        let PendingWork { work, _activity } = pending;
                        work.execute();
                    }
                    Err(_) => break,
                }
            });
            pool.workers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(worker);
        }

        pool
    }

    fn submit(&self, work: Work) -> TaskResult<()> {
        let sender = self
            .sender
            .lock()
            .map_err(|_| TaskError::ExecutorUnavailable)?;
        match sender.as_ref() {
            Some(sender) => sender
                .send(PendingWork {
                    work,
                    _activity: self.activity.enter(),
                })
                .map_err(|error| {
                    error.0.work.finish(Err(TaskError::ExecutorUnavailable));
                    TaskError::ExecutorUnavailable
                }),
            None => {
                work.finish(Err(TaskError::ExecutorUnavailable));
                Err(TaskError::ExecutorUnavailable)
            }
        }
    }

    fn shutdown(&self) {
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
    }
}

impl BlockingExecutor {
    pub fn new(config: BlockingExecutorConfig) -> Self {
        Self {
            id: ExecutorId::new(),
            thread_pool: BlockingThreadPool::new(config.num_threads),
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            config,
            closed: AtomicBool::new(false),
        }
    }

    pub fn with_id(mut self, id: ExecutorId) -> Self {
        self.id = id;
        self
    }
}

#[async_trait]
impl Executor for BlockingExecutor {
    fn is_idle(&self) -> bool {
        self.thread_pool.activity.is_idle() && self.active_tasks.read().is_empty()
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

        tasks.insert(task_id, work.cancellation.clone());
        drop(tasks);
        let active_tasks = self.active_tasks.clone();
        let task_id_clone = task_id;

        let work = work.on_complete(move |_| {
            active_tasks.write().remove(&task_id_clone);
        });
        self.thread_pool.submit(work)?;

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
        drop(tasks);
        self.thread_pool.shutdown();
        Ok(())
    }

    async fn is_task_alive(&self, task_id: TaskId) -> bool {
        self.active_tasks.read().contains_key(&task_id)
    }

    async fn shutdown_and_wait(&self) -> TaskResult<()> {
        self.shutdown().await?;
        loop {
            {
                let mut workers = self
                    .thread_pool
                    .workers
                    .lock()
                    .map_err(|_| TaskError::Internal("worker handles poisoned".into()))?;
                if self.is_idle() && workers.iter().all(thread::JoinHandle::is_finished) {
                    for worker in workers.drain(..) {
                        worker
                            .join()
                            .map_err(|_| TaskError::Internal("blocking worker panicked".into()))?;
                    }
                    return Ok(());
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocking_executor_creation() {
        let executor = BlockingExecutor::new(BlockingExecutorConfig::default());
        assert!(executor.is_available());
    }

    #[test]
    fn blocking_executor_config() {
        let config = BlockingExecutorConfig::default();
        assert!(config.num_threads > 0);
        assert!(config.max_queue_size.is_some());
    }
}
