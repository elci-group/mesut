use async_trait::async_trait;
use mesut_core::{CancellationToken, TaskError, TaskId, TaskResult, Work, WorkKind};
use mesut_executor::{ArcExecutor, Executor};
use mesut_observe::{EventObserver, NoOpObserver, TaskEvent, TraceCollector};
use mesut_router::WorkRouter;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub use config::*;
pub use pipeline::*;
pub use native::*;

mod config;
mod pipeline;
mod native;

/// The Mesut runtime orchestrator.
pub struct MesuT {
    router: WorkRouter,
    observer: Arc<dyn EventObserver>,
    task_states: Arc<RwLock<HashMap<TaskId, String>>>,
    config: RuntimeConfig,
}

impl MesuT {
    pub fn new(config: RuntimeConfig) -> Self {
        Self {
            router: WorkRouter::new(),
            observer: Arc::new(NoOpObserver),
            task_states: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub fn with_observer(mut self, observer: Arc<dyn EventObserver>) -> Self {
        self.observer = observer;
        self
    }

    pub fn with_async_executor(mut self, executor: ArcExecutor) -> Self {
        self.router = self.router.clone().with_async_executor(executor);
        self
    }

    pub fn with_compute_executor(mut self, executor: ArcExecutor) -> Self {
        self.router = self.router.clone().with_compute_executor(executor);
        self
    }

    pub fn with_blocking_executor(mut self, executor: ArcExecutor) -> Self {
        self.router = self.router.clone().with_blocking_executor(executor);
        self
    }

    pub async fn submit(&self, work: Work) -> TaskResult<()> {
        let task_id = work.id;

        self.observer
            .observe(&TaskEvent::submitted(task_id, work.kind));

        let route = self.router.route(&work)?;
        let executor = self
            .router
            .get_executor(&route)
            .ok_or(TaskError::NoExecutor)?;

        self.observer
            .observe(&TaskEvent::routed(task_id, format!("{}", route)));

        self.observer
            .observe(&TaskEvent::queued(task_id, executor.queue_depth()));

        let _submission = executor.submit(work).await?;

        self.observer
            .observe(&TaskEvent::started(task_id, format!("{}", executor.id())));

        self.task_states
            .write()
            .insert(task_id, format!("{}", route));

        Ok(())
    }

    pub async fn cancel(&self, task_id: TaskId) -> TaskResult<()> {
        if self.task_states.read().contains_key(&task_id) {
            self.observer
                .observe(&TaskEvent::cancelled(task_id, "User requested".to_string()));
            self.task_states.write().remove(&task_id);
        }
        Ok(())
    }

    pub fn task_state(&self, task_id: TaskId) -> Option<String> {
        self.task_states.read().get(&task_id).cloned()
    }

    pub fn active_tasks(&self) -> usize {
        self.task_states.read().len()
    }
}

impl Default for MesuT {
    fn default() -> Self {
        Self::new(RuntimeConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_creation() {
        let runtime = MesuT::default();
        assert_eq!(runtime.active_tasks(), 0);
    }

    #[test]
    fn task_state_tracking() {
        let runtime = MesuT::default();
        let task_id = TaskId::new();

        assert!(runtime.task_state(task_id).is_none());
    }
}
