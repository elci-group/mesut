use mesut_core::{Route, TaskError, TaskId, TaskResult, TaskScope, Work};
use mesut_executor::ArcExecutor;
use mesut_observe::{AssistObserver, EventObserver, NoOpObserver, TaskEvent};
use mesut_router::WorkRouter;
use mesut_scheduler::ShutdownState;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use config::*;
pub use errors::*;
pub use mesut_scheduler::BackpressurePolicy;
pub use native::*;
pub use pipeline::*;

mod config;
mod coordination;
mod errors;
mod native;
mod pipeline;

/// Unified execution, priority admission, lifecycle ownership, and deadlines.
#[derive(Clone)]
pub struct MesuT {
    router: WorkRouter,
    coordination: Arc<coordination::Coordination>,
    config: RuntimeConfig,
}

impl MesuT {
    pub fn new(config: RuntimeConfig) -> Self {
        let observer: Arc<dyn EventObserver> = if config.enable_animations {
            Arc::new(AssistObserver::auto())
        } else {
            Arc::new(NoOpObserver)
        };
        Self {
            router: WorkRouter::new(),
            coordination: coordination::Coordination::new(config.clone(), observer),
            config,
        }
    }

    pub fn with_observer(self, observer: Arc<dyn EventObserver>) -> Self {
        *self.coordination.observer.write() = observer;
        self
    }

    pub fn with_async_executor(mut self, executor: ArcExecutor) -> Self {
        self.router = self.router.with_async_executor(executor);
        self
    }

    pub fn with_compute_executor(mut self, executor: ArcExecutor) -> Self {
        self.router = self.router.with_compute_executor(executor);
        self
    }

    pub fn with_blocking_executor(mut self, executor: ArcExecutor) -> Self {
        self.router = self.router.with_blocking_executor(executor);
        self
    }

    /// Accept work into the priority scheduler. Completion is delivered through
    /// its callback; accepted work can subsequently fail during dispatch.
    pub async fn submit(&self, work: Work) -> TaskResult<()> {
        let id = work.id;
        self.coordination
            .state
            .lock()
            .runtime_state
            .add_submission();
        self.coordination
            .observe(TaskEvent::submitted(id, work.kind));
        let prepared = (|| {
            let route = self.router.route(&work)?;
            let executor = self
                .router
                .get_executor(&route)
                .ok_or(TaskError::NoExecutor)?;
            if (work.is_sync() && matches!(route, Route::Async(_) | Route::Realtime(_)))
                || (work.is_async() && !matches!(route, Route::Async(_) | Route::Realtime(_)))
            {
                return Err(TaskError::InvalidConfiguration(
                    "job type is incompatible with its routed executor".into(),
                ));
            }
            tokio::runtime::Handle::try_current().map_err(|_| TaskError::ExecutorUnavailable)?;
            if executor.is_shutdown() {
                return Err(TaskError::ExecutorUnavailable);
            }
            Ok((executor, route.to_string()))
        })();
        match prepared {
            Ok((executor, route)) => self.coordination.admit(work, executor, route).await,
            Err(error) => {
                self.coordination.state.lock().runtime_state.add_failure();
                self.coordination
                    .observe(TaskEvent::failed(id, error.to_string()));
                work.finish(Err(error.clone()));
                Err(error)
            }
        }
    }

    pub async fn cancel(&self, task_id: TaskId) -> TaskResult<()> {
        if let Some(entry) = self.coordination.state.lock().tasks.get(&task_id) {
            entry.token.cancel();
        }
        self.coordination.changed.notify_waiters();
        Ok(())
    }

    pub fn task_state(&self, task_id: TaskId) -> Option<String> {
        self.coordination
            .state
            .lock()
            .tasks
            .get(&task_id)
            .map(|entry| entry.route.clone())
    }

    pub fn active_tasks(&self) -> usize {
        self.coordination.state.lock().tasks.len()
    }

    /// The learned duration profile for `label` (see [`Work::with_label`]),
    /// used by `SchedulingMode::Adaptive`. `None` until at least one labeled
    /// task with that label has completed successfully.
    pub fn workload_profile(&self, label: &str) -> Option<mesut_scheduler::WorkloadProfile> {
        self.coordination.workload.profile(label)
    }

    /// A snapshot of cumulative submission/completion counters and
    /// per-executor statistics. `active_tasks`/`queue_depth` reflect this
    /// exact moment; the rest accumulate for the life of the runtime.
    pub fn runtime_state(&self) -> mesut_scheduler::RuntimeState {
        let mut snapshot = {
            let state = self.coordination.state.lock();
            let mut snapshot = state.runtime_state.clone();
            snapshot.queued_tasks = state.tasks.values().filter(|e| !e.dispatched).count();
            snapshot.in_flight_tasks = state.tasks.values().filter(|e| e.dispatched).count();
            snapshot
        };
        for executor in self.router.executors() {
            let stats = snapshot
                .executor_stats
                .entry(executor.id().to_string())
                .or_default();
            stats.active_tasks = executor.active_tasks();
            stats.queue_depth = executor.queue_depth();
        }
        snapshot
    }

    /// Where the runtime is in its shutdown lifecycle.
    pub fn shutdown_state(&self) -> ShutdownState {
        self.coordination.state.lock().runtime_state.shutdown
    }

    /// Wait until every admitted task exits. Does not close admission.
    pub async fn join(&self) -> TaskResult<()> {
        loop {
            let notified = self.coordination.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active_tasks() == 0 {
                return Ok(());
            }
            notified.await;
        }
    }

    /// Close admission, request cancellation, and wait for jobs and adapters
    /// to become quiescent within the configured grace period
    /// ([`RuntimeConfig::with_shutdown_grace`]). Cooperative work is given
    /// the whole grace to exit; if the deadline expires with work still
    /// active, the runtime enters `ShutdownState::ForceTerminated`: queued
    /// work is cancelled, dispatchable handles are cancelled, and tracked
    /// bookkeeping is cleared. Closures that never cooperate may still run on
    /// executor threads afterward — threads cannot be killed, only abandoned.
    /// Uncooperative callers can bound the wait differently with
    /// [`MesuT::shutdown_with_grace`].
    pub async fn shutdown(&self) -> TaskResult<()> {
        self.shutdown_with_grace(self.config.shutdown_grace).await
    }

    /// [`MesuT::shutdown`] with an explicit grace period; `None` waits
    /// unboundedly for cooperative work.
    pub async fn shutdown_with_grace(&self, grace: Option<Duration>) -> TaskResult<()> {
        {
            let mut state = self.coordination.state.lock();
            state.closed = true;
            state.runtime_state.shutdown = ShutdownState::Draining;
            for entry in state.tasks.values() {
                entry.token.cancel();
            }
        }
        self.coordination.changed.notify_waiters();
        for executor in self.router.executors() {
            executor.shutdown().await?;
        }
        let deadline = grace.map(|grace| Instant::now() + grace);
        let forced = loop {
            if self.active_tasks() == 0 {
                break false;
            }
            if deadline.is_some_and(|due| Instant::now() >= due) {
                break true;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        if forced {
            self.coordination.force_terminate().await;
        }
        for executor in self.router.executors() {
            match grace {
                // Post-drain quiescence is normally instant; bound it so an
                // abandoned closure cannot hang shutdown past the deadline.
                Some(grace) => {
                    let _ = tokio::time::timeout(grace, executor.shutdown_and_wait()).await;
                }
                None => executor.shutdown_and_wait().await?,
            }
        }
        if !forced {
            self.coordination.state.lock().runtime_state.shutdown = ShutdownState::Terminated;
        }
        Ok(())
    }

    pub async fn submit_scoped(&self, scope: &TaskScope, work: Work) -> TaskResult<()> {
        self.submit(work.with_scope(scope)).await
    }

    pub async fn join_scope(&self, scope: &TaskScope) -> TaskResult<()> {
        while !scope.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        scope.result()
    }

    /// Children are joined before returning. Child failures cancel siblings;
    /// errors or dropping this future cancel its scope. Nested scopes can use
    /// TaskScope::with_parent with the outer scope's token.
    pub async fn scope<T, F, Fut>(&self, body: F) -> TaskResult<T>
    where
        F: FnOnce(TaskScope) -> Fut,
        Fut: std::future::Future<Output = TaskResult<T>>,
    {
        struct Guard(TaskScope);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.cancel();
            }
        }
        let scope = TaskScope::new(TaskId::new());
        let guard = Guard(scope.clone());
        let result = body(scope.clone()).await;
        if result.is_err() {
            scope.cancel();
        }
        let children = self.join_scope(&scope).await;
        drop(guard);
        let value = result?;
        children?;
        Ok(value)
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
    use mesut_core::WorkKind;
    use mesut_observe::TraceCollector;

    #[test]
    fn runtime_creation() {
        assert_eq!(MesuT::default().active_tasks(), 0);
    }

    #[test]
    fn task_state_tracking() {
        assert!(MesuT::default().task_state(TaskId::new()).is_none());
    }

    #[tokio::test]
    async fn failed_submission_closes_the_observer_lifecycle() {
        let collector = Arc::new(TraceCollector::new());
        let runtime = MesuT::default().with_observer(collector.clone());
        assert!(runtime.submit(Work::new(WorkKind::Io)).await.is_err());
        let events = collector.events();
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events.last().map(|event| &event.event_type),
            Some(mesut_observe::TaskEventType::Failed { .. })
        ));
    }
}
