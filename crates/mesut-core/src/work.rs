use crate::{
    CancellationToken, ExecutionMetadata, Priority, TaskError, TaskId, TaskResult, WorkKind,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Resource hint for scheduling.
#[derive(Debug, Clone)]
pub struct ResourceHint {
    pub estimated_cpu_cycles: Option<u64>,
    pub estimated_memory_bytes: Option<usize>,
    pub estimated_io_operations: Option<usize>,
    pub parallelism_hint: Option<usize>,
}

impl ResourceHint {
    pub fn new() -> Self {
        Self {
            estimated_cpu_cycles: None,
            estimated_memory_bytes: None,
            estimated_io_operations: None,
            parallelism_hint: None,
        }
    }
}

impl Default for ResourceHint {
    fn default() -> Self {
        Self::new()
    }
}

/// Routing hint from application.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RoutingHint {
    /// No preference; use default routing.
    #[default]
    Default,
    /// Prefer async/I/O executor.
    PreferAsync,
    /// Require async/I/O executor.
    RequireAsync,
    /// Prefer compute executor.
    PreferCompute,
    /// Require compute executor.
    RequireCompute,
    /// Prefer blocking executor.
    PreferBlocking,
    /// Require blocking executor.
    RequireBlocking,
}

/// Represents a unit of executable work submitted to Mesut.
pub struct Work {
    pub id: TaskId,
    pub kind: WorkKind,
    pub priority: Priority,
    pub routing_hint: RoutingHint,
    pub metadata: ExecutionMetadata,
    pub resource_hint: ResourceHint,
    pub payload: Arc<Vec<u8>>,
    pub cancellation: CancellationToken,
    /// Override the runtime deadline, measured from the start of submission.
    pub timeout: Option<std::time::Duration>,
    /// Identifies a recurring workload across submissions for adaptive
    /// scheduling. Unlabeled work is scheduled without duration history.
    pub label: Option<String>,
    /// Phase-timing handle shared with the runtime coordination layer.
    /// Attached automatically at admission when not set beforehand.
    timing: Option<Arc<crate::TaskTiming>>,
    job: Option<Job>,
    completion: Vec<Completion>,
    started: Option<Box<dyn FnOnce() + Send>>,
}

type Completion = Box<dyn FnOnce(&TaskResult<Vec<u8>>) + Send>;
enum Job {
    Sync(Box<dyn FnOnce(CancellationToken) -> TaskResult<Vec<u8>> + Send>),
    Async(Pin<Box<dyn Future<Output = TaskResult<Vec<u8>>> + Send>>),
}

impl Work {
    pub fn new(kind: WorkKind) -> Self {
        Self {
            id: TaskId::new(),
            kind,
            priority: Priority::default(),
            routing_hint: RoutingHint::default(),
            metadata: ExecutionMetadata::new(),
            resource_hint: ResourceHint::default(),
            payload: Arc::new(Vec::new()),
            cancellation: CancellationToken::new(),
            timeout: None,
            label: None,
            timing: None,
            job: None,
            completion: Vec::new(),
            started: None,
        }
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Attach the phase-timing handle the runtime coordination layer reads
    /// latency phases from. Callers that pre-attach a timing can read the
    /// final breakdown after completion; otherwise admission creates one.
    pub fn with_timing(mut self, timing: Arc<crate::TaskTiming>) -> Self {
        self.timing = Some(timing);
        self
    }

    /// The attached phase-timing handle, if any.
    pub fn timing(&self) -> Option<Arc<crate::TaskTiming>> {
        self.timing.clone()
    }

    pub fn with_scope(mut self, scope: &crate::TaskScope) -> Self {
        let id = self.id;
        scope.add_child(id);
        self.cancellation = CancellationToken::with_parents(vec![
            self.cancellation.clone(),
            scope.cancellation_token(),
        ]);
        let scope = scope.clone();
        self.on_complete(move |result| scope.complete_child(id, result))
    }

    pub fn with_hint(mut self, hint: RoutingHint) -> Self {
        self.routing_hint = hint;
        self
    }

    pub fn with_resource_hint(mut self, hint: ResourceHint) -> Self {
        self.resource_hint = hint;
        self
    }

    pub fn with_payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = Arc::new(payload);
        self
    }

    /// Execute a closure on a compute or blocking worker. Running closures
    /// must check the supplied token to respond promptly to cancellation.
    pub fn with_job(
        mut self,
        job: impl FnOnce(CancellationToken) -> TaskResult<Vec<u8>> + Send + 'static,
    ) -> Self {
        self.job = Some(Job::Sync(Box::new(job)));
        self
    }

    /// Execute a future on the Tokio adapter.
    pub fn with_future(
        mut self,
        future: impl Future<Output = TaskResult<Vec<u8>>> + Send + 'static,
    ) -> Self {
        self.job = Some(Job::Async(Box::pin(future)));
        self
    }

    /// Register a nonblocking result callback. Callbacks run once, in reverse
    /// registration order, so adapter/runtime cleanup precedes user callbacks.
    /// Dropping unfinished work reports cancellation. Callback panics are isolated.
    pub fn on_complete(
        mut self,
        callback: impl FnOnce(&TaskResult<Vec<u8>>) + Send + 'static,
    ) -> Self {
        self.completion.push(Box::new(callback));
        self
    }

    pub fn is_async(&self) -> bool {
        matches!(self.job, Some(Job::Async(_)))
    }

    pub fn finish(mut self, result: TaskResult<Vec<u8>>) {
        self.notify(&result);
    }

    fn notify(&mut self, result: &TaskResult<Vec<u8>>) {
        let cancelled = self.cancellation.reason().map(Err);
        let result = cancelled.as_ref().unwrap_or(result);
        for callback in self.completion.drain(..).rev() {
            // A user callback must not prevent executor/runtime cleanup.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(result)));
        }
    }

    pub fn execute(mut self) {
        if !self.cancellation.is_cancelled() {
            self.start();
        }
        let result = if self.cancellation.is_cancelled() {
            Err(TaskError::Cancelled)
        } else {
            match self.job.take() {
                Some(Job::Sync(job)) => {
                    let cpu_start = crate::thread_cpu_ns();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        job(self.cancellation.clone())
                    }))
                    .unwrap_or_else(|_| Err(TaskError::ExecutionFailed("work panicked".into())));
                    let job_cpu_ns = match (cpu_start, crate::thread_cpu_ns()) {
                        (Some(start), Some(end)) => Some(end.saturating_sub(start)),
                        _ => None,
                    };
                    if let Some(timing) = &self.timing {
                        timing.mark_job_end(job_cpu_ns);
                    }
                    result
                }
                Some(Job::Async(_)) => Err(TaskError::InvalidConfiguration(
                    "async work requires Tokio".into(),
                )),
                None => {
                    if let Some(timing) = &self.timing {
                        timing.mark_job_end(None);
                    }
                    Ok((*self.payload).clone())
                }
            }
        };
        let result = if self.cancellation.is_cancelled() {
            Err(TaskError::Cancelled)
        } else {
            result
        };
        self.notify(&result);
    }

    pub async fn execute_async(mut self) {
        if self.cancellation.is_cancelled() {
            self.notify(&Err(TaskError::Cancelled));
            return;
        }
        self.start();
        if self.is_sync() {
            self.notify(&Err(TaskError::InvalidConfiguration(
                "closures require a compute or blocking executor".into(),
            )));
        } else if let Some(Job::Async(future)) = self.job.take() {
            let result = future.await;
            let result = if self.cancellation.is_cancelled() {
                Err(TaskError::Cancelled)
            } else {
                result
            };
            // A future's thread-CPU time is not attributable: between awaits
            // the worker runs unrelated tasks on the same thread.
            if let Some(timing) = &self.timing {
                timing.mark_job_end(None);
            }
            self.notify(&result);
        } else {
            if let Some(timing) = &self.timing {
                timing.mark_job_end(None);
            }
            self.notify(&Ok((*self.payload).clone()));
        }
    }

    pub fn is_sync(&self) -> bool {
        matches!(self.job, Some(Job::Sync(_)))
    }

    pub fn on_start(mut self, callback: impl FnOnce() + Send + 'static) -> Self {
        self.started = Some(Box::new(callback));
        self
    }

    fn start(&mut self) {
        if let Some(timing) = &self.timing {
            timing.mark_started();
        }
        if let Some(callback) = self.started.take() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback));
        }
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        let error = if std::thread::panicking() {
            TaskError::ExecutionFailed("work panicked".into())
        } else {
            TaskError::Cancelled
        };
        self.notify(&Err(error));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_creation() {
        let work = Work::new(WorkKind::Io);
        assert_eq!(work.kind, WorkKind::Io);
        assert_eq!(work.priority, Priority::Normal);
    }

    #[test]
    fn work_builder() {
        let work = Work::new(WorkKind::Compute)
            .with_priority(Priority::High)
            .with_hint(RoutingHint::RequireCompute);

        assert_eq!(work.kind, WorkKind::Compute);
        assert_eq!(work.priority, Priority::High);
        assert_eq!(work.routing_hint, RoutingHint::RequireCompute);
    }
}
