use crate::{RuntimeState, SchedulingPolicy};
use async_trait::async_trait;
use mesut_core::{Route, TaskError, TaskResult, Work, WorkKind};

/// Default scheduling policy: routes by work kind.
pub struct DefaultSchedulingPolicy;

#[async_trait]
impl SchedulingPolicy for DefaultSchedulingPolicy {
    async fn select_executor(&self, work: &Work, _state: &RuntimeState) -> TaskResult<Route> {
        match work.kind {
            WorkKind::Io => Err(TaskError::NoExecutor),
            WorkKind::Compute => Err(TaskError::NoExecutor),
            WorkKind::Blocking => Err(TaskError::NoExecutor),
            WorkKind::Realtime => Err(TaskError::NoExecutor),
            WorkKind::Periodic => Err(TaskError::NoExecutor),
        }
    }
}

/// Priority-based scheduling policy.
pub struct PrioritySchedulingPolicy;

#[async_trait]
impl SchedulingPolicy for PrioritySchedulingPolicy {
    async fn select_executor(&self, work: &Work, _state: &RuntimeState) -> TaskResult<Route> {
        // Priority-based scheduling would select executors based on task priority.
        // Implementation deferred to Phase 2.
        Err(TaskError::Internal(
            "Priority scheduling not yet implemented".to_string(),
        ))
    }
}

/// Load-aware scheduling policy.
pub struct LoadAwareSchedulingPolicy;

#[async_trait]
impl SchedulingPolicy for LoadAwareSchedulingPolicy {
    async fn select_executor(&self, work: &Work, state: &RuntimeState) -> TaskResult<Route> {
        // Would analyze executor load and route accordingly.
        // Implementation deferred to Phase 2.
        Err(TaskError::Internal(
            "Load-aware scheduling not yet implemented".to_string(),
        ))
    }
}
