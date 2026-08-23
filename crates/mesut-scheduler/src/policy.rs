use crate::{RuntimeState, SchedulingPolicy};
use async_trait::async_trait;
use mesut_core::{Route, TaskError, TaskResult, Work};

/// Default scheduling policy: routes by work kind.
pub struct DefaultSchedulingPolicy;

#[async_trait]
impl SchedulingPolicy for DefaultSchedulingPolicy {
    async fn select_executor(&self, _work: &Work, _state: &RuntimeState) -> TaskResult<Route> {
        Err(TaskError::NoExecutor)
    }
}

/// Priority-based scheduling policy.
pub struct PrioritySchedulingPolicy {
    pub priority_factor: f64,
}

impl PrioritySchedulingPolicy {
    pub fn new() -> Self {
        Self {
            priority_factor: 1.5,
        }
    }
}

#[async_trait]
impl SchedulingPolicy for PrioritySchedulingPolicy {
    async fn select_executor(&self, _work: &Work, _state: &RuntimeState) -> TaskResult<Route> {
        Err(TaskError::Internal(
            "Priority scheduling delegated to router".to_string(),
        ))
    }
}

impl Default for PrioritySchedulingPolicy {
    fn default() -> Self {
        Self::new()
    }
}

/// Load-aware scheduling policy.
pub struct LoadAwareSchedulingPolicy {
    pub threshold: f64,
}

impl LoadAwareSchedulingPolicy {
    pub fn new() -> Self {
        Self { threshold: 0.8 }
    }
}

#[async_trait]
impl SchedulingPolicy for LoadAwareSchedulingPolicy {
    async fn select_executor(&self, _work: &Work, _state: &RuntimeState) -> TaskResult<Route> {
        Err(TaskError::Internal(
            "Load-aware scheduling delegated to router".to_string(),
        ))
    }
}

impl Default for LoadAwareSchedulingPolicy {
    fn default() -> Self {
        Self::new()
    }
}
