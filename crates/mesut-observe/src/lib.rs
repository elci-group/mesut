use mesut_core::{TaskId, WorkKind};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;

pub use diagnostics::*;
pub use metrics::*;
pub use tracing::*;

mod diagnostics;
mod metrics;
mod tracing;

/// Event representing a task lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEvent {
    pub timestamp: SystemTime,
    pub task_id: TaskId,
    pub event_type: TaskEventType,
}

/// Types of task events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskEventType {
    Submitted { work_kind: WorkKind },
    Routed { route: String },
    Queued { queue_depth: usize },
    Started { executor_id: String },
    Completed { duration_ms: u128 },
    Failed { error: String },
    Cancelled { reason: String },
}

impl TaskEvent {
    pub fn submitted(task_id: TaskId, work_kind: WorkKind) -> Self {
        Self {
            timestamp: SystemTime::now(),
            task_id,
            event_type: TaskEventType::Submitted { work_kind },
        }
    }

    pub fn routed(task_id: TaskId, route: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            task_id,
            event_type: TaskEventType::Routed { route },
        }
    }

    pub fn queued(task_id: TaskId, queue_depth: usize) -> Self {
        Self {
            timestamp: SystemTime::now(),
            task_id,
            event_type: TaskEventType::Queued { queue_depth },
        }
    }

    pub fn started(task_id: TaskId, executor_id: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            task_id,
            event_type: TaskEventType::Started { executor_id },
        }
    }

    pub fn completed(task_id: TaskId, duration_ms: u128) -> Self {
        Self {
            timestamp: SystemTime::now(),
            task_id,
            event_type: TaskEventType::Completed { duration_ms },
        }
    }

    pub fn failed(task_id: TaskId, error: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            task_id,
            event_type: TaskEventType::Failed { error },
        }
    }

    pub fn cancelled(task_id: TaskId, reason: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            task_id,
            event_type: TaskEventType::Cancelled { reason },
        }
    }
}

/// Event observer trait.
pub trait EventObserver: Send + Sync {
    fn observe(&self, event: &TaskEvent);
}

/// Default no-op observer.
pub struct NoOpObserver;

impl EventObserver for NoOpObserver {
    fn observe(&self, _event: &TaskEvent) {}
}

/// Trace collector for events.
#[derive(Clone)]
pub struct TraceCollector {
    events: Arc<parking_lot::RwLock<Vec<TaskEvent>>>,
}

impl TraceCollector {
    pub fn new() -> Self {
        Self {
            events: Arc::new(parking_lot::RwLock::new(Vec::new())),
        }
    }

    pub fn events(&self) -> Vec<TaskEvent> {
        self.events.read().clone()
    }

    pub fn clear(&self) {
        self.events.write().clear();
    }

    pub fn len(&self) -> usize {
        self.events.read().len()
    }
}

impl Default for TraceCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl EventObserver for TraceCollector {
    fn observe(&self, event: &TaskEvent) {
        self.events.write().push(event.clone());
    }
}
