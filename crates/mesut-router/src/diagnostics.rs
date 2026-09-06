use mesut_core::TaskId;

/// Diagnostic event for routing operations.
pub struct RoutingDiagnostic {
    pub task_id: TaskId,
    pub event: RoutingEvent,
}

/// Types of routing diagnostic events.
pub enum RoutingEvent {
    DecisionMade {
        work_kind: String,
        route: String,
        confidence: f64,
    },
    NoExecutorAvailable {
        reason: String,
    },
    QueueFull {
        executor_name: String,
    },
    Error {
        error: String,
    },
}

impl RoutingDiagnostic {
    pub fn decision(task_id: TaskId, work_kind: String, route: String, confidence: f64) -> Self {
        Self {
            task_id,
            event: RoutingEvent::DecisionMade {
                work_kind,
                route,
                confidence,
            },
        }
    }

    pub fn no_executor(task_id: TaskId, reason: String) -> Self {
        Self {
            task_id,
            event: RoutingEvent::NoExecutorAvailable { reason },
        }
    }

    pub fn queue_full(task_id: TaskId, executor_name: String) -> Self {
        Self {
            task_id,
            event: RoutingEvent::QueueFull { executor_name },
        }
    }

    pub fn error(task_id: TaskId, error: String) -> Self {
        Self {
            task_id,
            event: RoutingEvent::Error { error },
        }
    }
}

/// Trait for routing diagnostics observers.
pub trait RoutingDiagnosticsObserver: Send + Sync {
    fn observe(&self, diagnostic: &RoutingDiagnostic);
}

/// No-op implementation.
pub struct NoOpRoutingObserver;

impl RoutingDiagnosticsObserver for NoOpRoutingObserver {
    fn observe(&self, _diagnostic: &RoutingDiagnostic) {}
}
