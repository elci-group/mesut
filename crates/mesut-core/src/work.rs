use crate::{ExecutionMetadata, Priority, TaskId, WorkKind};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingHint {
    /// No preference; use default routing.
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

impl Default for RoutingHint {
    fn default() -> Self {
        RoutingHint::Default
    }
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
        }
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
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
