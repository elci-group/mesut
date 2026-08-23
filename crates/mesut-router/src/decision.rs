use mesut_core::{Route, TaskId};

/// Represents a routing decision with explanation.
pub struct RoutingDecision {
    pub task_id: TaskId,
    pub route: Route,
    pub reason: String,
    pub confidence: f64,
    pub metadata: RoutingMetadata,
}

/// Metadata about a routing decision.
pub struct RoutingMetadata {
    pub explicit_hint: bool,
    pub used_classification: bool,
    pub used_availability_check: bool,
}

impl RoutingDecision {
    pub fn new(task_id: TaskId, route: Route, reason: String, confidence: f64) -> Self {
        Self {
            task_id,
            route,
            reason,
            confidence,
            metadata: RoutingMetadata {
                explicit_hint: false,
                used_classification: false,
                used_availability_check: false,
            },
        }
    }

    pub fn with_metadata(mut self, metadata: RoutingMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

impl std::fmt::Display for RoutingDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RoutingDecision {{ task: {}, route: {}, reason: {}, confidence: {:.2} }}",
            self.task_id, self.route, self.reason, self.confidence
        )
    }
}
