use serde::{Deserialize, Serialize};

/// Metrics for an executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorMetrics {
    pub executor_id: String,
    pub active_tasks: usize,
    pub queue_depth: usize,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub avg_queue_wait_ms: f64,
    pub avg_execution_ms: f64,
    pub throughput_per_sec: f64,
}

impl ExecutorMetrics {
    pub fn new(executor_id: String) -> Self {
        Self {
            executor_id,
            active_tasks: 0,
            queue_depth: 0,
            completed_tasks: 0,
            failed_tasks: 0,
            avg_queue_wait_ms: 0.0,
            avg_execution_ms: 0.0,
            throughput_per_sec: 0.0,
        }
    }

    pub fn utilisation(&self) -> f64 {
        if self.active_tasks == 0 && self.queue_depth == 0 {
            0.0
        } else {
            (self.active_tasks as f64) / ((self.active_tasks + self.queue_depth).max(1) as f64)
        }
    }
}

/// Metrics for a routing decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingMetrics {
    pub total_routed: u64,
    pub by_work_kind: std::collections::HashMap<String, u64>,
    pub by_executor: std::collections::HashMap<String, u64>,
    pub routing_errors: u64,
}

impl RoutingMetrics {
    pub fn new() -> Self {
        Self {
            total_routed: 0,
            by_work_kind: std::collections::HashMap::new(),
            by_executor: std::collections::HashMap::new(),
            routing_errors: 0,
        }
    }
}

impl Default for RoutingMetrics {
    fn default() -> Self {
        Self::new()
    }
}
