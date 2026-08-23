use serde::{Deserialize, Serialize};

/// Diagnostic information about the runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeDiagnostics {
    pub uptime_ms: u128,
    pub total_submitted: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_cancelled: u64,
    pub currently_queued: usize,
    pub currently_running: usize,
}

impl RuntimeDiagnostics {
    pub fn new() -> Self {
        Self {
            uptime_ms: 0,
            total_submitted: 0,
            total_completed: 0,
            total_failed: 0,
            total_cancelled: 0,
            currently_queued: 0,
            currently_running: 0,
        }
    }

    pub fn completion_rate(&self) -> f64 {
        if self.total_submitted == 0 {
            return 0.0;
        }
        (self.total_completed as f64) / (self.total_submitted as f64)
    }

    pub fn failure_rate(&self) -> f64 {
        if self.total_submitted == 0 {
            return 0.0;
        }
        (self.total_failed as f64) / (self.total_submitted as f64)
    }
}

impl Default for RuntimeDiagnostics {
    fn default() -> Self {
        Self::new()
    }
}
