use std::time::Duration;

/// Configuration for the Mesut runtime.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub max_queue_size: Option<usize>,
    pub task_timeout: Option<Duration>,
    pub enable_telemetry: bool,
    pub backpressure_threshold: usize,
}

impl RuntimeConfig {
    pub fn new() -> Self {
        Self {
            max_queue_size: Some(10000),
            task_timeout: None,
            enable_telemetry: true,
            backpressure_threshold: 1000,
        }
    }

    pub fn with_max_queue_size(mut self, size: usize) -> Self {
        self.max_queue_size = Some(size);
        self
    }

    pub fn with_task_timeout(mut self, timeout: Duration) -> Self {
        self.task_timeout = Some(timeout);
        self
    }

    pub fn with_telemetry(mut self, enabled: bool) -> Self {
        self.enable_telemetry = enabled;
        self
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self::new()
    }
}
