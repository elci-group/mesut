use std::time::Duration;

/// Configuration for the Mesut runtime.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Maximum admitted tasks (running plus waiting); None is unbounded.
    pub max_queue_size: Option<usize>,
    /// Maximum tasks dispatched to executors at once.
    pub max_in_flight: usize,
    pub saturation_policy: mesut_scheduler::BackpressurePolicy,
    pub pipeline_concurrency: usize,
    pub scheduling: mesut_scheduler::SchedulingMode,
    pub task_timeout: Option<Duration>,
    pub enable_telemetry: bool,
    /// Show adaptive, lifecycle-driven terminal animations.
    ///
    /// Enabled by default; rendering is automatically suppressed outside an
    /// interactive terminal unless `MESUT_ANIMATIONS=always` is set.
    pub enable_animations: bool,
    pub backpressure_threshold: usize,
    /// Recent-sample window per label for `SchedulingMode::Adaptive`.
    pub adaptive_window: usize,
}

impl RuntimeConfig {
    pub fn new() -> Self {
        Self {
            max_queue_size: Some(10000),
            max_in_flight: 256,
            saturation_policy: mesut_scheduler::BackpressurePolicy::Queue,
            pipeline_concurrency: 1,
            scheduling: mesut_scheduler::SchedulingMode::Priority,
            task_timeout: None,
            enable_telemetry: true,
            enable_animations: true,
            backpressure_threshold: 1000,
            adaptive_window: 16,
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

    pub fn with_max_in_flight(mut self, limit: usize) -> Self {
        self.max_in_flight = limit;
        self
    }

    pub fn with_backpressure(mut self, policy: mesut_scheduler::BackpressurePolicy) -> Self {
        self.saturation_policy = policy;
        self
    }

    pub fn with_pipeline_concurrency(mut self, limit: usize) -> Self {
        self.pipeline_concurrency = limit;
        self
    }

    pub fn with_scheduling(mut self, scheduling: mesut_scheduler::SchedulingMode) -> Self {
        self.scheduling = scheduling;
        self
    }

    /// Number of recent durations kept per label for `SchedulingMode::Adaptive`.
    pub fn with_adaptive_window(mut self, window: usize) -> Self {
        self.adaptive_window = window;
        self
    }

    pub fn with_telemetry(mut self, enabled: bool) -> Self {
        self.enable_telemetry = enabled;
        self
    }

    pub fn with_animations(mut self, enabled: bool) -> Self {
        self.enable_animations = enabled;
        self
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self::new()
    }
}
