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
    /// Maximum distinct labels tracked by the Adaptive history. New labels
    /// beyond the cap schedule from the neutral bucket (FIFO within priority)
    /// and are counted as overflow, bounding observer memory and dispatch
    /// scan cost under high-cardinality labeling.
    pub adaptive_max_labels: usize,
    /// How long a label's learned service-time profile stays trusted after
    /// its last sample. `None` disables expiry. Expiry matters when a
    /// workload's service shape changes while the label runs rarely: without
    /// it, the stale estimate misorders that label's tasks until the rolling
    /// window refills (which can be arbitrarily long for rare labels).
    pub adaptive_profile_ttl: Option<Duration>,
    /// Queue-wait threshold after which `SchedulingMode::Adaptive` promotes a
    /// candidate to FIFO order within its priority tier, bounding starvation
    /// of long work. `None` disables aging (pure shortest-service-time-first).
    pub adaptive_aging: Option<Duration>,
    /// How long `MesuT::shutdown` waits for cooperative work to drain before
    /// forcing termination. `None` waits unboundedly.
    pub shutdown_grace: Option<Duration>,
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
            adaptive_max_labels: 1024,
            adaptive_profile_ttl: Some(Duration::from_secs(60)),
            adaptive_aging: Some(Duration::from_millis(250)),
            shutdown_grace: Some(Duration::from_secs(30)),
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

    /// Maximum distinct labels tracked for `SchedulingMode::Adaptive`.
    pub fn with_adaptive_max_labels(mut self, max_labels: usize) -> Self {
        self.adaptive_max_labels = max_labels;
        self
    }

    /// Profile trust window after a label's last sample; `None` disables.
    pub fn with_adaptive_profile_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.adaptive_profile_ttl = ttl;
        self
    }

    /// Queue-wait threshold after which `SchedulingMode::Adaptive` promotes a
    /// candidate to FIFO order within its tier. `None` disables aging.
    pub fn with_adaptive_aging(mut self, aging: Option<Duration>) -> Self {
        self.adaptive_aging = aging;
        self
    }

    /// Grace period `MesuT::shutdown` waits for cooperative work before
    /// forcing termination. `None` waits unboundedly.
    pub fn with_shutdown_grace(mut self, grace: Option<Duration>) -> Self {
        self.shutdown_grace = grace;
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
