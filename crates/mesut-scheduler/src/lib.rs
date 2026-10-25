use mesut_core::{Priority, TaskId};
use std::sync::Arc;
use std::time::Duration;

pub use adaptive::*;
pub use policy::*;
pub use queue::*;

mod adaptive;
mod policy;
mod queue;

/// Scheduling policy trait.
pub trait SchedulingPolicy: Send + Sync {
    /// Choose among already-routed tasks whose executors have capacity.
    fn select(&self, candidates: &[SchedulingCandidate]) -> Option<TaskId>;
}

#[derive(Clone, Copy, Debug)]
pub struct SchedulingCandidate {
    pub task_id: TaskId,
    pub priority: Priority,
    pub sequence: u64,
    pub executor_active: usize,
    /// Historical mean execution (service) time for this task's label, if any
    /// has been observed. `None` for unlabeled work or a label seen for the
    /// first time. Never includes queue or dispatch wait.
    pub expected_service_time: Option<Duration>,
    /// How long the task has been waiting since admission. Adaptive policies
    /// use this to age starved candidates past the configured threshold.
    pub waited: Duration,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum SchedulingMode {
    Fifo,
    #[default]
    Priority,
    LoadAware,
    /// Like `Priority`, but breaks ties within a priority tier in favor of the
    /// candidate with the shorter historical *execution* time for its label,
    /// and promotes candidates whose queue wait exceeds the configured aging
    /// threshold back to FIFO order so long work cannot starve.
    Adaptive,
}

/// Lifecycle of runtime shutdown.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ShutdownState {
    /// Accepting work normally.
    #[default]
    Running,
    /// Admission is closed; cooperative work is being drained within the
    /// configured grace period.
    Draining,
    /// All work drained and executors are quiescent; the runtime closed
    /// cleanly.
    Terminated,
    /// The grace period expired with work still active: queued work was
    /// cancelled, dispatchable handles were cancelled, and runtime
    /// bookkeeping was cleared. Uncooperative closures may still be running
    /// on executor threads; the runtime no longer tracks them.
    ForceTerminated,
}

/// Point-in-time contents of a [`LatencyHistogram`]: bucketed sample counts
/// on a log2 microsecond scale plus the unbucketed count and sum.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum_us: u64,
    /// `(bucket_lower_us, samples)` for every bucket with samples, ascending.
    /// Bucket `i` covers `[2^i, 2^(i+1))` microseconds.
    pub buckets: Vec<(u64, u64)>,
}

impl HistogramSnapshot {
    /// Mean in microseconds, or `None` when empty.
    pub fn mean_us(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum_us as f64 / self.count as f64)
    }

    /// Upper bound (in microseconds) of the bucket containing percentile
    /// `p` in [0, 1], by sample count. Coarse by design: one log2 bucket.
    pub fn bucket_quantile_us(&self, p: f64) -> Option<u64> {
        if self.count == 0 || !(0.0..=1.0).contains(&p) {
            return None;
        }
        let target = (self.count as f64 * p).ceil() as u64;
        let mut seen = 0u64;
        for (lower_us, samples) in &self.buckets {
            seen += samples;
            if seen >= target {
                return Some(lower_us * 2);
            }
        }
        None
    }
}

/// Cumulative latency histogram kept by the coordination layer. Bounded by
/// construction: 33 log2 buckets spanning 1µs..8s+, no label cardinality.
#[derive(Clone, Default)]
pub struct LatencyHistogram {
    inner: Arc<HistogramInner>,
}

impl std::fmt::Debug for LatencyHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.snapshot();
        f.debug_struct("LatencyHistogram")
            .field("count", &snapshot.count)
            .field("sum_us", &snapshot.sum_us)
            .finish()
    }
}

struct HistogramInner {
    buckets: parking_lot::Mutex<Box<[u64; 33]>>,
    count: std::sync::atomic::AtomicU64,
    sum_us: std::sync::atomic::AtomicU64,
}

impl Default for HistogramInner {
    fn default() -> Self {
        Self {
            buckets: parking_lot::Mutex::new(Box::new([0; 33])),
            count: std::sync::atomic::AtomicU64::new(0),
            sum_us: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    pub fn record_us(&self, us: u64) {
        let index = (63 - (us.max(1)).leading_zeros()).min(32) as usize;
        self.inner.buckets.lock()[index] += 1;
        self.inner
            .count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.inner
            .sum_us
            .fetch_add(us, std::sync::atomic::Ordering::AcqRel);
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        let buckets = self.inner.buckets.lock();
        let mut out = Vec::new();
        for (index, samples) in buckets.iter().enumerate() {
            if *samples > 0 {
                out.push((1u64 << index, *samples));
            }
        }
        HistogramSnapshot {
            count: self.inner.count.load(std::sync::atomic::Ordering::Acquire),
            sum_us: self.inner.sum_us.load(std::sync::atomic::Ordering::Acquire),
            buckets: out,
        }
    }
}

/// Aggregate runtime counters and per-executor statistics.
///
/// `active_tasks`/`queue_depth` in `executor_stats` are point-in-time and
/// only meaningful in a freshly built snapshot (see `MesuT::runtime_state`);
/// `queued_tasks`/`in_flight_tasks` are the same kind of point-in-time
/// runtime-level reading. The cumulative counters (`completed_tasks`,
/// `failed_tasks`, `total_wall_ms`, `busy_ms`) accumulate for the life of the
/// runtime.
#[derive(Clone, Debug, Default)]
pub struct RuntimeState {
    pub total_submitted: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_cancelled: u64,
    /// Submissions rejected at admission (queue full, closed runtime, or
    /// busy executor under `Reject` backpressure).
    pub total_rejected: u64,
    /// Admitted tasks discarded to make room for higher-priority work under
    /// `Shed` backpressure.
    pub total_shed: u64,
    pub shutdown: ShutdownState,
    /// Admitted but not yet dispatched, at snapshot time.
    pub queued_tasks: usize,
    /// Dispatched and not yet finished, at snapshot time.
    pub in_flight_tasks: usize,
    /// Runtime-level queue-wait histogram (dispatch-decision latency),
    /// cumulative for the life of the runtime.
    pub queue_latency_us: HistogramSnapshot,
    /// Runtime-level execution histogram (job run time only), cumulative.
    pub execution_latency_us: HistogramSnapshot,
    pub executor_stats: std::collections::HashMap<String, ExecutorStats>,
}

/// Statistics for an executor. CPU-time instrumentation is per-task only
/// (see `mesut_core::TaskTiming`); executor-level `busy_ms` is the sum of
/// measured execution time of its finished tasks.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutorStats {
    pub active_tasks: usize,
    pub queue_depth: usize,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub total_wall_ms: u64,
    /// Sum of measured execution time (job run only, no queue wait) across
    /// finished tasks. Against a known worker count this yields utilisation:
    /// `busy_ms / (elapsed_ms * workers)`.
    pub busy_ms: u64,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_executor_stats(&mut self, executor_id: String, stats: ExecutorStats) {
        self.executor_stats.insert(executor_id, stats);
    }

    /// Fold one finished task's wall-clock duration, measured execution time,
    /// and outcome into `executor_id`'s cumulative statistics.
    pub fn record_executor_completion(
        &mut self,
        executor_id: String,
        wall_ms: u64,
        execution_ms: u64,
        failed: bool,
    ) {
        let stats = self.executor_stats.entry(executor_id).or_default();
        stats.total_wall_ms += wall_ms;
        stats.busy_ms += execution_ms;
        if failed {
            stats.failed_tasks += 1;
        } else {
            stats.completed_tasks += 1;
        }
    }

    pub fn add_submission(&mut self) {
        self.total_submitted += 1;
    }

    pub fn add_completion(&mut self) {
        self.total_completed += 1;
    }

    pub fn add_failure(&mut self) {
        self.total_failed += 1;
    }

    pub fn add_rejection(&mut self) {
        self.total_rejected += 1;
    }

    pub fn add_shed(&mut self) {
        self.total_shed += 1;
    }

    pub fn add_cancellation(&mut self) {
        self.total_cancelled += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_state_creation() {
        let state = RuntimeState::new();
        assert_eq!(state.total_submitted, 0);
        assert_eq!(state.total_completed, 0);
        assert_eq!(state.shutdown, ShutdownState::Running);
    }

    #[test]
    fn executor_stats_separate_wall_from_busy_time() {
        let mut state = RuntimeState::new();
        state.record_executor_completion("compute".into(), 100, 40, false);
        state.record_executor_completion("compute".into(), 50, 50, true);
        let stats = &state.executor_stats["compute"];
        assert_eq!(stats.total_wall_ms, 150);
        assert_eq!(stats.busy_ms, 90);
        assert_eq!(stats.completed_tasks, 1);
        assert_eq!(stats.failed_tasks, 1);
    }

    #[test]
    fn histogram_buckets_are_log2_microseconds() {
        let histogram = LatencyHistogram::default();
        for us in [1u64, 1, 3, 1_000, 2_000_000] {
            histogram.record_us(us);
        }
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.count, 5);
        assert_eq!(snapshot.sum_us, 2_001_005);
        let buckets = &snapshot.buckets;
        // Bucket 0 covers [1, 2)µs and holds the two 1µs samples.
        assert_eq!(buckets[0], (1, 2));
        // The 2s sample lands in bucket 21 covering [2_097_152, ...): index
        // is the floor of log2, so 2_000_000 maps to bucket 20 ([1_048_576,
        // 2_097_152)).
        assert_eq!(*buckets.last().unwrap(), (1_048_576, 1));
        assert!(snapshot.mean_us().unwrap() > 400_000.0);
    }

    #[test]
    fn histogram_quantile_bounds_are_bucket_upper() {
        let histogram = LatencyHistogram::default();
        histogram.record_us(100);
        histogram.record_us(100);
        histogram.record_us(5_000);
        let snapshot = histogram.snapshot();
        // p50 lands in the [64, 128)µs bucket; reported bound is 128.
        assert_eq!(snapshot.bucket_quantile_us(0.5), Some(128));
        // p99.9 lands in the [4096, 8192)µs bucket.
        assert_eq!(snapshot.bucket_quantile_us(0.999), Some(8_192));
        assert_eq!(snapshot.bucket_quantile_us(0.0), Some(128));
    }
}
