use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Latency breakdown for one task, in microseconds.
///
/// `queue_us` runs from submission until the coordination layer selects the
/// task for dispatch (this includes waiting for admission under backpressure).
/// `dispatch_us` runs from that decision until execution begins, covering the
/// handoff to the executor and any executor-internal queueing. `execution_us`
/// is the job's own run time. `completion_us` runs from job return until the
/// runtime's completion bookkeeping and callbacks have finished.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyPhases {
    pub queue_us: u64,
    pub dispatch_us: u64,
    pub execution_us: u64,
    pub completion_us: u64,
}

impl LatencyPhases {
    /// Sum of all phases; the submit-to-completion total.
    pub fn total(&self) -> Duration {
        Duration::from_micros(
            self.queue_us + self.dispatch_us + self.execution_us + self.completion_us,
        )
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Stamps {
    submitted: Option<Instant>,
    dispatched: Option<Instant>,
    started: Option<Instant>,
    job_end: Option<Instant>,
    completed: Option<Instant>,
    /// Thread CPU nanoseconds consumed by the job itself. Measured only for
    /// synchronous jobs, which run to completion on one worker thread; async
    /// futures cannot attribute thread CPU without crediting unrelated tasks
    /// on the same worker.
    job_cpu_ns: Option<u64>,
}

/// Per-task phase timestamps shared between a [`crate::Work`] unit and the
/// runtime coordination layer.
///
/// The coordination layer creates the timing (or adopts one attached with
/// `Work::with_timing`), marks the admission and dispatch stamps, and the
/// executor-side work unit marks start, job end, and completion. Duration
/// history for adaptive scheduling is learned from `execution_us` only —
/// never from the queue-inclusive total.
#[derive(Debug, Default)]
pub struct TaskTiming {
    stamps: parking_lot::Mutex<Stamps>,
}

impl TaskTiming {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wraps the timing in a shared handle, the form the runtime passes around.
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn mark_submitted(&self) {
        self.stamps.lock().submitted = Some(Instant::now());
    }

    pub fn mark_dispatched(&self) {
        self.stamps.lock().dispatched = Some(Instant::now());
    }

    pub fn mark_started(&self) {
        self.stamps.lock().started = Some(Instant::now());
    }

    /// Records job return. `job_cpu_ns` is the thread-CPU delta measured
    /// around a synchronous job; `None` where not measurable.
    pub fn mark_job_end(&self, job_cpu_ns: Option<u64>) {
        let mut stamps = self.stamps.lock();
        stamps.job_end = Some(Instant::now());
        stamps.job_cpu_ns = job_cpu_ns;
    }

    pub fn mark_completed(&self) {
        self.stamps.lock().completed = Some(Instant::now());
    }

    /// Time the task spent queued before its dispatch decision was made.
    pub fn queue_wait(&self) -> Option<Duration> {
        let stamps = *self.stamps.lock();
        Some(stamps.dispatched?.duration_since(stamps.submitted?))
    }

    /// Time from dispatch decision until execution began.
    pub fn dispatch_wait(&self) -> Option<Duration> {
        let stamps = *self.stamps.lock();
        Some(stamps.started?.duration_since(stamps.dispatched?))
    }

    /// The job's own wall-clock run time, without queue or dispatch wait.
    pub fn execution_time(&self) -> Option<Duration> {
        let stamps = *self.stamps.lock();
        Some(stamps.job_end?.duration_since(stamps.started?))
    }

    /// Thread CPU nanoseconds consumed by the job, for synchronous jobs.
    pub fn job_cpu_ns(&self) -> Option<u64> {
        self.stamps.lock().job_cpu_ns
    }

    /// The full phase breakdown once the task has completed, or `None` while
    /// the task has not both started and finished executing.
    pub fn phases(&self) -> Option<LatencyPhases> {
        let stamps = *self.stamps.lock();
        let queue_us = stamps
            .dispatched?
            .duration_since(stamps.submitted?)
            .as_micros() as u64;
        let dispatch_us = stamps
            .started?
            .duration_since(stamps.dispatched?)
            .as_micros() as u64;
        let execution_us = stamps
            .job_end?
            .duration_since(stamps.started?)
            .as_micros() as u64;
        let completion_us = stamps
            .completed?
            .duration_since(stamps.job_end?)
            .as_micros() as u64;
        Some(LatencyPhases {
            queue_us,
            dispatch_us,
            execution_us,
            completion_us,
        })
    }
}

/// Thread CPU time used by the calling thread, where the platform exposes it.
///
/// Linux `CLOCK_THREAD_CPUTIME_ID`; other platforms report `None` rather than
/// a misleading wall-clock reading.
pub fn thread_cpu_ns() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
        (rc == 0).then(|| ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn phases_are_none_until_completed() {
        let timing = TaskTiming::new();
        assert!(timing.phases().is_none());
        timing.mark_submitted();
        assert!(timing.phases().is_none());
        timing.mark_dispatched();
        timing.mark_started();
        timing.mark_job_end(None);
        assert!(timing.phases().is_none());
        timing.mark_completed();
        let phases = timing.phases().unwrap();
        assert_eq!(phases.execution_us, 0);
        assert!(phases.queue_us < 50_000);
    }

    #[test]
    fn execution_time_measures_the_job_not_the_queue() {
        let timing = TaskTiming::new();
        timing.mark_submitted();
        thread::sleep(Duration::from_millis(10));
        timing.mark_dispatched();
        timing.mark_started();
        thread::sleep(Duration::from_millis(20));
        timing.mark_job_end(None);
        timing.mark_completed();
        // Queue wait (10ms) must not leak into the execution phase.
        assert!(timing.execution_time().unwrap() >= Duration::from_millis(19));
        assert!(timing.queue_wait().unwrap() >= Duration::from_millis(9));
        let phases = timing.phases().unwrap();
        assert!(phases.execution_us >= 19_000 && phases.execution_us < 60_000);
        assert!(phases.queue_us >= 9_000 && phases.queue_us < 50_000);
    }

    #[test]
    fn thread_cpu_is_reported_on_linux_and_grows_with_work() {
        let Some(start) = thread_cpu_ns() else {
            return;
        };
        let mut n = 1_u64;
        for _ in 0..200_000 {
            n = n.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
        std::hint::black_box(n);
        let end = thread_cpu_ns().unwrap();
        assert!(end >= start);
    }
}
