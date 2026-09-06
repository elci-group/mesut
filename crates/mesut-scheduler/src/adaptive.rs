use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

/// Rolling execution-duration statistics for a named, recurring workload.
///
/// Built only from wall-clock durations the runtime already measures; there
/// is no attempt to attribute CPU vs. I/O time within a task, since nothing
/// in the runtime instruments that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadProfile {
    pub samples: usize,
    pub mean_duration: Duration,
    pub max_duration: Duration,
}

/// Observes completed work durations, keyed by [`crate::SchedulingCandidate`]
/// label (see [`mesut_core::Work::with_label`]), over a bounded recent window.
pub struct WorkloadObserver {
    window: usize,
    history: RwLock<HashMap<String, VecDeque<Duration>>>,
}

impl WorkloadObserver {
    pub fn new(window: usize) -> Self {
        Self {
            window: window.max(1),
            history: RwLock::new(HashMap::new()),
        }
    }

    /// Record one completed task's duration under `label`.
    pub fn record(&self, label: &str, duration: Duration) {
        let mut history = self.history.write();
        let samples = history.entry(label.to_string()).or_default();
        samples.push_back(duration);
        while samples.len() > self.window {
            samples.pop_front();
        }
    }

    /// The current profile for `label`, or `None` if nothing has been
    /// recorded for it yet.
    pub fn profile(&self, label: &str) -> Option<WorkloadProfile> {
        let history = self.history.read();
        let samples = history.get(label)?;
        if samples.is_empty() {
            return None;
        }
        let total: Duration = samples.iter().sum();
        Some(WorkloadProfile {
            samples: samples.len(),
            mean_duration: total / samples.len() as u32,
            max_duration: samples.iter().copied().max().unwrap_or_default(),
        })
    }
}

impl Default for WorkloadObserver {
    fn default() -> Self {
        Self::new(16)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrecorded_label_has_no_profile() {
        let observer = WorkloadObserver::default();
        assert!(observer.profile("unseen").is_none());
    }

    #[test]
    fn profile_reflects_recent_samples() {
        let observer = WorkloadObserver::new(3);
        for millis in [10, 20, 30, 40] {
            observer.record("resize", Duration::from_millis(millis));
        }
        let profile = observer.profile("resize").unwrap();
        // The oldest sample (10ms) has rolled out of the window.
        assert_eq!(profile.samples, 3);
        assert_eq!(profile.mean_duration, Duration::from_millis(30));
        assert_eq!(profile.max_duration, Duration::from_millis(40));
    }

    #[test]
    fn labels_are_independent() {
        let observer = WorkloadObserver::default();
        observer.record("fast", Duration::from_millis(1));
        observer.record("slow", Duration::from_secs(1));
        assert_eq!(
            observer.profile("fast").unwrap().mean_duration,
            Duration::from_millis(1)
        );
        assert_eq!(
            observer.profile("slow").unwrap().mean_duration,
            Duration::from_secs(1)
        );
    }
}
