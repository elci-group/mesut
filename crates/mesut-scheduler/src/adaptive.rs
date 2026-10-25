use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Rolling execution-duration statistics for a named, recurring workload.
///
/// Built only from measured execution time — the job's own run, without queue
/// or dispatch wait — so the learned estimate is a service-time estimate. No
/// CPU/I/O attribution beyond the measured execution window is attempted here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadProfile {
    pub samples: usize,
    pub mean_service_time: Duration,
    pub max_service_time: Duration,
    /// When the most recent sample for this label was recorded. Exposes
    /// profile staleness so operators can tell whether Adaptive is ordering
    /// by a current or an obsolete workload shape.
    pub last_updated: Instant,
}

/// One label's profile in a point-in-time [`WorkloadObserver::profiles`]
/// snapshot.
#[derive(Debug, Clone)]
pub struct LabeledProfile {
    pub label: String,
    pub profile: WorkloadProfile,
}

/// Observes completed work execution durations, keyed by
/// [`crate::SchedulingCandidate`] label (see [`mesut_core::Work::with_label`]),
/// over a bounded recent window.
///
/// Label cardinality is capped: once `max_labels` distinct labels are
/// tracked, further new labels are ignored (their tasks schedule from the
/// neutral bucket, i.e. FIFO within priority) and counted in
/// [`WorkloadObserver::overflow`], so unbounded metric cardinality cannot
/// grow memory or slow the dispatch scan.
pub struct WorkloadObserver {
    window: usize,
    max_labels: usize,
    history: RwLock<HashMap<String, VecDeque<Duration>>>,
    last_updated: RwLock<HashMap<String, Instant>>,
    overflow: AtomicUsize,
}

impl WorkloadObserver {
    pub fn new(window: usize) -> Self {
        Self::with_limits(window, 1024)
    }

    pub fn with_limits(window: usize, max_labels: usize) -> Self {
        Self {
            window: window.max(1),
            max_labels: max_labels.max(1),
            history: RwLock::new(HashMap::new()),
            last_updated: RwLock::new(HashMap::new()),
            overflow: AtomicUsize::new(0),
        }
    }

    /// Record one task's measured execution duration under `label`.
    ///
    /// Callers must pass the execution phase only (job start to job return);
    /// recording a queue-inclusive total would poison the service-time
    /// estimate under load, exactly when scheduling decisions matter.
    pub fn record(&self, label: &str, duration: Duration) {
        {
            let mut history = self.history.write();
            let samples = match history.get_mut(label) {
                Some(samples) => samples,
                None => {
                    if history.len() >= self.max_labels {
                        drop(history);
                        self.overflow.fetch_add(1, Ordering::AcqRel);
                        return;
                    }
                    history.entry(label.to_string()).or_default()
                }
            };
            samples.push_back(duration);
            while samples.len() > self.window {
                samples.pop_front();
            }
        }
        self.last_updated
            .write()
            .insert(label.to_string(), Instant::now());
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
            mean_service_time: total / samples.len() as u32,
            max_service_time: samples.iter().copied().max().unwrap_or_default(),
            last_updated: self
                .last_updated
                .read()
                .get(label)
                .copied()
                .unwrap_or_else(Instant::now),
        })
    }

    /// All tracked labels' profiles, sorted by label. Point-in-time snapshot
    /// for operators and evidence runs.
    pub fn profiles(&self) -> Vec<LabeledProfile> {
        let history = self.history.read();
        let stamps = self.last_updated.read();
        let mut out: Vec<LabeledProfile> = history
            .iter()
            .filter_map(|(label, samples)| {
                if samples.is_empty() {
                    return None;
                }
                let total: Duration = samples.iter().sum();
                Some(LabeledProfile {
                    label: label.clone(),
                    profile: WorkloadProfile {
                        samples: samples.len(),
                        mean_service_time: total / samples.len() as u32,
                        max_service_time: samples.iter().copied().max().unwrap_or_default(),
                        last_updated: stamps.get(label).copied().unwrap_or_else(Instant::now),
                    },
                })
            })
            .collect();
        out.sort_by(|a, b| a.label.cmp(&b.label));
        out
    }

    /// Number of distinct labels currently tracked (bounded by the cap).
    pub fn label_count(&self) -> usize {
        self.history.read().len()
    }

    /// Times a new label was dropped because the cardinality cap was full.
    pub fn overflow(&self) -> usize {
        self.overflow.load(Ordering::Acquire)
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
        assert_eq!(profile.mean_service_time, Duration::from_millis(30));
        assert_eq!(profile.max_service_time, Duration::from_millis(40));
    }

    #[test]
    fn labels_are_independent() {
        let observer = WorkloadObserver::default();
        observer.record("fast", Duration::from_millis(1));
        observer.record("slow", Duration::from_secs(1));
        assert_eq!(
            observer.profile("fast").unwrap().mean_service_time,
            Duration::from_millis(1)
        );
        assert_eq!(
            observer.profile("slow").unwrap().mean_service_time,
            Duration::from_secs(1)
        );
    }

    #[test]
    fn cardinality_cap_drops_new_labels_and_counts_overflow() {
        let observer = WorkloadObserver::with_limits(4, 2);
        observer.record("a", Duration::from_millis(1));
        observer.record("b", Duration::from_millis(1));
        observer.record("c", Duration::from_millis(1));
        assert_eq!(observer.label_count(), 2);
        assert_eq!(observer.overflow(), 1);
        // Capped labels keep learning: dropping "c" does not evict "a"/"b".
        observer.record("a", Duration::from_millis(2));
        assert_eq!(
            observer.profile("a").unwrap().mean_service_time,
            Duration::from_micros(1_500)
        );
        assert!(observer.profile("c").is_none());
    }

    #[test]
    fn profiles_snapshot_lists_every_tracked_label() {
        let observer = WorkloadObserver::with_limits(4, 8);
        observer.record("beta", Duration::from_millis(2));
        observer.record("alpha", Duration::from_millis(1));
        let names: Vec<String> = observer.profiles().into_iter().map(|p| p.label).collect();
        assert_eq!(names, vec!["alpha".to_string(), "beta".to_string()]);
    }
}
