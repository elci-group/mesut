use crate::{SchedulingCandidate, SchedulingPolicy};
use mesut_core::TaskId;

#[derive(Default)]
pub struct DefaultSchedulingPolicy;
impl SchedulingPolicy for DefaultSchedulingPolicy {
    fn select(&self, candidates: &[SchedulingCandidate]) -> Option<TaskId> {
        candidates
            .iter()
            .min_by_key(|entry| entry.sequence)
            .map(|entry| entry.task_id)
    }
}

#[derive(Default)]
pub struct PrioritySchedulingPolicy;
impl PrioritySchedulingPolicy {
    pub fn new() -> Self {
        Self
    }
}
impl SchedulingPolicy for PrioritySchedulingPolicy {
    fn select(&self, candidates: &[SchedulingCandidate]) -> Option<TaskId> {
        candidates
            .iter()
            .max_by_key(|entry| (entry.priority, std::cmp::Reverse(entry.sequence)))
            .map(|entry| entry.task_id)
    }
}

#[derive(Default)]
pub struct AdaptiveSchedulingPolicy;
impl AdaptiveSchedulingPolicy {
    pub fn new() -> Self {
        Self
    }

    /// Buckets a duration on a log2 scale so occasional outliers don't
    /// dominate ordering; unseen work gets a neutral bucket so it is neither
    /// favored nor starved relative to work with known history.
    fn bucket(duration: Option<std::time::Duration>) -> u32 {
        const NEUTRAL_BUCKET: u32 = 20; // roughly one second
        match duration {
            Some(duration) => (duration.as_micros().max(1) as f64).log2().floor() as u32,
            None => NEUTRAL_BUCKET,
        }
    }
}
impl SchedulingPolicy for AdaptiveSchedulingPolicy {
    fn select(&self, candidates: &[SchedulingCandidate]) -> Option<TaskId> {
        candidates
            .iter()
            .max_by_key(|entry| {
                (
                    entry.priority,
                    std::cmp::Reverse(Self::bucket(entry.expected_duration)),
                    std::cmp::Reverse(entry.sequence),
                )
            })
            .map(|entry| entry.task_id)
    }
}

#[derive(Default)]
pub struct LoadAwareSchedulingPolicy;
impl LoadAwareSchedulingPolicy {
    pub fn new() -> Self {
        Self
    }
}
impl SchedulingPolicy for LoadAwareSchedulingPolicy {
    fn select(&self, candidates: &[SchedulingCandidate]) -> Option<TaskId> {
        candidates
            .iter()
            .min_by_key(|entry| {
                (
                    entry.executor_active,
                    std::cmp::Reverse(entry.priority),
                    entry.sequence,
                )
            })
            .map(|entry| entry.task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesut_core::Priority;
    use std::time::Duration;

    fn candidate(
        priority: Priority,
        sequence: u64,
        expected_duration: Option<Duration>,
    ) -> SchedulingCandidate {
        SchedulingCandidate {
            task_id: TaskId::new(),
            priority,
            sequence,
            executor_active: 0,
            expected_duration,
        }
    }

    #[test]
    fn adaptive_prefers_shorter_history_at_same_priority() {
        let slow = candidate(Priority::Normal, 0, Some(Duration::from_secs(10)));
        let fast = candidate(Priority::Normal, 1, Some(Duration::from_millis(1)));
        let selected = AdaptiveSchedulingPolicy.select(&[slow, fast]).unwrap();
        assert_eq!(selected, fast.task_id);
    }

    #[test]
    fn adaptive_never_lets_duration_override_priority() {
        let fast_low = candidate(Priority::Low, 0, Some(Duration::from_millis(1)));
        let slow_critical = candidate(Priority::Critical, 1, Some(Duration::from_secs(10)));
        let selected = AdaptiveSchedulingPolicy
            .select(&[fast_low, slow_critical])
            .unwrap();
        assert_eq!(selected, slow_critical.task_id);
    }

    #[test]
    fn adaptive_falls_back_to_fifo_for_unseen_work() {
        let first = candidate(Priority::Normal, 0, None);
        let second = candidate(Priority::Normal, 1, None);
        let selected = AdaptiveSchedulingPolicy.select(&[second, first]).unwrap();
        assert_eq!(selected, first.task_id);
    }
}
