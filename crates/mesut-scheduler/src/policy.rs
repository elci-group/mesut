use crate::{SchedulingCandidate, SchedulingPolicy};
use mesut_core::TaskId;
use std::time::Duration;

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

/// Shortest-service-time-first within a priority tier, with aging.
///
/// Ranking uses the label's historical *execution* time — the job's own run,
/// never the queue-inclusive total, which would make the estimate grow under
/// load and order tasks by how backed-up the system already is. Candidates
/// whose queue wait exceeds the aging threshold are promoted to FIFO order
/// ahead of fresh work in their tier, bounding how long any admitted task can
/// wait: roughly the threshold plus the drain time of other aged tasks.
/// Unseen work gets a neutral bucket so it is neither favored nor starved.
#[derive(Clone, Copy)]
pub struct AdaptiveSchedulingPolicy {
    aging: Option<Duration>,
}

impl AdaptiveSchedulingPolicy {
    pub fn new(aging: Option<Duration>) -> Self {
        Self { aging }
    }

    /// Buckets a duration on a log2 scale so occasional outliers don't
    /// dominate ordering.
    fn bucket(duration: Duration) -> u32 {
        (duration.as_micros().max(1) as f64).log2().floor() as u32
    }
}

impl SchedulingPolicy for AdaptiveSchedulingPolicy {
    fn select(&self, candidates: &[SchedulingCandidate]) -> Option<TaskId> {
        const NEUTRAL_BUCKET: u32 = 20; // roughly one second
        candidates
            .iter()
            .max_by_key(|entry| {
                let aged = self
                    .aging
                    .is_some_and(|threshold| entry.waited >= threshold);
                if aged {
                    // Aged tier: FIFO among everything the threshold rescued.
                    (entry.priority, 1_u8, u64::MAX - entry.sequence, 0_u64)
                } else {
                    let bucket = entry
                        .expected_service_time
                        .map_or(NEUTRAL_BUCKET, Self::bucket);
                    (
                        entry.priority,
                        0_u8,
                        u64::MAX - (bucket as u64),
                        u64::MAX - entry.sequence,
                    )
                }
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
        expected_service_time: Option<Duration>,
    ) -> SchedulingCandidate {
        SchedulingCandidate {
            task_id: TaskId::new(),
            priority,
            sequence,
            executor_active: 0,
            expected_service_time,
            waited: Duration::ZERO,
        }
    }

    #[test]
    fn adaptive_prefers_shorter_history_at_same_priority() {
        let slow = candidate(Priority::Normal, 0, Some(Duration::from_secs(10)));
        let fast = candidate(Priority::Normal, 1, Some(Duration::from_millis(1)));
        let selected = AdaptiveSchedulingPolicy::new(None)
            .select(&[slow, fast])
            .unwrap();
        assert_eq!(selected, fast.task_id);
    }

    #[test]
    fn adaptive_never_lets_duration_override_priority() {
        let fast_low = candidate(Priority::Low, 0, Some(Duration::from_millis(1)));
        let slow_critical = candidate(Priority::Critical, 1, Some(Duration::from_secs(10)));
        let selected = AdaptiveSchedulingPolicy::new(None)
            .select(&[fast_low, slow_critical])
            .unwrap();
        assert_eq!(selected, slow_critical.task_id);
    }

    #[test]
    fn adaptive_falls_back_to_fifo_for_unseen_work() {
        let first = candidate(Priority::Normal, 0, None);
        let second = candidate(Priority::Normal, 1, None);
        let selected = AdaptiveSchedulingPolicy::new(None)
            .select(&[second, first])
            .unwrap();
        assert_eq!(selected, first.task_id);
    }

    #[test]
    fn aging_promotes_starved_work_to_fifo_before_fresh_short_work() {
        let policy = AdaptiveSchedulingPolicy::new(Some(Duration::from_millis(100)));
        let starved_long = candidate(Priority::Normal, 0, Some(Duration::from_secs(1)));
        let mut starved_long = starved_long;
        starved_long.waited = Duration::from_millis(150);
        let fresh_short_a = candidate(Priority::Normal, 1, Some(Duration::from_millis(1)));
        let fresh_short_b = candidate(Priority::Normal, 2, Some(Duration::from_millis(1)));
        // The aged long task runs first, then fresh work stays FIFO.
        assert_eq!(
            policy
                .select(&[fresh_short_b, starved_long, fresh_short_a])
                .unwrap(),
            starved_long.task_id
        );
        assert_eq!(
            policy.select(&[fresh_short_b, fresh_short_a]).unwrap(),
            fresh_short_a.task_id
        );
    }

    #[test]
    fn aging_never_promotes_above_priority() {
        let policy = AdaptiveSchedulingPolicy::new(Some(Duration::from_millis(10)));
        let aged_low = candidate(Priority::Low, 0, Some(Duration::from_secs(1)));
        let mut aged_low = aged_low;
        aged_low.waited = Duration::from_secs(60);
        let fresh_critical = candidate(Priority::Critical, 1, Some(Duration::from_secs(1)));
        assert_eq!(
            policy.select(&[fresh_critical, aged_low]).unwrap(),
            fresh_critical.task_id
        );
    }

    #[test]
    fn without_aging_long_work_can_starve_indefinitely() {
        // Documents the tradeoff the threshold exists to bound: with aging
        // disabled, a continuous stream of short work outranks the long task
        // forever.
        let policy = AdaptiveSchedulingPolicy::new(None);
        let long = candidate(Priority::Normal, 0, Some(Duration::from_secs(1)));
        let short = candidate(Priority::Normal, 1, Some(Duration::from_millis(1)));
        assert_eq!(policy.select(&[long, short]).unwrap(), short.task_id);
    }
}
