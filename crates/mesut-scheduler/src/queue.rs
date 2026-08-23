use mesut_core::{TaskError, TaskResult, Work};

/// Backpressure policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressurePolicy {
    /// Accept all submissions until queue is full.
    Accept,
    /// Queue work up to configured limit.
    Queue,
    /// Delay submission if queue is full.
    Delay,
    /// Reject submission if queue is full.
    Reject,
    /// Shed lower-priority work to make room.
    Shed,
}

/// Bounded work queue with backpressure.
pub struct BoundedQueue {
    max_size: usize,
    policy: BackpressurePolicy,
}

impl BoundedQueue {
    pub fn new(max_size: usize, policy: BackpressurePolicy) -> Self {
        Self { max_size, policy }
    }

    pub fn check_backpressure(&self, current_depth: usize) -> TaskResult<()> {
        if current_depth >= self.max_size {
            match self.policy {
                BackpressurePolicy::Accept => Ok(()),
                BackpressurePolicy::Queue => Ok(()),
                BackpressurePolicy::Reject => Err(TaskError::QueueFull),
                BackpressurePolicy::Delay => Ok(()),
                BackpressurePolicy::Shed => Ok(()),
            }
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backpressure_reject() {
        let queue = BoundedQueue::new(10, BackpressurePolicy::Reject);
        assert!(queue.check_backpressure(9).is_ok());
        assert!(queue.check_backpressure(10).is_err());
    }

    #[test]
    fn backpressure_accept() {
        let queue = BoundedQueue::new(10, BackpressurePolicy::Accept);
        assert!(queue.check_backpressure(100).is_ok());
    }
}
