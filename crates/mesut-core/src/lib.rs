use std::fmt;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub use identifier::*;
pub use lifecycle::*;
pub use timing::{thread_cpu_ns, LatencyPhases, TaskTiming};
pub use work::{ResourceHint, RoutingHint, Work};

mod identifier;
mod lifecycle;
mod timing;
mod work;

/// Unique stable task identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TaskId(Uuid);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_str(&self) -> String {
        self.0.to_string()
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Classification of executable work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum WorkKind {
    /// I/O-oriented work (network, filesystem, timers).
    Io,
    /// CPU-bound parallel work.
    Compute,
    /// Blocking operations that may stall threads.
    Blocking,
    /// Real-time or latency-sensitive work.
    Realtime,
    /// Periodic/scheduled work.
    Periodic,
}

impl fmt::Display for WorkKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WorkKind::Io => write!(f, "Io"),
            WorkKind::Compute => write!(f, "Compute"),
            WorkKind::Blocking => write!(f, "Blocking"),
            WorkKind::Realtime => write!(f, "Realtime"),
            WorkKind::Periodic => write!(f, "Periodic"),
        }
    }
}

/// Priority level for scheduling.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord,
)]
pub enum Priority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Priority::Low => write!(f, "Low"),
            Priority::Normal => write!(f, "Normal"),
            Priority::High => write!(f, "High"),
            Priority::Critical => write!(f, "Critical"),
        }
    }
}

/// Task execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    /// Task created but not yet submitted.
    Created,
    /// Task queued for execution.
    Queued,
    /// Task currently executing.
    Running,
    /// Task completed successfully.
    Completed,
    /// Task failed.
    Failed,
    /// Task was cancelled.
    Cancelled,
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TaskState::Created => write!(f, "Created"),
            TaskState::Queued => write!(f, "Queued"),
            TaskState::Running => write!(f, "Running"),
            TaskState::Completed => write!(f, "Completed"),
            TaskState::Failed => write!(f, "Failed"),
            TaskState::Cancelled => write!(f, "Cancelled"),
        }
    }
}

/// Errors that can occur during task execution.
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum TaskError {
    #[error("task was cancelled")]
    Cancelled,

    #[error("no suitable executor found for task")]
    NoExecutor,

    #[error("executor queue is full")]
    QueueFull,

    #[error("task execution failed: {0}")]
    ExecutionFailed(String),

    #[error("task timed out after {0:?}")]
    Timeout(Duration),

    #[error("invalid task configuration: {0}")]
    InvalidConfiguration(String),

    #[error("executor is unavailable")]
    ExecutorUnavailable,

    #[error("runtime is shutting down")]
    Shutdown,

    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for Mesut operations.
pub type TaskResult<T> = Result<T, TaskError>;

/// Metadata about task execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetadata {
    pub submitted_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub completed_at: Option<SystemTime>,
    pub queue_wait: Option<Duration>,
    pub execution_duration: Option<Duration>,
}

impl ExecutionMetadata {
    pub fn new() -> Self {
        Self {
            submitted_at: SystemTime::now(),
            started_at: None,
            completed_at: None,
            queue_wait: None,
            execution_duration: None,
        }
    }

    pub fn mark_started(&mut self) {
        let now = SystemTime::now();
        self.started_at = Some(now);
        if let Ok(wait) = now.duration_since(self.submitted_at) {
            self.queue_wait = Some(wait);
        }
    }

    pub fn mark_completed(&mut self) {
        let now = SystemTime::now();
        self.completed_at = Some(now);
        if let Some(started) = self.started_at {
            if let Ok(duration) = now.duration_since(started) {
                self.execution_duration = Some(duration);
            }
        }
    }
}

impl Default for ExecutionMetadata {
    fn default() -> Self {
        Self::new()
    }
}
