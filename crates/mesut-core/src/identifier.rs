use std::sync::atomic::{AtomicU64, Ordering};

/// Identifies an executor instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutorId(u64);

static EXECUTOR_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

impl ExecutorId {
    pub fn new() -> Self {
        let id = EXECUTOR_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        Self(id)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl Default for ExecutorId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ExecutorId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "executor-{}", self.0)
    }
}

/// Identifies a scheduling policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyId(u64);

static POLICY_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

impl PolicyId {
    pub fn new() -> Self {
        let id = POLICY_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        Self(id)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl Default for PolicyId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PolicyId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "policy-{}", self.0)
    }
}

/// Represents a route decision for a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// Route to async/I/O executor.
    Async(ExecutorId),
    /// Route to compute executor.
    Compute(ExecutorId),
    /// Route to blocking executor.
    Blocking(ExecutorId),
    /// Route to real-time executor.
    Realtime(ExecutorId),
    /// No executor available.
    Unavailable,
}

impl Route {
    pub fn executor_id(&self) -> Option<ExecutorId> {
        match self {
            Route::Async(id) | Route::Compute(id) | Route::Blocking(id) | Route::Realtime(id) => {
                Some(*id)
            }
            Route::Unavailable => None,
        }
    }
}

impl std::fmt::Display for Route {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Route::Async(id) => write!(f, "Async({})", id),
            Route::Compute(id) => write!(f, "Compute({})", id),
            Route::Blocking(id) => write!(f, "Blocking({})", id),
            Route::Realtime(id) => write!(f, "Realtime({})", id),
            Route::Unavailable => write!(f, "Unavailable"),
        }
    }
}
