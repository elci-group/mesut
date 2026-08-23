pub use mesut_core::{
    CancellationToken, ExecutionMetadata, Priority, ResourceHint, RoutingHint, TaskError,
    TaskResult, WorkKind,
};
pub use mesut_executor::{ArcExecutor, Executor, ExecutorPool};
pub use mesut_observe::{EventObserver, NoOpObserver, TaskEvent, TraceCollector};
pub use mesut_router::WorkRouter;
pub use mesut_runtime::{MesuT, RuntimeConfig};

pub use mesut_blocking::BlockingExecutor;
pub use mesut_rayon::RayonExecutor;
pub use mesut_tokio::TokioExecutor;

pub mod prelude {
    pub use crate::{MesuT, RuntimeConfig, WorkKind, Priority};
    pub use mesut_core::{TaskId, Work};
    pub use mesut_tokio::TokioExecutor;
    pub use mesut_rayon::RayonExecutor;
    pub use mesut_blocking::BlockingExecutor;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prelude_imports() {
        let runtime = RuntimeConfig::new();
        assert!(runtime.enable_telemetry);
    }
}
