pub use mesut_core::TaskScope;
pub use mesut_core::{
    CancellationToken, ExecutionMetadata, LatencyPhases, Priority, ResourceHint, RoutingHint,
    TaskError, TaskResult, TaskTiming, WorkKind,
};
pub use mesut_executor::{ArcExecutor, Executor, ExecutorPool};
pub use mesut_observe::{AssistObserver, EventObserver, NoOpObserver, TaskEvent, TraceCollector};
pub use mesut_router::WorkRouter;
pub use mesut_runtime::BackpressurePolicy;
pub use mesut_runtime::{
    MesuT, Pipeline, PipelineBuilder, PipelineOutputs, PipelineStage, RuntimeConfig,
};
pub use mesut_scheduler::{
    ExecutorStats, RuntimeState, SchedulingMode, ShutdownState, WorkloadProfile,
};

pub use mesut_blocking::BlockingExecutor;
pub use mesut_rayon::RayonExecutor;
pub use mesut_tokio::TokioExecutor;

pub mod animations;

pub mod prelude {
    pub use crate::{MesuT, Pipeline, PipelineStage, Priority, RuntimeConfig, WorkKind};
    pub use mesut_blocking::BlockingExecutor;
    pub use mesut_core::{TaskId, Work};
    pub use mesut_rayon::RayonExecutor;
    pub use mesut_tokio::TokioExecutor;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prelude_imports() {
        let runtime = RuntimeConfig::new();
        assert!(runtime.enable_telemetry);
        assert!(runtime.enable_animations);
    }
}
