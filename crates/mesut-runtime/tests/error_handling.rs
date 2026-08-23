// Integration tests for error handling and observability

use mesut_core::TaskId;
use mesut_runtime::MesuT;

#[test]
fn test_submission_without_executors() {
    let runtime = MesuT::default();
    assert_eq!(runtime.active_tasks(), 0);
}

#[test]
fn test_task_state_tracking() {
    let runtime = MesuT::default();
    let task_id = TaskId::new();

    // Initially no state
    assert!(runtime.task_state(task_id).is_none());

    // Active tasks should be 0
    assert_eq!(runtime.active_tasks(), 0);
}

#[test]
fn test_runtime_config() {
    use mesut_runtime::RuntimeConfig;

    let config = RuntimeConfig::default();
    assert!(config.enable_telemetry);
    assert!(config.max_queue_size.is_some());
}


#[test]
fn test_pipeline_validation() {
    use mesut_runtime::Pipeline;

    let pipeline = Pipeline::new();
    assert!(pipeline.validate().is_ok());
}

#[test]
fn test_native_component_registry() {
    use mesut_runtime::{NativeComponentRegistry, ComponentStatus};

    let mut registry = NativeComponentRegistry::new();
    assert_eq!(registry.production_ready_components().len(), 0);

    registry.register("test-component".to_string(), ComponentStatus::Stable);
    assert_eq!(registry.production_ready_components().len(), 1);
}
