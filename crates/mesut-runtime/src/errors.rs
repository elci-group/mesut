use mesut_core::{TaskError, TaskId};
use std::fmt;

/// Enhanced error information with context.
#[derive(Debug)]
pub struct ErrorContext {
    pub task_id: TaskId,
    pub error: TaskError,
    pub context: String,
    pub recovery_hint: Option<String>,
}

impl ErrorContext {
    pub fn new(task_id: TaskId, error: TaskError, context: String) -> Self {
        Self {
            task_id,
            error,
            context,
            recovery_hint: None,
        }
    }

    pub fn with_recovery_hint(mut self, hint: String) -> Self {
        self.recovery_hint = Some(hint);
        self
    }
}

impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Task {}: {} | Context: {}",
            self.task_id, self.error, self.context
        )?;
        if let Some(hint) = &self.recovery_hint {
            write!(f, " | Recovery: {}", hint)?;
        }
        Ok(())
    }
}

/// Result type with error context.
pub type ContextResult<T> = Result<T, ErrorContext>;

/// Helper to create error contexts.
pub mod context {
    use super::*;

    pub fn no_executor(task_id: TaskId) -> ErrorContext {
        ErrorContext::new(
            task_id,
            TaskError::NoExecutor,
            "No suitable executor available".to_string(),
        )
        .with_recovery_hint("Ensure at least one executor is configured".to_string())
    }

    pub fn queue_full(task_id: TaskId, executor_name: &str) -> ErrorContext {
        ErrorContext::new(
            task_id,
            TaskError::QueueFull,
            format!("{} queue is at capacity", executor_name),
        )
        .with_recovery_hint("Wait for pending tasks to complete or increase queue size".to_string())
    }

    pub fn execution_failed(task_id: TaskId, reason: String) -> ErrorContext {
        ErrorContext::new(
            task_id,
            TaskError::ExecutionFailed(reason.clone()),
            format!("Execution failed: {}", reason),
        )
    }

    pub fn timeout(task_id: TaskId, duration: std::time::Duration) -> ErrorContext {
        ErrorContext::new(
            task_id,
            TaskError::Timeout(duration),
            format!("Task did not complete within {:?}", duration),
        )
        .with_recovery_hint("Increase timeout or optimize task performance".to_string())
    }

    pub fn invalid_config(task_id: TaskId, reason: String) -> ErrorContext {
        ErrorContext::new(
            task_id,
            TaskError::InvalidConfiguration(reason),
            "Invalid task configuration".to_string(),
        )
    }

    pub fn executor_unavailable(task_id: TaskId) -> ErrorContext {
        ErrorContext::new(
            task_id,
            TaskError::ExecutorUnavailable,
            "Executor is currently unavailable".to_string(),
        )
        .with_recovery_hint("Retry the task or use a different executor".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_context_display() {
        let task_id = TaskId::new();
        let err = ErrorContext::new(
            task_id,
            TaskError::NoExecutor,
            "Test context".to_string(),
        );
        let display = format!("{}", err);
        assert!(display.contains("no suitable executor"));
    }

    #[test]
    fn error_context_with_recovery() {
        let task_id = TaskId::new();
        let err = ErrorContext::new(
            task_id,
            TaskError::QueueFull,
            "Queue at capacity".to_string(),
        )
        .with_recovery_hint("Increase queue size".to_string());

        assert!(err.recovery_hint.is_some());
    }
}
