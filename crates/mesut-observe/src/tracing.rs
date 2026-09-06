use crate::TaskEvent;
use serde::{Deserialize, Serialize};

/// Represents a complete trace of a task's execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTrace {
    pub task_id: String,
    pub events: Vec<TaskEvent>,
}

impl TaskTrace {
    pub fn new(task_id: String) -> Self {
        Self {
            task_id,
            events: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: TaskEvent) {
        self.events.push(event);
    }

    pub fn duration_ms(&self) -> Option<u128> {
        if let (Some(first), Some(last)) = (self.events.first(), self.events.last()) {
            last.timestamp
                .duration_since(first.timestamp)
                .ok()
                .map(|d| d.as_millis())
        } else {
            None
        }
    }
}

/// Trace store for task execution histories.
pub struct TraceStore {
    traces: parking_lot::RwLock<std::collections::HashMap<String, TaskTrace>>,
}

impl TraceStore {
    pub fn new() -> Self {
        Self {
            traces: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub fn add_trace(&self, trace: TaskTrace) {
        self.traces.write().insert(trace.task_id.clone(), trace);
    }

    pub fn get_trace(&self, task_id: &str) -> Option<TaskTrace> {
        self.traces.read().get(task_id).cloned()
    }

    pub fn all_traces(&self) -> Vec<TaskTrace> {
        self.traces.read().values().cloned().collect()
    }

    pub fn clear(&self) {
        self.traces.write().clear();
    }
}

impl Default for TraceStore {
    fn default() -> Self {
        Self::new()
    }
}
