use crate::TaskId;
use parking_lot::RwLock;
use std::sync::Arc;

/// Handle for cancellation of a task.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    inner: Arc<RwLock<CancellationState>>,
}

#[derive(Debug)]
struct CancellationState {
    cancelled: bool,
    parent: Option<CancellationToken>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(CancellationState {
                cancelled: false,
                parent: None,
            })),
        }
    }

    pub fn with_parent(parent: CancellationToken) -> Self {
        Self {
            inner: Arc::new(RwLock::new(CancellationState {
                cancelled: false,
                parent: Some(parent),
            })),
        }
    }

    /// Check if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        let state = self.inner.read();
        if state.cancelled {
            return true;
        }
        if let Some(ref parent) = state.parent {
            return parent.is_cancelled();
        }
        false
    }

    /// Request cancellation of this token.
    pub fn cancel(&self) {
        self.inner.write().cancelled = true;
    }

    /// Create a child token that cancels when parent cancels.
    pub fn child(&self) -> Self {
        Self::with_parent(self.clone())
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a task's scope and lifecycle.
pub struct TaskScope {
    task_id: TaskId,
    token: CancellationToken,
    children: Arc<RwLock<Vec<TaskId>>>,
}

impl TaskScope {
    pub fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            token: CancellationToken::new(),
            children: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn with_parent(task_id: TaskId, parent_token: CancellationToken) -> Self {
        Self {
            task_id,
            token: CancellationToken::with_parent(parent_token),
            children: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Register a child task.
    pub fn add_child(&self, child_id: TaskId) {
        self.children.write().push(child_id);
    }

    /// Get all registered children.
    pub fn children(&self) -> Vec<TaskId> {
        self.children.read().clone()
    }

    /// Check if this scope has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Request cancellation of this scope and all children.
    pub fn cancel(&self) {
        self.token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_token_basic() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancellation_propagates_to_child() {
        let parent = CancellationToken::new();
        let child = parent.child();
        assert!(!child.is_cancelled());
        parent.cancel();
        assert!(child.is_cancelled());
    }

    #[test]
    fn child_cancellation_doesnt_affect_parent() {
        let parent = CancellationToken::new();
        let child = parent.child();
        child.cancel();
        assert!(child.is_cancelled());
        assert!(!parent.is_cancelled());
    }

    #[test]
    fn task_scope_tracks_children() {
        let task_id = TaskId::new();
        let scope = TaskScope::new(task_id);
        let child1 = TaskId::new();
        let child2 = TaskId::new();

        scope.add_child(child1);
        scope.add_child(child2);

        let children = scope.children();
        assert_eq!(children.len(), 2);
        assert!(children.contains(&child1));
        assert!(children.contains(&child2));
    }
}
