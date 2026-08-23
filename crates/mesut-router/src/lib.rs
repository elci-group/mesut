use mesut_core::{Route, RoutingHint, TaskError, TaskResult, Work, WorkKind};
use mesut_executor::ArcExecutor;

pub use classification::*;
pub use decision::*;
pub use adaptive::*;
pub use diagnostics::*;

mod classification;
mod decision;
mod adaptive;
mod diagnostics;

/// Router that determines execution substrate for work.
#[derive(Clone)]
pub struct WorkRouter {
    async_executor: Option<ArcExecutor>,
    compute_executor: Option<ArcExecutor>,
    blocking_executor: Option<ArcExecutor>,
    realtime_executor: Option<ArcExecutor>,
}

impl WorkRouter {
    pub fn new() -> Self {
        Self {
            async_executor: None,
            compute_executor: None,
            blocking_executor: None,
            realtime_executor: None,
        }
    }

    pub fn with_async_executor(mut self, executor: ArcExecutor) -> Self {
        self.async_executor = Some(executor);
        self
    }

    pub fn with_compute_executor(mut self, executor: ArcExecutor) -> Self {
        self.compute_executor = Some(executor);
        self
    }

    pub fn with_blocking_executor(mut self, executor: ArcExecutor) -> Self {
        self.blocking_executor = Some(executor);
        self
    }

    pub fn with_realtime_executor(mut self, executor: ArcExecutor) -> Self {
        self.realtime_executor = Some(executor);
        self
    }

    /// Determine the route for the given work.
    pub fn route(&self, work: &Work) -> TaskResult<Route> {
        match work.routing_hint {
            RoutingHint::RequireAsync => self.require_async(),
            RoutingHint::RequireCompute => self.require_compute(),
            RoutingHint::RequireBlocking => self.require_blocking(),
            RoutingHint::PreferAsync => self.prefer_async(work),
            RoutingHint::PreferCompute => self.prefer_compute(work),
            RoutingHint::PreferBlocking => self.prefer_blocking(work),
            RoutingHint::Default => self.route_by_kind(work),
        }
    }

    fn require_async(&self) -> TaskResult<Route> {
        self.async_executor
            .as_ref()
            .map(|e| Route::Async(e.id()))
            .ok_or(TaskError::NoExecutor)
    }

    fn require_compute(&self) -> TaskResult<Route> {
        self.compute_executor
            .as_ref()
            .map(|e| Route::Compute(e.id()))
            .ok_or(TaskError::NoExecutor)
    }

    fn require_blocking(&self) -> TaskResult<Route> {
        self.blocking_executor
            .as_ref()
            .map(|e| Route::Blocking(e.id()))
            .ok_or(TaskError::NoExecutor)
    }

    fn prefer_async(&self, work: &Work) -> TaskResult<Route> {
        if let Some(e) = self.async_executor.as_ref() {
            Ok(Route::Async(e.id()))
        } else {
            self.route_by_kind(work)
        }
    }

    fn prefer_compute(&self, work: &Work) -> TaskResult<Route> {
        if let Some(e) = self.compute_executor.as_ref() {
            Ok(Route::Compute(e.id()))
        } else {
            self.route_by_kind(work)
        }
    }

    fn prefer_blocking(&self, work: &Work) -> TaskResult<Route> {
        if let Some(e) = self.blocking_executor.as_ref() {
            Ok(Route::Blocking(e.id()))
        } else {
            self.route_by_kind(work)
        }
    }

    /// Route by work kind (default deterministic routing).
    fn route_by_kind(&self, work: &Work) -> TaskResult<Route> {
        match work.kind {
            WorkKind::Io => self.route_io(),
            WorkKind::Compute => self.route_compute(),
            WorkKind::Blocking => self.route_blocking(),
            WorkKind::Realtime => self.route_realtime(),
            WorkKind::Periodic => self.route_periodic(),
        }
    }

    fn route_io(&self) -> TaskResult<Route> {
        self.async_executor
            .as_ref()
            .map(|e| Route::Async(e.id()))
            .ok_or(TaskError::NoExecutor)
    }

    fn route_compute(&self) -> TaskResult<Route> {
        self.compute_executor
            .as_ref()
            .map(|e| Route::Compute(e.id()))
            .ok_or(TaskError::NoExecutor)
    }

    fn route_blocking(&self) -> TaskResult<Route> {
        self.blocking_executor
            .as_ref()
            .map(|e| Route::Blocking(e.id()))
            .ok_or(TaskError::NoExecutor)
    }

    fn route_realtime(&self) -> TaskResult<Route> {
        if let Some(e) = self.realtime_executor.as_ref() {
            Ok(Route::Realtime(e.id()))
        } else if let Some(e) = self.async_executor.as_ref() {
            Ok(Route::Async(e.id()))
        } else {
            Err(TaskError::NoExecutor)
        }
    }

    fn route_periodic(&self) -> TaskResult<Route> {
        self.async_executor
            .as_ref()
            .map(|e| Route::Async(e.id()))
            .ok_or(TaskError::NoExecutor)
    }

    /// Get the executor for a route.
    pub fn get_executor(&self, route: &Route) -> Option<ArcExecutor> {
        match route {
            Route::Async(_) => self.async_executor.clone(),
            Route::Compute(_) => self.compute_executor.clone(),
            Route::Blocking(_) => self.blocking_executor.clone(),
            Route::Realtime(_) => self.realtime_executor.clone(),
            Route::Unavailable => None,
        }
    }
}

impl Default for WorkRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_creation() {
        let router = WorkRouter::new();
        assert!(router.async_executor.is_none());
        assert!(router.compute_executor.is_none());
    }
}
