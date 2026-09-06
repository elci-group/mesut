// Phase 5: Native Components
// This module provides the foundation for replacing external execution components
// with native Mesut implementations in future releases.

/// Marker trait for native components.
pub trait NativeComponent: Send + Sync {
    /// Get the component name.
    fn name(&self) -> &str;

    /// Check if this component is production-ready.
    fn is_production_ready(&self) -> bool;

    /// Get implementation status.
    fn status(&self) -> ComponentStatus;
}

/// Status of a native component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentStatus {
    /// Not yet implemented.
    Planned,
    /// Early implementation, not recommended for production.
    Experimental,
    /// Usable but may have performance limitations.
    Beta,
    /// Production-ready and recommended.
    Stable,
    /// Deprecated in favor of newer version.
    Deprecated,
}

/// Configuration for native component adoption.
pub struct NativeComponentConfig {
    /// Whether to use native worker scheduler.
    pub native_worker_scheduler: bool,
    /// Whether to use native work-stealing.
    pub native_work_stealing: bool,
    /// Whether to use native reactor.
    pub native_reactor: bool,
    /// Whether to use integrated timer system.
    pub integrated_timers: bool,
    /// Whether to use native async executor.
    pub native_async_executor: bool,
}

impl NativeComponentConfig {
    pub fn new() -> Self {
        Self {
            native_worker_scheduler: false,
            native_work_stealing: false,
            native_reactor: false,
            integrated_timers: false,
            native_async_executor: false,
        }
    }

    pub fn all_enabled() -> Self {
        Self {
            native_worker_scheduler: true,
            native_work_stealing: true,
            native_reactor: true,
            integrated_timers: true,
            native_async_executor: true,
        }
    }

    pub fn enabled_components(&self) -> Vec<&'static str> {
        let mut components = Vec::new();
        if self.native_worker_scheduler {
            components.push("worker-scheduler");
        }
        if self.native_work_stealing {
            components.push("work-stealing");
        }
        if self.native_reactor {
            components.push("reactor");
        }
        if self.integrated_timers {
            components.push("timers");
        }
        if self.native_async_executor {
            components.push("async-executor");
        }
        components
    }

    pub fn count(&self) -> usize {
        self.enabled_components().len()
    }
}

impl Default for NativeComponentConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Tracks native component adoption.
pub struct NativeComponentRegistry {
    components: std::collections::HashMap<String, ComponentStatus>,
}

impl NativeComponentRegistry {
    pub fn new() -> Self {
        Self {
            components: std::collections::HashMap::new(),
        }
    }

    pub fn register(&mut self, name: String, status: ComponentStatus) {
        self.components.insert(name, status);
    }

    pub fn status(&self, name: &str) -> Option<ComponentStatus> {
        self.components.get(name).copied()
    }

    pub fn production_ready_components(&self) -> Vec<&String> {
        self.components
            .iter()
            .filter(|(_, status)| **status == ComponentStatus::Stable)
            .map(|(name, _)| name)
            .collect()
    }

    pub fn experimental_components(&self) -> Vec<&String> {
        self.components
            .iter()
            .filter(|(_, status)| {
                matches!(
                    **status,
                    ComponentStatus::Experimental | ComponentStatus::Beta
                )
            })
            .map(|(name, _)| name)
            .collect()
    }
}

impl Default for NativeComponentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_component_config_default() {
        let config = NativeComponentConfig::new();
        assert_eq!(config.count(), 0);
    }

    #[test]
    fn native_component_config_all_enabled() {
        let config = NativeComponentConfig::all_enabled();
        assert_eq!(config.count(), 5);
    }

    #[test]
    fn registry_creation() {
        let registry = NativeComponentRegistry::new();
        assert!(registry.status("worker-scheduler").is_none());
    }
}
