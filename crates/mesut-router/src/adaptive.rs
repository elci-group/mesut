use mesut_core::{Work, WorkKind};
use std::time::Duration;

/// Represents observed workload characteristics.
#[derive(Debug, Clone)]
pub struct WorkloadProfile {
    pub cpu_percentage: f64,
    pub io_percentage: f64,
    pub memory_mb: usize,
    pub parallelism_score: f64,
    pub observed_duration: Duration,
}

impl WorkloadProfile {
    pub fn new() -> Self {
        Self {
            cpu_percentage: 0.0,
            io_percentage: 0.0,
            memory_mb: 0,
            parallelism_score: 0.0,
            observed_duration: Duration::from_millis(0),
        }
    }

    pub fn infer_kind(&self) -> WorkKind {
        match (self.cpu_percentage, self.io_percentage) {
            (cpu, _io) if cpu > 70.0 => WorkKind::Compute,
            (_cpu, io) if io > 70.0 => WorkKind::Io,
            (cpu, _io) if cpu > 50.0 && self.parallelism_score > 2.0 => WorkKind::Compute,
            _ => WorkKind::Io,
        }
    }

    pub fn confidence(&self) -> f64 {
        let mut confidence: f64 = 0.5;
        if self.cpu_percentage > 70.0 || self.io_percentage > 70.0 {
            confidence += 0.3;
        }
        if self.observed_duration.as_millis() > 100 {
            confidence += 0.15;
        }
        confidence.min(1.0)
    }
}

impl Default for WorkloadProfile {
    fn default() -> Self {
        Self::new()
    }
}

/// Adaptive routing decision based on observation.
pub struct AdaptiveRoutingDecision {
    pub primary_kind: WorkKind,
    pub fallback_kind: Option<WorkKind>,
    pub confidence: f64,
    pub rationale: String,
}

impl AdaptiveRoutingDecision {
    pub fn new(kind: WorkKind, confidence: f64, rationale: String) -> Self {
        Self {
            primary_kind: kind,
            fallback_kind: None,
            confidence,
            rationale,
        }
    }

    pub fn with_fallback(mut self, fallback: WorkKind) -> Self {
        self.fallback_kind = Some(fallback);
        self
    }
}

/// Observer for collecting workload data.
pub struct WorkloadObserver {
    profiles: parking_lot::RwLock<std::collections::HashMap<String, WorkloadProfile>>,
    max_samples: usize,
}

impl WorkloadObserver {
    pub fn new(max_samples: usize) -> Self {
        Self {
            profiles: parking_lot::RwLock::new(std::collections::HashMap::new()),
            max_samples,
        }
    }

    pub fn record_profile(&self, task_name: String, profile: WorkloadProfile) {
        let mut profiles = self.profiles.write();
        if profiles.len() < self.max_samples {
            profiles.insert(task_name, profile);
        }
    }

    pub fn infer_kind(&self, _work: &Work) -> Option<AdaptiveRoutingDecision> {
        let profiles = self.profiles.read();
        if profiles.is_empty() {
            return None;
        }

        let avg_profile = self.average_profile(&profiles);
        let inferred_kind = avg_profile.infer_kind();
        let confidence = avg_profile.confidence();

        Some(AdaptiveRoutingDecision::new(
            inferred_kind,
            confidence,
            format!(
                "Adaptive inference: CPU={:.1}%, IO={:.1}%, Parallelism={:.1}",
                avg_profile.cpu_percentage, avg_profile.io_percentage, avg_profile.parallelism_score
            ),
        ))
    }

    fn average_profile(&self, profiles: &std::collections::HashMap<String, WorkloadProfile>) -> WorkloadProfile {
        if profiles.is_empty() {
            return WorkloadProfile::new();
        }

        let count = profiles.len() as f64;
        let mut avg = WorkloadProfile::new();

        for profile in profiles.values() {
            avg.cpu_percentage += profile.cpu_percentage / count;
            avg.io_percentage += profile.io_percentage / count;
            avg.memory_mb += profile.memory_mb / profiles.len();
            avg.parallelism_score += profile.parallelism_score / count;
        }

        avg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_profile_cpu_heavy() {
        let profile = WorkloadProfile {
            cpu_percentage: 85.0,
            io_percentage: 5.0,
            memory_mb: 512,
            parallelism_score: 4.0,
            observed_duration: Duration::from_secs(1),
        };

        assert_eq!(profile.infer_kind(), WorkKind::Compute);
        assert!(profile.confidence() > 0.8);
    }

    #[test]
    fn workload_profile_io_heavy() {
        let profile = WorkloadProfile {
            cpu_percentage: 5.0,
            io_percentage: 85.0,
            memory_mb: 256,
            parallelism_score: 1.0,
            observed_duration: Duration::from_secs(1),
        };

        assert_eq!(profile.infer_kind(), WorkKind::Io);
    }
}
