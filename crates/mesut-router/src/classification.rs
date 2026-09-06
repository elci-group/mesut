use mesut_core::{Work, WorkKind};

/// Trait for custom work classifiers.
pub trait WorkClassifier: Send + Sync {
    /// Classify the work and return a confidence level.
    fn classify(&self, work: &Work) -> (WorkKind, f64);
}

/// Built-in classifiers.
pub struct DefaultClassifier;

impl WorkClassifier for DefaultClassifier {
    fn classify(&self, work: &Work) -> (WorkKind, f64) {
        (work.kind, 1.0)
    }
}

/// Classifies based on resource hints.
pub struct HintBasedClassifier;

impl WorkClassifier for HintBasedClassifier {
    fn classify(&self, work: &Work) -> (WorkKind, f64) {
        let hints = &work.resource_hint;

        let mut score = 0.0;
        let mut factors = 0;

        if let Some(cpu) = hints.estimated_cpu_cycles {
            if cpu > 1_000_000_000 {
                return (WorkKind::Compute, 0.8);
            }
            score += (cpu as f64) / 1_000_000_000.0;
            factors += 1;
        }

        if let Some(io) = hints.estimated_io_operations {
            if io > 0 {
                return (WorkKind::Io, 0.8);
            }
        }

        if let Some(para) = hints.parallelism_hint {
            if para > 1 {
                score += para as f64;
                factors += 1;
            }
        }

        if factors > 0 {
            let avg_score = score / factors as f64;
            if avg_score > 2.0 {
                return (WorkKind::Compute, avg_score.min(1.0));
            }
        }

        (work.kind, 0.5)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_classifier() {
        let classifier = DefaultClassifier;
        let work = Work::new(WorkKind::Io);
        let (kind, confidence) = classifier.classify(&work);
        assert_eq!(kind, WorkKind::Io);
        assert_eq!(confidence, 1.0);
    }
}
