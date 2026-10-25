//! Workload distributions: service times, priorities, deadlines, work kinds.

use crate::rng::Rng;
use serde::{Deserialize, Serialize};
use std::time::Duration;

fn default_one() -> f64 {
    1.0
}

/// Named service-time class; doubles as the Adaptive history label.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Class {
    Short,
    Medium,
    Long,
}

impl Class {
    pub fn name(self) -> &'static str {
        match self {
            Class::Short => "short",
            Class::Medium => "medium",
            Class::Long => "long",
        }
    }

    /// Size class for distributions without explicit component names.
    /// Boundaries: short < 1ms, medium < 10ms, long >= 10ms.
    pub fn from_duration(d: Duration) -> Self {
        let us = d.as_micros();
        if us < 1_000 {
            Class::Short
        } else if us < 10_000 {
            Class::Medium
        } else {
            Class::Long
        }
    }
}

/// Service-time distribution. Sampling returns the duration plus the label
/// class the task should carry (component name where present, size class
/// otherwise) so per-class latency and starvation are measurable.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServiceDist {
    /// Fixed duration.
    Constant { mean_us: f64 },
    /// Uniform in [min_us, max_us).
    Uniform { min_us: f64, max_us: f64 },
    /// Lognormal parameterised by desired mean and shape sigma.
    Lognormal { mean_us: f64, sigma: f64 },
    /// Pareto with scale xm_us and shape alpha (alpha <= 1: infinite mean).
    Pareto { xm_us: f64, alpha: f64 },
    /// Weighted mixture of named components, e.g. the 95/4/1 short/medium/long
    /// adversary. Component names become the task labels.
    Mixture { components: Vec<MixtureComponent> },
    /// Mostly fast tasks with a periodic spike window: within every
    /// `period_s` of arrival time, arrivals in the first `spike_fraction`
    /// fraction of the period sample from `spike_us`, others from `base_us`.
    Spiky { base_us: f64, spike_us: f64, period_s: f64, spike_fraction: f64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MixtureComponent {
    pub weight: f64,
    pub label: String,
    #[serde(flatten)]
    pub dist: ServiceDist,
}

impl ServiceDist {
    /// Sample one service time. `arrival_offset` enables time-dependent
    /// shapes (the spiky distribution).
    pub fn sample(&self, rng: &mut Rng, arrival_offset: Duration) -> (Duration, Option<String>) {
        match self {
            ServiceDist::Constant { mean_us } => (duration_us(*mean_us), None),
            ServiceDist::Uniform { min_us, max_us } => {
                (duration_us(rng.uniform_range(*min_us, *max_us)), None)
            }
            ServiceDist::Lognormal { mean_us, sigma } => {
                (duration_us(rng.lognormal(*mean_us, *sigma)), None)
            }
            ServiceDist::Pareto { xm_us, alpha } => {
                (duration_us(rng.pareto(*xm_us, *alpha)), None)
            }
            ServiceDist::Mixture { components } => {
                let index = rng.pick_weighted(&components.iter().map(|c| c.weight).collect::<Vec<_>>());
                let (d, _) = components[index].dist.sample(rng, arrival_offset);
                (d, Some(components[index].label.clone()))
            }
            ServiceDist::Spiky { base_us, spike_us, period_s, spike_fraction } => {
                let phase = arrival_offset.as_secs_f64() % period_s.max(1e-9);
                let in_spike = phase < period_s * spike_fraction;
                (duration_us(if in_spike { *spike_us } else { *base_us }), None)
            }
        }
    }

    /// The label a sampled task should carry when the distribution itself
    /// does not name components: the size class of the drawn duration.
    pub fn label_for(sampled: Duration, named: Option<String>) -> String {
        named.unwrap_or_else(|| Class::from_duration(sampled).name().to_string())
    }
}

fn duration_us(us: f64) -> Duration {
    Duration::from_micros(us.max(1.0) as u64)
}

/// Priority mixture: weights over the four priority levels.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriorityDist {
    #[serde(default = "default_one")]
    pub low: f64,
    #[serde(default = "default_one")]
    pub normal: f64,
    #[serde(default)]
    pub high: f64,
    #[serde(default)]
    pub critical: f64,
}

impl Default for PriorityDist {
    fn default() -> Self {
        Self { low: 0.0, normal: 1.0, high: 0.0, critical: 0.0 }
    }
}

impl PriorityDist {
    pub fn sample(&self, rng: &mut Rng) -> mesut_core::Priority {
        let index = rng.pick_weighted(&[self.low, self.normal, self.high, self.critical]);
        match index {
            0 => mesut_core::Priority::Low,
            1 => mesut_core::Priority::Normal,
            2 => mesut_core::Priority::High,
            _ => mesut_core::Priority::Critical,
        }
    }
}

/// Deadline (end-to-end SLA) distribution over submitted tasks.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeadlineDist {
    #[default]
    None,
    Fixed { sla_ms: f64 },
    Uniform { min_ms: f64, max_ms: f64 },
}

impl DeadlineDist {
    pub fn sample(&self, rng: &mut Rng) -> Option<Duration> {
        match self {
            DeadlineDist::None => None,
            DeadlineDist::Fixed { sla_ms } => Some(Duration::from_secs_f64(sla_ms / 1e3)),
            DeadlineDist::Uniform { min_ms, max_ms } => {
                Some(Duration::from_secs_f64(rng.uniform_range(*min_ms, *max_ms) / 1e3))
            }
        }
    }
}

/// Work-kind mixture. `compute` burns CPU, `io` awaits a timer future,
/// `blocking` parks a worker thread — all representing the same requested
/// service time through the executor path that kind routes to.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkKindDist {
    #[serde(default = "default_one")]
    pub compute: f64,
    #[serde(default)]
    pub io: f64,
    #[serde(default)]
    pub blocking: f64,
}

impl Default for WorkKindDist {
    fn default() -> Self {
        Self { compute: 1.0, io: 0.0, blocking: 0.0 }
    }
}

impl WorkKindDist {
    pub fn sample(&self, rng: &mut Rng) -> mesut_core::WorkKind {
        let index = rng.pick_weighted(&[self.compute, self.io, self.blocking]);
        match index {
            0 => mesut_core::WorkKind::Compute,
            1 => mesut_core::WorkKind::Io,
            _ => mesut_core::WorkKind::Blocking,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixture_labels_and_weights() {
        let dist = ServiceDist::Mixture {
            components: vec![
                MixtureComponent {
                    weight: 95.0,
                    label: "short".into(),
                    dist: ServiceDist::Constant { mean_us: 2000.0 },
                },
                MixtureComponent {
                    weight: 4.0,
                    label: "medium".into(),
                    dist: ServiceDist::Constant { mean_us: 20_000.0 },
                },
                MixtureComponent {
                    weight: 1.0,
                    label: "long".into(),
                    dist: ServiceDist::Constant { mean_us: 200_000.0 },
                },
            ],
        };
        let mut rng = Rng::new(5);
        let mut counts = [0usize; 3];
        for _ in 0..10_000 {
            let (_, label) = dist.sample(&mut rng, Duration::ZERO);
            let index = match label.as_deref() {
                Some("short") => 0,
                Some("medium") => 1,
                _ => 2,
            };
            counts[index] += 1;
        }
        assert!(counts[0] > 9_000, "short {counts:?}");
        assert!(counts[2] > 50, "long {counts:?}");
    }

    #[test]
    fn spiky_is_time_dependent_not_rng_dependent() {
        let dist = ServiceDist::Spiky {
            base_us: 1000.0,
            spike_us: 50_000.0,
            period_s: 1.0,
            spike_fraction: 0.1,
        };
        let mut rng = Rng::new(3);
        let (in_spike, _) = dist.sample(&mut rng, Duration::from_millis(50));
        let (outside, _) = dist.sample(&mut rng, Duration::from_millis(500));
        assert_eq!(in_spike, Duration::from_micros(50_000));
        assert_eq!(outside, Duration::from_micros(1_000));
    }
}
