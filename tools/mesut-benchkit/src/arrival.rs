//! Open-loop arrival processes: arrivals are generated on a schedule
//! independent of completions, so queue pressure is a property of the
//! workload rather than of the harness.

use crate::rng::Rng;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// How arrival times are generated. All rates are arrivals per second.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArrivalProcess {
    /// Deterministic interval `1/rate`.
    Fixed { rate: f64 },
    /// Memoryless exponential inter-arrivals with mean `1/rate`.
    Poisson { rate: f64 },
    /// Bursts of `burst_size` arrivals spaced `1/inner_rate` apart, followed
    /// by `gap_s` of silence. `arrival_rate` is ignored for scheduling.
    Bursty { burst_size: usize, inner_rate: f64, gap_s: f64 },
    /// Alternating `low_rate` and `high_rate` phases, switching every
    /// `phase_s`.
    Alternating { low_rate: f64, high_rate: f64, phase_s: f64 },
}

impl ArrivalProcess {
    /// Seconds until the next arrival after wall-time offset `t`, or `None`
    /// when the process ends (currently: never — the run duration bounds it).
    pub fn next_interval(&self, rng: &mut Rng, t: Duration) -> f64 {
        match self {
            ArrivalProcess::Fixed { rate } => 1.0 / rate.max(1e-9),
            ArrivalProcess::Poisson { rate } => rng.exponential(rate.max(1e-9)),
            ArrivalProcess::Bursty { burst_size, inner_rate, gap_s } => {
                let period = *gap_s + *burst_size as f64 / inner_rate.max(1e-9);
                let phase = t.as_secs_f64() % period;
                let burst_span = *burst_size as f64 / inner_rate.max(1e-9);
                if phase >= burst_span {
                    period - phase
                } else {
                    1.0 / inner_rate.max(1e-9)
                }
            }
            ArrivalProcess::Alternating { low_rate, high_rate, phase_s } => {
                let in_high = (t.as_secs_f64() / phase_s.max(1e-9)) as usize % 2 == 1;
                let rate = if in_high { *high_rate } else { *low_rate };
                rng.exponential(rate.max(1e-9))
            }
        }
    }

    pub fn describe(&self) -> String {
        match self {
            ArrivalProcess::Fixed { rate } => format!("fixed({rate:.1}/s)"),
            ArrivalProcess::Poisson { rate } => format!("poisson({rate:.1}/s)"),
            ArrivalProcess::Bursty { burst_size, inner_rate, gap_s } => {
                format!("bursty({burst_size} @ {inner_rate:.1}/s, gap {gap_s}s)")
            }
            ArrivalProcess::Alternating { low_rate, high_rate, phase_s } => {
                format!("alternating({low_rate:.1}/{high_rate:.1}/s every {phase_s}s)")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_is_exact() {
        let mut rng = Rng::new(1);
        let p = ArrivalProcess::Fixed { rate: 100.0 };
        assert!((p.next_interval(&mut rng, Duration::ZERO) - 0.01).abs() < 1e-9);
    }

    #[test]
    fn bursty_gaps_after_burst_span() {
        let mut rng = Rng::new(1);
        let p = ArrivalProcess::Bursty { burst_size: 10, inner_rate: 100.0, gap_s: 0.5 };
        // Inside the burst: 10ms spacing.
        assert!((p.next_interval(&mut rng, Duration::from_millis(5)) - 0.01).abs() < 1e-9);
        // At t=0.1s (burst done at 0.1): gap until 0.1 + 0.5.
        assert!((p.next_interval(&mut rng, Duration::from_millis(100)) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn poisson_mean_is_near_rate() {
        let mut rng = Rng::new(4);
        let p = ArrivalProcess::Poisson { rate: 200.0 };
        let total: f64 = (0..20_000).map(|_| p.next_interval(&mut rng, Duration::ZERO)).sum();
        let mean = total / 20_000.0;
        assert!((mean - 1.0 / 200.0).abs() / (1.0 / 200.0) < 0.1, "mean {mean}");
    }
}
