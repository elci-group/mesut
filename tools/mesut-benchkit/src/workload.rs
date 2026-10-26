//! Work bodies shared by benchmark tools. CPU burn is deliberately the same
//! LCG loop everywhere so service times mean the same thing in every
//! evidence file.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Busy-wait for `service` of wall time using an LCG loop. Used for compute
/// work; timer futures represent I/O service instead.
pub fn burn(service: Duration) {
    let deadline = Instant::now() + service;
    let mut n = 0xdeadbeef_u64;
    while Instant::now() < deadline {
        for _ in 0..512 {
            n = n.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
    }
    std::hint::black_box(n);
}

/// Cycle-calibrated burn: performs roughly the same amount of CPU work per
/// invocation regardless of machine speed or contention, unlike [`burn`],
/// whose wall-deadline loop consumes variable CPU under oversubscription.
/// Calibrate once per process, then clone the iterations-per-ms factor.
#[derive(Clone, Copy)]
pub struct CalibratedBurn {
    iterations_per_ms: f64,
}

impl CalibratedBurn {
    /// Measure this host's LCG throughput with a ~30ms spin. Calibration
    /// cost is paid once per process.
    pub fn calibrate() -> Self {
        let probes = 5;
        let mut iterations_per_ms = 0.0;
        for _ in 0..probes {
            let units = 2_000_u64;
            let began = Instant::now();
            let mut n = 0xdeadbeef_u64;
            for _ in 0..units {
                for _ in 0..512 {
                    n = n.wrapping_mul(6364136223846793005).wrapping_add(1);
                }
            }
            std::hint::black_box(n);
            iterations_per_ms += units as f64 / began.elapsed().as_secs_f64() * 1e-3;
        }
        Self { iterations_per_ms: iterations_per_ms / probes as f64 }
    }

    /// Spin approximately `service` worth of calibrated CPU work.
    pub fn burn(&self, service: Duration) {
        let units = (self.iterations_per_ms * service.as_secs_f64() * 1e3) as u64;
        let mut n = 0xdeadbeef_u64;
        for _ in 0..units {
            for _ in 0..512 {
                n = n.wrapping_mul(6364136223846793005).wrapping_add(1);
            }
        }
        std::hint::black_box(n);
    }
}

/// Busy-wait that exits early on cooperative cancellation, polling every
/// ~5ms. Returns `Err(())` if cancelled before `service` elapsed.
pub fn burn_cooperative(service: Duration, cancelled: &AtomicBool) -> Result<(), ()> {
    let deadline = Instant::now() + service;
    let mut n = 0xdeadbeef_u64;
    while Instant::now() < deadline {
        if cancelled.load(Ordering::Relaxed) {
            std::hint::black_box(n);
            return Err(());
        }
        for _ in 0..512 {
            n = n.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
    }
    std::hint::black_box(n);
    Ok(())
}

/// Shared cancellation flag for cooperative work bodies.
pub fn cancel_flag() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burn_runs_for_requested_time() {
        let start = Instant::now();
        burn(Duration::from_millis(20));
        assert!(start.elapsed() >= Duration::from_millis(19));
    }

    #[test]
    fn cooperative_burn_respects_flag() {
        let flag = cancel_flag();
        flag.store(true, Ordering::Relaxed);
        let start = Instant::now();
        assert!(burn_cooperative(Duration::from_secs(5), &flag).is_err());
        assert!(start.elapsed() < Duration::from_millis(100));
    }
}
