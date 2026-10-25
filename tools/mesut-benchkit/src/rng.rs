//! Deterministic xoshiro256** RNG seeded through splitmix64.

/// Small, fast, seedable PRNG. Deterministic across platforms and releases —
/// evidence runs must be reproducible from a config file alone.
#[derive(Clone)]
pub struct Rng {
    state: [u64; 4],
    /// Second variate from a pending Box-Muller pair.
    cached_normal: Option<f64>,
}

fn splitmix64(x: &mut u64) -> u64 {
    *x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        let mut sm = seed;
        Self {
            state: [splitmix64(&mut sm), splitmix64(&mut sm), splitmix64(&mut sm), splitmix64(&mut sm)],
            cached_normal: None,
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let result = self.state[1]
            .wrapping_mul(5)
            .rotate_left(7)
            .wrapping_mul(9);
        let t = self.state[1] << 17;
        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];
        self.state[2] ^= t;
        self.state[3] = self.state[3].rotate_left(45);
        result
    }

    /// Uniform in [0, 1).
    pub fn uniform(&mut self) -> f64 {
        // 53 bits of mantissa: every f64 in [0,1) is representable.
        ((self.next_u64() >> 11) as f64) * (1.0 / (1u64 << 53) as f64)
    }

    /// Uniform in [a, b).
    pub fn uniform_range(&mut self, a: f64, b: f64) -> f64 {
        a + (b - a) * self.uniform()
    }

    /// Exponential with `rate` (mean `1/rate`), via inverse transform.
    pub fn exponential(&mut self, rate: f64) -> f64 {
        -self.uniform().max(1e-12).ln() / rate
    }

    /// Normal via Box-Muller, caching the second variate of each pair.
    pub fn normal(&mut self, mean: f64, std: f64) -> f64 {
        let z = match self.cached_normal.take() {
            Some(z) => z,
            None => {
                let (u, v) = (self.uniform().max(1e-12), self.uniform());
                let mag = (-2.0 * u.ln()).sqrt();
                let z0 = mag * (std::f64::consts::TAU * v).cos();
                self.cached_normal =
                    Some(mag * (std::f64::consts::TAU * v).sin());
                z0
            }
        };
        mean + std * z
    }

    /// Lognormal whose *mean* is `mean` with shape `sigma`. Parameterising by
    /// desired mean keeps workload configs interpretable.
    pub fn lognormal(&mut self, mean: f64, sigma: f64) -> f64 {
        let mu = mean.max(1.0).ln() - 0.5 * sigma * sigma;
        (mu + sigma * self.normal(0.0, 1.0)).exp()
    }

    /// Pareto with scale `xm` and shape `alpha`; mean is
    /// `alpha * xm / (alpha - 1)` for `alpha > 1`.
    pub fn pareto(&mut self, xm: f64, alpha: f64) -> f64 {
        xm * (1.0 - self.uniform()).max(1e-12).powf(-1.0 / alpha)
    }

    /// Weighted pick: returns an index into `weights`.
    pub fn pick_weighted(&mut self, weights: &[f64]) -> usize {
        let total: f64 = weights.iter().sum();
        let mut point = self.uniform() * total;
        for (index, weight) in weights.iter().enumerate() {
            point -= weight;
            if point < 0.0 {
                return index;
            }
        }
        weights.len() - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn uniform_stays_in_range() {
        let mut rng = Rng::new(7);
        for _ in 0..10_000 {
            let x = rng.uniform();
            assert!((0.0..1.0).contains(&x));
        }
    }

    #[test]
    fn lognormal_mean_is_near_target() {
        let mut rng = Rng::new(9);
        let mean: f64 =
            (0..50_000).map(|_| rng.lognormal(100.0, 1.5)).sum::<f64>() / 50_000.0;
        assert!((mean - 100.0).abs() / 100.0 < 0.1, "mean {mean}");
    }

    #[test]
    fn pareto_heavy_tail_beats_lognormal_tail() {
        let mut rng = Rng::new(11);
        let p999 = |samples: Vec<f64>| {
            let mut s = samples;
            s.sort_by(f64::total_cmp);
            s[(s.len() as f64 * 0.999) as usize]
        };
        let logn = p999((0..20_000).map(|_| rng.lognormal(100.0, 1.0)).collect());
        let par = p999((0..20_000).map(|_| rng.pareto(20.0, 1.2)).collect());
        assert!(par > logn, "pareto p999 {par} should exceed lognormal {logn}");
    }
}
