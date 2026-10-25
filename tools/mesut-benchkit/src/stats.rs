//! Statistics for run-to-run evidence: percentiles, dispersion, seeded
//! bootstrap confidence intervals, paired comparisons, and trend slopes.
//!
//! Deliberately dependency-free. Latency data is not assumed normal: primary
//! summaries are medians and quantiles; the bootstrap is used for intervals.

use crate::rng::Rng;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct Describe {
    pub count: usize,
    pub mean: f64,
    pub sd: f64,
    pub min: f64,
    pub p5: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
    /// 95% bootstrap percentile interval for the median, from `resamples`
    /// seeded resamples. `null` for degenerate inputs.
    pub p50_ci95: Option<(f64, f64)>,
}

pub fn percentile(values: &[f64], p: f64) -> f64 {
    assert!(!values.is_empty(), "percentile of empty slice");
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() as f64 * p).ceil() as usize).saturating_sub(1);
    sorted[index.min(sorted.len() - 1)]
}

pub fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

pub fn sd(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let m = mean(values);
    (values.iter().map(|v| (v - m) * (v - m)).sum::<f64>() / (values.len() - 1) as f64).sqrt()
}

/// Bootstrap percentile CI for `statistic` over `resamples` seeded resamples.
pub fn bootstrap_ci(
    values: &[f64],
    statistic: fn(&[f64]) -> f64,
    resamples: usize,
    seed: u64,
) -> Option<(f64, f64)> {
    if values.is_empty() {
        return None;
    }
    if values.len() == 1 {
        return Some((values[0], values[0]));
    }
    let mut rng = Rng::new(seed);
    let mut stats = Vec::with_capacity(resamples);
    for _ in 0..resamples {
        let sample: Vec<f64> = (0..values.len())
            .map(|_| values[rng.next_u64() as usize % values.len()])
            .collect();
        stats.push(statistic(&sample));
    }
    stats.sort_by(f64::total_cmp);
    let lo = stats[((resamples as f64 * 0.025) as usize).min(resamples - 1)];
    let hi = stats[((resamples as f64 * 0.975) as usize).min(resamples - 1)];
    Some((lo, hi))
}

pub fn describe(values: &[f64]) -> Option<Describe> {
    let non_empty = !values.is_empty()?;
    Some(Describe {
        count: values.len(),
        mean: mean(values),
        sd: sd(values),
        min: values.iter().copied().fold(f64::MAX, f64::min),
        p5: percentile(values, 0.05),
        p50: percentile(values, 0.50),
        p95: percentile(values, 0.95),
        p99: percentile(values, 0.99),
        p999: percentile(values, 0.999),
        max: values.iter().copied().fold(f64::MIN, f64::max),
        p50_ci95: bootstrap_ci(values, |v| percentile(v, 0.5), 2_000, 0xB00C),
    }
    .filter(|_| non_empty)
)

}

/// Paired comparison between two systems measured over the same run seeds.
/// `a` and `b` are per-repetition values of one statistic (e.g. p50 in ms).
#[derive(Clone, Debug, Serialize)]
pub struct PairedComparison {
    pub n: usize,
    pub median_a: f64,
    pub median_b: f64,
    /// median_a / median_b (< 1 means a is faster/lower).
    pub ratio: Option<f64>,
    pub mean_diff: f64,
    pub sd_diff: f64,
    /// Paired Cohen's d: mean_diff / sd_diff.
    pub effect_size_d: Option<f64>,
    /// Fraction of pairs where a < b.
    pub a_wins: f64,
}

pub fn paired(a: &[f64], b: &[f64]) -> Option<PairedComparison> {
    if a.is_empty() || a.len() != b.len() {
        return None;
    }
    let diffs: Vec<f64> = a.iter().zip(b).map(|(x, y)| x - y).collect();
    let sd_diff = sd(&diffs);
    let median_a = percentile(a, 0.5);
    let median_b = percentile(b, 0.5);
    Some(PairedComparison {
        n: a.len(),
        median_a,
        median_b,
        ratio: (median_b > 0.0).then(|| median_a / median_b),
        mean_diff: mean(&diffs),
        sd_diff,
        effect_size_d: (sd_diff > 0.0).then(|| mean(&diffs) / sd_diff),
        a_wins: a.iter().zip(b).filter(|(x, y)| x < y).count() as f64 / a.len() as f64,
    })
}

/// Ordinary least squares slope with a 95% t-interval approximation (normal
/// CI; adequate at the sample counts used here). Used for queue-growth and
/// soak-drift trend checks.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Trend {
    pub slope_per_second: f64,
    pub ci95: (f64, f64),
    pub r2: f64,
}

pub fn trend(times_s: &[f64], values: &[f64]) -> Option<Trend> {
    let n = times_s.len();
    if n < 3 || n != values.len() {
        return None;
    }
    let (mx, my) = (mean(times_s), mean(values));
    let mut sxx = 0.0;
    let mut sxy = 0.0;
    let mut syy = 0.0;
    for (x, y) in times_s.iter().zip(values) {
        let dx = x - mx;
        let dy = y - my;
        sxx += dx * dx;
        sxy += dx * dy;
        syy += dy * dy;
    }
    if sxx <= 0.0 {
        return None;
    }
    let slope = sxy / sxx;
    let intercept = my - slope * mx;
    let residual: f64 = times_s
        .iter()
        .zip(values)
        .map(|(x, y)| {
            let e = y - (intercept + slope * x);
            e * e
        })
        .sum();
    let dof = (n - 2) as f64;
    if dof <= 0.0 {
        return None;
    }
    let se = (residual / dof / sxx).sqrt();
    // 1.96 is the normal approximation; documented as such.
    let ci = 1.96 * se;
    let r2 = if syy > 0.0 { (sxy * sxy) / (sxx * syy) } else { 0.0 };
    Some(Trend { slope_per_second: slope, ci95: (slope - ci, slope + ci), r2 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_bracket() {
        let v: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&v, 0.5), 50.0);
        assert_eq!(percentile(&v, 0.95), 95.0);
        assert_eq!(percentile(&v, 0.0), 1.0);
    }

    #[test]
    fn bootstrap_ci_covers_sample_median() {
        let v: Vec<f64> = (0..500).map(|i| (i * i) as f64).collect();
        let (lo, hi) = bootstrap_ci(&v, |s| percentile(s, 0.5), 500, 3).unwrap();
        let med = percentile(&v, 0.5);
        assert!(lo <= med && med <= hi, "{lo} <= {med} <= {hi}");
    }

    #[test]
    fn paired_detects_consistent_winner() {
        let a: Vec<f64> = (0..20).map(|i| 100.0 + i as f64 * 0.1).collect();
        let b: Vec<f64> = (0..20).map(|i| 140.0 + i as f64 * 0.1).collect();
        let p = paired(&a, &b).unwrap();
        assert!(p.ratio.unwrap() < 0.8);
        assert_eq!(p.a_wins, 1.0);
        assert!(p.effect_size_d.unwrap() < -5.0);
    }

    #[test]
    fn trend_finds_no_slope_in_noise_free_constant() {
        let t: Vec<f64> = (0..50).map(|i| i as f64).collect();
        let v: Vec<f64> = vec![10.0; 50];
        let tr = trend(&t, &v).unwrap();
        assert!(tr.slope_per_second.abs() < 1e-9);
        assert!(tr.ci95.0.abs() < 1e-9 && tr.ci95.1.abs() < 1e-9);
    }

    #[test]
    fn trend_recovers_true_slope() {
        let t: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let v: Vec<f64> = t.iter().map(|x| 5.0 * x + 3.0).collect();
        let tr = trend(&t, &v).unwrap();
        assert!((tr.slope_per_second - 5.0).abs() < 1e-9);
        assert!(tr.r2 > 0.999);
    }
}
