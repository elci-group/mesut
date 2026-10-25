//! Host and process environment capture for reproducibility metadata.

use serde_json::{json, Value};
use std::process::Command;

fn read_trim(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

/// First CPU's model name from /proc/cpuinfo (Linux).
fn cpu_model() -> Value {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|info| {
            info.lines()
                .find(|l| l.starts_with("model name"))
                .and_then(|l| l.split(':').nth(1))
                .map(|m| json!(m.trim()))
        })
        .unwrap_or(Value::Null)
}

fn cpu_governor() -> Value {
    read_trim(
        "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor",
    )
    .map(|v| json!(v))
    .unwrap_or(Value::Null)
}

fn kernel() -> Value {
    read_trim("/proc/sys/kernel/osrelease").map(|v| json!(v)).unwrap_or(Value::Null)
}

fn hostname() -> Value {
    read_trim("/proc/sys/kernel/hostname").map(|v| json!(v)).unwrap_or(Value::Null)
}

fn loadavg() -> Value {
    read_trim("/proc/loadavg").map(|v| json!(v)).unwrap_or(Value::Null)
}

fn rustc_version() -> Value {
    Command::new("rustc")
        .arg("-V")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| json!(String::from_utf8_lossy(&o.stdout).trim()))
        .unwrap_or(Value::Null)
}

fn git_commit() -> Value {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| json!(String::from_utf8_lossy(&o.stdout).trim()))
        .unwrap_or(Value::Null)
}

fn git_dirty() -> Value {
    Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .map(|o| json!(!o.stdout.is_empty()))
        .unwrap_or(Value::Null)
}

/// Machine-readable environment description for evidence files.
pub fn machine_json() -> Value {
    json!({
        "hostname": hostname(),
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "kernel": kernel(),
        "cpu_model": cpu_model(),
        "logical_cpus": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        "cpufreq_governor": cpu_governor(),
        "loadavg_at_start": loadavg(),
        "rustc": rustc_version(),
        "mesut_commit": git_commit(),
        "mesut_dirty_tree": git_dirty(),
        "unix_seconds": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    })
}

/// Resident set size in bytes, where the platform exposes it (Linux).
pub fn rss_bytes() -> Option<u64> {
    // VmRSS from /proc/self/status is exact and carries no page-size
    // assumption; statm is the fallback.
    if let Some(status) = read_trim("/proc/self/status") {
        let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
        let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
        return Some(kb * 1024);
    }
    let statm = read_trim("/proc/self/statm")?;
    let pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(pages * 4096)
}

/// Total process CPU time (user + system) in nanoseconds, Linux only.
pub fn process_cpu_ns() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Fields after the parenthesised comm: split from the last ')'.
    let after = stat.rsplit(')').next()?.trim_start();
    let fields: Vec<&str> = after.split_whitespace().collect();
    // Field 14 utime and 15 stime of /proc/pid/stat; index 11 and 12
    // after the state field at index 0 of `after`.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) * 10_000_000) // USER_HZ is 100 on Linux
}

/// Current thread count, Linux only.
pub fn thread_count() -> Option<usize> {
    let status = read_trim("/proc/self/status")?;
    status
        .lines()
        .find(|l| l.starts_with("Threads:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

/// Count of open file descriptors, Linux only.
pub fn open_fds() -> Option<usize> {
    std::fs::read_dir("/proc/self/fd").ok().map(|entries| {
        entries.filter(|e| e.as_ref().is_ok()).count()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_json_has_core_fields() {
        let m = machine_json();
        assert!(m["arch"].is_string());
        assert!(m["logical_cpus"].as_u64().unwrap_or(0) > 0);
    }

    #[test]
    fn rss_is_plausible() {
        let rss = rss_bytes().expect("linux test host exposes statm");
        assert!(rss > 1_000_000, "rss {rss}");
        assert!(rss < 1_000_000_000_000);
    }
}
