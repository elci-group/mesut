//! Reads real files, hashes their real contents, and writes a manifest,
//! exercising pipelines, parallel branches, adaptive scheduling, and
//! observability together against genuine (if synthetic) file I/O and
//! compute — not sleeps or placeholder data.
use mesut::prelude::*;
use mesut::SchedulingMode;
use mesut_core::TaskError;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Non-cryptographic content fingerprint; real per-byte work, not a stand-in.
fn fnv1a_hash(data: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    data.iter().fold(OFFSET, |hash, &byte| {
        (hash ^ byte as u64).wrapping_mul(PRIME)
    })
}

struct SourceFile {
    name: &'static str,
    path: PathBuf,
    size_class: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir =
        std::env::temp_dir().join(format!("mesut-file-bundle-example-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let files = write_source_files(&dir)?;

    let runtime = MesuT::new(
        RuntimeConfig::new()
            .with_animations(false)
            .with_pipeline_concurrency(files.len())
            .with_scheduling(SchedulingMode::Adaptive),
    )
    .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
    .with_compute_executor(Arc::new(RayonExecutor::new(Default::default())?))
    .with_blocking_executor(Arc::new(BlockingExecutor::new(Default::default())));

    let mut pipeline = Pipeline::new();
    let mut hash_stages = Vec::new();
    for file in &files {
        let path = file.path.clone();
        let read = PipelineStage::new(format!("read:{}", file.name), WorkKind::Io).with_work(
            Work::new(WorkKind::Io).with_future(async move {
                tokio::fs::read(&path)
                    .await
                    .map_err(|error| TaskError::ExecutionFailed(error.to_string()))
            }),
        );
        let read_id = read.id;
        let size_class = file.size_class;
        let hash = PipelineStage::new(format!("hash:{}", file.name), WorkKind::Compute)
            .with_dependency(read_id)
            .with_factory(move |inputs| {
                let bytes = inputs[&read_id].clone();
                Ok(Work::new(WorkKind::Compute)
                    .with_label(format!("hash-{size_class}"))
                    .with_job(move |_| Ok(fnv1a_hash(&bytes).to_be_bytes().to_vec())))
            });
        let hash_id = hash.id;
        hash_stages.push((file.name, hash_id));
        pipeline = pipeline.add_stage(read).add_stage(hash);
    }

    let manifest_path = dir.join("manifest.txt");
    let manifest_deps = hash_stages.clone();
    let manifest_write_path = manifest_path.clone();
    let manifest = PipelineStage::new("manifest".into(), WorkKind::Blocking);
    let manifest = manifest_deps
        .iter()
        .fold(manifest, |stage, (_, id)| stage.with_dependency(*id))
        .with_factory(move |inputs| {
            let mut lines = Vec::new();
            for (name, id) in &manifest_deps {
                let hash = u64::from_be_bytes(inputs[id][..8].try_into().unwrap());
                lines.push(format!("{name}: {hash:016x}"));
            }
            let manifest_path = manifest_write_path.clone();
            Ok(Work::new(WorkKind::Blocking).with_job(move |_| {
                std::fs::write(&manifest_path, lines.join("\n"))
                    .map_err(|error| TaskError::ExecutionFailed(error.to_string()))?;
                Ok(lines.join("\n").into_bytes())
            }))
        });
    let manifest_id = manifest.id;
    pipeline = pipeline.add_stage(manifest);

    let outputs = runtime.execute_pipeline(pipeline).await?;
    println!("--- manifest ---");
    println!("{}", String::from_utf8_lossy(&outputs[&manifest_id]));

    println!("\n--- runtime state ---");
    let state = runtime.runtime_state();
    println!(
        "submitted={} completed={} failed={} cancelled={}",
        state.total_submitted, state.total_completed, state.total_failed, state.total_cancelled
    );
    for (executor, stats) in &state.executor_stats {
        println!(
            "  {executor}: completed={} failed={} total_wall_ms={}",
            stats.completed_tasks, stats.failed_tasks, stats.total_wall_ms
        );
    }

    println!("\n--- learned per-label duration (adaptive scheduling) ---");
    for label in ["hash-small", "hash-large"] {
        match runtime.workload_profile(label) {
            Some(profile) => println!(
                "  {label}: samples={} mean={:?} max={:?}",
                profile.samples, profile.mean_duration, profile.max_duration
            ),
            None => println!("  {label}: no samples"),
        }
    }

    runtime.shutdown().await?;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

fn write_source_files(dir: &Path) -> std::io::Result<Vec<SourceFile>> {
    let mut files = Vec::new();
    for i in 0..3 {
        let name: &'static str = Box::leak(format!("small-{i}.bin").into_boxed_str());
        let path = dir.join(name);
        std::fs::write(&path, vec![i as u8; 5 * 1024])?;
        files.push(SourceFile {
            name,
            path,
            size_class: "small",
        });
    }
    for i in 0..3 {
        let name: &'static str = Box::leak(format!("large-{i}.bin").into_boxed_str());
        let path = dir.join(name);
        std::fs::write(&path, vec![i as u8; 3 * 1024 * 1024])?;
        files.push(SourceFile {
            name,
            path,
            size_class: "large",
        });
    }
    Ok(files)
}
