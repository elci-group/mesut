// Performance benchmarks for Mesut routing layer

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mesut::prelude::*;
use mesut_core::WorkKind;
use std::sync::Arc;

fn benchmark_routing(c: &mut Criterion) {
    let router = mesut_router::WorkRouter::new()
        .with_async_executor(Arc::new(mesut::TokioExecutor::new(
            mesut_tokio::TokioExecutorConfig::default(),
        )))
        .with_compute_executor(Arc::new(
            mesut::RayonExecutor::new(mesut_rayon::RayonExecutorConfig::default())
                .expect("Failed to create Rayon executor"),
        ))
        .with_blocking_executor(Arc::new(mesut::BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::default(),
        )));

    c.bench_function("route_io_work", |b| {
        b.iter(|| {
            let work = black_box(mesut_core::Work::new(WorkKind::Io));
            let _route = router.route(&work);
        })
    });

    c.bench_function("route_compute_work", |b| {
        b.iter(|| {
            let work = black_box(mesut_core::Work::new(WorkKind::Compute));
            let _route = router.route(&work);
        })
    });

    c.bench_function("route_blocking_work", |b| {
        b.iter(|| {
            let work = black_box(mesut_core::Work::new(WorkKind::Blocking));
            let _route = router.route(&work);
        })
    });

    c.bench_function("route_with_hint", |b| {
        b.iter(|| {
            let work = black_box(
                mesut_core::Work::new(WorkKind::Io).with_hint(mesut_core::RoutingHint::PreferAsync),
            );
            let _route = router.route(&work);
        })
    });
}

fn benchmark_work_creation(c: &mut Criterion) {
    c.bench_function("create_io_work", |b| {
        b.iter(|| {
            let _work = black_box(mesut_core::Work::new(WorkKind::Io));
        })
    });

    c.bench_function("create_compute_work", |b| {
        b.iter(|| {
            let _work = black_box(mesut_core::Work::new(WorkKind::Compute));
        })
    });

    c.bench_function("create_work_with_metadata", |b| {
        b.iter(|| {
            let _work = black_box(
                mesut_core::Work::new(WorkKind::Io)
                    .with_priority(Priority::High)
                    .with_hint(mesut_core::RoutingHint::PreferAsync),
            );
        })
    });
}

criterion_group!(benches, benchmark_routing, benchmark_work_creation);
criterion_main!(benches);
