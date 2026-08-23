# Mesut

A Rust-native unified execution orchestration layer for heterogeneous workloads.

Mesut operates as an execution router and scheduler, determining the appropriate execution substrate (Tokio for I/O, Rayon for compute, blocking workers) for application work.

## Architecture

```
                         APPLICATION
                              │
                              ▼
                         MESUT API
                              │
                              ▼
                    ┌──────────────────┐
                    │ Work Description │
                    └────────┬─────────┘
                             │
                    ┌────────▼────────┐
                    │ Classification   │
                    │ + Scheduling     │
                    │ + Policy         │
                    └────────┬─────────┘
                             │
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
       ASYNC              COMPUTE            BLOCKING
          │                  │                  │
       Tokio              Rayon             worker pool
          │                  │                  │
          └──────────────────┼──────────────────┘
                             ▼
                           RESULT
```

## Workspace Crates

- **mesut** — Public API
- **mesut-core** — Work model, identifiers, lifecycle
- **mesut-router** — Classification and routing
- **mesut-scheduler** — Scheduling policy
- **mesut-runtime** — Runtime coordination
- **mesut-executor** — Executor abstraction
- **mesut-tokio** — Tokio integration
- **mesut-rayon** — Rayon integration
- **mesut-blocking** — Blocking execution
- **mesut-observe** — Telemetry and diagnostics

## Phase 1: Foundation

Implement core execution model, adapters for Tokio/Rayon/Blocking, and end-to-end routing.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
