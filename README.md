# Mesut

A Rust-native unified execution orchestration layer for heterogeneous workloads.

Mesut operates as an execution router and scheduler, determining the appropriate execution substrate (Tokio for I/O, Rayon for compute, blocking workers) for application work.

Mesut's default terminal output follows each task with an adaptive
"scan → lane → assist" animation inspired by Özil's playmaking. It is
lifecycle-driven, adds no artificial delay, and automatically stays quiet in
CI or when stderr is redirected. Set `MESUT_ANIMATIONS=always` to force it,
`MESUT_ANIMATIONS=never` to suppress it, or use
`RuntimeConfig::new().with_animations(false)` for a programmatic opt-out.

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
# mesut
