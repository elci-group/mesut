#!/usr/bin/env bash
# scripts/reproduce-scheduler-evidence.sh — MESUT-AVP-01 evidence pipeline.
#
# One command: build release binaries, capture the environment, run the
# benchmark matrix (open-loop, 25 reps on critical workloads), the overhead
# microbenchmarks, real-I/O scenarios, soak runs, and the leak probe; write
# raw JSON + summaries under docs/evidence/ and exit non-zero when a
# mandatory gate fails.
#
# Runtime: roughly 10 hours end to end. Run on a quiet machine (the
# environment capture records loadavg so contamination is detectable).
set -uo pipefail
cd "$(dirname "$0")/.."

EV=docs/evidence
RAW="$EV/raw"
mkdir -p "$RAW"
fail=0

step() { echo "=== [$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $* ==="; }

step "build release binaries"
cargo build --release -p mesut --tests 2>&1 | tail -1
for tool in mesut-sched-bench mesut-open-loop; do
  cargo build --release --manifest-path "tools/$tool/Cargo.toml" 2>&1 | tail -1
done

step "environment capture"
./tools/mesut-open-loop/target/release/mesut-open-loop env > "$EV/environment.json"

step "closed-batch scheduler bench (25 repetitions)"
# NOTE: requires the repeat-cap fix in mesut-sched-bench (1..=100); if this
# stage fails, rebuild tools and rerun this stage before the gate rollup.
./tools/mesut-sched-bench/target/release/mesut-sched-bench --repeat 25 \
  > "$EV/scheduler-batch-25.json" 2>"$RAW/scheduler-batch-25.log" || { fail=1; echo "batch bench failed (see $RAW/scheduler-batch-25.log)"; }

for workload in adversarial-95-4-1 bimodal homogeneous-short deadline-heavy; do
  reps=25
  [[ "$workload" == "deadline-heavy" ]] && reps=15
  step "open-loop matrix: $workload (reps $reps, x0.5..x1.5 capacity)"
  mkdir -p "$RAW/matrix-$workload"
  ./tools/mesut-open-loop/target/release/mesut-open-loop matrix \
    --preset "$workload" --reps "$reps" --duration 10 --warmup 2 \
    --out "$RAW/matrix-$workload" \
    > "$RAW/matrix-$workload/summary.out" 2>"$RAW/matrix-$workload.log" || fail=1
done

step "scheduler overhead (WS5, Gate D)"
./tools/mesut-open-loop/target/release/mesut-open-loop overhead \
  --reps 7 --workers 8 --out "$EV/overhead.json" \
  2>"$RAW/overhead.log" || fail=1

step "real filesystem/network I/O (WS4)"
./tools/mesut-open-loop/target/release/mesut-open-loop realio \
  --reps 5 --ops 200 --out "$EV/realio.json" \
  2>"$RAW/realio.log" || fail=1

step "soak 30min adaptive (Gate F)"
./tools/mesut-open-loop/target/release/mesut-open-loop soak \
  --mode adaptive --minutes 30 --warmup-minutes 3 --out "$EV/soak-adaptive.json" \
  2>"$RAW/soak-adaptive.log" || fail=1

step "soak 30min fifo (Gate F baseline)"
./tools/mesut-open-loop/target/release/mesut-open-loop soak \
  --mode fifo --minutes 30 --warmup-minutes 3 --out "$EV/soak-fifo.json" \
  2>"$RAW/soak-fifo.log" || fail=1

step "lifecycle torture (Gate E)"
cargo test --release -p mesut --test torture 2>&1 | tail -2 || fail=1

step "leak probe (Gate F, load-sensitive; run on the quiet evidence host)"
cargo test --release -p mesut --test leak_probe -- --ignored 2>&1 | tail -2 || fail=1

step "gate rollup"
python3 - "$EV" <<'PYEOF' || fail=1
import json, sys, glob
from pathlib import Path

ev = Path(sys.argv[1])
gates = {}

def load(path):
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return None

for name in ["adversarial-95-4-1", "bimodal", "homogeneous-short", "deadline-heavy"]:
    matrix = load(ev / "raw" / f"matrix-{name}" / f"{name}-matrix.json")
    if matrix:
        gates[f"matrix:{name}"] = {
            g["name"]: {"passed": g["passed"], "detail": g["detail"]}
            for g in matrix["gates"]
        }

overhead = load(ev / "overhead.json")
if overhead:
    gates["D:overhead"] = {
        "passed": overhead["gate_d"]["passed"],
        "detail": overhead["gate_d"]["detail"],
        "supported_granularity": overhead.get("supported_granularity"),
    }

for mode in ["adaptive", "fifo"]:
    soak = load(ev / f"soak-{mode}.json")
    if soak:
        gates[f"F:soak-{mode}"] = {
            "passed": soak["gate_f"]["passed"],
            "checks": soak["gate_f"]["checks"],
        }

batch = load(ev / "scheduler-batch-25.json")
if batch:
    gates["batch-gate"] = {
        "passed": batch["gate"]["passed"],
        "checks": batch["gate"]["checks"],
    }

mandatory_fail = []
for section, checks in gates.items():
    for name, check in checks.items():
        if not check.get("passed", False) and not name.startswith("batch"):
            mandatory_fail.append(f"{section}/{name}")

rollup = {"gates": gates, "mandatory_failures": mandatory_fail,
          "all_passed": not mandatory_fail}
(ev / "gate-rollup.json").write_text(json.dumps(rollup, indent=1))
print(json.dumps({"all_passed": rollup["all_passed"],
                  "failures": mandatory_fail}, indent=1))
sys.exit(0 if rollup["all_passed"] else 1)
PYEOF

step "done (fail=$fail)"
exit "$fail"
