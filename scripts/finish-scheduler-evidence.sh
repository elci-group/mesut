#!/usr/bin/env bash
# scripts/finish-scheduler-evidence.sh — completes the stages that the main
# pipeline could not run in its first invocation: the closed-batch bench
# (repeat-cap fix) and the four open-loop matrices (out-dir redirect fix),
# then regenerates the gate rollup. Run after
# scripts/reproduce-scheduler-evidence.sh has finished, on a quiet machine.
set -uo pipefail
cd "$(dirname "$0")/.."

EV=docs/evidence
RAW="$EV/raw"
mkdir -p "$RAW"
fail=0

step() { echo "=== [$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $* ==="; }

step "rebuild tools"
cargo build --release --manifest-path tools/mesut-sched-bench/Cargo.toml 2>&1 | tail -1
cargo build --release --manifest-path tools/mesut-open-loop/Cargo.toml 2>&1 | tail -1

step "validate dispatch-scan repair (full mesut suite)"
cargo test --release -p mesut 2>&1 | grep -E "result:" | awk -F'[ .;]+' '{p+=$4; f+=$6} END {print "passed", p, "failed", f; exit f > 0}'

step "closed-batch scheduler bench (25 repetitions)"
./tools/mesut-sched-bench/target/release/mesut-sched-bench --repeat 25 \
  > "$EV/scheduler-batch-25.json" 2>"$RAW/scheduler-batch-25.log" || fail=1

for workload in adversarial-95-4-1 bimodal homogeneous-short deadline-heavy; do
  reps=25
  [[ "$workload" == "deadline-heavy" ]] && reps=15
  step "open-loop matrix: $workload (reps $reps)"
  mkdir -p "$RAW/matrix-$workload"
  ./tools/mesut-open-loop/target/release/mesut-open-loop matrix \
    --preset "$workload" --reps "$reps" --duration 10 --warmup 2 \
    --out "$RAW/matrix-$workload" \
    > "$RAW/matrix-$workload/summary.out" 2>"$RAW/matrix-$workload.log" || fail=1
done

step "gate rollup"
python3 - "$EV" <<'PYEOF' || fail=1
import json, sys
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

mandatory_fail = [
    f"{section}/{name}"
    for section, checks in gates.items()
    for name, check in checks.items()
    if not check.get("passed", False) and not name.startswith("batch")
]

rollup = {"gates": gates, "mandatory_failures": mandatory_fail,
          "all_passed": not mandatory_fail}
(ev / "gate-rollup.json").write_text(json.dumps(rollup, indent=1))
print(json.dumps({"all_passed": rollup["all_passed"],
                  "failures": mandatory_fail}, indent=1))
sys.exit(0 if rollup["all_passed"] else 1)
PYEOF

step "done (fail=$fail)"
exit "$fail"
