#! /bin/sh -
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
PYTHON="${PYTHON:-python3}"
SCHEDULERS="${SAMPLE_GATE_SCHEDULERS:-pbs,sge,slurm}"
MAX_FAILURES="${SAMPLE_GATE_MAX_FAILURES:-0}"
ARTIFACT_DIR="${SAMPLE_GATE_ARTIFACT_DIR:-artifacts/sample-gate}"

cd "$ROOT_DIR"
exec "$PYTHON" tools/validate_scheduler_samples.py \
    --schedulers "$SCHEDULERS" \
    --max-failures "$MAX_FAILURES" \
    --artifact-dir "$ARTIFACT_DIR"
