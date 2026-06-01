#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
PYTHON=${PYTHON:-python3}
ARTIFACT_DIR=${ARTIFACT_DIR:-sample-artifacts}
MAX_FAILURES=${MAX_FAILURES:-0}

exec "$PYTHON" "$REPO_ROOT/scripts/sample_gate.py" \
    --repo-root "$REPO_ROOT" \
    --artifact-dir "$ARTIFACT_DIR" \
    --max-failures "$MAX_FAILURES"
