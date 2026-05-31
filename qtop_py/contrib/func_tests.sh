#! /bin/sh -
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
python_cmd=${PYTHON:-python3}

case "$python_cmd" in
    /*) ;;
    */*) python_cmd=$(CDPATH= cd -- "$(dirname -- "$python_cmd")" && pwd)/$(basename -- "$python_cmd") ;;
esac

cd "$repo_root"
"$python_cmd" tools/sample_gate.py --output "${SAMPLE_GATE_OUTPUT:-artifacts/sample-gate}" --max-failures "${SAMPLE_GATE_MAX_FAILURES:-0}"
