#! /bin/sh -
# Run the shared scheduler sample gate used by CI.

cd "$(dirname "$0")/../.."

python3 scripts/sample_gate.py "$@"
