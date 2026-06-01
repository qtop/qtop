#!/usr/bin/env sh
set -eu

PYTHON=${PYTHON:-python3}
REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

exec "$PYTHON" "$REPO_ROOT/scripts/fortify.py"
