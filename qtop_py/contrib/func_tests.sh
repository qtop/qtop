#! /bin/sh -
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

SCRIPT_DIR=$(CDPATH= cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd "$SCRIPT_DIR/../.." && pwd)

cd "$REPO_ROOT" || exit 1

PYTHON=${PYTHON:-python3}
SAMPLE_GATE_ARTIFACT_DIR=${SAMPLE_GATE_ARTIFACT_DIR:-artifacts/sample-gate-contrib}

exec make sample-gate \
	PYTHON="$PYTHON" \
	SAMPLE_GATE_ARTIFACT_DIR="$SAMPLE_GATE_ARTIFACT_DIR" \
	"$@"
