#!/usr/bin/env bash
# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
# Compare the entire supported-version union against the branch point.
set -euo pipefail
BASE_REF=${1:-origin/develop}
BASE=$(git merge-base "$BASE_REF" HEAD)
if ! git cat-file -e "$BASE:schema/cluster-state.cue" 2>/dev/null; then
    echo "schema-compat: baseline $BASE has no contract; validating first introduction through schema-compat-test"
    exit 0
fi
BASE_SCHEMA=$(mktemp)
trap 'rm -f -- "$BASE_SCHEMA"' EXIT
git show "$BASE:schema/cluster-state.cue" > "$BASE_SCHEMA"
make schema-compat BASE_SCHEMA="$BASE_SCHEMA" PROPOSED_SCHEMA=schema/cluster-state.cue
