#!/usr/bin/env bash
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 qtop contributors
##
## SPDX-License-Identifier: MIT
##
# Retry helpers sourced by matrix_lane.sh. A nightly lane re-runs a failed gate
# LANE_RETRIES times (default 2) after LANE_RETRY_WAIT seconds (default 60):
# runner, mirror and PTY-timing flakes otherwise turn a whole night red. The PR
# lanes set LANE_RETRIES=0, so a flaky change still fails its review. Every retry
# is logged, and annotated on GitHub, so flakes stay visible on green runs.

LANE_RETRIES="${LANE_RETRIES:-2}"
LANE_RETRY_WAIT="${LANE_RETRY_WAIT:-60}"

wait_with_dots() {
    # Print one dot per second, ten per line, so the log shows the lane is alive.
    local seconds="$1" i
    for ((i = 1; i <= seconds; i++)); do
        sleep 1
        printf '.'
        if ((i % 10 == 0)); then printf '\n'; fi
    done
    if ((seconds % 10 != 0)); then printf '\n'; fi
}

retry() {
    # Run "$@"; on failure wait and run it again, up to LANE_RETRIES more times.
    local attempt=1 rc
    while true; do
        if "$@"; then return 0; else rc=$?; fi
        if ((attempt > LANE_RETRIES)); then
            echo "giving up after ${attempt} attempts (exit ${rc}): $*" >&2
            return "${rc}"
        fi
        echo "attempt ${attempt} failed (exit ${rc}): $*; retrying in ${LANE_RETRY_WAIT}s" >&2
        if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
            echo "::warning title=lane retry::attempt ${attempt} of '$*' failed (exit ${rc}); retrying"
        fi
        wait_with_dots "${LANE_RETRY_WAIT}"
        attempt=$((attempt + 1))
    done
}
