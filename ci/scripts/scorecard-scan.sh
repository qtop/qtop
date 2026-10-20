#!/bin/sh
# SPDX-License-Identifier: MIT
set -eu

: "${GITHUB_AUTH_TOKEN:?Set a read-only GitHub token}"
: "${SCORECARD_REPO:=github.com/qtop/qtop}"
: "${SCORECARD_COMMIT:=$(git rev-parse HEAD)}"
image=ghcr.io/ossf/scorecard@sha256:3f24714e9366917adb7a05635382c97dfecb14b21eaef3dfa2ea48c8e23e0795 # v5.5.0
mkdir -p artifacts/scorecard

# Pass the token by environment name so it is absent from command arguments.
for format in json sarif; do
    docker run --rm --user "$(id -u):$(id -g)" \
        -e GITHUB_AUTH_TOKEN -e ENABLE_SARIF=true \
        --mount "type=bind,source=$(pwd),target=/workspace" \
        --workdir /workspace "$image" \
        --repo="$SCORECARD_REPO" --commit="$SCORECARD_COMMIT" \
        --format="$format" --show-details --policy=ci/scorecard-policy.yml \
        --output="artifacts/scorecard/scorecard.$format"
done
