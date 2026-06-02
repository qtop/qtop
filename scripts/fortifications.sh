#!/usr/bin/env bash
# qtop fortifications: code health check for CI/CD
# Checks for problematic characters and unexpected binary/generated changes
set -euo pipefail

BASE="${1:-origin/develop}"

echo "== weird unicode/control chars =="
git diff -U0 "${BASE}...HEAD" 2>/dev/null \
  | grep -nP '[^\x09\x0A\x0D\x20-\x7E]|\x{202A}|\x{202B}|\x{202C}|\x{202D}|\x{202E}|\x{2066}|\x{2067}|\x{2068}|\x{2069}' \
  && { echo "FAIL: found unexpected unicode/control characters"; exit 1; } || echo "OK"

echo "== unexpected binary/generated/build changes =="
git diff --name-only "${BASE}...HEAD" 2>/dev/null \
  | grep -Ei '(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|\.xz$|\.lzma$|\.gz$|\.bin$|\.dat$' \
  && { echo "FAIL: manual review required"; exit 1; } || echo "OK"

echo "== trailing whitespace check =="
git diff --check "${BASE}...HEAD" 2>/dev/null \
  | grep -v "^Copy" \
  && { echo "FAIL: trailing whitespace found"; exit 1; } || echo "OK"

echo "fortifications: all checks passed"
