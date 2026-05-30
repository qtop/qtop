#!/usr/bin/env bash
set -euo pipefail

echo "== weird unicode/control chars =="
git diff -U0 origin/main...HEAD \
  | grep -nP '[^\x09\x0A\x0D\x20-\x7E]|\x{202A}|\x{202B}|\x{202C}|\x{202D}|\x{202E}|\x{2066}|\x{2067}|\x{2068}|\x{2069}' \
  && exit 1 || true

echo "== unexpected binary/generated/build changes =="
git diff --name-only origin/main...HEAD \
  | grep -Ei '(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|\.xz$|\.lzma$|\.gz$|\.bin$|\.dat$' \
  && { echo "Manual review required"; exit 1; } || true

echo "== eval usage check =="
grep -rn '\beval(' --include="*.py" qtop_py/ 2>/dev/null \
  && { echo "eval() calls found — prefer safe alternatives"; exit 1; } || true

echo "OK"
