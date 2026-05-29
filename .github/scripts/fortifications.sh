#!/usr/bin/env bash
set -euo pipefail

echo "== weird unicode/control chars =="
git diff -U0 origin/main...HEAD \
  | grep -nP '[^\x09\x0A\x0D\x20-\x7E]' \
  && echo "FAIL: weird chars found" && exit 1 || echo "PASS"

echo "== unexpected binary/generated changes =="
git diff --name-only origin/main...HEAD \
  | grep -Ei '(m4|autogen|configure|Makefile\.in|cmake)' \
  && echo "FAIL: generated files in diff" && exit 1 || echo "PASS"

echo "== checking for eval in codebase =="
grep -rn "eval(" qtop_py/ --include="*.py" && echo "FAIL: eval found" && exit 1 || echo "PASS: no eval"

echo "All fortifications passed"
