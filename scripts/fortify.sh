#!/usr/bin/env bash
# scripts/fortify.sh — codebase health and security gate
# Run via: make fortify   or directly: bash scripts/fortify.sh
set -euo pipefail

FAILED=0

# ---------------------------------------------------------------------------
# 1. Weird unicode / control chars introduced in this branch
# ---------------------------------------------------------------------------
echo "== checking for weird unicode/control chars in diff =="
if git diff -U0 origin/main...HEAD 2>/dev/null \
    | grep -nP '[^\x09\x0A\x0D\x20-\x7E]|\x{202A}|\x{202B}|\x{202C}|\x{202D}|\x{202E}|\x{2066}|\x{2067}|\x{2068}|\x{2069}'; then
  echo "FAIL: unexpected unicode/control chars found in diff"
  FAILED=1
else
  echo "OK"
fi

# ---------------------------------------------------------------------------
# 2. Bidirectional text injection (Trojan Source attack vectors)
# ---------------------------------------------------------------------------
echo "== checking for bidirectional text markers in source =="
if grep -rn --include='*.py' --include='*.sh' --include='*.yml' --include='*.yaml' \
    -P '\x{202A}|\x{202B}|\x{202C}|\x{202D}|\x{202E}|\x{2066}|\x{2067}|\x{2068}|\x{2069}' \
    qtop_py/ tests/ scripts/ .github/ .gitlab-ci.yml 2>/dev/null; then
  echo "FAIL: bidirectional text markers found"
  FAILED=1
else
  echo "OK"
fi

# ---------------------------------------------------------------------------
# 3. Remaining eval() calls (informational — active ones are flagged)
# ---------------------------------------------------------------------------
echo "== scanning for active eval() calls =="
EVAL_COUNT=$(grep -rn --include='*.py' '\beval(' qtop_py/ | grep -v '^\s*#' | wc -l || true)
echo "Active eval() calls found: ${EVAL_COUNT}"
if [ "${EVAL_COUNT}" -gt 0 ]; then
  grep -rn --include='*.py' '\beval(' qtop_py/ | grep -v '^\s*#' || true
  echo "WARNING: eval() calls remain; lambda-based ones (sort, remapping) are deferred — see CONTRIBUTING.md"
fi

# ---------------------------------------------------------------------------
# 4. No __pycache__ or compiled artefacts committed
# ---------------------------------------------------------------------------
echo "== checking for accidental compiled artefacts in index =="
if git ls-files | grep -qP '\.pyc$|__pycache__'; then
  echo "FAIL: compiled Python artefacts are tracked by git"
  FAILED=1
else
  echo "OK"
fi

# ---------------------------------------------------------------------------
# 5. Trailing whitespace in Python files
# ---------------------------------------------------------------------------
echo "== checking for trailing whitespace =="
if grep -rn --include='*.py' ' $' qtop_py/ tests/ | head -20; then
  echo "WARNING: trailing whitespace found (run: ruff format)"
else
  echo "OK"
fi

# ---------------------------------------------------------------------------
exit ${FAILED}
