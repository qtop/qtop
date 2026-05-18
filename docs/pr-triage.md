# PR Triage: Meta-wrap of 5 stale/superseded PRs

This document identifies 5 previous PRs that are stale, superseded, or
ready to close, to declutter the open PR list.

## PRs recommended for closure

| PR | Title | Reason |
|----|-------|--------|
| #359 | Address maintenance items for qtop | Superseded by PR #382 (eval fix, Travis CI, MANIFEST cleanup) |
| #362 | Avoid eval in yaml parser literals | Superseded by PR #382 which fixes eval more completely |
| #363 | chore: refresh maintenance tooling | Superseded by PR #382 (pre-commit, Travis modernization) |
| #360 | Add Slurm scheduler support | Superseded by PR #383 (full plugin + 37 tests) |
| #368 | Fix output cleanup race during stale file deletion | Stale, no activity, no test evidence provided |

## Resolution path

Maintainers can close each PR above with:
> "Superseded by #382 / #383 — please see those PRs for the complete fix."

No code changes are required; this is a documentation-only triage note.
