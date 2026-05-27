# PR Closeout Plan for Challenge 357

This note wraps up stale or superseded pull requests so maintainers can reduce
the open PR queue without losing useful context. It targets challenge #357 and
uses the current `develop` branch as the baseline.

## Decisions

| PR | Decision | Reason |
| --- | --- | --- |
| #336 | Close as superseded | Slurm support was completed and merged through #415. |
| #339 | Close as superseded | The PBS parser/debugging path was later handled through the merged #343 and #347 work. |
| #344 | Close | It targets `main`, contains generated state artifacts, and #337 is already closed. |
| #345 | Close or ask for a fresh `develop` PR | It targets `main`, lacks current validation evidence, and does not satisfy the challenge proof requirements. |
| #349 | Close as superseded | PBS sample regression coverage was merged through #347; this PR remains on the wrong base. |
| #365 | Close after this meta-PR | It is an earlier #357 documentation attempt and this note provides the stricter closeout table. |
| #381 | Close | It targets `main`, has only a loose text file, and does not meet the updated #357 requirements. |
| #390 | Close after this meta-PR | It is another #357 documentation attempt, but points to old supersession state and the wrong base. |
| #429 | Close | It does not address #357; adding version flags is unrelated to wrapping five previous PRs. |
| #430 | Close | It does not address #357; a split refactor is unrelated to the requested PR closeout. |

## Current Good-Run Evidence

Three current Slurm sample renders from `develop` are published in the artifact
repository:

- `pr-357/meta-pr-good-runs/large_cluster.png`
- `pr-357/meta-pr-good-runs/large_mixed.png`
- `pr-357/meta-pr-good-runs/large_multi_partition.png`

The matching text renders are stored beside the screenshots.

## Validation

- `venv/bin/python -m pytest -q`
- `PYTHON=venv/bin/python make test-slurm-samples`
- `venv/bin/python -m ruff check .`
- `git diff --check`
- AlmaLinux 8 / Python 3.6.8:
  - `python3 -m pytest tests/plugins/test_slurm.py -q`
  - `python3 tools/validate_slurm_samples.py tests/plugins/slurm_samples --output /tmp/qtop-slurm-rendered-py36`

AI assistance disclosure: OpenAI Codex based on GPT-5 was used to inspect the
stale PR list, prepare this closeout note, and run the validation commands.
