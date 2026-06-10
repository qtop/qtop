# CI sample gates

Issue #433 asks for one reviewable validation path that local runs, GitHub
Actions, and GitLab CI can share. The shared entry point is:

```bash
make ci
```

That target runs unit tests, the fast committed scheduler sample gate, and the
fortification checks. The PR-triggered GitHub `build-qtop` workflow and GitLab
CI call the same target so the meaning of a failure is the same on both
platforms.

Provider-specific aliases such as `make github-ci`, `make gitlab-ci`,
`make github-build`, and `make gitlab-build` deliberately resolve back to the
same Makefile targets. The YAML files stay thin and provider-facing while the
actual validation contract remains local and reviewable.

The GitHub Actions workflows pin third-party actions to full commit SHAs, pin
hosted runners to `ubuntu-24.04`, and set `permissions: contents: read`,
because the jobs only need repository read access plus artifact upload.

`.github/workflows/build.yml` carries the PR-facing modern Python, AlmaLinux 8,
and build lanes. `.github/workflows/pytest.yml` stays as a manual
`workflow_dispatch` validation check and calls the same `make github-ci` path,
so the historical workflow remains available without carrying separate pytest
logic.

Python packages installed by CI, including transitive helper packages and ruff,
are pinned in `requirements-ci.txt`; both GitHub Actions and GitLab CI install
from that same file before calling the Makefile targets. `make build` depends
on `make ci-deps` and uses `python -m build --no-isolation` so the build backend
also comes from those pinned CI requirements instead of being resolved
dynamically during the build.

## Fast committed sample gate

The every-PR gate is:

```bash
make sample-gate SAMPLE_GATE_SCHEDULERS=pbs,sge,slurm,oar,demo SAMPLE_GATE_MAX_FAILURES=0
```

Sources:

- PBS, SGE, and OAR: `qtop_py/contrib`
- Slurm: every directory under `tests/plugins/slurm_samples`
- demo: generated locally by the demo backend

The PBS, SGE, and OAR cases intentionally reuse committed scheduler captures
from `qtop_py/contrib`. Slurm uses every committed sample directory under
`tests/plugins/slurm_samples`, and demo renders a generated cluster. All five
backends run through the shared Python gate, so local, GitHub, and GitLab
validation exercise the same command path.

Policy:

- `SAMPLE_GATE_MAX_FAILURES=0` is the default and means any failed scheduler
  case fails CI.
- Rendered qtop output, ANSI-stripped normalized output, SVG terminal
  screenshots, stderr, command lines, and `summary.json` are written under
  `artifacts/sample-gate/`, including for non-zero and timeout failures.
- `stdout.ans` preserves ANSI colour sequences for human review. Run
  `make backend-colour-artifacts` when the desired result is explicitly the
  five-backend coloured-output artifact set.
- Each qtop subprocess receives an isolated `HOME` under its artifact
  directory, so log creation does not depend on a writable runner home.
- CI uploads that artifact directory so reviewers can inspect the rendered
  output without rerunning locally.
- `qtop_py/contrib/func_tests.sh` delegates to this same sample gate, so the
  historical manual wrapper exercises the current CI path instead of carrying a
  separate drift-prone shell implementation.

The named local entry points are:

```bash
make backend-validation
make backend-colour-artifacts
```

Both use the same implementation and fail on any backend regression. This
keeps validation logic out of provider-specific CI YAML.

## Clean reruns

`make all` runs the local validation and build path. The optional archived PBS
sample sweep is skipped with an explanatory message when the external corpus is
not present. To remove generated artifacts, caches, and build output before
repeating that path, use:

```bash
printf 'y\n' | make rerun
```

The destructive `clean` target requires `confirm`, so an accidental
`make clean` does not silently remove local artifacts.

## Issue #483 trace and symbol contract

Issue #483 links a cluster trace for replay review. Decode or inspect any trace
data only in a temporary directory and do not commit decoded cluster data unless
it has been explicitly approved and anonymized.

For an anonymized exported qtop JSON trace, use:

```bash
make trace-export-validation TRACE_JSON_B64=/path/to/qtop_json.json.b64 TRACE_FULLVIEW=/path/to/qtop_fullview.out
```

This target decodes `base64 -> gzip -> JSON`, rehydrates the exported qtop
document, renders it locally, checks the account-symbol contract in both plain
and colour-on output, checks that `-4` / `--accounttotals` adds the totals row,
checks the combined colour/account-totals rendering, and writes generated
artifacts under `artifacts/trace-export/`, including
`rendered-color-accounttotals.ans` and an ANSI-stripped
`rendered-color-accounttotals.out`.

Expected output rules for this branch:

- section 3 must preserve the same user symbols that the worker-node occupancy
  matrix uses, even after the account table is sorted
- `_` remains reserved for unused/free cores and must never identify a user
- `#` remains reserved for non-existent node/core positions and must never
  identify a user
- `?` remains reserved for unknown node-state style output and help-facing
  conventions, so it is not used as the long-tail user marker here
- `*` is the bounded long-tail user marker for overflow or otherwise reserved
  user initials
- `*` is displayed without mapping it to one specific user's colour rule,
  because it represents a grouped tail rather than a single account
- the colour-on trace artifact must not render `_`, `#`, or `?` as user-account
  identifiers after ANSI sequences are stripped for review
- `-4` / `--accounttotals` currently adds the account totals row; it must not
  change the meaning of user symbols or alter prior sections

## Python 3.6 / AlmaLinux 8 gate

Clusters still running RHEL8-family userspace need a dependency-light check.
The CI job uses an `almalinux:8` container and runs:

```bash
make compat-py36 PYTHON=python3.6
```

That target compiles `qtop_py` and `tools`, then runs the same fast sample gate
with artifacts under `artifacts/sample-gate-py36/`.

## Fortifications

`make fortifications` compares the current branch against
`FORTIFY_BASE_REF`, which defaults to `origin/develop`. It rejects:

- control, bidi, or non-ASCII characters added in the diff
- generated-looking or binary-looking changed paths
- any Python `eval()` call site in the source tree

The generated/binary path check is deliberately conservative because
`CONTRIBUTING.md` asks contributors not to store heavy artifacts in `qtop`.
`make lint` keeps the dependency-light source and diff health checks available
without installing Python packages, `make ruff-check` runs the repository's ruff
configuration after `make ci-deps` has installed pinned CI dependencies, and
`make format-check` runs both `ruff format --check .` and
`ruff format --check --diff .` before checking whitespace errors in the branch
diff against the same `FORTIFY_BASE_REF`.

## Coverage roadmap

The Makefile exposes:

```bash
make coverage
```

That target runs `coverage.py` through the same `PYTHON` override used by the
other targets. To enable coverage in CI without changing the abstraction,
install `requirements-ci.txt` and insert `make coverage` in the modern Python
job after `make ci`, then publish `coverage xml` or terminal summary artifacts
from the workflow. The AlmaLinux 8 / Python 3.6 lane should stay
dependency-light and continue to run `make compat-py36`.

## Develop / CONTRIBUTING.md alignment

Rechecked on 2026-06-02 against `origin/develop` and the current
`CONTRIBUTING.md`: the PR targets `develop`, keeps generated screenshots and
logs as CI artifacts rather than repository files, uses DCO signed commits, and
documents the AI-assisted validation surface in the pull request body.

## Independent structure reference

The same Makefile-as-contract shape is used by other open source projects that
need local, GitHub, and GitLab runs to converge on the same commands. One small
example is `mattermost/mattermost-mattermod`, which has a project `Makefile`
plus both `.github/workflows/ci.yml` and `.gitlab-ci.yml` wired around shared
project commands:

- https://github.com/mattermost/mattermost-mattermod/blob/master/Makefile
- https://github.com/mattermost/mattermost-mattermod/blob/master/.github/workflows/ci.yml
- https://github.com/mattermost/mattermost-mattermod/blob/master/.gitlab-ci.yml
