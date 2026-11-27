# CI structure

qtop's GitLab pipeline follows the layout of
[pvgis](https://code.europa.eu/pvgis/pvgis/): a thin root `.gitlab-ci.yml`
holding only shared defaults (`image`, `variables`, `stages`) and one
`include: local:` line per concern, with the actual jobs living in
per-concern child files under `ci/`. CI-only helper scripts go to
`ci/scripts/`. GitLab-side templates (merge requests) live under `.gitlab/`,
mirroring `.github/`.

```
.gitlab-ci.yml              # root: defaults + stages + includes, no jobs
ci/
  test.gitlab-ci.yml        # test concern: modern python + py3.6/rhel8 gate
  build.gitlab-ci.yml       # build concern: sdist/wheel
  scripts/                  # CI-only helpers (never on the runtime path)
.gitlab/
  merge_request_templates/  # mirrors .github/PULL_REQUEST_TEMPLATE.md
```

## The Makefile contract

Project-owned test, build, coverage, and reporting logic lives in Makefile
targets, `tools/`, or `ci/scripts/`, called by thin platform wrappers. Provider
setup, permissions, artifacts, and publishing stay in platform configuration.
CodeQL's initialization/analysis actions, Scorecard's restricted publishing
action and GitLab CLI entry point, and included GitLab security analyzers are
explicit provider-native exceptions. Scorecard's branch scan uses the shared
`make scorecard-scan` target. These exceptions do not permit duplicating qtop's
own validation logic in YAML.
The weekly `osps-baseline-qtop` workflow is GitHub-only because its OSPS
Baseline controls read GitHub repository settings; it calls the shared
`make osps-audit` target and is report-only (see
[OpenSSF readiness](openssf-readiness.md#osps-baseline-audit)).
Codacy analyses `qtop_py/` and the CI configuration; `.codacy.yml` excludes
`tests/` and `tools/`, harnesses that run subprocesses and use fixture paths by
design and are covered by ruff, CodeQL and the gates above. Runtime findings left
by design carry `# nosec <rule> - reason` (and `nosemgrep` where Semgrep also
matches) at the line; Bandit reports none in `qtop_py/`. Two kinds of Codacy
finding stay by design and are marked in Codacy rather than changed:
`defusedxml` for the SGE parser, which rejects every DTD and keeps qtop free of
runtime dependencies, and `id-token: write` in `release.yml` and `scorecard.yml`,
an OIDC permission (PyPI trusted publishing, Scorecard publishing), not a
stored token. Its report that `AGENTS.md` cites a missing `CONTRIBUTING.md` is
wrong: the file is at the repository root.

`make pre-pr` combines coverage (including the full pytest/Hypothesis suite),
scheduler samples, deterministic and Atheris fuzzing, lint/format, and type
checks. It requires Linux x86-64 CPython 3.12 with `make ci-deps` installed and
runs pytest once. GitLab runs `make pre-pr coverage-xml` in its primary PR job:
coverage, samples, lint, types and fuzzing share one environment. GitHub runs
the same checks split across two workflows, so the `coverage-qtop` badge
reports only the coverage gate: `.github/workflows/coverage.yml` runs
`make coverage-xml`, and the 3.12 lane of `build.yml` runs the other `pre-pr`
prerequisites. `tests/test_ci_matrix.py` fails if the split drifts from `pre-pr`.
GitLab emits its Ruff code-quality report in `after_script`, including on
validation failures. `make ci` remains the portable shared test gate and the
additional modern GitHub interpreter lane.

| Concern | Makefile target | GitHub Actions | GitLab CI |
|---|---|---|---|
| Primary PR validation | `make pre-pr coverage-xml` | `.github/workflows/build.yml` (`modern-python`, all but coverage) | `ci/test.gitlab-ci.yml` (`modern-python`) |
| Coverage gate and Cobertura | `make coverage-xml` | `.github/workflows/coverage.yml` | `ci/test.gitlab-ci.yml` (`modern-python`) |
| Python 3.6 / RHEL8 floor | `make compat-py36` | `.github/workflows/build.yml` (`almalinux-python36`) | `ci/test.gitlab-ci.yml` (`almalinux-python36`) |
| Validated package build | `make github-build` / `make gitlab-build` | `.github/workflows/build.yml` (`build`) | `ci/build.gitlab-ci.yml` (`build`) |

Both build entry points use `make validated-build`: build into `DIST_DIR`
(default `dist/`), then run `make release-check` on exactly one sdist and one
wheel. Use a fresh directory; extra or stale distributions fail validation.
The checker installs the supplied wheel and a wheel rebuilt from the
supplied sdist in separate disposable environments, outside the checkout import
path. It checks version metadata, entry points, bundled defaults, help, demo and
committed PBS/OAR/SGE/Slurm samples, and reports unchanged input SHA-256 digests.
Rebuilding the sdist uses the current pinned backend without dependency downloads.
The original release files are never replaced by that rebuild.
Then `make sbom sbom-ci` writes CycloneDX SBOMs of the installed wheel and of
the build environment to `artifacts/sbom/`, kept as build artifacts; see
[supply-chain-sbom.md](supply-chain-sbom.md).

The GitHub release workflow already calls `make github-build`, so validation must
pass before its artifact upload and downstream PyPI publishing job. GitLab's build
job uses the same gate. This does not verify PyPI account configuration, successful
publication or every supported interpreter on the exact release files.

```sh
make validated-build                    # build and validate dist/
make release-check DIST_DIR=/path/to/dist # validate an existing pair; leave its files unchanged
```

When the coverage gate passes, it produces a Cobertura report. GitHub stores it as
a workflow artifact, while GitLab reads the percentage and report natively.
Neither path requires an external coverage service.

### BASELINE image (proof of concept)

BASELINE is `almalinux:8` with `make`, `findutils` and `python36` preinstalled
(`ci/baseline/Dockerfile`), so a Python 3.6 job runs no `dnf` and a slow mirror
can break only the image rebuild, never a push. For now it runs only by hand:
`.github/workflows/baseline-image.yml` (`workflow_dispatch`, available once the
file is on the default branch) builds it, publishes
`ghcr.io/<owner>/qtop-baseline:almalinux8-py36` plus a `YYYYMMDD-HHMM` (UTC) tag
per build, listed with its digest in the run summary, and then runs
`make compat-py36` in it, pulling with the job's `GITHUB_TOKEN`. Publishing
needs `packages: write`. Dependabot bumps the Dockerfile's base image digest.

Before it can replace `almalinux-python36`:

- re-enable the parked weekly schedule and `develop` trigger;
- check pulls from fork pull requests, or make the GHCR package public;
- pull it by digest rather than tag (Scorecard flags tags), and automate the
  digest bump after each rebuild, for example as a bot pull request;
- give GitLab the same job (`ci/test.gitlab-ci.yml`), as both platforms run the same checks.

## Release preparation

On a branch whose first eight characters are a valid next-release date
(`YYYYMMDD`), preview changes before writing them:

```sh
python -m tools.prepare_release
python -m tools.prepare_release --write
```

`--version` overrides the derived `0.9.YYYYMMDD`. The version must advance;
existing tags and dirty release-file overwrites are rejected. The helper
synchronizes package metadata, the manpage, changelog and EasyBuild recipe.
`tests/test_prepare_release.py` covers preview/history preservation and invalid
inputs. Preparation neither commits nor tags nor publishes; exact artifact
validation and independent release approval remain separate requirements.

Tag the release `v<version>`: the EasyBuild recipe downloads that tarball.
`release.yml` runs `make release-tag-check` first, so a published release whose
tag differs from `qtop_py.__version__` fails before building or reaching PyPI.
This gate is temporary until the 1.0 namespace change.

After `--write`, the helper prints the annotated `git tag -a v<version>` command to
run on the merge commit. `make release-audit` reports CHANGELOG sections without a
tag, bare or lightweight tags and tags whose `__version__` differs, from local tags
(`git fetch --tags` first). It is non-blocking for now: `--strict` (via
`RELEASE_AUDIT_ARGS`) exits 1 on findings, and it is not part of `pre-pr` or CI.

## Nightly compatibility lanes

The nightly matrix runs as two separately reported tiers, mirrored lane for lane
on both forges:

| Tier | Where | Lanes | A failure means |
|---|---|---:|---|
| core | `nightly-core.yml`; GitLab job `nightly-core` | 8 | a qtop regression: fix it before the next release |
| wide | `nightly-wide.yml`; GitLab job `nightly-wide` (`allow_failure`) | 26 | possible distro, mirror or interpreter drift: look when convenient |

The core lanes take one lane per dimension that has caught regressions:
python.org CPython 3.12 and 3.15, Debian bookworm and Ubuntu 24.04 system
Python, Fedora 42, AlmaLinux 9 with Python 3.12, and the runtime floors
AlmaLinux 8 Python 3.6 and Rocky Linux 9 system Python 3.9 (`compat-py36`).
Pull requests also run the core `debian:bookworm` lane (job `nightly-lane` in
`build.yml` and `ci/test.gitlab-ci.yml`): as root, with the distro's Python and
a checkout owned by another user, it meets the conditions behind most nightly
failures before merge. It needs a Linux container runtime, so on macOS rely on
the PR job rather than reproducing it locally.

Scheduled runs test `develop`: GitHub schedules the default branch, `main`, which
only moves at releases, so the checkout switches to `develop`, and the GitLab
schedule targets `develop`. Manual runs test the ref they are started on, so
run both workflows on `main` before tagging a release.

All lanes use `ci/scripts/matrix_lane.sh` and the shared CI pins.
A failed lane gate (`ci-deps`, `nightly-ci`, `compat-py36`) is retried twice after a
one-minute wait that prints a dot a second, ten per line (`ci/scripts/retry.sh`;
`LANE_RETRIES`, `LANE_RETRY_WAIT`). Each retry is logged, and annotated on GitHub,
so a flake stays visible on a green run. The PR lanes set `LANE_RETRIES=0`.
Every lane that installs them runs Python 3.10+, so all get the patched
setuptools 84.2.1 and pytest 9.2.0. Python 3.10-3.11 also install wheel 0.48.0
so the installation check uses a backend that supports the project's PEP 639
license metadata, instead of inheriting an older distro backend. Distributions
whose system Python is 3.9 (RHEL 9 and its rebuilds, Amazon Linux 2023) run the
dependency-light `compat-py36` gate, as Python 3.6-3.8 do.

The build requirement in [pyproject.toml](../pyproject.toml) keeps setuptools
ranges for Python 3.6-3.9 users who build from source; CI never installs those
older releases, and Python 3.6 installs the wheel built on CPython 3.12.

The three PyPy lanes (wide tier) use `make compat-py36`, the existing
stdlib-only compatibility gate: syntax compilation, unittest regressions
(including XML DTD rejection and parser budgets) and all scheduler sample gates.
They do not install `requirements-ci.txt` or run the full pytest suite.
The full test, coverage, type-checking and fuzzing gates remain on CPython;
qtop itself has no required third-party runtime dependencies.

The earlier [PyPy failures](https://github.com/qtop/qtop/actions/runs/39755187632)
combined missing DTD callbacks in PyPy's ElementTree with unsupported developer
tooling (`librt`). The parser now registers its DTD rejection directly with
Expat when exposed by ElementTree; the dependency-free lane avoids importing
the CPython tooling stack.

## Adding a new concern

Extend an existing test table, tool configuration or Make prerequisite first.
A new scheduler usually needs contract cases and sample data, without another
script or workflow. Add a provider job only for a different interpreter,
privilege boundary or genuinely independent integration. Keep its entry point
in Make; document provider-owned analyzers and their requirements here.

The [consolidation inventory](verification-consolidation.md) records retained
entry points, Unicode detection boundaries and measured savings.

## Why this layout

- **Review isolation** -- security, matrix, coverage, and release changes stop
  competing for the same monolithic file; diffs stay per-concern.
- **GitLab linkage** ([#488](https://github.com/qtop/qtop/issues/488)) -- the
  same tracked configuration can validate upstream branch updates and
  qualifying external pull requests. Account-side setup and GitLab tier
  limitations are documented separately.
- **Precedent** -- the structure is proven on
  [code.europa.eu/pvgis/pvgis](https://code.europa.eu/pvgis/pvgis/), the
  reference named in #488.

## Scorecard branches

The publishing action supports only the repository's default branch. On that
branch, it updates the public OpenSSF API/badge and GitHub code scanning.
Pushes to `develop` and manual runs on other branches use `make scorecard-scan`
with the exact triggering commit instead. These runs retain JSON and SARIF
artifacts and upload SARIF to code scanning without publishing to the public API.
Commit-specific scans run only checks supported by Scorecard at a named commit;
their results are not a full replacement for the default-branch score.

To reproduce a branch scan locally, export a read-only `GITHUB_AUTH_TOKEN` and
run `make scorecard-scan` with Docker available. `SCORECARD_REPO` defaults to
`github.com/qtop/qtop`; `SCORECARD_COMMIT` defaults to `git rev-parse HEAD` and
must exist in that remote repository. Both values can be overridden in the
environment. The GitLab scheduled report scans the canonical default branch.


## Schema compatibility gate

Both providers run `make schema-ci` in a Go 1.26 Bookworm container, separately
from the Python gates. This runs the Go contract tests, then compares the
working contract with the contract at the merge base of `FORTIFY_BASE_REF`
(default `origin/develop`). Narrowing any supported schema version fails.
When that historical revision has no schema file, the gate reports the first
introduction explicitly; the committed-schema tests still run. To reproduce:

```sh
make schema-ci FORTIFY_BASE_REF=origin/develop
```

`make pre-pr` remains the Python gate; schema changes additionally require
`make schema-ci` and Go 1.26 or newer. CUE checks structural compatibility;
Python tests own referential integrity and scheduler-specific constraints.


## Terminal integration and soak evidence

`make test` includes real POSIX PTY tests for watch refreshes, single-key input,
window resize, capture/export, and restoring terminal attributes after `q`,
SIGINT, and SIGTERM. No scheduler service is required for these demo tests. They are skipped on
native Windows before importing the POSIX harness dependencies. Run the full
Linux suite through WSL on Windows; native Windows support is not certified.
`make soak` runs the same harness for 60 seconds and writes revision, Python,
backend, observed frame intervals, peak Linux RSS, output-file high water, and
capture counts to `artifacts/soak/report.json`, with an ANSI transcript beside it.
Default budgets are 256 MiB RSS, 1,000 files and 10 seconds between frames.
Reports and raw captures may contain identities; review before sharing.

```sh
make soak SOAK_ARGS='--duration 300 --backend slurm --source tests/plugins/slurm_samples/basic'
# On an authorized scheduler host; this explicitly runs live scheduler commands:
make soak SOAK_ARGS='--duration 3600 --backend slurm --live --report artifacts/soak/slurm.json'
```

Repeat on PBS, SGE and OAR hosts and record scheduler version and cluster size
alongside the report. Fixture and demo runs cannot certify live scheduler behavior.
Capture archives are checked for integrity; portable capture/replay semantics
remain covered by `tests/test_sample.py`. Live evidence remains open in ROADMAP.md.


## Coverage interpretation

The shared coverage targets measure branches as well as statements. Runtime
statement coverage has a 95% floor; branch coverage has a separate 93%
floor, so one metric cannot conceal a regression in the other. Coverage.py's
`TOTAL` combines statements and branches; the gate compares their unrounded
percentages separately. The JSON report
at `artifacts/coverage/details.json` retains per-file missing lines and branches.
Developer tooling is measured and printed separately, without an initial floor;
its uncovered paths must not dilute or inflate the runtime gates. Both providers
publish the runtime Cobertura report. Prioritize missing cleanup, capture and
collection branches rather than writing tests only to increase percentages.


## Fuzz input scope and corpus reuse

Atheris exercises direct bounded YAML configuration, SGE XML, PBS JSON, OAR
rows, Slurm rows and snapshot JSON documents, alongside the existing state/range
properties. Inputs are capped at 4 KiB per document. Known syntax/shape rejections
are allowed; unexpected exceptions and assertions fail the run. Reviewed seed
documents live as hex text under `tests/fuzz_corpus/`; `coverage-fuzz` decodes
them without replacing locally discovered inputs. Preserve `artifacts/fuzz/`
between local runs. CI publishes that corpus for reproduction; it does not
silently trust arbitrary corpus artifacts from other revisions or contributors.


## Python 3.6 runtime contracts

`compat-py36` now runs `test_legacy_contracts.py` as well as the existing
parser/security compatibility checks. These stdlib-only contracts cover invalid
configuration timeouts, successful pipe draining, timeout kill/reap, and schema
v1/v2/v3 anonymization, archive import and JSON export. They run under pytest on
modern interpreters and unittest on the Python 3.6/PyPy lanes, without importing
pytest or modern analysis dependencies. This is a bounded contract suite, not a
claim that the entire modern pytest suite runs on Python 3.6. The exact 3.6/RHEL8
run evidence remains owned by its dedicated CI lane.


## Extended property testing

Both nightly workflows have a dedicated CPython 3.12 job calling
`make nightly-properties`. It selects the existing 1,000-example Hypothesis
profile once per pipeline, rather than multiplying the cost across every distro
lane. Regular PR tests keep the deterministic 100-example profile. Failing
examples remain in the job output and `.hypothesis/` is retained as an artifact.
Locally run `make nightly-properties`; no scheduler access is required.


## Branch coverage and container checkout setup

The shared targets default to `COVERAGE_CORE=ctrace`. Python 3.12 cannot
measure branches with coverage's sysmon core; its fallback warning would be
inherited by instrumented subprocesses and contaminate their stderr. GitHub
and GitLab therefore use the same branch-capable default, keeping exact-output
assertions intact. Explicit local core overrides remain available.

The GitHub coverage-qtop job is the exception: it runs on CPython 3.15.2, like
build.yml's newest lane, where the sysmon core measures branches, with
`COVERAGE_CORE=sysmon`. That cuts the
instrumented suite by about a quarter. The gate covers `qtop_py/` only, so the
Atheris tests skipped there (atheris is pinned to 3.12) do not affect it.

Schema container jobs trust only their provider's checkout directory before
running Git commands. This handles bind-mount ownership without disabling Git's
ownership check globally or trusting unrelated repositories.


## CI tool updates from PR #591

The compatible updates from #591 are included: setuptools 84.2.1, build 1.6.1,
coverage 7.16.2, importlib-metadata 9.0.1, iniconfig 2.3.1, pluggy 1.7.0,
pytest 9.2.0, Ruff 0.17.3, wheel 0.48.0 and zipp 4.1.2. Identical old/new
interpreter pins are consolidated; the Ruff hook matches the CI pin.

The proposed setuptools 84.2.1 requirement on Python 3.9 is deliberately not
adopted: that release requires Python 3.10+. Python 3.9 retains its compatible
`>=78.1.1,<83` backend range, and Python 3.6-3.8 retain `>=59.6,<60`.
The modern source-build floor is updated to 84.2.1 on Python 3.10+ only.


`make release-check` emits `artifacts/release/evidence.json` only after both
installations and unchanged-input hash checks pass. It records version, source
revision (null outside Git), Python and exact artifact hashes/sizes. A previous
report is removed before validation; failed runs cannot reuse stale success.
This evidence supports independent review and does not authorize publication.
The revision is null for archive checkouts or when Git is unavailable;
the installation validator does not require Git.


Preview release-file changes with `python -m tools.prepare_release`. It derives
`0.9.YYYYMMDD` from the branch prefix, or accepts `--version`. `--write` applies
the reviewed plan only when release files are clean and no matching local tag
exists. It does not commit, tag, fetch checksums or publish. Run the release
gates and obtain independent review before those separate operations.

The ci-matrix-check target (included in lint and pre-pr) compares expanded
nightly lane multisets, including package/interpreter choices, Make target
and tier; it also checks that PRs run the core debian:bookworm lane. Matrix edits must update both provider files together.
PyYAML is pinned for developer validation only; qtop keeps its stdlib parser.

GitHub build and release workflows retain the verified record in the separate
qtop-release-evidence artifact. GitLab retains it with the build artifacts.

Terminal probes also record a null revision when Git is missing or refuses the
checkout ownership. The PTY, capture and terminal-restoration checks still run;
nightly containers need no additional Git ownership exception for this metadata.

The Arch nightly container upgrades packages together with pacman -Syu in both
checkout bootstrap and the shared lane runner. Mixing a current package index
with an older base image can otherwise cause OpenSSL file conflicts.
