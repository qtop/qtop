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

`make pre-pr` combines coverage (including the full pytest/Hypothesis suite),
scheduler samples, deterministic and Atheris fuzzing, lint/format, and type
checks. It requires Linux x86-64 CPython 3.12 with `make ci-deps` installed and
runs pytest once. The CI workflows distribute these same targets across jobs
and interpreter lanes; `make ci` remains the portable shared test gate.

| Concern | Makefile target | GitHub Actions | GitLab CI |
|---|---|---|---|
| Shared test validation | `make github-ci` / `make gitlab-ci` | `.github/workflows/build.yml` | `ci/test.gitlab-ci.yml` (`modern-python`) |
| Python 3.6 / RHEL8 floor | `make compat-py36` | `.github/workflows/build.yml` (`almalinux-python36`) | `ci/test.gitlab-ci.yml` (`almalinux-python36`) |
| Package build | `make github-build` / `make gitlab-build` | `.github/workflows/build.yml` (`build`) | `ci/build.gitlab-ci.yml` (`build`) |

When the coverage gate passes, it produces a Cobertura report. GitHub stores it as
a workflow artifact, while GitLab reads the percentage and report natively.
Neither path requires an external coverage service.

## Adding a new concern

1. Create `ci/<concern>.gitlab-ci.yml` with a header comment explaining what
   runs, when, and why (see existing child files for the expected register).
2. Add any new stage to the root `stages:` list.
3. Add `- local: ci/<concern>.gitlab-ci.yml` to the root `include:` list.
4. If the concern introduces new validation logic, expose it as a Makefile
   target first and keep the job body a thin wrapper; put CI-only helper
   scripts in `ci/scripts/`. For provider-owned analyzers, document the native
   integration and its platform requirements under the exceptions above.

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
