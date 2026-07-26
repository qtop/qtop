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

Every CI job -- on either forge -- is a thin wrapper around a Makefile target.
The Makefile is the single source of truth for *what* validation means; the
CI files only decide *where and when* it runs. This keeps GitHub Actions and
GitLab CI behaviourally identical and lets contributors reproduce any CI
failure locally with the same command.

| Concern | Makefile target | GitHub Actions | GitLab CI |
|---|---|---|---|
| Full validation | `make github-ci` / `make gitlab-ci` | `.github/workflows/build.yml` | `ci/test.gitlab-ci.yml` (`modern-python`) |
| Python 3.6 / RHEL8 floor | `make compat-py36` | `.github/workflows/build.yml` (`almalinux-python36`) | `ci/test.gitlab-ci.yml` (`almalinux-python36`) |
| Package build | `make github-build` / `make gitlab-build` | `.github/workflows/build.yml` (`build`) | `ci/build.gitlab-ci.yml` (`build`) |

## Adding a new concern

1. Create `ci/<concern>.gitlab-ci.yml` with a header comment explaining what
   runs, when, and why (see existing child files for the expected register).
2. Add any new stage to the root `stages:` list.
3. Add `- local: ci/<concern>.gitlab-ci.yml` to the root `include:` list.
4. If the concern introduces new validation logic, expose it as a Makefile
   target first and keep the job body a one-line wrapper; put CI-only helper
   scripts in `ci/scripts/`.

## Why this layout

- **Review isolation** -- security, matrix, coverage, and release changes stop
  competing for the same monolithic file; diffs stay per-concern.
- **GitLab linkage** ([#488](https://github.com/qtop/qtop/issues/488)) -- a
  pegged GitLab mirror picks up the same pipeline unchanged, because nothing
  in `ci/` assumes which forge triggered the run; everything routes through
  the Makefile.
- **Precedent** -- the structure is proven on
  [code.europa.eu/pvgis/pvgis](https://code.europa.eu/pvgis/pvgis/), the
  reference named in #488.
