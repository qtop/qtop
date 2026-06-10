# GitLab linkage experiment

Issue #488 asks for an experimental path where qtop keeps GitHub as the daily
collaboration surface while using GitLab for stronger scheduled CI/CD,
reporting, and security controls. This note records the first reviewable slice
implemented on 2026-06-11.

## Scope selected for review

This PR covers these checklist items from #488:

- realign the CI shape around a thin provider file and Makefile entry points
- add coverage publishing through Cobertura XML
- add a scheduled GitLab matrix with more than 30 distro/python image lanes
- explore GitLab SAST, Semgrep-backed analyzers, Secret Detection, Dependency
  Scanning, and Code Quality through GitLab templates
- add an explicit trust audit for control, bidi, and non-ASCII text
- add an OpenSSF Scorecard job for scheduled or manual GitLab runs
- align next security work to OpenSSF's Secure Coding Guide for Python
- propose how contributors can benefit from GitLab without leaving GitHub
- propose a path for SBOMs and verifiable supply-chain artifacts

The intentionally skipped items for this first slice are:

- a live personal GitLab mirror with a visible project URL, because that
  requires account-side setup outside this repository
- GitLab container registry deployment of the qtop web interface
- DAST against a deployed qtop web surface

Those skips stay within the issue allowance to skip up to three items.

## GitHub-to-GitLab model

Keep `qtop/qtop` on GitHub as the source of truth for issues, pull requests,
DCO, maintainer review, and the normal contributor workflow. Create a pegged
GitLab project that mirrors GitHub and runs extra pipelines:

1. Create an empty GitLab project such as `qtop-github-pegged`.
2. Configure repository mirroring to pull from `https://github.com/qtop/qtop`.
3. Keep `develop` as the default branch in the mirrored project.
4. Add a nightly schedule against `develop`.
5. Keep merge requests and code review on GitHub; use GitLab pipeline links and
   artifacts as additional review evidence in GitHub PR comments.

This keeps the familiar GitHub interface while letting maintainers use GitLab
for scheduled matrix breadth, security analyzers, and richer artifacts.

## Transcend lessons applied

Public GitLab Transcend material for 2026 describes agentic engineering as
needing lifecycle context and human control rather than raw code generation.
For qtop, two practical lessons follow:

- Keep agent-assisted CI changes grounded in local commands. The GitHub and
  GitLab files call Makefile targets such as `make gitlab-ci`,
  `make coverage-xml`, and `make gitlab-build`, so reviewers can rerun the same
  behavior locally before trusting platform output.
- Put expensive or exploratory automation behind schedules and artifacts. The
  broad GitLab matrix and OpenSSF Scorecard run on schedules or manual web
  pipelines, and they upload reports instead of blocking ordinary GitHub review
  on every experimental lane.

Tools used in this slice: GitLab CI templates, GitLab `parallel:matrix`,
coverage.py Cobertura XML, OpenSSF Scorecard, and qtop's
`tools/fortifications.py` trust audit.

References:

- https://about.gitlab.com/events/transcend/virtual/
- https://about.gitlab.com/events/transcend/london/

## GitLab CI shape

The normal GitLab lanes are intentionally close to GitHub Actions:

- `modern-python` runs `make gitlab-ci` on Python 3.12.
- `almalinux-python36` keeps the dependency-light RHEL8-family compatibility
  lane.
- `coverage-cobertura` runs `make coverage-xml` and uploads
  `artifacts/coverage/coverage.xml` as a Cobertura report.
- `build` waits for tests and coverage before running `make gitlab-build`.

The scheduled `nightly-python-matrix` job runs the same `make gitlab-ci` target
across 31 images, including CPython 3.9 through 3.14 release-candidate tags,
PyPy lanes, manylinux, and musllinux. It is `allow_failure: true` because image
availability and prerelease interpreters are signal-gathering work, not a
promise that every platform is ready to gate merges.

## Security analyzer posture

The GitLab file includes:

- `Jobs/SAST.gitlab-ci.yml`
- `Jobs/Secret-Detection.gitlab-ci.yml`
- `Jobs/Dependency-Scanning.v2.gitlab-ci.yml`
- `Jobs/Code-Quality.gitlab-ci.yml`

Those templates cover the available GitLab analyzers for qtop's current Python
and repository shape. GitLab Advanced SAST, License Compliance, and DAST remain
tier- or deployment-dependent follow-ups:

- Advanced SAST: enable when the mirrored project has the needed GitLab tier.
- License Compliance: add once dependency scanning confirms the current
  `requirements-ci.txt` and package metadata are enough input.
- DAST: add only after a scripted qtop web demo deployment exists, so DAST has a
  truthful target URL.

## Trust and secure-coding checks

`make trust-audit` now asks `tools/fortifications.py` to scan reviewable text
files for control, bidi, and non-ASCII characters, and writes a report under
`artifacts/trust-audit/report.txt`. Generated-style fixtures such as
`helpfile.txt` stay out of the full-tree source audit, while the existing
`make fortifications` path still rejects risky added diff lines,
generated-looking paths, and Python `eval()` call sites.

OpenSSF's Secure Coding Guide for Python is a good next reference because it is
framework-independent and targets core CPython behavior. The immediate qtop
mapping is:

- keep command execution explicit and avoid shell-string assembly where Python
  subprocess calls are needed
- keep parsing failures explicit, narrow, and testable
- continue rejecting `eval()`
- keep Unicode/control-character trust boundaries visible in review artifacts

References:

- https://openssf.org/blog/2026/05/12/secure-coding-guide-for-python-pyscg-first-release/
- https://best.openssf.org/Secure-Coding-Guide-for-Python/

## OpenSSF Scorecard

The `openssf-scorecard` job installs Scorecard in a disposable Go image and
writes `artifacts/scorecard/scorecard.json`. It runs only for scheduled or
manual web pipelines and is allowed to fail, because it uses live GitHub state
and should not turn transient GitHub/API availability into a normal PR blocker.

## Reporting and publishing

Coverage is now platform-readable through Cobertura XML. Allure remains a
possible follow-up, but it should be added only after the maintainers agree to
carry the extra dependency in CI. Until then, qtop keeps pytest output,
coverage.py reports, and scheduler sample artifacts as the primary reporting
surface.

## SBOM and supply-chain next steps

The first SBOM milestone should be proposal-only:

1. Generate a CycloneDX or SPDX SBOM from the pinned CI environment.
2. Attach it as a GitLab artifact for scheduled pipelines.
3. Compare the SBOM against `requirements-ci.txt`, `pyproject.toml`, and the
   built wheel metadata.
4. Consider Sigstore or GitLab release evidence only after the package build
   and release flow is stable.

That keeps supply-chain evidence reviewable without changing qtop's release
contract in the same PR.
