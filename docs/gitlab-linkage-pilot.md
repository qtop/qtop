# GitLab linkage pilot: GitHub-first validation

Status: repository configuration prepared for #488; account-side validation
remains unverified. No live GitLab project, mirror, schedule, or status
integration is claimed by this document.

## Progress checklist

- [x] Track GitLab jobs and shared Makefile targets for tests, builds, coverage
  and selected security reports. See [CI structure](ci-structure.md).
- [x] Provide nightly-matrix and Scorecard jobs with opt-in variables.
- [ ] Connect an eligible GitLab project to the canonical GitHub repository.
- [ ] Validate an upstream-branch pipeline and retain its artifact evidence.
- [ ] Verify supported events and GitHub commit-status reporting; document
  fork-PR limitations.
- [ ] Activate and verify nightly and weekly schedules as described below.

Checked items record repository support, not a successful live pilot.

GitHub remains the collaboration home for issues, pull requests, reviews, DCO,
and bounties. A qualifying GitLab project can add a second validation surface
for upstream branches, schedules, and some external pull requests.

## Supported model

```
github.com/qtop/qtop upstream update
                 |
                 | GitLab CI/CD for external repositories
                 v
       qualifying GitLab project
                 |
       tracked .gitlab-ci.yml configuration
                 |
       tests, build, coverage, selected scanners,
       nightly matrix, and Scorecard artifacts
```

GitLab's CI/CD for external repositories feature requires Premium or Ultimate.
Its GitHub pull-request pipelines use
`CI_PIPELINE_SOURCE == "external_pull_request_event"`. Pull requests from
forks are ignored, so qtop's normal fork-and-pull-request contributors cannot
be promised a GitLab pre-merge check. Upstream branch and scheduled pipelines
remain useful for post-push validation.

## Account-side setup

- [ ] On an eligible GitLab tier, create a project using "Run CI/CD for external
   repository" and connect the canonical GitHub repository.
- [ ] Confirm which GitHub events and commit statuses are available to the chosen
   integration. Do not assume fork pull requests are covered.
- [ ] Validate the tracked configuration on an upstream branch.
- [ ] Add a nightly schedule with `NIGHTLY_MATRIX=1`.
- [ ] Add a weekly schedule with `SCORECARD=1` and a read-only
   `GITHUB_AUTH_TOKEN`.

These are maintainer-owned settings. They cannot be completed or demonstrated
by repository changes alone.

## Feature boundaries

| Feature | Repository support | Account or tier boundary |
|---|---|---|
| Tests and package build | tracked jobs and Makefile targets | runner and external-repository project required |
| Basic SAST | standard analyzer template | basic analyzer and JSON report are available on all tiers; richer UI features vary |
| Secret Detection | standard template | basic pipeline scanning is available on all tiers |
| Dependency Scanning | v2 analyzer configured for `requirements-ci.txt` | Ultimate; template rules cover branch and native GitLab MR pipelines |
| Code Quality | ruff emits GitLab report JSON | artifact inspection depends on GitLab UI support |
| Coverage | Cobertura XML and percentage regex | GitLab MR widgets require a GitLab MR; branch artifacts still work |
| Nightly matrix | opt-in interpreter/distro jobs | schedule must be created account-side |
| Scorecard | digest-pinned image and JSON artifact | schedule and GitHub token must be configured |
| GitHub commit status | possible integration output | must be configured and verified; fork PRs are not covered |

Local `pip-audit` remains the cross-tier dependency evidence. The dated local
integration baseline on 2026-07-26 is 7 advisories in 3 pinned CI packages.

## Contributor impact

Contributors keep using GitHub and the documented local Makefile targets.
GitLab is an additional maintainer validation surface, not a required
contributor destination and not a guaranteed PR check.

## Relation to the #488 checklist

This document records a technically bounded model and its limitations. The
account-side pilot remains unverified. SBOM proposals are in
`docs/supply-chain-sbom.md`, secure-coding evidence is in
`docs/secure-coding-pyscg.md`, and the repository layout is in
`docs/ci-structure.md`.
