# GitLab linkage pilot: GitHub-first, GitLab-pegged

Status: working model for #488. GitHub stays the collaboration home
(issues, PRs, reviews, bounties); a pegged GitLab project adds the ci/cd,
scanner and compliance machinery GitLab is better at. Contributors never
have to leave GitHub for day-to-day work.

## The model in one diagram

```
contributor PR --> github.com/qtop/qtop  (reviews, DCO, fortifications)
                        |
                        |  pull mirror (GitLab "CI/CD for external repo",
                        |  default <= 5 min cadence on gitlab.com)
                        v
                gitlab.com/<space>/qtop  (pegged, read-only mirror)
                        |
            .gitlab-ci.yml + ci/*.gitlab-ci.yml run unchanged:
            tests, build, SAST, secret detection, dependency
            scanning, code quality, coverage, nightly matrix,
            scorecard
                        |
                        v
            commit status posted back to GitHub via the GitLab
            GitHub integration -> shows as a check on the PR
```

## Why this split

- GitHub: the existing community, the bounty workflow, familiar review UX.
- GitLab: included security templates (SAST/Secret Detection/Dependency
  Scanning run on the free tier), MR-widget coverage rendering from
  Cobertura, pipeline schedules with per-schedule variables, and
  regulated-environment controls -- without asking contributors to migrate.

## Setting up the peg (account-side, one time)

1. GitLab: New project -> "Run CI/CD for external repository" -> GitHub.
   Authenticate with a token that can read qtop and write commit statuses.
2. GitLab pulls the repo and keeps mirroring (pull mirroring; on
   gitlab.com mirrors update on push events or at the platform cadence).
3. The mirrored ``.gitlab-ci.yml`` (this repository, after the pvgis-style
   realign) runs as-is: nothing in ``ci/`` assumes which forge triggered
   the pipeline, because every job wraps a Makefile target.
4. Add two pipeline schedules: nightly with ``NIGHTLY_MATRIX=1`` (35-lane
   matrix) and weekly with ``SCORECARD=1`` plus a read-only
   ``GITHUB_AUTH_TOKEN`` variable.
5. Enable the GitHub integration (Settings -> Integrations -> GitHub) so
   pipeline results appear as commit statuses on GitHub PRs.

A personal namespace works for the experiment (the issue allows it);
moving the peg under a project-owned GitLab group later is a settings
change, not a migration.

## What contributors see

Nothing new is required of them: same fork-and-PR flow, same DCO, same
Makefile commands locally. GitLab appears only as an extra status check
("gitlab/pipeline") plus richer security/coverage data the maintainers can
consult. Contributors who *want* the GitLab view get it read-only at the
mirror URL.

## What works out of the box (explored for #488)

| Feature | OOTB on the pegged mirror | Notes |
|---|---|---|
| Pipelines from mirrored repo | yes | external-repo CI/CD project type |
| SAST (Semgrep-based for Python) | yes, free tier | 4 local baseline findings; see docs/secure-coding-pyscg.md |
| Secret Detection | yes, free tier | one known sample-ID false positive expected |
| SAST IaC (KICS) | yes, free tier | scans the ci yaml itself |
| Dependency Scanning | job runs; findings UI needs Ultimate | local pip-audit baseline: 6 advisories in 2 pinned CI deps |
| License Compliance | template removed upstream | free-tier pip-licenses job kept instead |
| Code Quality | CodeClimate template deprecated | ruff emits the report format natively |
| Coverage MR widget | yes | Cobertura artifact + TOTAL regex |
| Advanced SAST | no (Ultimate) | variable stub left in ci/security.gitlab-ci.yml |
| DAST / Container Scanning | no (needs deployed target/registry) | gated on the unclaimed registry item |
| Pipeline schedules | yes | nightly matrix + weekly scorecard |
| Status back to GitHub PRs | yes | GitHub integration |

## Limits and honesty

- A pull mirror lags push events by the mirroring cadence; it is a
  *reporting* surface, not a second source of truth. Merges happen only on
  GitHub.
- MR-only features (coverage widget on MRs, MR security widget) light up
  fully when GitLab sees merge requests; for mirrored PRs the practical
  surface is branch pipelines plus commit statuses. The configs here run
  in both modes (``rules`` cover MR events and branch pushes) so the same
  files serve a future deeper integration unchanged.
- Scanner findings views vary by tier; everything above is stated against
  the free tier unless marked Ultimate.

## Relation to the #488 checklist

This document covers the contributor-benefit model item and records the
out-of-the-box exploration; the pegged-repo creation itself is account-side
and intentionally not claimed as completed inside this repository. SBOM
and supply-chain next steps live in docs/supply-chain-sbom.md; secure-coding
alignment lives in docs/secure-coding-pyscg.md; the CI layout contract
lives in docs/ci-structure.md.
