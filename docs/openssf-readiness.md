# OpenSSF readiness

Progress checklist and badge references updated 2026-11-02. Keep security metadata and evidence small, public and
reusable across assessment tools; keep integrations outside qtop's runtime.

## Progress checklist

- [x] Add repository-scoped [Security Insights](../security-insights.yml)
  using the released v2.2.0 schema and source-backed policy/evidence links.
- [x] Register qtop as [Best Practices project 16179](https://www.bestpractices.dev/projects/16179)
  and publish its dynamic README badge alongside Scorecard.
- [x] [CI] Maintain test, coverage, fixture, type and fuzz gates through the
  [shared contract](ci-structure.md#the-makefile-contract).
- [ ] Confirm individual administrator/core-team roles before adding the
  project-level Security Insights section; the initial contact is the team.
- [ ] Add pinned schema validation to the shared CI contract if this metadata
  becomes a required gate. Local validation is not marked `[CI]`.
- [ ] Complete the Best Practices assessment and review every answer against
  evidence; registration alone does not establish a passing status.
- [ ] Complete an OSPS Baseline assessment and resolve or document gaps.
  - [x] Run a pinned, report-only Darnit audit (`make osps-audit`, weekly
    `osps-baseline-qtop` workflow); see [OSPS Baseline audit](#osps-baseline-audit).
  - [ ] Review its findings and the manual controls, and record the outcome.
- [ ] Produce release SBOMs/provenance and document consumer verification.
  - [x] Configure SBOM generation, build provenance and SBOM attestations in
    the release workflow, with online and offline verification recipes; see
    [supply-chain plan](supply-chain-sbom.md).
  - [ ] Confirm them on the first published release.
- [x] Evaluate policy tooling against a chosen baseline and scoped permissions;
  see [Future integrations](#future-integrations).

## Security Insights

The [specification](https://security-insights.openssf.org/) discovers lowercase
`security-insights.yml` or `.yaml` at the root or forge directory. Use one
canonical file. Update its review dates and evidence when practices change.
The initial repository-only declaration avoids inventing individual admin
permissions, bug-bounty commitments or release attestations. Its rejection of
automated change requests follows [CONTRIBUTING.md](../CONTRIBUTING.md);
human-reviewed AI-assisted contributions remain allowed.

Validate with [CUE](https://cuelang.org/docs/install/) and the
[released schema](https://github.com/ossf/security-insights/blob/v2.2.0/spec/schema.cue):

```sh
qtop_si_schema_dir=$(mktemp -d)
curl -fsSL https://raw.githubusercontent.com/ossf/security-insights/v2.2.0/spec/schema.cue \
  -o "$qtop_si_schema_dir/schema.cue"
cue vet -d '#SecurityInsights' "$qtop_si_schema_dir/schema.cue" security-insights.yml
```

Run from the repository root. Successful validation prints nothing. CUE is an
optional maintainer tool, not a qtop dependency. Schema validity checks structure;
maintainers must still verify the declarations. Do not use the evolving upstream
`main` schema as an implicit replacement for the declared release.

## Best Practices badge

The [badge application](https://www.bestpractices.dev/en) is a separate
registration/self-assessment service, not a GitHub Action to install. qtop is
registered as [project 16179](https://www.bestpractices.dev/projects/16179), and
the README uses its service-provided dynamic badge. Assessment completion
remains separate from registration.

Use these starting points when completing the
[criteria](https://www.bestpractices.dev/en/criteria/0):

| Evidence area | Repository evidence |
|---|---|
| Purpose, install and usage | [README](../README.md), [Quickstart](quickstart.md), [user guide](documentation.md) |
| License and contribution process | [LICENSE](../LICENSE), [CONTRIBUTING](../CONTRIBUTING.md), [Code of Conduct](../CODE_OF_CONDUCT.md) |
| Vulnerability reporting | [SECURITY.md](../SECURITY.md); confirm the private reporting channel works |
| Automated quality/security checks | [CI contract](ci-structure.md), [security review](secure-coding-pyscg.md); retain revision-specific run evidence |
| Release and supply-chain gaps | [Roadmap](../ROADMAP.md), [supply-chain plan](supply-chain-sbom.md) |

The README badge points to the registered project:

```markdown
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/16179/badge)](https://www.bestpractices.dev/projects/16179)
```

The service badge reports actual progress; do not hard-code a passing status.

## OSPS Baseline audit

`make osps-audit` installs pinned Darnit (`OSPS_TOOLS`, with zizmor for its
workflow checks) into `build/osps-venv` and runs `darnit audit --no-fail -o
json` on the checkout. Remediation is never invoked. The raw audit stays in
`build/` because its evidence includes command output such as the caller's
GitHub profile; `tools/osps_report.py` writes only each control's ID, level,
status and details to `artifacts/osps/osps-baseline.{json,md}`. The weekly
`osps-baseline-qtop` workflow runs it on the default branch with a read-only
workflow token and posts the Markdown to the job summary. It is not a gate.

Run it on the default branch: branch-protection controls inspect the
checked-out branch, so a feature branch reports them as failing. Darnit needs
Python 3.11+ and its tree-sitter wheels (Linux x86-64/aarch64, macOS arm64,
Windows). It warns that its plugins "failed verification but will be loaded
anyway": plugin signatures are not checked, which is acceptable only because
the audit reads and reports.

First local run (2026-11-20, Darnit 0.1.1, OSPS v2026.02.19, feature branch):
66 controls, 49 pass, 13 fail, 4 warn. Level 1 had no failures beyond the
branch-protection controls, which need a default-branch run to judge, and two
controls that need manual verification. The level 2-3 failures are document
patterns Darnit could not find, such as a threat model (OSPS-SA-03.02) and
dependency/SBOM and vulnerability-handling statements. Each needs review
before treating it as a gap, since qtop may document it under another name.

## Future integrations

| Option | Next bounded step |
|---|---|
| [OSPS Baseline / ORBIT](https://github.com/ossf/wg-orbit) | Map requirements to existing evidence, complete a self-assessment, then choose automation. ORBIT coordinates resources; it is not a scanner to install. |
| [Gemara](https://gemara.openssf.org/) | A CUE data model, not a scanner. Author documents only when a consumer requires them. Its OSPS evaluator, [Privateer](https://privateerproj.com/) with `pvtr-github-repo`, was spiked on 2026-11-20 (pvtr 0.24.3, plugin 0.33.2): its docs and plugin README disagree on the config format, and that plugin needs the token in the config file. Retry once a plugin release reads `PVTR_VARS_TOKEN`, as a report-only job like Darnit's. |
| [Darnit](https://github.com/darnitdevorg/darnit) | Adopted report-only; see [OSPS Baseline audit](#osps-baseline-audit). Remediation stays disabled: it writes files and PRs and can change branch protection. |
| [Minder](https://mindersec.dev/) | Not adopted: it needs enrolling the repository in a hosted (Custcodian) or self-hosted service with repository access, a maintainer decision. It does not map to OSPS Baseline and overlaps Scorecard, Dependabot and CodeQL. |
| SLSA provenance | Prefer [GitHub artifact attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations) for the existing GitHub build, with verification of the exact published sdist/wheel digests. |
| Harness or another CI platform | If adopted, reuse Makefile gates and specify the build identity, evidence retention and consumer verification contract before adding provider configuration. |
