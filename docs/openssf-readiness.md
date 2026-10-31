# OpenSSF readiness

Reviewed 2026-10-30. Keep security metadata and evidence small, public and
reusable across assessment tools; keep integrations outside qtop's runtime.

## Progress checklist

- [x] Add repository-scoped [Security Insights](../security-insights.yml)
  using the released v2.2.0 schema and source-backed policy/evidence links.
- [x] Label README preparation badges explicitly; retain the existing dynamic
  Scorecard badge. Preparation badges do not indicate an earned certification.
- [x] [CI] Maintain test, coverage, fixture, type and fuzz gates through the
  [shared contract](ci-structure.md#the-makefile-contract).
- [ ] Confirm individual administrator/core-team roles before adding the
  project-level Security Insights section; the initial contact is the team.
- [ ] Add pinned schema validation to the shared CI contract if this metadata
  becomes a required gate. Local validation is not marked `[CI]`.
- [ ] Register qtop for Best Practices and review every answer against evidence.
- [ ] Complete an OSPS Baseline assessment and resolve or document gaps.
- [ ] Produce release SBOMs/provenance and document consumer verification.
- [ ] Evaluate policy tooling against a chosen baseline and scoped permissions.

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
registration/self-assessment service, not a GitHub Action to install. A lookup
for `https://github.com/qtop/qtop` found no registration on the review date.
The README therefore links a static **preparing** badge to this checklist.

Use these starting points when completing the
[criteria](https://www.bestpractices.dev/en/criteria/0):

| Evidence area | Repository evidence |
|---|---|
| Purpose, install and usage | [README](../README.md), [Quickstart](quickstart.md), [user guide](documentation.md) |
| License and contribution process | [LICENSE](../LICENSE), [CONTRIBUTING](../CONTRIBUTING.md), [Code of Conduct](../CODE_OF_CONDUCT.md) |
| Vulnerability reporting | [SECURITY.md](../SECURITY.md); confirm the private reporting channel works |
| Automated quality/security checks | [CI contract](ci-structure.md), [security review](secure-coding-pyscg.md); retain revision-specific run evidence |
| Release and supply-chain gaps | [Roadmap](../ROADMAP.md), [supply-chain plan](supply-chain-sbom.md) |

After a maintainer registers and reviews the answers, replace the preparation
badge with the service-provided dynamic badge for the actual project ID:

```markdown
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/PROJECT_ID/badge)](https://www.bestpractices.dev/projects/PROJECT_ID)
```

Keep `PROJECT_ID` out of the README until a real registration exists. The
service badge reports actual progress; do not hard-code a passing status.

## Future integrations

| Option | Next bounded step |
|---|---|
| [OSPS Baseline / ORBIT](https://github.com/ossf/wg-orbit) | Map requirements to existing evidence, complete a self-assessment, then choose automation. ORBIT coordinates resources; it is not a scanner to install. |
| [Gemara](https://gemara.openssf.org/) | Export assessment/control evidence into its model when a consumer requires it; metadata alone is not a Gemara assessment. |
| [Darnit](https://github.com/darnitdevorg/darnit) | Trial an audit on a fixed revision, retain findings and review remediation before enabling changes. |
| [Minder](https://mindersec.dev/) | Choose policy profiles and repository permissions separately from metadata; start with evaluation before enforcement. |
| SLSA provenance | Prefer [GitHub artifact attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations) for the existing GitHub build, with verification of the exact published sdist/wheel digests. |
| Harness or another CI platform | If adopted, reuse Makefile gates and specify the build identity, evidence retention and consumer verification contract before adding provider configuration. |
