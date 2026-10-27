# qtop Roadmap

qtop's immediate goal is a validated 1.0 release: dependable terminal
monitoring across supported schedulers, reproducible installation,
actionable failures and documented compatibility.

**Python 3.6 compatibility is a requirement for 1.0.** qtop must remain
usable on common Linux HPC systems with limited internet access, no administrator
privileges and minimal Python installations, including the widespread rhel8/py36 combination.

Last reviewed: 2026-10-12

## How to Read This Roadmap

- **Now** identifies work needed to validate and deliver 1.0.
- **Next** identifies improvements to pursue after the release gates.
- **Later** identifies proposals that need an owner, evidence, or a design.
- A checked item means its acceptance criteria are met on `main`.
  Release availability is recorded separately in the changelog.

Issues track implementation details. This roadmap tracks priorities,
dependencies and completion criteria. Update it when a milestone changes
or a release is prepared.

## Now: Validate and Deliver 1.0

### Compatibility and Runtime Portability

- [ ] Validate Python 3.6 compatibility throughout the release path.
  - Keep runtime syntax and standard-library usage compatible with 3.6.
  - Run the dependency-light AlmaLinux 8 / Python 3.6 lane (part of the CI/CD cycle)
  - Verify installation, demo startup, CLI behavior and scheduler fixtures.
  - Confirm package metadata and build tooling permit a documented installation path on Python 3.6.
  - Keep modern developer tools isolated from the runtime and legacy lane.

- [ ] Document the supported environment matrix.
  - Identify primary Linux environments and tested Python versions.
  - Distinguish runtime compatibility from developer-tool requirements.
  - State the scope of Windows support and portability checks.
  - Track newer Python compatibility in [#546](https://github.com/qtop/qtop/issues/546).

### Scheduler Validation and Soak Testing

- [ ] Complete a documented soak-testing campaign.
  - Track reports in [#567](https://github.com/qtop/qtop/issues/567).
  - Record qtop revision, scheduler version, Python version, cluster size, test duration and observed failures.
  - Exercise sustained watch mode, refreshes, filtering, resizing, capture and replay.
  - Observe memory use, refresh latency and temporary-file growth.
  - Triage findings and resolve release blockers before declaring 1.0 ready.

- [ ] Establish explicit validation coverage for SLURM, PBS, SGE and OAR.
  - Record fixture coverage and live-test evidence separately.
  - Cover empty clusters, missing fields, unusual node and job states, parallel jobs and inconsistent scheduler snapshots.
  - Verify job, queue, node and core totals against fixture expectations.
  - Resolve or explicitly defer reproducible correctness bugs.
  - Define and test queue/partition ordering: [#560](https://github.com/qtop/qtop/issues/560).

### Predictable Failures and Safe Capture

- [ ] Make expected operational failures actionable.
  - Handle missing scheduler commands, invalid configuration, unreadable traces and unusable input with concise messages.
  - Use consistent nonzero exit codes for failed operations.
  - Preserve detailed diagnostics in debug mode or logs.
  - Add regression coverage for representative failure paths.
  - Track related QA work in [#484](https://github.com/qtop/qtop/issues/484).

- [ ] Validate capture and anonymization behavior.
  - Verify generated samples remain useful for reproducing bugs.
  - Check identifiers are anonymized consistently across related files.
  - Test archive creation and reading, including rejection of unsafe entries.
  - Document anonymization scope and remaining limitations.
  - Resolve the experimental-status decision for `-A`: [#548](https://github.com/qtop/qtop/issues/548).

### Packaging and Release Delivery

- [ ] Validate release artifacts before publication.
  - Build wheel and source distribution with checked version metadata.
  - Install both artifacts in clean environments.
  - Verify required configuration and documentation files are included.
  - Run demo, CLI and committed scheduler-sample checks from the installation.
  - Include the Python 3.6 installation path in artifact validation.
  - Make release preparation and dry-run checks reproducible.

- [ ] Publish a current, consistent release.
  - Align the source version, Git tag, release notes and PyPI artifacts.
  - Summarize user-visible changes and known limitations.
  - Verify installation from the published package.
  - Update installation documentation once publication is confirmed.

### Documentation and Release Decision

- [ ] Validate the 1.0 user documentation.
  - Check quickstart, installation, capture, replay, export and troubleshooting examples against the release revision.
  - Document scheduler-specific requirements and supported environments.
  - Identify experimental features individually.
  - Preserve historical release information.
  - Track documentation work in [#498](https://github.com/qtop/qtop/issues/498) and history reconciliation in [#489](https://github.com/qtop/qtop/issues/489).

- [ ] Record the 1.0 release decision.
  - Link compatibility, artifact, scheduler and soak-test evidence.
  - Confirm no unresolved release-blocking failures remain.
  - List deferred issues and known limitations with their rationale.
  - Record the validated release revision.

## Next: Strengthen Data and Maintainability

- [ ] Define the machine-readable output contract.
  - Audit existing `-E` behavior before proposing additional export modes.
  - Document fields, types, timestamps, units and missing-value semantics.
  - Define schema-version and compatibility expectations.
  - Add fixtures that verify exported state matches scheduler expectations.
  - Open a separate proposal if live streaming has a demonstrated use case.

- [ ] Separate parsing, normalized state and presentation incrementally.
  - Select bounded extractions with a concrete testing or maintenance benefit.
  - Preserve terminal output and scheduler behavior through regression checks.
  - Keep extracted interfaces compatible with Python 3.6 while it is supported.
  - Avoid making a broad internal rewrite a prerequisite for feature fixes.

- [ ] Expand scheduler regression coverage as bugs are resolved.
  - Add a focused fixture for each reproducible parsing or accounting failure.
  - Use synthetic or explicitly approved anonymized snapshots.
  - Document fixture provenance and expected behavior.
  - Keep routine validation fast and reproducible.

## Later: Conditional Expansion

These items are proposals, not 1.0 release requirements.

- [ ] Add LSF support.
  - Requires an owner and representative synthetic or approved anonymized traces.
  - Define the supported command/output variants and normalized-state mapping.
  - Include parser, accounting and terminal smoke checks.

- [ ] Provide EasyBuild packaging.
  - Requires a site use case and a maintainer able to validate deployment.
  - Verify configuration discovery and operation without administrator privileges.

- [ ] Define an initial telemetry vocabulary and optional export integration.
  - Track [#553](https://github.com/qtop/qtop/issues/553).
  - Specify useful measurements, units, identifiers and cardinality limits.
  - Reuse the normalized-state contract where practical.
  - Keep integration dependencies optional and isolated from terminal monitoring.

## Delivered Foundations

The following are recorded foundations to preserve and revalidate for 1.0:

- [x] Python 3 migration and removal of Python 2 runtime support.
- [x] Slurm support with committed trace fixtures.
- [x] Shared Makefile entry points for local, GitHub and GitLab CI.
- [x] Fast committed SLURM, PBS, SGE and OAR sample gates.
- [x] A dependency-light AlmaLinux 8 / Python 3.6 compatibility lane.
- [x] Display support for clusters with more than 1,000 nodes.

### Implemented CI/CD Tooling

Checked items below mean the tooling or workflow is implemented in this
repository. They do not certify a particular run, release, or external
service activation. The release gates above still require evidence for the
chosen release revision.

#### Shared Local Validation and Artifacts

- [x] Shared Makefile validation entry points: `ci`, `github-ci`,
  `gitlab-ci` and the git-free `nightly-ci` path ([Makefile](Makefile)).
- [x] Ruff lint and format checks, explicit fix targets and a local
  pre-commit Ruff hook ([.pre-commit-config.yaml](.pre-commit-config.yaml)).
- [x] Modern-Python type checking with ty through `make type-check`.
- [x] Pinned modern CI dependencies with a separate dependency-light legacy
  path ([requirements-ci.txt](requirements-ci.txt)).
- [x] pytest suite execution through `make test`.
- [x] coverage.py terminal reporting and Cobertura XML through
  `make coverage` and `make coverage-xml`.
- [x] Diff/source fortifications: generated-file review warnings, added-text
  checks and AST checks rejecting `eval()` and wildcard imports
  ([tools/fortifications.py](tools/fortifications.py)).
- [x] Tracked-text UTF-8, bidi, invisible-character and confusable-character
  audits with report artifacts through `make repo-sanity`.
- [x] Seeded parser stress checks through `make deterministic-fuzz`.
- [x] Committed PBS, SGE, Slurm, OAR and demo fixture gates through
  `make sample-gate` / `make backend-validation`.
- [x] ANSI-colour text and SVG backend review artifacts through
  `make backend-colour-artifacts` / `make render-backends`.
- [x] Exported JSON trace validation through `make trace-export-validation`;
  requires a supplied `TRACE_JSON_B64` payload.
- [x] Larger archived PBS sample sweep through `make test-pbs-samples`;
  requires the external sample corpus and skips when it is absent.
- [x] Slurm parser tests and committed-sample rendering through
  `make test-slurm-samples`.
- [x] Python 3.6 syntax compilation and dependency-light scheduler sample
  validation through `make compat-py36`.
- [x] Independent GitHub Actions auditing and safe fixes through
  `make zizmor` / `make zizmor-fix`.

#### GitHub Actions

- [x] Branch/PR validation across configured modern Python versions,
  a separate AlmaLinux 8 / Python 3.6 gate and a ty job; package builds
  depend on these checks ([build.yml](.github/workflows/build.yml)).
- [x] Manual shared pytest/CI workflow
  ([pytest.yml](.github/workflows/pytest.yml)).
- [x] Coverage workflow retaining Cobertura XML artifacts
  ([coverage.yml](.github/workflows/coverage.yml)).
- [x] Scheduled/manual distro and interpreter matrix with separate
  experimental lanes and retained validation artifacts
  ([nightly-matrix.yml](.github/workflows/nightly-matrix.yml)).
- [x] OpenSSF Scorecard analysis, SARIF artifacts/code-scanning uploads
  and public-result publication from `main`
  ([scorecard.yml](.github/workflows/scorecard.yml)).
- [x] Commit-SHA-pinned actions, explicit workflow permissions and
  checkout without persisted credentials.
- [x] Published-release workflow that validates, builds distributions
  and publishes through the `pypi` environment using OIDC
  ([release.yml](.github/workflows/release.yml)); successful delivery
  requires the corresponding PyPI trusted-publisher/environment setup.

#### GitLab CI

- [x] Modular test, build, security, coverage, nightly-matrix and Scorecard
  configuration with duplicate-push-pipeline suppression
  ([.gitlab-ci.yml](.gitlab-ci.yml)).
- [x] Modern-Python validation and AlmaLinux 8 / Python 3.6 compatibility
  jobs using shared Makefile targets ([test configuration](ci/test.gitlab-ci.yml)).
- [x] Source/wheel builds after validation, with retained distribution
  artifacts ([build configuration](ci/build.gitlab-ci.yml)).
- [x] Cobertura coverage reports for merge-request presentation
  ([coverage configuration](ci/coverage.gitlab-ci.yml)).
- [x] SAST, IaC SAST and Secret Detection template integration
  ([security configuration](ci/security.gitlab-ci.yml)).
- [x] Dependency Scanning v2 template integration targeting
  `requirements-ci.txt`; execution requires the applicable GitLab tier.
- [x] Ruff-derived GitLab Code Quality reports, pinned pip-licenses
  Markdown/JSON reports and repository-sanity report jobs.
- [x] Scheduled/manual compatibility matrix using `NIGHTLY_MATRIX=1`
  ([matrix configuration](ci/nightly-matrix.gitlab-ci.yml)); a GitLab
  pipeline schedule must be configured separately.
- [x] Optional scheduled/manual Scorecard job using `SCORECARD=1`, with
  a digest-pinned image and JSON artifacts
  ([Scorecard configuration](ci/scorecard.gitlab-ci.yml)); requires
  schedule/variable setup and the documented GitHub authentication token.

#### Compatibility Matrix and Packaging Helpers

- [x] Shared distro bootstrap/lane runner for both CI providers, including
  isolated environments for modern distro interpreters and private
  pytest temporary roots ([ci/scripts/matrix_lane.sh](ci/scripts/matrix_lane.sh)).
- [x] Source and wheel builds through `build`, `github-build`,
  `gitlab-build` and `dist`, plus package-version reporting.
- [x] Generated-output cleanup and complete local rerun helpers;
  cleanup requires explicit confirmation.

Bandit, standalone pip-audit/gitleaks/detect-secrets gates, DAST, container
scanning and GitLab Advanced SAST are not implemented as active gates in
the current pipeline. Historical local scanner baselines are documented
in [the security assessment](docs/secure-coding-pyscg.md).

See [CHANGELOG.md](CHANGELOG.md) for historical changes and released versions.

## Contribution Constraints

Runtime dependencies must remain minimal. CI and developer-only dependencies
must remain pinned and isolated from the runtime path.

Changes intended for 1.0 must preserve Python 3.6 compatibility. Any future
change to the minimum Python version requires an explicit support-policy
decision and documentation.

Scheduler fixtures must be synthetic or explicitly approved and anonymized.
Contributions should identify the user-visible problem, relevant roadmap
item or issue and evidence that the change works.
