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

- [ ] Complete a documented pre-1.0 soak-testing campaign.
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
  - Check quickstart, installation, capture, replay, export and troubleshooting examples against the release candidate.
  - Document scheduler-specific requirements and supported environments.
  - Identify experimental features individually.
  - Preserve historical release information.
  - Track documentation work in [#498](https://github.com/qtop/qtop/issues/498) and history reconciliation in [#489](https://github.com/qtop/qtop/issues/489).

- [ ] Record the 1.0 release decision.
  - Link compatibility, artifact, scheduler and soak-test evidence.
  - Confirm no unresolved release-blocking failures remain.
  - List deferred issues and known limitations with their rationale.
  - Record the validated release-candidate revision.

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
