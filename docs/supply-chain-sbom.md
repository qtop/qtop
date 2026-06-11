# SBOMs and verifiable supply-chain artifacts: proposal

Status: proposal only, per the #488 checklist ("only propose, nothing
beyond that"). No SBOM or signing automation is introduced by this change;
this document records a path the maintainers can adopt incrementally.

## Where qtop starts from

qtop's supply-chain posture is unusually clean, which makes the remaining
steps cheap:

- **Zero runtime dependencies** (deliberate, per CONTRIBUTING.md): the
  runtime SBOM is essentially "qtop itself plus the interpreter". The
  interesting inventory is the *build and CI* tool chain.
- **Fully pinned CI dependencies** (`requirements-ci.txt`); Scorecard rates
  Pinned-Dependencies 10/10.
- Gaps, per the 2026-06-08 Scorecard snapshot: Packaging -1 (no publish
  workflow), Signed-Releases -1 (no releases/signatures).

## Phase 1 -- generate and publish SBOMs (low effort)

1. **CycloneDX for the built artefacts**: run `cyclonedx-py environment`
   against the pinned CI venv and `cyclonedx-py` against the sdist/wheel in
   the existing `build` jobs; attach `sbom.cdx.json` as a CI artifact on
   both forges (the build concern already publishes `dist/`).
2. **GitLab native route**: GitLab Dependency Scanning (wired in this
   change) emits a CycloneDX report per pipeline on supported tiers; the
   pegged mirror gets this for free once enabled.
3. **SPDX alternative**: `syft dir:.` or `syft dist/*.whl` if the
   maintainers prefer SPDX; both formats can be published side by side.
4. Attach SBOMs to GitHub Releases so consumers get inventory with the
   artefact, not from a separate channel.

## Phase 2 -- provenance and signatures (moderate effort)

1. **Build provenance on GitHub**: `actions/attest-build-provenance` in the
   build workflow generates SLSA v1 provenance for `dist/*`, backed by the
   Sigstore public-good infrastructure; verification is
   `gh attestation verify`.
2. **Build provenance on GitLab**: set
   `RUNNER_GENERATE_ARTIFACTS_METADATA: "true"` on the build job to emit a
   SLSA attestation alongside artifacts on the mirror.
3. **Keyless signing of release artefacts**: cosign sign-blob with OIDC
   (no long-lived keys to manage) for sdist/wheel at release time.
4. **PyPI Trusted Publishing** when a packaging workflow lands: OIDC-based,
   no stored API tokens, and it closes the Scorecard Packaging gap.

## Phase 3 -- consumption, policy, and verification (ongoing)

1. Document a verification recipe for cluster operators (HPC sites often
   install from mirrors inside air-gapped networks; a signed sdist plus an
   SBOM is exactly what their security reviews ask for).
2. Hash-pin the CI set (`pip install --require-hashes`) on top of the
   existing version pins.
3. Diff SBOMs in merge requests once generation is routine, so dependency
   drift in the CI chain is visible at review time.
4. Track the Scorecard Signed-Releases and Packaging checks as the
   external measure of progress.

## Forge mapping

| Capability | GitHub | GitLab (pegged mirror) |
|---|---|---|
| SBOM generation | cyclonedx-py / syft in workflow | Dependency Scanning CycloneDX or same CLI in job |
| Provenance | actions/attest-build-provenance (SLSA v1) | RUNNER_GENERATE_ARTIFACTS_METADATA (SLSA) |
| Signing | cosign keyless via OIDC | cosign keyless via OIDC (id_tokens) |
| Publish trust | PyPI Trusted Publishing | release artefacts + attestation |

## Explicit non-goals here

No container/registry SBOMs (the registry deployment item is not claimed in
this slice), no signing keys checked into CI, and no new runtime
dependencies under any phase.
