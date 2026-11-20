# SBOMs and verifiable supply-chain artifacts

Repository configuration reviewed on 2026-11-02. This checklist records
implemented tooling separately from proposed work; it does not verify external
service settings or claim a SLSA level.

## Progress checklist

- [x] Version-pin direct CI requirements in `requirements-ci.txt` and commit-pin
  actions in `.github/workflows/release.yml`.
- [x] Provide `make license-report` and a GitLab job publishing its reports.
  This is a license inventory, not an SBOM or provenance attestation.
- [x] Prepare tokenless PyPI Trusted Publishing in
  `.github/workflows/release.yml`.
- [ ] Confirm the maintainer-managed `pypi` environment, trusted publisher and
  successful publication. Workflow configuration alone does not establish these.
- [x] Generate the release-package SBOM (`make sbom`, CycloneDX JSON) and attest
  it to the release distributions in `.github/workflows/release.yml`.
- [ ] Generate and publish a separately scoped CI/build-environment SBOM.
- [x] Configure GitHub build provenance for `dist/*` and document consumer
  verification (Phase 3).
- [ ] Confirm attestations on the first published release; workflow
  configuration alone does not establish them.
- [ ] Produce SBOMit inputs: per-step in-toto attestations and a signed policy
  (see "SBOMit readiness" below).
- [ ] Hash-lock the complete CI toolchain, including the license-report helper's
  transitive dependencies.

The 2026-10-06 Scorecard snapshot is historical evidence, not current publication
or signing status. Recheck Packaging and Signed-Releases after release changes.

## Phase 1 -- inventory and SBOMs

qtop declares no required third-party runtime packages; `lxml` and `ujson` are
optional accelerators. A package SBOM does not automatically inventory the Python
interpreter, operating system or optional packages installed at an HPC site.
Keep these inventories separate:

| Inventory | Scope |
|---|---|
| Release package | qtop installed from the exact built wheel in a clean environment |
| Build and CI | Packages actually installed in the build or validation environment |
| Deployment | Interpreter, system libraries and optional packages at the installation site |

The smallest implementation is a shared optional Makefile target, called by both
forges, that generates and validates CycloneDX JSON and uploads it alongside
`dist/`. Keep the generator pinned in a separate modern-Python tooling environment;
it must not become a qtop runtime dependency or pollute the environment scanned.

[CycloneDX Python](https://cyclonedx-bom-tool.readthedocs.io/en/latest/usage.html)
accepts environments and dependency manifests, not wheel/sdist files directly.
After installing the built wheel into a clean environment, the scan command is:

```sh
# Run from the separate environment containing the pinned generator.
cyclonedx-py environment /path/to/release-venv/bin/python \
  --output-format JSON --output-file artifacts/sbom/release.cdx.json
cyclonedx-py environment /path/to/ci-venv/bin/python \
  --output-format JSON --output-file artifacts/sbom/ci.cdx.json
```

`make sbom` implements the release-package row: after `make build`, it installs
the wheel from `DIST_DIR` alone into `build/sbom-venv` (`--no-index
--find-links`, so no local paths enter the SBOM and the component gets its
`pkg:pypi` purl), removes the bootstrap `pip`, and writes the validated,
reproducible CycloneDX 1.6 file `artifacts/sbom/qtop.cdx.json` with the pinned
`cyclonedx-bom` from the CI environment. The generator version is recorded in
the SBOM metadata; the wheel digest is bound by the attestation subject. This
installed-wheel scan does not inventory every file in the sdist. The CI
environment row is not implemented.

Publish the inventories with CI build artifacts and, for releases, with the exact
release distributions. GitLab's native dependency-scanning reports are an
alternative subject to project eligibility; they do not replace release-package
inventory. Add SPDX only if a consumer needs it, rather than maintaining two
formats immediately.

## Phase 2 -- provenance and publication

1. **GitHub provenance:** use the current
   [artifact-attestation integration](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
   with an action pinned to a reviewed commit, binding provenance to the exact
   `dist/*` files. This requires `contents: read`, `id-token: write` and
   `attestations: write` in the generating job. The release `build` job runs
   `actions/attest-build-provenance` and `actions/attest-sbom` over `dist/*`,
   commit-pinned, after `make github-build sbom`. Both produce in-toto
   statements signed through Sigstore with the workflow's OIDC identity.
2. **GitLab provenance:** enable
   [runner artifact provenance metadata](https://docs.gitlab.com/ci/runners/configure_runners/#artifact-provenance-metadata)
   with `RUNNER_GENERATE_ARTIFACTS_METADATA: "true"`. Its JSON statement is
   provenance metadata; the flag alone does not establish cryptographic signing
   or equivalence to GitHub's signed attestations.
3. **PyPI:** confirm the prepared Trusted Publishing workflow works with the
   maintainer-managed publisher/environment settings. Publishing authentication
   and consumer verification of distributions are separate controls.
4. **Additional signing:** consider keyless blob signing only if consumers need
   a verification path beyond the chosen attestations. Avoid introducing two
   signing systems before documenting one working verification recipe.

See [OpenSSF readiness](openssf-readiness.md) for related metadata and assessments.

## Phase 3 -- consumption and enforcement

- [ ] Test this verification recipe against the first attested release:

  ```sh
  gh attestation verify qtop-<version>-py3-none-any.whl --repo qtop/qtop \
    --signer-workflow qtop/qtop/.github/workflows/release.yml
  gh attestation verify qtop-<version>-py3-none-any.whl --repo qtop/qtop \
    --predicate-type https://cyclonedx.org/bom --format json \
    --jq '.[].verificationResult.statement.predicate'
  ```

  The first checks build provenance; the second checks and prints the attested
  SBOM. The `qtop-sbom` workflow artifact holds the same file.
- [ ] Document [offline verification](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/verify-attestations-offline),
  including exporting attestation bundles and trusted roots before transfer to
  an air-gapped site; the online command alone is insufficient there.
- [ ] Add platform-appropriate hash locks for all CI dependencies and enforce
  them with `pip install --require-hashes`.
- [ ] Compare SBOMs in reviews once generation is stable. Document expected
  differences between build tooling and shipped package inventories.

## SBOMit readiness

[SBOMit](https://sbomit.dev/) derives SBOMs from in-toto attestations of the
build; its specification is a 0.1.0 draft without conformance levels. The
release SBOM and attestations above are inputs it can use. Still missing:

- per-step attestations (checkout, dependency install, tests, build, SBOM), e.g.
  `witness run --step <name>` with keyless signing through the GitHub OIDC token;
- an in-toto layout (Witness policy) naming each step's allowed signer, signed by
  a maintainer- or KMS-held key: choosing that key is a maintainer decision;
- hash-locked CI requirements, so recorded materials are exact (Phase 3).

No container/registry inventory, new runtime dependencies or checked-in signing
keys are required by this plan. SBOMs describe inventory; they do not certify
that a release is vulnerability-free.
