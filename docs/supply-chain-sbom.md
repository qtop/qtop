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
- [ ] Generate and publish separately scoped release and CI SBOMs.
- [ ] Generate provenance and document consumer verification.
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

These are proposed commands, not existing make targets. Create `artifacts/sbom/`
first. Record the scanned environment, generator version, source revision and
SHA-256 of the wheel used. Include or explicitly exclude environment bootstrap
packages in the inventory description. Do not label this installed-wheel scan
as an inventory of every file in the sdist.

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
   `attestations: write` in the generating job. It is not configured today.
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

- [ ] Publish a tested verification recipe tied to the expected repository and
  artifact digest. Once GitHub provenance exists, online verification takes the
  form `gh attestation verify /path/to/qtop.whl --repo qtop/qtop`.
- [ ] Document [offline verification](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/verify-attestations-offline),
  including exporting attestation bundles and trusted roots before transfer to
  an air-gapped site; the online command alone is insufficient there.
- [ ] Add platform-appropriate hash locks for all CI dependencies and enforce
  them with `pip install --require-hashes`.
- [ ] Compare SBOMs in reviews once generation is stable. Document expected
  differences between build tooling and shipped package inventories.

No container/registry inventory, new runtime dependencies or checked-in signing
keys are required by this plan. SBOMs describe inventory; they do not certify
that a release is vulnerability-free.
