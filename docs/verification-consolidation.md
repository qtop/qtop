# Verification consolidation

Baseline: `f1b72ac`; review and measurements use Linux-native snapshots.
LOC means physical lines, including comments and blanks. Report nonblank,
noncomment lines separately so moving comments cannot masquerade as savings.

## Before implementation: script decisions

Savings below are estimates, before regression testing. A merge removes the
old entry point and updates its callers; it does not remove its guarantees.

| File | Decision | Callers and unique responsibility | Expected net LOC saved |
| --- | --- | --- | ---: |
| tools/atheris_fuzz.py | Keep, absorb corpus seeding | Make coverage-fuzz; byte-guided instrumentation, replay, crashes | 15 with seed helper |
| tools/build_py.py | Keep | setuptools configuration and setup.py; installed default assets | 0 |
| tools/check_ci_matrix.py | Merge into existing test | Make lint and tests/test_ci_matrix.py; expanded lane/failure-policy parity | 15 |
| tools/check_coverage.py | Keep | Make coverage; independent runtime statement and branch floors | 0 |
| tools/deterministic_fuzz.py | Keep, absorb parser targets | Make deterministic-fuzz, Atheris and tests; seeded primitive invariants | 5 with parser helper |
| tools/fortifications.py | Merge unique diff checks into repo_sanity | Make lint and focused tests; added-line ASCII and generated-path review | 70 |
| tools/loc_authors.py | Keep | Make authors; blame attribution merged by identity, unlike authors-lines | 0 |
| tools/local_tests.py | Keep | Windows wrapper and documented Linux CLI; fresh native snapshot and evidence | 0 |
| tools/parser_fuzz.py | Merge into deterministic_fuzz | Atheris and parser tests; six bounded document targets | counted above |
| tools/prepare_fuzz_corpus.py | Merge into Atheris startup | Make coverage-fuzz; seed without overwriting discovered corpus | counted above |
| tools/prepare_release.py | Keep | Release documentation and tests; synchronized metadata and release guards | 0 |
| tools/private_output.py | Keep | Report tools and security tests; ownership, links, atomic replacement | 0 |
| tools/repo_sanity.py | Keep and simplify | Make repo-sanity/lint and tests; tracked-text trust and diff policy | 100 |
| tools/terminal_probe.py | Keep | Make soak and tests; real PTY, captures, refresh/resource budgets | 0 |
| tools/test-local.ps1 | Keep | Windows contributors; argument-safe WSL entry | 0 |
| tools/validate_installation.py | Keep | Make install/release checks and tests; exact distributions, clean installs | 0 |
| tools/validate_pbs_samples.py | Keep | Make test-pbs-samples, docs, legacy/security tests; curated external corpus | 0 |
| tools/validate_scheduler_samples.py | Keep, merge repeated result assembly | Make sample/render gates and tests; semantic ANSI and sample evidence | 40 |
| tools/validate_trace_export.py | Keep | Make trace-export-validation and tests; masked exports and rendering checks | 0 |
| tools/schema-compat/main.go | Keep | Make schema-compat; CUE structural subsumption | 0 |
| tools/schema-compat/main_test.go | Keep | Make schema-compat-test/schema-ci; negative schema contracts | 0 |
| ci/scripts/matrix_lane.sh | Keep | Both nightly matrices; distro setup and git-free Make dispatch | 0 |
| ci/scripts/schema_compat.sh | Merge into Make schema-ci | Make only; merge-base contract and first-introduction handling | 3 |
| ci/scripts/scorecard-scan.sh | Keep | Make scorecard-scan; exact-commit container scan and token handling | 0 |
| ci/baseline/Dockerfile | Keep | Manual baseline workflow; reusable Python 3.6 environment | 0 |

## Workflow and configuration decisions

| File/interface | Decision and callers | Estimated LOC saved |
| --- | --- | ---: |
| .github/workflows/build.yml | Keep; primary Python lane runs pre-pr plus Cobertura once; absorb type/fuzz job | 20 |
| .github/workflows/coverage.yml | Delete; move report/upload into build.yml, update README badge/docs. Restored in 0.9.20261118 as the only coverage run, so the README badge reports the coverage gate alone; build.yml runs the rest of pre-pr | 48 |
| .github/workflows/pytest.yml | Delete redundant manual wrapper; add workflow_dispatch to build.yml | 38 |
| .github/workflows/nightly-matrix.yml | Keep all 34 lanes and extended properties; retain thin Make dispatch. Split in 0.9.20261122 into nightly-core.yml (8 blocking lanes) and nightly-wide.yml (26 lanes) | 0 |
| .github/workflows/release.yml | Keep publishing boundary and exact-artifact guarantees | 0 |
| .github/workflows/codeql.yml | Keep provider-owned security-and-quality analyzer | 0 |
| .github/workflows/scorecard.yml | Keep provider-owned analyzer and security permissions | 0 |
| .github/workflows/baseline-image.yml | Keep manual container publication and compatibility proof | 0 |
| ci/test.gitlab-ci.yml | Keep; pre-pr/Cobertura/code-quality in modern job; keep 3.6/schema jobs | -10 |
| ci/coverage.gitlab-ci.yml | Delete; move Cobertura declaration to modern test job | 42 |
| ci/security.gitlab-ci.yml | Keep native scanners/license report; absorb Ruff and text-audit jobs into tests | 40 |
| ci/build.gitlab-ci.yml | Keep exact artifacts and legacy wheel installation | 0 |
| ci/nightly-matrix.gitlab-ci.yml | Keep all lanes/properties; same contract as GitHub | 0 |
| ci/scorecard.gitlab-ci.yml, ci/scorecard-policy.yml | Keep provider integration and policy | 0 |
| .gitlab-ci.yml | Keep native includes; remove obsolete coverage include | 1 |
| .pre-commit-config.yaml | Keep pinned Ruff lint/format as fast commit path; remove parked hook comments | 6 |
| pyproject.toml | Keep Ruff/type/coverage contracts; remove obsolete parked Pylama config | 8 |
| Makefile | Keep useful target names; alias duplicate renders, remove duplicate Ruff call; shared entry points | 5 |
| requirements-ci.txt | Keep pins; add no dependency or orchestration framework | 0 |

## Make target inventory and callers

Targets remain public; no new orchestration layer was added. Caller groups
below include local use in addition to the named automatic callers.

| Targets | Callers / responsibility |
| --- | --- |
| help | Default target; documented command menu |
| all, rerun | Local complete verification; rerun calls clean then all |
| clean, confirm | Local cleanup; confirm guards clean |
| validation-suite | Local sample/render/trace convenience aggregate |
| ci-entrypoints | Local shared/provider alias parity |
| packaging | all; shared build aliases and version |
| ci-deps | Provider setup, build and all; pinned environment |
| test | ci, nightly-ci; full suite, serial web subprocess tests |
| test-fast | Local parser/model/UI inner loop |
| coverage, coverage-xml | pre-pr and primary PR jobs; independent floors and Cobertura |
| sample-gate, backend-validation | ci, pre-pr, nightly-ci, validation-suite |
| backend-colour-artifacts, render-backends | Local/validation-suite; explicit alternate artifact location |
| trace-export-validation, test-pbs-samples | all, validation-suite; optional supplied trace/external corpus |
| test-slurm-samples | Local/validation-suite; focused Slurm parsing and rendering |
| deterministic-fuzz, coverage-fuzz | pre-pr; ci also runs deterministic mode |
| fortifications, repo-sanity | lint; diff-only policy and tracked-text audit respectively |
| code-quality | GitLab primary job after_script; Ruff report even on failure |
| license-report | GitLab security inventory; pinned in-job pip-licenses |
| type-contracts, type-check | pre-pr; mypy, Pyright and ty |
| property-test, nightly-properties | Nightly extended properties; normal properties also in full suite |
| ruff-check, lint | ci/pre-pr; normal rules plus unsuppressible F403/S307 |
| lint-fix, format-fix, normalize-crlf | Local edits only |
| format-check | ci/pre-pr; one formatter pass and merge-base whitespace |
| zizmor, zizmor-fix | Independent local workflow audit/safe fixes |
| compat-py36 | Both providers, nightly legacy lanes and all |
| install-check, release-check, legacy-install-check | Local install; validated-build; both legacy install jobs |
| validated-build, build, dist | Packaging and provider build aliases; exact distribution pair |
| ci, github-ci, gitlab-ci | Portable/shared aliases; release and extra modern interpreter |
| pre-pr | Primary PR jobs and local complete gate, cheap checks first |
| nightly-ci | Shared matrix runner; Git-free runtime verification |
| github-build, gitlab-build | Both providers and packaging; validated-build aliases |
| authors-lines, authors, version | Local attribution/version; packaging prints version |
| scorecard-scan | Local and GitHub branch Scorecard job |
| schema-compat, schema-compat-test, schema-ci | Both schema jobs; structural subsumption and negative contracts |
| soak | Local real terminal/capture/refresh probe |
| ci-matrix-check | Focused pytest invocation for matrix configuration changes |

`all` no longer invokes ci-entrypoints after coverage or rerenders identical
samples into several destinations. Explicit rendering targets and custom
output variables still work. local_tests passes requested targets to one Make
invocation, retaining sequential default execution and shared prerequisites.

## Unicode replacement experiment

Compared the existing detector with isolated `check-unicode==0.6.0`, invoking
its real CLI on 49 adversarial/positive fixtures. Modes: default strict and
`--allow-printable --check-confusables`. No repository dependency was added.
Upstream: <https://github.com/mit-d/check-unicode>.

Both modes miss U+000B/U+000C/U+001C/U+001D/U+001E/U+0085/U+2028/U+2029 and
embedded CR because line splitting consumes them before scanning. Both skip a
leading BOM and reject legitimate SGR in the exact terminal fixtures. Default
strict catches bidi, zero-width, mixed-script examples and invalid UTF-8;
the printable/confusable mode additionally misses U+034F and fullwidth Latin.
The existing detector reports these as critical or warning as appropriate.

Strict check-unicode catches some codepoints currently only informational
(U+2061, variation selectors and Unicode tags). Existing diff policy already
rejects every added non-ASCII codepoint, including these. Keep that policy.
Neither tool is a complete Unicode TR39 confusable detector. The experiment
is a comparison of concrete cases, not a proof of complete Unicode safety.

Decision: retain the stdlib detector and exact fixture exceptions. Adding
check-unicode would duplicate scanning while still requiring custom controls
and fixture handling, plus a Python >=3.11 developer dependency. Results and
exact CLI output are retained in artifacts/unicode-comparison.json locally.

## Validation and final measurements

Measured against the baseline above, using the same counting script:

| Measure | Before | After | Difference |
| --- | ---: | ---: | ---: |
| tools Python modules | 18 | 14 | -4 |
| Python/shell/PowerShell files under tools and ci | 22 | 17 | -5 |
| tools Python physical LOC | 2,594 | 2,329 | -265 |
| tools nonblank/noncomment LOC | 2,155 | 1,953 | -202 |
| All Python physical LOC, including every test/helper | 22,288 | 22,111 | -177 |
| All Python nonblank/noncomment LOC | 18,281 | 18,164 | -117 |
| Configuration files in scope | 20 | 17 | -3 |
| Configuration physical LOC | 1,717 | 1,530 | -187 |
| Configuration nonblank/noncomment LOC | 1,207 | 1,096 | -111 |
| GitHub workflows | 8 | 6 | -2 |
| GitHub job definitions / expanded configured jobs | 17 / 51 | 14 / 48 | -3 / -3 |
| GitLab owned job definitions / expanded configured jobs | 13 / 45 | 10 / 42 | -3 / -3 |
| requirements-ci pinned requirement rows | 31 | 31 | 0 |
| Required runtime dependencies | 0 | 0 | 0 |
| Full Python 3.12 pytest cases | 1,569 | 1,616 | +47 |
| Runtime statement / branch coverage | 95.28 / 91.47% | 95.28 / 91.47% | unchanged |

Net deletion: **158 lines with docs**, **379 verification lines**, counting
all Python tests/helpers and the removed 15-line shell wrapper. New regression
coverage is included, not hidden in a separate source count. Configuration
scope is workflow/ci YAML, Makefile, root GitLab, pre-commit, pyproject and
requirements-ci; unchanged Go module manifests and report documentation are
outside that LOC measure. The attribution/schema/packaging helpers remain.

Expanded job counts include mutually exclusive/manual jobs and ignore
provider-owned GitLab templates; they are not simultaneous runner counts.
Primary GitHub PR jobs decrease from 9 to 7 (including CodeQL), GitLab-owned
regular jobs from 9 to 6. Both providers remove one duplicate full-suite run
and two full dependency installations from their primary PR job set.
Explicit scheduled/manual NIGHTLY_MATRIX=1 or SCORECARD=1 runs omit regular
PR jobs; ordinary pushes/MRs retain them regardless of those variables.
All 34 nightly matrix lanes and the extended-property job remain configured.
GitLab include rules use supported scheduled/manual pipeline variables:
<https://docs.gitlab.com/ci/yaml/includes/#use-variables-with-include>.

Measured local pre-pr/schema time: baseline **113.7 s**; consolidated runs
**110.2 s** and **107.8 s**, also emitting Cobertura. These single-run timings
are observational, not a reliable performance claim. The concrete orchestration
saving is verified through Make expansion: all reduces full-suite invocations
from 2 to 1 and sample-renderer invocations from 4 to 2; packaging remains one
build. ci/github-ci/gitlab-ci together share one test execution. CI runner-time
and dependency-download savings remain estimates until hosted runs complete.

Validation: final pre-pr plus coverage-xml/schema-ci passed 1,612 parallel
pytest cases and 4 serial web cases. Added cases cover 40 text-policy/fixture
regressions, five additional Ruff scenarios, corpus retention and input replay
plus the existing scenario visibility. Both fuzzers ran 5,000 cases, seed 488;
the deterministic SHA-256 remained
`a566215188e379a71c833230d4a3cb55e117f91b0f54d63566aa105abf3b9bd9`.
Python 3.15.2 also passed make ci: 1,597 parallel cases, four serial web
cases and one Atheris module skip (unsupported there), plus samples,
deterministic fuzzing, lint and formatting. Its existing environment first
needed the pinned PyYAML and Ruff updates provided by make ci-deps.
Ruff lint/format, mypy, Pyright, ty, all 13 scheduler renders and Go contract
tests passed. Actual CPython 3.6.9 compiled runtime/tools, passed eight unittest
methods and all 13 scheduler samples. The compiler was extracted from Ubuntu's
published packages into /tmp; no system install or runtime dependency changed.

Independent checks: actionlint 1.7.12 and Zizmor passed. Make schema-ci accepts
the identical HEAD contract, rejects a narrowed contract and rejects an invalid
base; the first-introduction path also passed. github-build and gitlab-build
shared one build and exact-artifact validation, including wheel and sdist-derived
wheel installations outside the source tree, with unchanged input hashes.

No meaningful verification was intentionally removed. Duplicate entry points,
AST scans and formatter invocations were removed; tracked-text critical findings
now also gate GitHub's primary lane. Ruff's non-waivable eval/wildcard policy,
negative security cases, corpus retention and replay are preserved. Runtime,
scheduler behavior, serialized schemas and release metadata were unchanged.
Native Windows execution was unavailable (the installed launcher points to a
missing interpreter); the documented WSL path was exercised. The complete
hosted matrix, GitLab-native template execution, publication and
branch-protection configuration were not exercised locally. Repositories that
require the removed coverage/type-check status names must select the surviving
primary job; no remote settings were changed.
