# Secure Coding Guide for Python (PySCG) alignment

Reviewed against the working tree on 2026-10-30; input boundaries updated 2026-11-02. This document maps implemented
controls and remaining gaps to the OpenSSF
[Secure Coding Guide for Python](https://github.com/ossf/python-secure-coding-guide).
It replaces the original proposal for #488; it is not a certification or a
fresh vulnerability scan. Source references use files and function names
instead of line numbers that drift as fixes land.

## Progress checklist

`[x]` records an implemented control; `[ ]` records remaining work.
`[CI]` marks only the bounded assertion enforced by a configured CI gate,
not a completed security audit, a particular green run or required branch protection.
The [CI contract](ci-structure.md#the-makefile-contract) identifies the jobs and targets.

- [x] [CI] Reject scheduler XML DTDs and missing required XML fields;
  `tests/plugins/test_sge.py` runs in `make test` ([details](#1-scheduler-xml)).
- [x] [CI] Cover command failures and temporary-file cleanup with CLI/runtime
  regressions in `make test` ([details](#2-commands-return-values-and-cleanup)).
- [x] [CI] Validate imported records and preserve runtime checks under
  `python -O`; security/cluster-state tests run in `make test`
  ([details](#3-deserialization-and-runtime-validation)).
- [x] [CI] Test bounded parser expansion through backend/property regressions,
  deterministic fuzzing and the dedicated Atheris gate.
- [x] [CI] Test snapshot replay, identifier masking and disabled imported
  lookup commands in `make test` ([details](#4-support-archives-replay-and-privacy)).
- [x] [CI] Test private POSIX save/log directories, safe log opens and isolated launcher imports
  in `make test` ([details](#5-private-output-and-import-paths)).
- [x] Document non-cryptographic randomness and masking scope
  ([details](#6-randomness)); this is a source review, not a cryptographic CI proof.
- [x] [CI] Enforce diff/source fortifications in shared CI and tracked-text
  critical findings in the GitLab repo-sanity job
  ([details](#7-source-text-and-terminal-data)).
- [x] [CI] Run lint/format, type checks, coverage-floor, fixture and fuzz gates
  in their configured jobs ([validation](#validation-and-dependency-maintenance)).
- [x] [CI] Bound XML/JSON/YAML inputs, snapshot archive scanning and command
  output; adversarial regressions run in `make test` (`tests/test_input_safety.py`).
- [ ] Review broader process-wide memory/CPU budgets beyond these input limits.
- [ ] Replace predictable render-tool output defaults.
- [x] [CI] Escape terminal identifiers and log controls; preserve original exports.
- [x] [CI] Default the experimental web listener to loopback and validate
  configurable host/port settings (`tests/test_web.py`, CLI/config regressions).
- [ ] Review remaining web-server isolation, asset serving and remote access controls.
- [ ] Refresh standalone scanner/dependency/Scorecard results and verify external
  repository settings for the assessed commit ([remaining work](#remaining-work)).
- [x] Add source-backed [Security Insights](../security-insights.yml) and
  [OpenSSF readiness guidance](openssf-readiness.md); full Best Practices and
  Baseline assessments remain open.

## Scope and evidence

Scheduler output, saved traces and support archives are data, not instructions.
Live command configuration and the executable search path are operator-controlled
trust boundaries. qtop retains Python 3.6 compatibility and a dependency-light
runtime; modern analysis tools run separately.

The July 2026 baselines recorded 53 Bandit findings (50 low, 3 medium), four
Semgrep findings, no known vulnerabilities in the selected Python 3.12 CI
dependencies, no gitleaks findings and one detect-secrets sample-ID false
positive. The recorded Scorecard score was 8.1 on 2026-10-06. These are historical
observations, not current results. Rerun tools before claiming a clean scan or
current score. Record the scanned commit, interpreter and tool/rule versions
with reports in CI or review artifacts.

## Implemented controls and limits

### 1. Scheduler XML

[SGEStatExtractor.get_xml_tree and NoDTDTreeBuilder](../qtop_py/plugins/sge.py)
reject every DTD declaration using a standard-library parser target. This
replaces the earlier proposal to add optional `defusedxml`.
[Tests](../tests/plugins/test_sge.py) cover internal/nested entities, external
DTDs/entities and multiple encodings, including UTF-16. Predefined XML entities
remain accepted. Required element/text lookups raise explicit parse errors
(PySCG `pyscg-0034`).

The `ParseError` import in `qtop.py` is error handling, not another XML parsing
entry point. Bandit suppressions explain the custom DTD rejection; they do not
establish parsing-time limits. The parser now rejects input above 256 MiB,
64 nesting levels or two million elements during parsing. These reduce
resource-exhaustion exposure (CWE-400) without claiming a process-wide memory
or CPU-time ceiling.

### 2. Commands, return values and cleanup

For PySCG `pyscg-0009`, `pyscg-0036` and `pyscg-0052`,
[execute_shell_batch_commands](../qtop_py/qtop.py) passes `shlex.split()` argv
to `subprocess.Popen` without a shell. Startup/quoting failures and nonzero
exits raise `OperationalError`; stderr decoding replaces invalid bytes.
Output is collected in a temporary file and renamed only after success;
`finally` removes leftover temporary files. Discovery uses `shutil.which()`,
not external `/usr/bin/which`. Other runtime subprocesses also use argv lists.

This prevents shell syntax from being interpreted by a shell, but live commands,
optional account lookups and `PATH` must still be trusted: programs run with
the operator's privileges. Scheduler collection and account lookups have configurable deadlines (30 seconds
by default), 256 MiB stdout and 1 MiB stderr limits. The shared
[process helper](../qtop_py/process.py) drains both binary pipes with bounded
readers, kills failed commands/process groups on POSIX and reaps clients.
Partial scheduler traces never replace the previous successful capture.

### 3. Deserialization and runtime validation

For PySCG `pyscg-0023`, the [limited YAML parser](../qtop_py/yaml_parser.py)
uses `ast.literal_eval` for Python literals, not `eval` or object constructors.
It remains a limited YAML implementation. Input is capped at 256 MiB,
64 indentation levels and 1 MiB per line/literal. Literal parsing additionally
checks bracket depth and a 100,000-AST-node budget. JSON loading checks size,
64-level nesting and two million structural items before deserialization.
Shared limits live in [safety.py](../qtop_py/safety.py). [validate_cluster_state](../qtop_py/cluster_state.py) checks imported
record shapes, required text, nonnegative counts and supported resource metadata.
Runtime checks raise exceptions and survive `python -O` (`pyscg-0037`);
[security tests](../tests/test_security.py) verify that boundary.

[Slurm host/task expansion](../qtop_py/plugins/slurm.py) and
[GPU index parsing](../qtop_py/schedulers/gpu.py) have explicit expansion bounds.
Those limits apply to specific operations, not every input or the whole process.
See [backend regressions](../tests/plugins/) and
[property tests](../tests/test_properties.py).

### 4. Support archives, replay and privacy

For PySCG `pyscg-0012`, [load_snapshot](../qtop_py/sample.py) reads a named
JSON member without extracting archive files to disk. It requires a regular
member of at most 64 MiB and validates format, cluster state and known display
options. Imported account-lookup commands are disabled. Streaming archive scanning also caps members at 10,000 and decompressed tar
data at 1 GiB, including gzip/bzip2/xz inputs. Duplicate snapshot members are
rejected. These limits are not process-wide memory or parsing-time budgets.

For `pyscg-0019`, anonymous capture masks user/node/queue/job identifiers and
allocation references, drops unapproved job/node fields, and sanitizes local
commands, paths and identifier-bearing configuration rules. Original logs
are omitted in favor of a minimal capture log. See
[capture/replay tests](../tests/test_sample.py).
This is support-data masking, not cryptographic unlinkability: topology,
counts, state and permitted configuration can still identify a site.
Ordinary logs and non-anonymous captures can contain original identifiers
and malformed input; review artifacts before sharing them.

### 5. Private output and import paths

[private_save_directory](../qtop_py/fileutils.py) opens POSIX directory
components with `O_DIRECTORY` and `O_NOFOLLOW`. The final directory must belong
to the effective user, reject group/other writes and be restricted to `0700`.
On macOS, root-owned `/tmp` and `/var` aliases to `/private/tmp` and
`/private/var` are expanded before the same component checks. Other symlinks
and writable non-sticky ancestors are rejected; sticky shared parents
and non-writable site-owned ancestors are allowed. Unsupported non-POSIX checks
fail explicitly. Temporary outputs use `mkstemp` or `NamedTemporaryFile`.
Log creation now uses the same directory validation and opens the leaf relative
to the held directory descriptor with append/no-follow/nonblocking flags. It rejects
non-regular, foreign-owned, multiply linked or group/other-writable logs before
writing, tightens accepted logs to `0600`, and preserves append history. A directory
replacement cannot redirect that open. `PrivateFileHandler` retains normal
`FileHandler` error-reporting behavior. This does not prove every other later
pathname operation is race-free.

For PySCG `pyscg-0013`, the [source launcher](../qtop) runs Python with `-I`
and adds its own checkout to the import path, excluding caller working-directory,
`PYTHONPATH` and user-site imports. The checkout must itself be trusted.
Direct `python -m qtop_py.cli` retains normal Python import behavior.
[Security regressions](../tests/test_security.py) cover both boundaries.

### 6. Randomness

For PySCG `pyscg-0038`, `random` generates
[demo data](../qtop_py/plugins/demo.py) and reproducible
[fuzz inputs](../tools/deterministic_fuzz.py), not keys or tokens.
[Legacy masking](../qtop_py/serialiser.py) and
[snapshot anonymization](../qtop_py/sample.py) use mappings/counters, not random
secrets. New authentication or secrecy requirements would need a separate
design; changing the PRNG alone would not provide anonymity.

### 7. Source text and terminal data

Related PySCG guidance includes `pyscg-0043/0044/0045`.
[fortifications.py](../tools/fortifications.py) checks added diff lines for
non-ASCII/control characters, rejects `eval()` and disallows production/tooling
wildcard imports. Generated/binary-looking paths require manual review.
It does not enforce every rule in `AGENTS.md`.

[repo_sanity.py](../tools/repo_sanity.py) audits Git-tracked UTF-8 text up to
5 MiB for bidi/invisible/control characters and confusable letters. It requires
`git ls-files`; there is no directory-walk fallback. Binary and oversized files
are excluded. Exact fixtures allow complete ANSI SGR sequences and specific
historical carriage returns, not arbitrary terminal controls. Critical findings
fail; warnings fail with `--strict`. The self-test and
[tests](../tests/test_repo_sanity.py) verify these boundaries.

Runtime display adapters separately escape untrusted controls and backslashes
while retaining original canonical state for exports. qtop-generated ANSI colours
are preserved. The shared log formatter escapes controls in rendered records,
including exception text, to prevent forged lines (`pyscg-0022`).
[Adversarial regressions](../tests/test_input_safety.py) cover both boundaries.

## Validation and dependency maintenance

The [Makefile](../Makefile) supplies the shared checks:

| Check | Command / scope |
|---|---|
| Lint and source/diff checks | `make lint format-check`; override `FORTIFY_BASE_REF` if necessary |
| Tracked text and detector proof | `make repo-sanity`; `python3 tools/repo_sanity.py --selftest` |
| Tests, coverage and backend rendering | `make test coverage backend-validation` |
| Property checks | `make property-test` (also part of the full pytest suite) |
| Deterministic fuzzing | `make deterministic-fuzz`; defaults: seed 488, 5,000 cases |
| Coverage-guided fuzzing | `make coverage-fuzz`; bounded Atheris adapter, Linux x86-64 CPython 3.12 |
| Static contracts and types | `make type-check`; Mypy, Pyright and ty |
| Python 3.6 compatibility | `make compat-py36 PYTHON=python3.6` in a suitable environment |
| Complete PR gate | `make pre-pr`; coverage, backend gates, fuzzers, lint/format and types |

[requirements-ci.txt](../requirements-ci.txt) is the version source for pinned
build/test/analysis tools. Interpreter markers retain compatible older tools;
Python 3.6-3.8 use the dependency-light compatibility path. The
[matrix runner](../ci/scripts/matrix_lane.sh) gives pytest a random mode-0700
parent and a child `--basetemp`, protecting the older Python 3.9 pytest lane
from a predictable shared temporary root. Local runs on that lane need the
same private-temp boundary.

Pins do not prove absence of vulnerabilities. Run
`pip-audit -r requirements-ci.txt` in the intended interpreter environment.
Bandit, Semgrep, gitleaks, detect-secrets and pip-audit are separate scanner
tools, not all installed or run by `make pre-pr`. Reproduce the original
source scan scopes with `bandit -r qtop_py tools` and
`semgrep scan --config p/python --config p/security-audit qtop_py tools`;
record rule versions and triage against the current implementation.

[GitHub workflows](../.github/workflows/) and
[GitLab security configuration](../ci/security.gitlab-ci.yml) configure provider
analyzers and secret scanning. Configuration does not establish successful
execution or enabled repository settings. See [CI structure](ci-structure.md)
and [supply-chain controls](supply-chain-sbom.md); the old Scorecard table is
not a current settings audit.

## Remaining work

- [ ] Review process-wide resource budgets beyond the implemented input,
   nesting, archive and command-output limits.
- [ ] Replace predictable render-tool output defaults in
   [validate_pbs_samples.py](../tools/validate_pbs_samples.py),
   [validate_slurm_samples.py](../tools/validate_slurm_samples.py) and their
   Makefile overrides with private temporary directories or explicit output
   choices. This remains open (CWE-377).
- [ ] Review remaining optional [web server](../qtop_py/web.py) isolation and
   asset serving. It now defaults to `127.0.0.1:8080`, with experimental host/port
   configuration, but still has no authentication or TLS and is not a protected
   remote dashboard.
- [ ] Refresh scanner/dependency and Scorecard evidence for the assessed commit;
   verify private-reporting, publishing and badge settings outside the source
   tree. No new scan or external-settings audit is claimed here.
