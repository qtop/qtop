# Secure Coding Guide for Python (PySCG) alignment

Status: proposal for maintainer decision (#488). Nothing in this document
changes runtime behaviour; it maps real scanner findings on the qtop tree to
the OpenSSF [Secure Coding Guide for Python](https://github.com/ossf/wg-best-practices-os-developers/tree/main/docs/Secure-Coding-Guide-for-Python)
(first release, 2026-05-12) and proposes prioritised next steps.

## Method

All tools ran locally against a clean `develop` checkout (2026-06-11), plus
the public OpenSSF Scorecard API. Full machine-readable reports live outside
the main repo per CONTRIBUTING.md (artifact-light policy). Reproduce with:

| Tool | Command | Result |
|---|---|---|
| bandit 1.8.x | `bandit -r qtop_py tools` | 54 findings: 51 low, 3 medium |
| semgrep (`p/python`, `p/security-audit`) | `semgrep scan --config p/python --config p/security-audit qtop_py tools` | 4 findings, all ERROR severity |
| pip-audit | `pip-audit -r requirements-ci.txt` | 6 known vulnerabilities in 2 pinned CI deps |
| gitleaks 8.24 | `gitleaks dir .` | no leaks |
| detect-secrets | `detect-secrets scan --all-files` | 3 candidates, all false positives |
| repo-sanity (new) | `make repo-sanity` | 0 critical, 0 warning, 4 expected-fixture info |
| OpenSSF Scorecard | public REST API, 2026-06-08 snapshot | score 7.1 |

## Findings mapped to PySCG

### 1. Untrusted XML parsing -- highest priority

`xml.etree.ElementTree` parses scheduler output directly:
`qtop_py/plugins/sge.py:19,31` and `qtop_py/qtop.py:836` (bandit B314/B405,
semgrep `use-defused-xml*`, 3 hits). PySCG's first release has **no CWE-611
(XML external entity) page yet**, so this is both a qtop action item and an
upstream-contribution opportunity.

Context that bounds the risk: the XML comes from scheduler CLIs or files in
the cluster operator's trust domain, not from anonymous remote input, and
CPython's ElementTree does not resolve external entities by default. The
residual exposure is entity-expansion denial of service against a monitoring
tool, plus any future code path that feeds less-trusted XML.

Proposed next steps, compatible with qtop's zero-runtime-dependency rule
(CONTRIBUTING.md):
1. Document the XML trust boundary in the plugin docstrings.
2. Add a guarded `defusedxml` import (used when present, stdlib fallback
   otherwise) so hardened sites get protection without a new hard dependency.
3. Contribute a CWE-611 page upstream to PySCG, citing this case study.

### 2. Subprocess use -- core function, document the boundary

15 findings (bandit B603 x14, B607 x1; semgrep tainted-env x1 at
`qtop_py/qtop.py:2521`). Maps to PySCG `04_neutralization/pyscg-0009`
"Prevent OS Command Injection" (CWE-78). Invoking scheduler CLIs (qstat,
qhost, sinfo, oarnodes) IS qtop's job; what PySCG asks for is hygiene around
it, most of which qtop already has: list-form argv everywhere, no
`shell=True` anywhere in the tree (bandit B602: zero hits).

Proposed next steps:
1. Keep the existing list-form discipline as a stated rule (lint note).
2. Commands and arguments derive from `qtopconf.yaml`, which is in the
   operator's trust domain -- state this explicitly in the config docs.
3. The single partial-path call sites (`git` in `tools/`) are dev/CI-side
   tools resolving from PATH by design; annotate rather than change.

### 3. Pseudo-random use -- acceptable, scope it

bandit B311 x16, all in `qtop_py/plugins/demo.py` and the anonymisation
helpers. Maps to PySCG `09_cryptography/pyscg-0038` "Use Sufficiently Random
Values" (CWE-330), which targets *cryptographic* contexts. qtop's usage
generates demo/synthetic data and display anonymisation, not key material.

Proposed next steps: declare the non-cryptographic intent where `random` is
imported; if anonymisation is ever meant to be unlinkable across runs,
switch those call sites to `secrets`/`SystemRandom` (stdlib, no new deps).

### 4. Asserts on runtime paths

bandit B101 x12 (e.g. `qtop_py/fileutils.py:77,144`,
`qtop_py/plugins/oar.py:108,112`). Maps to PySCG
`08_coding_standards/pyscg-0037` (CWE-617): asserts vanish under `python -O`,
so they must not guard runtime invariants. Proposed next step: convert
runtime-path asserts to explicit `raise` of qtop's existing error types;
keep asserts in tests.

### 5. Hardcoded temp paths (dev tools only)

bandit B108 x2 (`tools/validate_pbs_samples.py:51`,
`tools/validate_slurm_samples.py:44`): default output dirs under `/tmp`.
PySCG first release has no CWE-377 page (second upstream-contribution
candidate). Proposed next step: default to `tempfile.mkdtemp()` while
keeping the CLI flag override; CI-side only, low risk.

### 6. Dependency currency (pinned CI set)

pip-audit on `requirements-ci.txt`: `pip 24.0` carries 5 advisories
(PYSEC-2026-196, GHSA-4xh5-x5gv-qwph, GHSA-6vgw-5pg2-w6jp,
GHSA-58qw-9mgm-455v, GHSA-jp4c-xjxw-mgf9) and `pytest 8.2.2` one
(GHSA-6w46-j5rx-g56g). These are CI-only dependencies -- the qtop runtime
deliberately has none -- so exposure is the CI environment, not clusters.
Proposed next steps: bump the two pins in a dedicated PR after the matrix
proves them green; keep the Dependency Scanning job (added in this change)
watching the set continuously.

### 7. Text and encoding trust

Maps to PySCG `02_encoding_and_strings` (pyscg-0043/0044/0045, CWE-175/180/176).
qtop already enforces ASCII-only diffs (`tools/fortifications.py`); this
change adds `make repo-sanity` (`tools/repo_sanity.py`) auditing the *entire
tracked tree* for Trojan-Source bidi controls, zero-width/invisible
characters, unicode line separators, C0/C1 controls and homoglyph-prone
letters. Current tree result: **0 critical, 0 warning**; the only non-ASCII
content is ANSI escape sequences inside declared terminal-output fixtures
(94,990 + 57,434 + 1,418 ESC in the three `.ref` files plus 34 in
`helpfile.txt`), aggregated as expected INFO. Detector correctness is proven
by `tools/repo_sanity.py --selftest`, which plants five payload classes and
shows caret-pointer proof of each detection.

### 8. Secrets

gitleaks: clean. detect-secrets: 3 candidates, all false positives (two
cache CACHEDIR.TAG signatures outside the tracked tree and one high-entropy
*sample job identifier* at `tools/validate_pbs_samples.py:22`). The GitLab
Secret Detection template (added in this change) takes over continuously;
expect and triage the same sample-ID false positive there.

## Scorecard gap-to-action map (baseline 7.1, 2026-06-08)

| Check | Score | Action in this change | Proposed follow-up |
|---|---|---|---|
| SAST | 0 | SAST templates + Semgrep-backed analyzer wired into GitLab CI; scorecard workflow on GitHub | expect uplift on next weekly cron |
| Security-Policy | 0 | -- | add SECURITY.md (maintainer decision on contact channel) |
| CII-Best-Practices | 0 | -- | register for the OpenSSF Best Practices badge |
| Fuzzing | 0 | -- | atheris harness over the PBS/SGE/OAR parsers is a natural fit |
| Packaging | -1 | -- | publish to PyPI via Trusted Publishing (see docs/supply-chain-sbom.md) |
| Signed-Releases | -1 | -- | release attestations (see docs/supply-chain-sbom.md) |
| Pinned-Dependencies | 10 | keep | -- |
| Token-Permissions | 10 | new workflows follow least privilege | -- |
| Dangerous-Workflow | 10 | new workflows keep this clean | -- |

## Priorities

1. P1: XML hardening decision (finding 1) and the two CI-dep pin bumps
   (finding 6).
2. P2: assert conversion (finding 4), SECURITY.md, temp-path fix (finding 5).
3. P3: random-use annotations (finding 3), PySCG upstream contributions
   (CWE-611, CWE-377 pages), fuzzing harness, Best Practices badge.
