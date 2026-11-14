# Changelog

Notable changes by version, newest first. See the
[roadmap](ROADMAP.md#now-validate-and-deliver-10) for 1.0 release gates.

## Unreleased

- CI: make Linux and WSL test runs reproducible with retained reports
- Bugfix: reject scalar and nested-list conflicts in YAML input
- CI: run extended property checks in both nightly pipelines
- CI: expand dependency-free legacy runtime contracts
- CI: broaden document fuzzing and versioned state properties
- CI: enforce independent branch and statement coverage floors
- Bugfix: cover real terminal lifecycle and add bounded soak evidence
- CI: gate schema compatibility on both CI providers
- Bugfix: synchronize scheduler timeout tests and verify child termination
- CI: normalize Go source line endings for CI
- Feature: experimental schema 3 beta1 workload `parent_qid`, with cycle
  validation and consistent anonymization of present or absent parents;
  existing renderers keep using legacy maps.

## 0.9.20261112

- Feature: cluster state schema 2, an optional observation envelope (backend, cluster ID) with namespaced job/node/queue qids and job kinds; schema 1 states and archives replay unchanged. Unknown top-level keys are rejected by name, per schema version, ready for the relations of issue #593.
- Performance: matrix highlight rules and the user-ID map are built once per frame, not per cell; Slurm parses each CPU_IDs list once per collection.
- CI: BASELINE proof of concept, run by hand: builds an AlmaLinux 8 + Python 3.6 image on GHCR and runs the 3.6 checks in it.
- Performance: cluster-state validation checks core maps and job-ID lists by exact type first, full isinstance() checks otherwise; about 33% faster on 2000 nodes.
- Performance: the core matrix fills core lines by position, without building a key per core; about 45% faster on 2000 nodes.
- Performance: snapshots make the raw and display-safe copies in one traversal; about 2x faster on a 2000-node trace.
- Performance: Slurm builds each node's core map with dict(enumerate()), about 2x faster for that step.
- Performance: watch mode re-parses qtopconf/palettes YAML layers only when a file changes; each frame gets its own copy.
- CI: AlmaLinux jobs use repo.almalinux.org directly, skip Extras and drop sources slower than 50 kB/s.
- Tools: make authors counts code lines per author with %, merging names sharing an email.
- Bugfix: a watch-mode terminal resize redraws instead of cycling the core lines (r).
- Bugfix: watch mode survives a terminal refusing cbreak, drops unread keys on exit; menus work without a terminal.
- Bugfix: watch mode restores the terminal on SIGTERM/SIGHUP and re-applies key input after fg (SIGCONT).
- Feature: watch mode redraws at once on a terminal resize (SIGWINCH), as watch(1) does.
- Performance: Slurm expands each job node list once per frame; malformed lists log once.
- Performance: matrix cores resolve their job once per frame, in one pass, not twice.
- Bugfix: SGE node queue letters keep their XML order instead of varying per run.
- Cleanup: drop WNOccupancy._valid_corejobs, unused since the single core pass.
- CI: Dependabot bumps checkout 7.0.1, setup-python 7.1.0, CodeQL 4.39.2 and 5 more actions (#590).
- CI: build and the Python 3.6 install check no longer wait for the test jobs.
- CI: coverage runs on sys.monitoring (COVERAGE_CORE=sysmon), about 30% faster.
- CI: tests run on all CPUs with pytest-xdist where installed; test_web.py stays serial.
- CI: AlmaLinux jobs skip git and use one lean dnf transaction; GitHub ones time out after 10 minutes.
- Release: update version metadata to 0.9.20261112

## 0.9.20261111

- Tests: the viewport property test bounds scrolling by the one-row-shorter page of #121.
- Tests: the #121 paging test reads pages with Path.read_text(), without bare open() in an assert (CodeQL).
- Feature: in watch mode keys 1, 2, 3 show/hide sections 1-3 and 4 the accounts totals row, like -1 to -4.
- Feature: the demo watch keys line shows its keys in light blue (underlined with -c OFF) and lists 1,2,3,4.
- Feature: watch-mode key p cycles colour palettes from palettes.yaml: default, solarized-dark, gruvbox, okabe-ito, monochrome.
- Feature: anonymization (-A) is no longer experimental and works without -e (#548).
- Feature: the accounts totals row is on by default; -4 (--noaccounttotals) now hides it.
- Feature: the Summary shows Total=R+Q+Other on blue, green, orange (red without 256 colours) and yellow; -c OFF terminals get reverse video.
- Feature: `summary_colours` in qtopconf.yaml picks the Summary count colours.
- Feature: the accounts totals row paints its counts in the Summary colours, for side-by-side comparison.
- Feature: the expanded queue view (Q) aligns one queue per line as Total=R+Q+Other, in the Summary colours.
- Feature: the accounts table and its totals row add an O (Other) column by default, as the Summary does.
- Feature: a green-monitor palette, monochrome in phosphor-green tones; the first p press selects it.
- Feature: Slurm jobs on missing nodes or with invalid node lists suggest trying --all.
- Feature: the Slurm incomplete-snapshot warning gives a next step for each case: missing nodes, invalid node lists, rejected rows.
- CI: Python 3.6 installs the wheel built on 3.12 with its bundled pip; the vulnerable legacy build pins are gone.
- CI: wheel 0.46.2 (CVE-2026-24049 in 0.45.1).
- CI: drop the Python 3.9 (EOL) pytest lane and its unpatchable pins; RHEL 9-family 3.9 lanes run compat-py36.
- CI: Dependabot proposes monthly, grouped updates of GitHub Actions and CI pins; SUPPORT.md says where to get help.
- Docs: where to record accepted advisories (osv-scanner.toml) for Scorecard's Vulnerabilities check.
- Docs: why the PBS Summary (qstat -q) can differ from the accounts totals row (qstat) by a few jobs.
- Docs: PBS qstat -q's Que counts W and H jobs; an empty qstat -q falls back to the job list.
- Bugfix: the watch keys line shows for every scheduler, not only the demo, and stays when section 1 is hidden.
- Bugfix: watch-mode paging no longer skips a line between pages (#121).
- Bugfix: demo lists its queued jobs and counts only running ones as R, so its Summary and accounts totals agree.
- Bugfix: matrix columns follow node numbers, -m keeps early empty nodes as documented, and -T masks them too.
- Bugfix: SGE Summary R and Q classify jobs by their state, as the accounts table does; held jobs count as Other.
- Bugfix: the Summary no longer counts PBS W/H jobs twice (qstat -q queues them) and counts the job list when qstat -q is empty.
- Cleanup: drop the unused navigate_viewport, yaml_parser.load_all and assign_color_to_each_qname.
- Tools: make test-slurm-samples uses the sample gate, which also refuses unsafe artifact directories; validate_slurm_samples.py removed.
- Tests: drop a duplicate import re in test_mvu.py (CodeQL).
- Release: update version metadata to 0.9.20261111

## 0.9.20261110

- Bugfix: unconfigured PBS/OAR/SGE/Slurm traces fail as OSError, not TypeError.
- Bugfix: an unsupported worker node name raises a clear ValueError.
- Bugfix: with -c ON, the watch-mode menu numbers are coloured again.
- Feature: demo watch mode lists its keys, q quit first, in a yellow line (bold with -c OFF) above the job summary.
- Feature: `Summary  :` lines up with `Queueing :`; tests, sample gates and validate_trace_export follow.
- Bugfix: watch mode lists queues one per line when the summary is wider than the terminal, so section 1 stays visible.
- Deprecated: qtop.py's global-based legacy adapters now warn; see docs/architecture.md.
- Tests: revive a disabled YAML dash-key case; drop an obsolete user_sort lambda case.
- Refactor: drop commented-out code; split two overlong state regexes.
- Refactor: split the demo simulator's setup & Markov steps into methods (complexity 30 -> 7), renamed as LittleClusterSimulator
- Refactor: split repo_sanity's file scan into read, classify and scan steps (complexity 21 -> 11).
- Refactor: `main()` sets no module globals; run state is local and passed explicitly (a test guards it).
- Refactor: split `main()` into option, path, config, replay, web and frame helpers (complexity 27 -> 12).
- Refactor: validate_trace_export and the key-handling tests use the model API.
- Refactor: the fuzz and trace-export tools run as `python -m tools.X`; no sys.path hacks or E402 noqa.
- CI: CodeQL unused-import and wrapper-lambda warnings fixed (RawQueue, lxml, tarfile, Web).
- CI: ty allows the optional atheris import; the Atheris test has no E402.
- CI: Scorecard action v2.5.0; v2.4.3 pulls from the retired gcr.io and would fail.
- CI: ruff enforces pydocstyle (`D`, Google convention); every public module, class and function has a one-line docstring.
- CI: ruff caps cyclomatic complexity (C901) at today's maximum, now 20, to be lowered.
- CI: ruff also enforces pycodestyle W and eradicate (no commented-out code); Pylama config kept, commented out.
- CI: mypy checks all of `qtop_py`, untyped bodies included; contract modules stay strict.
- CI: all of `qtop_py` is annotated; mypy runs its full `strict` rule set package-wide cleanly. Bingo.
- Release: update version metadata to 0.9.20261110.

## 0.9.20261109

- Refactor: Document is now Snapshot (snapshot.py); Document stays as an alias for one release.
- Refactor: one snapshot copy, precompiled colours, fast escaping; up to 1.9x faster.
- Refactor: ~1.3x faster on large clusters; no quadratic core-matrix fill, fewer deep copies.
- Refactor: the sample-file TypedDict is `SampleSnapshot`; tests finish the Document -> Snapshot rename.
- Refactor: annotate the user-ranking, accounts-header and matrix-cell helpers.
- Feature: Memoize matrix cells and colours; lazy web imports; one structural copy
- Feature: `user_id_ranking` tunable; IDs default to all, R, name_desc, as in 2012 qtop.
- Feature: `user_accounts_header` tunable for the accounts-table note.
- Bugfix: user IDs and node queue initials no longer depend on Python's hash seed.
- Bugfix: type-check CI passes again; ty sees the per-frame matrix-cell cache.
- Docs: document `user_id_ranking` and `user_accounts_header`; credits link to GitHub contributors.
- CI: explicit allowlist two B108 false positives inline and two detect-secrets, explaining reasons.
- CI: pre-commit runs CI's ruff 0.17.1 check and format, plus detect-secrets; a test keeps the ruff pins equal.
- Release: update version metadata to 0.9.20261109.

## 0.9.20261108

- Tests: cover sample-filename branches (#507).
- Refactor: drop the `qtop_py/cli.py` shim; use `python -m qtop_py.qtop`.
- Refactor: remove dead `fileutils` helpers.
- Refactor: SonarQube-style cleanups; behaviour unchanged.
- Refactor: rename variables; retire `quintuplet` and `re_search` names.
- Refactor: mark module constants `Final` (PEP 591), Python 3.6-safe.
- Refactor: split config validation and trace masking into smaller functions.
- Refactor: lazy log formatting in `init_logging`.
- Docs: configurable values stay UPPERCASE, deliberately (not PEP 8).
- Docs: Python 3.6 build pins and login-free web view are deliberate.
- Docs: add a qtop(1) manpage, checked against `--help` by a test.
- Docs: add an EasyBuild easyconfig example.
- CI: `make all` runs six batches, including the pre-PR gate.
- Bugfix: (security) cap per-node cores at 65536; SGE ignores larger `num_proc`.
- Bugfix: (security) web view serves only `/`, `/index.html` and `api.json`.
- Bugfix: `-c OFF` no longer emits colour-reset sequences.
- Bugfix: (security) PySCG fixes; web view no longer loads jQuery.
- Bugfix: (security) PySCG fixes; private, repo-local sample-render outputs (CWE-377).
- Bugfix: (pbs) keep queueless pending jobs from `qstat -prior`.
- Feature: (pbs) parse legacy `-prior` and NQS qstat formats (#353).
- Feature: (sge) opt-in whole-node queues, `sge_whole_queues` (#295).
- Feature: tunable account-table and Summary state columns (#305).
- Release: update version metadata to 0.9.20261108.

## 0.9.20261102

- CI: remove the obsolete transpose global flagged by CodeQL.
- CI: make permission-repair tests reliable in root-run nightly containers.
- Docs: explain 4 eyes principle for reviews and more.
- Release: update version metadata to 0.9.20261102.

## 0.9.20261101

- Bugfix: PyPy, reject DTDs on PyPy and restore dependency-free PyPy smoke lanes.
- Bugfix: repair restrictive and permissive owned directory/log permissions.
- Release: update version metadata to 0.9.20261101.

## 0.9.20261031

- Bugfix: repair MVU regressions and address CodeQL import, call and file checks.
- Refactor: remove redundant MVU state and clarify variable names.
- Refactor: separate MVU state, snapshot queries and terminal presentation. Bingo.
- Refactor: clarify state-types and snapshot-document module names.
- Refactor: move legacy parser and display policy out of core.
- Feature: configure the experimental web address/port; default to loopback.
- Bugfix: validate exact release artifacts and create private, symlink-safe logs.
- Bugfix: escape untrusted display/log controls and bound input/command resources.
- Bugfix: group scheduler messages and label their severity INFO, WARNING etc.
- Release: update version metadata to 0.9.20261031.

## 0.9.20261026

- Docs: (README) add registered OpenSSF Best Practices badge
- Refactor: separate scheduler support from core and backend plugins.
- Refactor: split refresh/control paths, centralize cleanup and retain canonical state.
- Refactor: centralize configuration preparation and pass collection inputs explicitly.
- Bugfix: clarify export initialization, cleanup and failure tests for CodeQL.
- Bugfix: isolate scheduler policy, validate configuration and bound user lookups.

## 0.9.20261025

- CI: measure production-code coverage and require 90%.
- CI: bump Python compatibility lanes to 3.15.2.
- Bugfix: exit interrupted captures cleanly and preserve original stdout.
- Bugfix: retire stale callbacks, repair web paths and reap viewport processes.
- Bugfix: bound scheduler collection time and discard timed-out partial traces.
- Bugfix: handle replay times across month and year boundaries.
- Bugfix: remove GPU/Slurm import cycles and clarify type-only imports.
- Bugfix: (build) require patched setuptools on Python 3.10+.
- Bugfix: (tar) accept trace tarballs with deterministic capture-set selection in -s.
- Bugfix: (tar) log reproducible trace commands and restore archive paths at exit.
- Bugfix: (tar) add optional masked source traces and private raw companion captures.
- Bugfix: (tar) complete expanded support archives and capture effective configuration.
- Release: update version metadata to 0.9.20261025.

## 0.9.20261022

- Release: update version metadata.
- Docs: improve openssf readiness
- Bugfix: tmp dir on macos should stop complaining

## 0.9.20261020

- Bugfix: (pbs) mark stopped queues; parse JSON job counts by state name.
- Bugfix: (pbs) preserve wrapped allocations, array IDs and legacy job/queue records.
- Bugfix: (pbs) tolerate invalid optional GPU counts.
- Bugfix: (pbs) show queue idle/total node counts (#210).
- Bugfix: (oar) show job and queue summaries (#246).
- Bugfix: (oar) preserve unknown/shared allocations; tolerate YAML variants & job names.
- Bugfix: (oar) make StrictCheck count unique running jobs.
- Bugfix: (slurm) flag incomplete snapshots; keep unknown GPU usage explicit.
- Bugfix: (slurm) retain job states; add `--all` for hidden partitions.
- Experimental: (slurm) show configured GPU pairs and job ownership.

## 0.9.20261018

- Bugfix: accept macOS system temporary-directory aliases safely.
- Metadata: remove project-wide development-stage labels.
- Bugfix: show Slurm node-state suffix flags and preserve raw states in snapshots.
- Feature: track Slurm GPU device bindings and MIG profiles in JSON snapshots. Bingo.
- Bugfix: retain Slurm jobs on shared CPUs; fill uneven node capacity.
- Bugfix: count multi-partition Slurm jobs without phantom queues.
- Feature: mark blocked Slurm partitions in the queueing summary.
- Feature: preserve explicit Slurm array and component identity.
- Bugfix: expand Slurm array tasks; clarify heterogeneous-component accounting.
- Bugfix: parse complete Slurm task/component identities across field layouts.
- Feature: collect Slurm CPU allocation details by default; retain safe fallback.
- Feature: read optional Slurm CPU allocations; label estimated placement.
- Feature: capture Slurm TRES/GRES fields for future GPU support. Bingo.

## 0.9.20261017

- Cleanup: clarify typing placeholders and expected directory exceptions for CodeQL.
- CI: fix older nightly build backends; suspend failing PyPy lanes with FIXMEs.
- CI: bump Python to 3.15.1 and modern pip to 26.2.2.
- CI: Make Makefile Great Again (MMGA)
- Bugfix: reject invalid date formats; make logging and output-size guards explicit.
- Bugfix: apply node filters without requiring blind remapping (#287).
- Bugfix: retain SGE queue membership for idle nodes.
- Bugfix: address CodeQL false positive feedback
- Bugfix: retain SGE queue-instance states and show blocking flags.
- Bugfix: account for allocated SGE slots in core occupancy (#289).
- Bugfix: preserve SGE task IDs and map job ownership by ID.
- Bugfix: skip missing or unavailable optional user-details commands (#98).
- Bugfix: align account columns with background and custom colors (#304).
- Bugfix: default missing coreline settings instead of raising KeyError (#290).
- Bugfix: accept PBS job-name punctuation and hyphenated usernames (#303).

## 0.9.20261016

- Bugfix: allow site-owned save-path ancestors; report unsafe parent paths, f.i. /tmp
- Bugfix: restore source installation on Python 3.6-3.8; test installed CLIs.
- Bugfix: keep archived PBS validation compatible with Python 3.6.
- CI: add complete local pre-PR gates; align contributor and agent guidance.
- CI: scan develop commits with Scorecard; keep default-branch publishing.
- CI: restrict Atheris installation to the supported CPython interpreter.
- CI: reuse parser fuzz checks with bounded Atheris inputs on both forges.
- CI: report fortification failures with file, line, column and escaped text; include staged and unstaged fixes in local diff checks.

## 0.9.20261014

- CI: restore initialized Python CodeQL analysis with security/quality queries;
  document the required switch from Default to Advanced setup.
- Security: create owner-verified private save directories; reject symlinks
  and unsafe shared paths, including command-line overrides.
- Security: isolate source-launcher imports while retaining Python 3.6 support.
- Bugfix: replace runtime asserts with explicit checks that survive `python -O`.
- Bugfix: validate required SGE XML text before use; retain optional defaults.
- Bugfix: restore configuration and interactive help in installed wheels.
- CI: refresh pinned build, coverage, lint, type and property-test tools;
  retain compatible versions for Python 3.9.
- CI: update legacy pytest to 8.4.2 and use patched pytest on Python 3.10+;
  pin Pygments for both modern and legacy test environments.

## 0.9.20261013

- Bugfix: reject malformed snapshot records, metadata and display options on
  import.
- Bugfix: reject mismatched job columns instead of silently dropping entries.
- Bugfix: preserve sibling keys and nested containers when normalizing YAML.
- Bugfix: fix queue anonymization for PBS JSON input.
- QA: add typed core contracts and gradual mypy/Pyright checks alongside `ty`.
  Developer tools remain separate from the Python 3.6 runtime.
- Tests: add bounded Hypothesis checks for state round trips, anonymization,
  host ranges, YAML normalization and viewport behavior.
- Tests: expand scheduler, rendering, watch-control and archive-replay tests.
  Require at least 85% overall coverage in local and CI coverage targets.
- Tests: rename the Slurm fixture `incomplete` to `partial_and_malformed`.
- CI: replace four retired nightly lanes with Ubuntu 26.10 on both CI platforms.
  GitHub and GitLab now define the same 35 lanes.
- Community: adopt the CNCF Code of Conduct and link it from contributor
  guidance.

## 0.9.20261012

- Tests: cover direct scheduler runtimes, rendering, watch controls and archive replay; enforce at least 85% TOTAL coverage in local and CI coverage targets.

- Documentation: align CLI, keys, configuration and scheduler examples with code; document exports, support archives and anonymization.
- Documentation: add an editable architecture diagram and source map based on the GitDiagram proposal in #571; link it from the README and guide.
- Bugfix: sort section 3 by the configured user ID alphabet, including first-letter IDs, with overflow users last.
- Bugfix: order users sharing an ID by username ascending, preserving the previous color mapping for shared first-letter IDs.
- Bugfix: include nested Python package sources in `-LL` archives from checkouts and installed layouts.
- Bugfix: add portable anonymized state/config replay via `--load-sample`; omit raw traces, logs, local paths and account/owner identities.
- Bugfix: make Slurm commands in qtopconf.yaml shell-copyable; honor quoting and legacy doubled-percent format codes.
- Bugfix: report input, filesystem and configuration failures with exit 1; keep log details and `--debug` tracebacks.
- Bugfix: require explicitly selected configuration files to exist and report invalid `-o KEY=VALUE` settings.
- Bugfix: check scheduler exit status, allow stderr warnings on success and discard failed captures while preserving the previous snapshot.
- Bugfix: expand comma-separated Slurm host groups and reject malformed or oversized host lists without hanging or crashing.
- Bugfix: skip incomplete Slurm identity rows, clamp invalid CPU counts and retain unknown job states in accounting totals.
- Tests: add synthetic incomplete Slurm snapshots and regressions for host lists, unknown states, empty inputs and missing nodes.

## 0.9.20261010

- Security: reject SGE XML DTDs before entity expansion, without new runtime dependencies; test UTF-8 and UTF-16 input.
- Licensing: add SPDX notices and fixture/asset sidecars; package attribution and MIT license text; document attribution conventions.
- Documentation: define verifiable 1.0 release gates, retain Python 3.6 compatibility and inventory CI/CD tooling.

## 0.9.20261007

- Packaging: drop the redundant `setup.py`; `pyproject.toml` is now the sole package configuration.
- Security: remove persisted checkout credentials from all workflows and add standalone `make zizmor` audit/fix targets.
- CI: update modern Python lanes to pytest 9.1.1 while retaining the Python 3.9 compatibility pin.
- CI: add required CodeQL SAST and `ty` type-check jobs with concise diagnostics.
- Security: add a reproducible parser stress harness and prepare tokenless PyPI Trusted Publishing.
- Security: link private vulnerability reporting directly from the security policy.
- Code quality: simplify accumulators, fix parser narrowing and counters, emit valid HTTP output and scope archive ownership.

## 0.9.20261005

- Bugfix: preserve Slurm partition order from `sinfo`, label the summary "Queueing" and add collapsed/expanded views, fixing #560.
- Bugfix: tolerate concurrent removal of stale output files, fixing #308.
- Feature enhancement: accept a scheduler output file for `-s` by using its parent directory, fixing #291.
- Feature enhancement: add opt-in primary Unix group display with `--show-groups`, addressing #299.
- CI: test Python 3.9 and stable 3.15, restore legacy lanes, retry stale APT metadata and add PyPy 3.11 v8.0.0 (#546).

## 0.9.20261001

- Bugfix: improve the contrast of the simulated-data notice in demo mode.
- Bugfix: fix account coloring when the UID is numeric.
- Bugfix: prioritize Slurm during automatic scheduler detection, fixing #552.
- Feature enhancement: require `-e`/`--experimental` when enabling the web interface with `-B`/`--web`, fixing #550.
- Documentation: modernize the README, interactive help and changelog formatting.
- Documentation: convert the roadmap and changelog to Markdown.
- Documentation: refresh README Python support and CI badge wording.
- Documentation: update the quickstart command examples to use the `qtop` wrapper.
- Documentation: clarify why qtop keeps runtime dependencies minimal on early or bare HPC clusters.
- Build tooling: add Makefile aliases for lint and format fix workflows.
- Code quality: address #549 and the remaining actionable CodeQL maintainability findings.
- Code quality: address the actionable findings from the CodeQL scan in #549.
- Security: document private vulnerability reporting contacts and guidance.

## 0.9.20260610

- Feature enhancement: add general-purpose Slurm scheduler support. Bingo.
- Feature enhancement: add develop-aligned sample gates; Merged PR #444.
- Feature enhancement: Completion of ci/cd baseline, with ground-truth validation.

## 0.9.20260515

- Documentation: update CONTRIBUTING.md for AI-assisted PRs
- Bugfix: typecasting in qtop_py/plugins/pbs.py
- Bugfix: several bugs in relation to historic PBS samples input processing
- Feature enhancement: introduce logic for regression & golden output PBS tests

## 0.9.20241027

- Feature enhancement: codebase improved via deepsource: pass #1, imports fixed

## 0.9.20241013

- Documentation: get paraphernalia files in shape for release

## 0.9.20240422

- Feature enhancement: codebase improved via ruff: pass #1

## 0.9.20231110

- Removed: support for Python 2, fixes #108
- Feature enhancement: Migrate to Python 3, fixes #108
- Feature enhancement: Use argparse instead of optparse, fixes #311
- Feature enhancement: Migrate to pyproject.toml, fixes #313
- Feature enhancement: Add CI Pipeline, fixes #313
- Feature enhancement: Use JSON output for PBS Plugin, fixes #315
- Bugfix: Fix parsing of CPU cores in PBS plugin, fixes #312
- Bugfix: Add full width output for piping into less, fixes #316

## 0.9.20161222

- Bugfix: hotfix for `tar_out` issue #283, #284

## 0.9.20161221

- Feature enhancement: Need for speed: compression of ansi codes #279
- Feature enhancement: report version #276
- Feature enhancement: some more fixes on help page, visible with `?` #273
- Bugfix: fix for Anonymisation feature temporarily unavailable until completely implemented (use `--experimental`) #170
- Bugfix: fix for stacktrace while collecting anonymized sample (which by then worked fine otherwise) #280, #281
- Bugfix: fix for when in filtered view, summary now reflects filtered jobs/cores #276
- Bugfix: fix for `svc.*` no longer with Cyan color #278
- Bugfix: fix-anonymize-SGE-and-optimisations #276
- Bugfix: fix broken coloring #276

## 0.9.20161216

- Feature enhancement: key `H`: highlight users and queues by string or regex #272
- Feature enhancement: add -`rr` option: filter out unused core lines #270, #271
- Feature enhancement: harmonize output filenames for window/full view and JSON exports #269
- Bugfix: -r Option to remove empty core lines now works as advertised #267
- Bugfix: fix for more arcane pbs node names in pbsnodes #266
- Bugfix: fix for PBS plugin fails when jobs use non-consecutive CPUs #265
- Bugfix: fix for adding `-G` flag under demo resulted in a crash, when `getent` was missing #262

## 0.9.20161207

- Feature enhancement: online help available with `?` ; #257
- Feature enhancement: unhardwire `__version__`, make it visible across code; #259
- Feature enhancement: Remove `WORKDIR` from banner, it was no longer serving a purpose; #243
- Bugfix around missing `core_job_map`, when not remapping; #253
- Bugfix unhardwire `/tmp`, make the choice of intermediate directory liberal; #254, #255
- Bugfix under linux watch mode, fix watch issues of various types; #206, #248, #256
- Bugfix around jobid consistency of input files in OAR - handle gracefully and report in debug log; #258

## 0.9.20161130

- Summary in the 1st section now shows `Total:, Up:, Free:` nodes
- Added a nodes column per user in the 3rd section where user info is presented
- Added -R feature to replay last frames; recording is automatic!
- Added setup.py, `pip install --user qtop` should work
- Added `--version`, in case anyone wondered about it
- Bail out if python version is \<2.5
- Supplied `qtop` launcher script, for better system integration
- Create timestamped intermediate files, to improve ability to look back
- Bugfix to avoid /tmp getting crowded with temporary files
- Bugfix for grepping and pipelining qtop's output (1st attempt to treat #206)
- Bugfixes on PBS (jobid arrays, node states etc)
- Bugfixes on SGE (job states added, all jobs now visible, userid is correct)
- Bugfixes on OAR (nodes per user are also visible)
- Converted documentation files to `.rst` format

## 0.8.9

- watch mode `(-w)`
  - `((s))` node sorting with custom presets/user-inserted RegEx sorts
  - `((f))` node filtering
  - `((t))` matrix transposition
  - `((F))` toggle full nodename display/numbering
  - `((m))` toggle coloring code (user-id based/queue name-based)
- queue info can now be colored, three different queues with the same initials can be colored distinctly
- a good first amount of documentation
- small bugfixes

## 0.8.8

- created a shiny new demo out of a demo mini-grid scheduler (!)

## 0.8.7

- added support for queuename display for OAR and PBS (SGE already there!)
- overspill feature (aka oversubscribe/overcommitment) for SGE systems
- strict checking cmdline var (will compare reported nr. of jobs against displayed/found jobs)
- id column is now more readable (put in brackets)
- fixed long-standing bug that would report incorrectly the queues of jobs in a node, in some cases
- restored deprecated document file deletion
- huge refactoring of the codebase

## 0.8.6

- Support for python 2.6 for early RHEL6/Centos6/ScientificLinux6 distros
- Added tarball creation for better user reporting
- Added Viewport class
- Several bugfixes

## 0.8.5

- initial support for data anonymisation
- minor bugfixes

## 0.8.4

- Watch replacement with simple pager included (with full color functionality, compatible with older RHEL6 systems)
- GECOS field completed by a less "intruding" command
- Numerous enhancements and bug fixes

## 0.8.3

- The worker node occupancy table can now be viewed horizontally (transposed)
- Custom conf files createable by users
- Filter/select nodes by name/regex
- Numerous enhancements and bug fixes

## 0.8.2

- Ability to select which of the three qtop parts to display
- GECOS field implemented
- States can be assigned more than one lines
- New node line displays the queue the job belongs to
- (trivial) auto-detection of batch-system
- Numerous enhancements and bug fixes

## 0.8.1

- Overwrote PyYAML dependency with custom YAML parser

## 0.8.0

- Wrapping together support for `PBS`, `OAR`, `SGE`

## 0.7.3

- Support for `OAR`

## 0.7.2

- Introduce support for `SGE`

## 0.7.1

- Finalise support for `PBS`

## 0.7

Enhancements:

- Input files now use YAML for dumping and loading.

## 0.6.7

Enhancements:

- Created YAML files now have the pid appended to the filename.
- PBS-related functions, which create the respective YAML files, have moved to a dedicated module.
- Removed `state_dict['highest_core_busy']`, which seemed useless and unused.

Bugfixes:

- Added a separate `read_qstatq_yaml` function for consistency, removed from `qstatq2yaml`.
- Changed `qstatq_list` from list of tuples to list of dictionaries.
- Moved `offline_down_nodes` from `pbs.pbsnodes2yaml` to `read_pbsnodes_yaml`.

## 0.6.6

Bugfixes:

- Got rid of all global variables (experimental).

## 0.6.5

Enhancements:

- `PBS` now supported.

## 0.6.4

Bugfixes:

- Lines that don't contain *any* actual core are now not printed in the matrices.

## 0.6.3

Enhancements:

- Optional stopping of vertical separators every `n` positions for a configured number of repetitions.
- Additional vertical separator in the beginning.

## 0.6.2

Bugfixes:

- WN matrix width bug ironed out.

## 0.6.1

Enhancements:

- Custom-cut matrices horizontally, too, via the `-o` switch.

## 0.5.2

Enhancements:

- Custom-cut matrices vertically, not horizontally, with width set by the user.

## 0.5.1

Enhancements:

- If more than 20% of the WNs are empty, perform a blind remap.
- Code cleanup.

## 0.5.0

Bugfixes:

- Major rewrite of matrices calculation fixed.

New features:

- True blind remapping.

Enhancements:

- Exotic cases of very high numbering schemes now handled.
- More qstat entries successfully parsed.
- Cases with many Unix accounts, `>62`, now handled.

## 0.4.1

Bugfixes:

- Now understands additional probable names for `pbsnodes`, `qstat` and `qstat-q` data files.

## 0.4.0

Bugfixes:

- Corrected colorless switch to have an ON/OFF option, default ON.
- `qstat_q` didn't recognize some faulty cpu time entries.
- Descriptions are now in white, as before.

Enhancements:

- Queues in the job accounting summary section are now coloured.

## 0.3.0

Enhancements:

- Command-line arguments, mostly empty for now.
- Non-numbered WNs can now be displayed instead of numbered WN IDs.

New features:

- Implement colorless switch `(-c)`.

Bugfixes:

- Fixed issue with single named WN.
- Better regex pattern and algorithm for catching complicated numbered WN domain names.

## 0.2.9

New features:

- Handles cases of non-numbered WNs, for example fruit names.
- Parses more complex domain names with more than one dash.

Bugfixes:

- Correction in WN ID numbers display, where tens were problematic for larger numbers.

## 0.2.8

Bugfixes:

- Colour implementation for all of the tables.

## 0.2.7

Bugfixes:

- Exiting when there are two jobs on the same core reported on pbsnodes, with remapping functionality to be added.
- Number of WNs `>1000` is now handled.

## 0.2.6

Bugfixes:

- Fixed some names not being detected, with `%` and `=` characters missing from the regex.

Enhancements:

- Changed name to `qtop`, introduced configuration file `qtop.conf` and colormap file `qtop.colormap`.

## 0.2.5

New features:

- Working cores added in usage totals.
- Map now splits into two if terminal width is smaller than the Worker Node number.

## 0.2.4

Enhancements:

- Implemented some stuff from PEP8.
- Un-hardwired the file paths.
- Refactored code around `cpu_core_dict` functionality, responsible for drawing the map.

## 0.2.3

Bugfixes:

- Corrected regex search pattern in `make_qstat` to recognize usernames like spec101u1, number followed by number followed by letter.
- Now handles non-uniform setups.
- Fixed R + Q / all display of E status.

## 0.2.2

Enhancements:

- Masking/clipping functionality, when nodes start from e.g. wn101 and empty columns 1-100 are omitted.

## 0.2.1

Enhancements:

- Hashes are displayed when the node has fewer cores than the maximum declared by a WN, its np variable.

## 0.2.0

Bugfixes:

- Unix accounts are now correctly ordered.

## 0.1.9

Bugfixes:

- All CPU lines displaying correctly.

## 0.1.8

Enhancements:

- Unix account id assignment to CPU0/CPU1 implemented.

## 0.1.7

Enhancements:

- `ReadQstatQ` function writes in YAML format using PyYAML.
- Output up to Node state.

## 0.1.6

Bugfixes:

- `ReadPbsNodes` function writes in YAML format using PyYAML.

## 0.1.5

Bugfixes:

- Implemented saving to 3 separate files: `QSTAT_ORIG_FILE`, `QSTATQ_ORIG_FILE`, `PBSNODES_ORIG_FILE`.

## 0.1.4

Bugfixes:

- Some "wiremelting" concerning the save directory.

## 0.1.3

Bugfixes:

- Fixed tabs-to-spaces. Formatting should be correct now.

Enhancements:

- Now each state is saved in a separate file in a results folder

## 0.1.2

Enhancements:

- Script reads qtop-input.out files from each job and displays status for each job.

## 0.1.1

Enhancements:

- Changed implementation in `get_state()`.

## 0.1.0

Enhancements:

- Initial reader for a `pbsnodes -a` output file gathers the results in a single line.
