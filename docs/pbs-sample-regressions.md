# PBS sample regression evidence

This document records the evidence requested in #346 steps 3, 4, and 5, using the differential-debugging format from #337: each bug has a failing `upstream/main` run, a passing run from this PR, and a short diagnosis of the difference.

Samples come from `fgeorgatos/qtop-test-repo/qtop5/results`.

## Progress checklist

- [x] Document the five crash fixes and their before/after screenshot references.
- [x] Record screenshot evidence for the five largest sample runs.
- [x] [CI] Include `tests/test_pbs_sample_regressions.py` in the ordinary
  `make test` suite used by both forges.
- [x] Provide `make test-pbs-samples` and the curated ten-sample golden set.
- [ ] Provision the external corpus in any CI job intended to run the archived
  sweep and retain results for the revision being reviewed.

The screenshot and local-run results below are historical evidence, not a fresh
validation run. The archived sweep skips when its external corpus is absent;
it is not an enforced CI gate.

## Step 3: before/after screenshots for five bug fixes

The PNG screenshots are intentionally not stored in the `qtop` git tree, per `CONTRIBUTING.md`. A regenerated monospace screenshot bundle was submitted to `qtop/qtop-artifacts` in https://github.com/qtop/qtop-artifacts/pull/1.

| Bug | Sample | Diagnosis | Screenshot files in qtop-artifacts PR bundle |
| --- | --- | --- | --- |
| Empty scheduler files / no worker nodes | `gef_0COupWwOLcbe0ETmuUMB2Q` | `upstream/main` raises `NotImplementedError` in `find_matrices_width()` when no worker nodes are present; this PR renders an empty but valid report. | `before-empty-files-no-worker-nodes.png`, `after-empty-files-no-worker-nodes.png` |
| Malformed `qstat` lines | `gef_LxnDgIQrIHjGZQ4RuHbwFg` | `upstream/main` can reference an uninitialised qstat match object; this PR skips unsupported/header/malformed qstat rows and keeps valid jobs. | `before-malformed-qstat-lines.png`, `after-malformed-qstat-lines.png` |
| Mixed numeric and named worker-node identifiers | `gef_-HHUHGmj9Z_TaPJ8QjMRMw` | `upstream/main` compares `int` and `str` node IDs while deciding remapping; this PR treats mixed/non-numeric node IDs as a remapping reason. | `before-mixed-wn-identifiers.png`, `after-mixed-wn-identifiers.png` |
| Stale/rogue job IDs in `pbsnodes` | `gef_hf1no6KxRDxydduEBngNiQ` | `upstream/main` aborts when a worker-node core references a job missing from qstat; this PR skips stale core/job references with a warning. | `before-rogue-job-ids.png`, `after-rogue-job-ids.png` |
| Hostnames with underscores | `gef_lY4ZSTmI-EVeA-8kDOLPYQ` | `upstream/main` fails to match PBS hostnames containing `_`; this PR accepts underscores in PBS worker-node names. | `before-underscore-hostnames.png`, `after-underscore-hostnames.png` |

## Step 4: screenshots for the five largest runs

Largest runs were selected by counting entries in `pbsnodes_a.txt`; each has 829 worker nodes and 7,872 cores. Output is intentionally truncated in the screenshots, as allowed by #346. The screenshots are in the `qtop-artifacts` PR bundle rather than the `qtop` git tree.

| Rank | Sample | Size | Screenshot file in qtop-artifacts PR bundle |
| --- | --- | --- | --- |
| 1 | `gef_yBVifVBTyE44AKnehzSrvA` | 829 nodes / 7,872 cores | `largest-gef_yBVifVBTyE44AKnehzSrvA.png` |
| 2 | `gef_y2pQK8d9fstnQElgx8wuCw` | 829 nodes / 7,872 cores | `largest-gef_y2pQK8d9fstnQElgx8wuCw.png` |
| 3 | `gef_xwILYNMpoabX4PDGYXIZAA` | 829 nodes / 7,872 cores | `largest-gef_xwILYNMpoabX4PDGYXIZAA.png` |
| 4 | `gef_wd3MkxlAScZhd6-vEQakrg` | 829 nodes / 7,872 cores | `largest-gef_wd3MkxlAScZhd6-vEQakrg.png` |
| 5 | `gef_ts1Dxv6MCHrBhBtWM3NhGw` | 829 nodes / 7,872 cores | `largest-gef_ts1Dxv6MCHrBhBtWM3NhGw.png` |

## Step 5: implemented regression wiring

The unit regressions run through `make test`; the larger external-corpus sweep
is a separate optional target:

```bash
make test
make test-pbs-samples PBS_SAMPLES_DIR=../qtop-test-repo/qtop5/results PBS_SAMPLE_LIMIT=100
```

The implemented regression flow is:

1. Keep small unit tests in `tests/test_pbs_sample_regressions.py` for each crash class, so ordinary CI catches the logic regressions quickly.
2. Use `tools/validate_pbs_samples.py` for the larger archived PBS sweep. It renders samples with colour enabled, writes `.ans` files, and records a manifest under the selected output directory.
3. Run `make test-pbs-samples` in jobs that have access to the external archived sample repository. This avoids vendoring the large historical trace corpus into `qtop`, while still making the regression sweep reproducible.
4. The helper now forces a curated 10-sample golden set into the rendered output before filling the remaining slots. The set includes large-cluster samples from the review discussion, so `PBS_SAMPLE_LIMIT=10 make test-pbs-samples` validates exactly those golden outputs.

## Local validation commands

```bash
python3 -m pytest tests/test_pbs_sample_regressions.py -q
python3 tools/validate_pbs_samples.py ../qtop-test-repo/qtop5/results --limit 10 --output /tmp/qtop-pbs-golden-10
python3 tools/validate_pbs_samples.py ../qtop-test-repo/qtop5/results --limit 100 --output /tmp/qtop-pbs-rendered-100
```

Historical local golden-output check recorded with the original patch: `validated=10 output=/tmp/qtop-pbs-golden-10`.

Historical local sample sweep recorded with the original patch: `validated=100 output=/tmp/qtop-pbs-rendered-100`.
