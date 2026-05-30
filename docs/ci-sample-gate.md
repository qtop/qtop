# CI Sample Gate

## Purpose

The sample gate runs qtop against archived scheduler outputs to verify
that parsing, rendering, and output generation work correctly across
different scheduler families (PBS, SLURM, SGE).

It is the shared entry point invoked by both GitHub Actions and GitLab CI
so the two platforms do not drift.

## Sample Sources

### PBS samples
- **Source**: `qtop-test-repo/qtop5/results/`
- **Format**: One subdirectory per sample, each containing scheduler output
  files (qstat, pbsnodes, etc.)
- **Golden set**: 10 curated samples listed in `tools/validate_pbs_samples.py`
  that must always pass.
- **Limit**: 100 samples max per run (configurable via `PBS_SAMPLE_LIMIT`).

### SLURM samples
- **Source**: `tests/plugins/slurm_samples/`
- **Format**: Subdirectories each containing `squeue.txt` and `sinfo.txt`.
- **Coverage**: basic, mixed, large_cluster, large_mixed,
  multi_partition, large_multi_partition.

### SGE samples
- **Source**: `tests/plugins/sge_samples/` (if available)
- **Format**: Subdirectories with SGE command output files.

## Failure Policy

The `--max-failures` threshold controls how many scheduler gates can fail
before the pipeline reports failure.  Default: `0` (all must pass).

A gate failure occurs when:
1. A required sample source directory does not exist (skipped, not failed).
2. qtop exits non-zero or produces no output for a sample.
3. The golden PBS sample set cannot be fully rendered.

## Artifacts

- PBS: rendered `.ans` files + `manifest.json` in `PBS_OUTPUT_DIR`.
- SLURM: rendered output in `SLURM_OUTPUT_DIR`.
- Aggregate manifest: `/tmp/sample-gate-manifest.json`.

## Adding New Samples

1. Place scheduler output files in a new subdirectory under the appropriate
   `tests/plugins/*_samples/` directory.
2. Verify locally: `make sample-gate`.
3. For new golden PBS samples, add the directory name to `GOLDEN_PBS_SAMPLES`
   in `tools/validate_pbs_samples.py`.

## Reproducing Locally

```sh
# Run the full sample gate
make sample-gate

# Run individual scheduler gates
make test-pbs-samples
make test-slurm-samples
```
