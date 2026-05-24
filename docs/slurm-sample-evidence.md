# Slurm sample conformance evidence

This document records the evidence requested in #356 for Slurm support. The
bundled command traces cover three synthetic Slurm clusters, each above 256
cores, with qtop render validation wired into `make test-slurm-samples`.

Screenshot images are intentionally not stored in the main `qtop` git tree, per
`CONTRIBUTING.md`. The render helper writes sanitized terminal output to `.ans`
files so screenshots can be regenerated or submitted through `qtop-artifacts`.

## Sample clusters

| Sample | Slurm traces | Cluster size | Coverage |
| --- | --- | --- | --- |
| `basic` | `squeue.txt`, `sinfo.txt` | 10 nodes / 320 cores | running, pending, mixed, allocated, idle and down nodes |
| `multi_partition` | `squeue.txt`, `sinfo.txt` | 7 nodes / 392 cores | one node in multiple partitions, GPU/long/interactive queues, completing jobs |
| `edge_cases` | `squeue.txt`, `sinfo.txt` | 11 nodes / 328 cores | array job IDs, bracketed nodelists with gaps, suspended and requeue-hold jobs |

## Validation commands

```bash
python3 -m pytest tests/plugins/test_slurm.py -q
python3 tools/validate_slurm_samples.py tests/plugins/slurm_samples --output /tmp/qtop-slurm-rendered
make test-slurm-samples
```

The validation helper renders each sample with:

```bash
python3 -c '<portable qtop runner>' -e -A -b slurm -s <sample-dir> -c ON
```

and writes one `.ans` output plus `manifest.json` under the chosen output
directory. The manifest records the sample name, output file and total cluster
cores, and fails if any sample has 256 cores or fewer.

The `-e -A` switches keep the screenshots anonymized. The helper also replaces
local checkout paths with `<qtop-repo>` before writing screenshot input, so the
shared examples do not expose personal workstation details.

## Screenshot artifacts

The three working-example screenshots submitted with the PR comment are:

| Sample | Screenshot artifact | Rendered size |
| --- | --- | --- |
| `basic` | `slurm-basic.svg` | 10 nodes / 320 cores |
| `multi_partition` | `slurm-multi_partition.svg` | 7 nodes / 392 cores |
| `edge_cases` | `slurm-edge_cases.svg` | 11 nodes / 328 cores |

## AI assistance disclosure

This contribution was prepared with OpenAI Codex (GPT-5) in Codex Desktop on
2026-05-24. The submitted code, tests, screenshots and documentation were
reviewed in this session and remain the contributor's responsibility.
