.PHONY: test test-pbs-samples test-slurm-samples

PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered

test:
	$(PYTHON) -m pytest

test-pbs-samples:
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples:
	$(PYTHON) -m pytest tests/plugins/test_slurm.py -q
