PYTHON ?= python3
PYTEST ?= $(PYTHON) -m pytest
SAMPLE_MAX_FAILURES ?= 0
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered

.PHONY: test sample-gate test-pbs-samples test-slurm-samples ci

test:
	$(PYTEST)

sample-gate:
	$(PYTHON) scripts/sample_gate.py --max-failures $(SAMPLE_MAX_FAILURES)

test-pbs-samples:
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples:
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)

ci: test sample-gate test-slurm-samples
