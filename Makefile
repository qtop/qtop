.PHONY: test test-samples test-contrib-samples test-pbs-samples test-slurm-samples

PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered
SAMPLE_MAX_FAILURES ?= 0
SKIP_MISSING_PBS_SAMPLES ?= 1

test:
	$(PYTHON) -m pytest

test-samples: test-contrib-samples test-slurm-samples
	@if [ -d "$(PBS_SAMPLES_DIR)" ]; then \
		$(MAKE) test-pbs-samples; \
	elif [ "$(SKIP_MISSING_PBS_SAMPLES)" = "1" ]; then \
		echo "Skipping PBS sample gate: $(PBS_SAMPLES_DIR) not found"; \
	else \
		echo "PBS sample gate required but $(PBS_SAMPLES_DIR) was not found"; \
		exit 1; \
	fi

test-contrib-samples:
	$(PYTHON) tools/validate_contrib_samples.py --output $(SLURM_OUTPUT_DIR)/../qtop-contrib-rendered

test-pbs-samples:
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples:
	$(PYTHON) -m pytest tests/plugins/test_slurm.py --maxfail=$(SAMPLE_MAX_FAILURES)
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)
