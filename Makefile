.PHONY: help test ci test-samples test-pbs-samples test-pbs-samples-if-present test-slurm-samples fortify

PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
SAMPLE_ARTIFACTS_DIR ?= sample-artifacts
PBS_OUTPUT_DIR ?= $(SAMPLE_ARTIFACTS_DIR)/pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= $(SAMPLE_ARTIFACTS_DIR)/slurm-rendered
MAX_FAILURES ?= 0

help:
	@echo "qtop development targets:"
	@echo "  make test                         Run the Python unit test suite"
	@echo "  make test-samples                 Run available scheduler sample gates"
	@echo "  make test-pbs-samples             Require PBS archive samples and render artifacts"
	@echo "  make test-pbs-samples-if-present  Render PBS samples when PBS_SAMPLES_DIR exists"
	@echo "  make test-slurm-samples           Run bundled Slurm command-trace samples"
	@echo "  make fortify                      Run lightweight source hardening checks"
	@echo "  make ci                           Shared local/GitHub/GitLab CI entrypoint"

test:
	$(PYTHON) -m pytest

ci: test test-samples fortify

test-samples: test-pbs-samples-if-present test-slurm-samples

test-pbs-samples:
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR) --max-failures $(MAX_FAILURES)

test-pbs-samples-if-present:
	@if [ -d "$(PBS_SAMPLES_DIR)" ]; then \
		$(MAKE) test-pbs-samples; \
	else \
		echo "Skipping PBS sample gate: PBS_SAMPLES_DIR=$(PBS_SAMPLES_DIR) is not present."; \
	fi

test-slurm-samples:
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR) --max-failures $(MAX_FAILURES)

fortify:
	$(PYTHON) tools/check_eval_usage.py
