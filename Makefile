.PHONY: help test lint sample-gate ci test-pbs-samples test-slurm-samples confirm

PYTHON ?= python3
SAMPLE_GATE_OUTPUT ?= artifacts/sample-gate
SAMPLE_GATE_MAX_FAILURES ?= 0
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered

.DEFAULT_GOAL := help

help:             ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test:             ## Run the unit test suite
	$(PYTHON) -m pytest

lint:             ## Run lightweight changed-file fortifications
	$(PYTHON) tools/fortifications.py

sample-gate:      ## Render bundled PBS/OAR/SGE/SLURM samples with zero-failure policy
	$(PYTHON) tools/sample_gate.py --output $(SAMPLE_GATE_OUTPUT) --max-failures $(SAMPLE_GATE_MAX_FAILURES)

ci: lint test sample-gate ## Run the shared local/GitHub/GitLab CI gate

test-pbs-samples: ## Render the external qtop-test-repo PBS sample archive
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples: ## Run Slurm parser tests and render bundled Slurm command traces
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)

confirm:          ## Ask for human confirmation before manual release-like operations
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]
