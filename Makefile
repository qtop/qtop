.DEFAULT_GOAL := help

.PHONY: help test test-pbs-samples test-slurm-samples fortifications lint format-check

PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered
FORTIFY_BASE_REF ?= origin/develop

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run the Python test suite
	$(PYTHON) -m pytest

test-pbs-samples: ## Run the larger archived PBS sample sweep
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples: ## Run Slurm parser tests and render committed Slurm samples
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)

fortifications: ## Check diff health and reject eval() call sites
	$(PYTHON) tools/fortifications.py --base-ref $(FORTIFY_BASE_REF)

lint: fortifications ## Run dependency-light source and diff health checks

format-check: ## Check the branch diff for whitespace errors
	git diff --check $(FORTIFY_BASE_REF)...HEAD
