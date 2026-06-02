.PHONY: help test ci sample-validate fortifications test-pbs-samples test-slurm-samples

PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_SAMPLE_LIMIT ?= 0
SAMPLE_SCHEDULERS ?= slurm
SAMPLE_MAX_FAILURES ?= 0
SAMPLE_OUTPUT_DIR ?= artifacts/qtop-sample-gate

.DEFAULT_GOAL := help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run the Python test suite
	$(PYTHON) -m pytest

ci: test sample-validate fortifications ## Run the default CI checks

sample-validate: ## Render small scheduler sample traces and write manifests
	$(PYTHON) tools/validate_samples.py --scheduler $(SAMPLE_SCHEDULERS) --slurm-samples-dir $(SLURM_SAMPLES_DIR) --pbs-samples-dir $(PBS_SAMPLES_DIR) --slurm-limit $(SLURM_SAMPLE_LIMIT) --pbs-limit $(PBS_SAMPLE_LIMIT) --max-failures $(SAMPLE_MAX_FAILURES) --output $(SAMPLE_OUTPUT_DIR)

fortifications: ## Check changed files for hidden control text, generated artifacts, and new dynamic evaluation
	$(PYTHON) tools/fortifications.py

test-pbs-samples: ## Render archived PBS samples from the external qtop-test-repo corpus
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --max-failures $(SAMPLE_MAX_FAILURES) --output $(SAMPLE_OUTPUT_DIR)/pbs

test-slurm-samples: ## Run Slurm parser tests and render local Slurm command traces
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --limit $(SLURM_SAMPLE_LIMIT) --max-failures $(SAMPLE_MAX_FAILURES) --output $(SAMPLE_OUTPUT_DIR)/slurm
