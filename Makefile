.DEFAULT_GOAL := help

.PHONY: help test fortifications test-pbs-samples test-contrib-samples test-slurm-samples sample-gate ci

PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= build/qtop-pbs-rendered
CONTRIB_SAMPLES_DIR ?= qtop_py/contrib
CONTRIB_OUTPUT_DIR ?= build/qtop-contrib-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= build/qtop-slurm-rendered
MAX_FAILURES ?= 0

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run the Python unit test suite
	$(PYTHON) -m pytest

fortifications: ## Run lightweight repository health checks
	$(PYTHON) tools/check_fortifications.py

test-pbs-samples: ## Replay external PBS samples from qtop-test-repo
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR) --max-failures $(MAX_FAILURES)

test-contrib-samples: ## Replay bundled PBS and SGE contrib samples
	$(PYTHON) tools/validate_contrib_samples.py $(CONTRIB_SAMPLES_DIR) --output $(CONTRIB_OUTPUT_DIR) --max-failures $(MAX_FAILURES)

test-slurm-samples: ## Run Slurm parser tests and replay bundled Slurm samples
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR) --max-failures $(MAX_FAILURES)

sample-gate: test-contrib-samples test-slurm-samples ## Run fast scheduler sample replays

ci: fortifications test sample-gate ## Run the default CI gate
