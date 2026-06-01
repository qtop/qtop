.DEFAULT_GOAL := help

.PHONY: help test test-pbs-samples test-slurm-samples test-scheduler-samples sample-gate ci build

PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered
SAMPLE_ARTIFACTS_DIR ?= qtop-sample-artifacts
MAX_FAILURES ?= 0

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'

test: ## Run the unit test suite.
	$(PYTHON) -m pytest

test-pbs-samples: ## Render archived external PBS samples when qtop-test-repo is available.
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples: ## Render bundled Slurm command-trace samples.
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)

test-scheduler-samples: ## Run the shared PBS/OAR/SGE/Slurm sample gate and write artifacts.
	$(PYTHON) tools/validate_scheduler_samples.py --output $(SAMPLE_ARTIFACTS_DIR) --max-failures $(MAX_FAILURES) --slurm-samples-dir $(SLURM_SAMPLES_DIR)

sample-gate: test-scheduler-samples ## Alias used by GitHub Actions and GitLab CI.

build: ## Build the source distribution and wheel.
	$(PYTHON) -m build

ci: test sample-gate ## Run the local CI entry points shared by hosted CI.
