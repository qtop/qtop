.PHONY: help test test-pbs-samples test-slurm-samples sample-gate lint lint-fix format-check format-fix coverage fortify build clean

PYTHON ?= python3
BASE_REF ?= origin/develop
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered
SAMPLE_SCHEDULERS ?= slurm,pbs,sge
SAMPLE_LIMIT ?= 6
SAMPLE_MAX_FAILURES ?= 0
SAMPLE_OUTPUT_DIR ?= artifacts/sample-gate

.DEFAULT_GOAL := help

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_.-]+:.*## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: ## Run the pytest suite
	$(PYTHON) -m pytest

test-pbs-samples: ## Validate external PBS sample renders
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples: ## Validate bundled Slurm sample renders
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)

sample-gate: ## Run the fast scheduler sample gate used by GitHub and GitLab CI
	$(PYTHON) tools/validate_samples.py --schedulers $(SAMPLE_SCHEDULERS) --limit $(SAMPLE_LIMIT) --max-failures $(SAMPLE_MAX_FAILURES) --output $(SAMPLE_OUTPUT_DIR) --pbs-samples-dir $(PBS_SAMPLES_DIR) --slurm-samples-dir $(SLURM_SAMPLES_DIR)

lint: ## Run ruff checks without modifying files
	$(PYTHON) -m ruff check .

lint-fix: ## Apply safe ruff fixes
	$(PYTHON) -m ruff check --fix .

format-check: ## Show ruff fixes that would be applied
	$(PYTHON) -m ruff check --diff .

format-fix: ## Apply safe formatting-style fixes
	$(PYTHON) -m ruff check --fix .

coverage: ## Run tests and print a coverage report
	$(PYTHON) -m coverage run -m pytest
	$(PYTHON) -m coverage report

fortify: ## Check the branch diff for accidental artifacts and suspicious text
	$(PYTHON) tools/fortify_diff.py --base $(BASE_REF)

build: ## Build source and wheel distributions
	$(PYTHON) -m build

clean: ## Remove local build and test outputs
	rm -rf build dist qtop.egg-info .coverage .pytest_cache artifacts
