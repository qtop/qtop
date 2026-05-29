.DEFAULT_GOAL := help

PYTHON ?= python
PYTEST ?= $(PYTHON) -m pytest
COVERAGE ?= $(PYTHON) -m coverage
SAMPLE_GATE_SCHEDULERS ?= pbs,sge
SAMPLE_GATE_MAX_FAILURES ?= 0
SAMPLE_GATE_ARTIFACT_DIR ?= artifacts/sample-gate
BASE_REF ?= origin/main

.PHONY: help test coverage lint format-check sample-gate fortifications ci dist clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run unit tests
	$(PYTEST)

coverage: ## Run unit tests with coverage report
	$(COVERAGE) run -m pytest
	$(COVERAGE) report -m

lint: ## Run ruff lint if installed
	$(PYTHON) -m ruff check qtop_py tests

format-check: ## Run ruff formatting check if installed
	$(PYTHON) -m ruff format --check qtop_py tests

sample-gate: ## Validate committed scheduler samples and save artifacts
	$(PYTHON) scripts/sample_gate.py --schedulers "$(SAMPLE_GATE_SCHEDULERS)" --max-failures "$(SAMPLE_GATE_MAX_FAILURES)" --artifact-dir "$(SAMPLE_GATE_ARTIFACT_DIR)"

fortifications: ## Inspect diff health, generated files, control chars, and new evals
	$(PYTHON) scripts/fortifications.py --base-ref "$(BASE_REF)"

ci: fortifications coverage sample-gate ## Run the shared CI entry point

dist: ## Build source and wheel distributions
	$(PYTHON) -m build

clean: ## Remove local build/test artifacts
	rm -rf .coverage build dist *.egg-info artifacts
