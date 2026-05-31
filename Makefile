.DEFAULT_GOAL := help

PYTHON ?= python3
SAMPLE_GATE_MAX_FAILURES ?= 0
SAMPLE_GATE_SCHEDULERS ?= pbs,oar,sge
SAMPLE_GATE_ARTIFACT_DIR ?= artifacts/qtop-sample-gate
FORTIFY_BASE_REF ?= origin/develop

.PHONY: help test sample-gate ci-sample-gate fortify coverage lint lint-fix format-check format-fix dist release confirm

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run the Python test suite
	$(PYTHON) -m pytest

sample-gate: ## Run bundled PBS/OAR/SGE qtop sample checks
	$(PYTHON) scripts/qtop_sample_gate.py \
		--schedulers "$(SAMPLE_GATE_SCHEDULERS)" \
		--artifact-dir "$(SAMPLE_GATE_ARTIFACT_DIR)" \
		--max-failures "$(SAMPLE_GATE_MAX_FAILURES)"

fortify: ## Check the current diff for risky content
	$(PYTHON) scripts/fortify_diff.py --base-ref "$(FORTIFY_BASE_REF)"

ci-sample-gate: fortify sample-gate ## Run the shared CI sample validation gate

coverage: ## Run tests with coverage when coverage tooling is installed
	$(PYTHON) -m coverage run -m pytest
	$(PYTHON) -m coverage report

lint: ## Run ruff linting when ruff is installed
	$(PYTHON) -m ruff check .

lint-fix: ## Run ruff linting with fixes when ruff is installed
	$(PYTHON) -m ruff check --fix .

format-check: ## Check formatting when ruff is installed
	$(PYTHON) -m ruff format --check .

format-fix: ## Format code when ruff is installed
	$(PYTHON) -m ruff format .

dist: ## Build a source/wheel distribution
	$(PYTHON) -m build

release: lint test dist ## Compose release checks without publishing

confirm: ## Ask for human confirmation before sensitive steps
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]
