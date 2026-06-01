.DEFAULT_GOAL := help

PYTHON ?= python3
PYTEST ?= $(PYTHON) -m pytest
MAX_FAILURES ?= 0
ARTIFACT_DIR ?= sample-artifacts

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: lint
lint: ## Run syntax-focused ruff checks without forcing a style cleanup.
	$(PYTHON) -m ruff check --select E9,F63,F7,F82 qtop_py tests scripts

.PHONY: test
test: ## Run the unit test suite.
	$(PYTEST) tests/ -q

.PHONY: coverage
coverage: ## Run unit tests with a coverage XML artifact.
	$(PYTEST) tests/ --cov=qtop_py --cov-report=term --cov-report=xml:coverage.xml -q

.PHONY: sample-validate
sample-validate: ## Run checked-in PBS/OAR/SGE scheduler samples.
	$(PYTHON) scripts/sample_gate.py --artifact-dir $(ARTIFACT_DIR) --max-failures $(MAX_FAILURES)

.PHONY: fortify
fortify: ## Check for eval(), bidi markers, and compiled artifacts.
	$(PYTHON) scripts/fortify.py

.PHONY: dist
dist: ## Build source and wheel distributions.
	$(PYTHON) -m build

.PHONY: ci
ci: lint test sample-validate fortify ## Shared CI entry point for GitHub and GitLab.
