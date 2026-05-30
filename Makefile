.DEFAULT_GOAL := help

PYTHON ?= python
PYTEST ?= $(PYTHON) -m pytest
ARTIFACT_DIR ?= artifacts/qtop-sample-gate
MAX_FAILURES ?= 0

.PHONY: help test coverage sample-gate fortifications ci confirm

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run the unit test suite
	$(PYTEST)

coverage: ## Run tests with coverage output for CI
	$(PYTEST) --cov=qtop_py --cov-report=term-missing --cov-report=xml

sample-gate: ## Run the fast sample validation gate and write review artifacts
	$(PYTHON) scripts/sample_gate.py --max-failures $(MAX_FAILURES) --artifact-dir "$(ARTIFACT_DIR)"

fortifications: ## Inspect codebase health and CI-sensitive changes
	$(PYTHON) scripts/fortifications.py

ci: fortifications sample-gate coverage ## Shared CI entry point for GitHub and GitLab

confirm: ## Ask for human confirmation before destructive release steps
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]
