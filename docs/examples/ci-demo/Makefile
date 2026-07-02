.DEFAULT_GOAL := help

.PHONY: help ci-deps test coverage coverage-report lint lint-fix format-check format-fix build clean ci

PYTHON ?= python3
PIP ?= $(PYTHON) -m pip
COVERAGE_HTML_DIR ?= htmlcov

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

ci-deps: ## Install pinned CI Python dependencies
	$(PIP) install -r requirements-ci.txt

test: ## Run the Python test suite
	$(PYTHON) -m pytest

coverage: ## Run tests with coverage.py and print a terminal report
	$(PYTHON) -m coverage run -m pytest
	$(PYTHON) -m coverage report

coverage-report: ## Generate an HTML coverage report (open index.html in a browser)
	$(PYTHON) -m coverage run -m pytest
	$(PYTHON) -m coverage report
	$(PYTHON) -m coverage html --directory=$(COVERAGE_HTML_DIR)
	@echo "Coverage HTML report written to $(COVERAGE_HTML_DIR)/index.html"

lint: ## Run ruff linter
	$(PYTHON) -m ruff check .

lint-fix: ## Auto-fix ruff lint issues where possible
	$(PYTHON) -m ruff check --fix .

format-check: ## Check ruff formatting
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m ruff format --check --diff .

format-fix: ## Auto-format Python files with ruff
	$(PYTHON) -m ruff format .

build: ci-deps ## Build source and wheel distributions
	$(PYTHON) -m build --no-isolation

ci: test lint format-check ## Run the shared local/CI validation path

clean: ## Remove generated build and cache files
	rm -rf build dist .coverage .pytest_cache .ruff_cache $(COVERAGE_HTML_DIR) ci_demo.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete
