.PHONY: help test lint lint-fix format-check format-fix coverage build clean install dist version release confirm all

.DEFAULT_GOAL := help

PYTHON := python
PYTEST := pytest
RUFF := ruff
BUILD := pyproject-build

help:             ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test:             ## Run tests
	$(PYTHON) -m $(PYTEST) -v

lint:             ## Check code with ruff
	$(RUFF) check qtop_py tests

lint-fix:         ## Auto-fix lint issues
	$(RUFF) check --fix qtop_py tests

format-check:     ## Check code formatting
	$(RUFF) format --check qtop_py tests

format-fix:       ## Auto-format code
	$(RUFF) format qtop_py tests

coverage:         ## Run tests with coverage report
	$(PYTHON) -m $(PYTEST) --cov=qtop_py --cov-report=term-missing --cov-report=xml

build:            ## Build package
	$(BUILD)

dist: build       ## Alias for build
	@echo "Package built in dist/"

version:          ## Show current version
	@$(PYTHON) -c "import qtop_py; print(qtop_py.__version__)"

clean:            ## Remove build artifacts
	rm -rf dist build *.egg-info .coverage coverage.xml .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

install:          ## Install in development mode
	pip install -e .

confirm:          ## Prompt for confirmation
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]

release: lint test build  ## Run full release pipeline: lint, test, build
	@echo "Release ready. Tag and publish manually."

all: lint test build  ## Run lint, test, build
