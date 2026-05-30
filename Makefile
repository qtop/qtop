.DEFAULT_GOAL := help

PACKAGE := qtop
PYTHON ?= python3
PYTEST ?= $(PYTHON) -m pytest
RUFF ?= $(PYTHON) -m ruff
PYPROJECT_BUILD ?= $(PYTHON) -m build

help:             ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:          ## Install package in editable mode
	$(PYTHON) -m pip install -e .

test:             ## Run test suite
	$(PYTEST)

coverage:         ## Run tests with coverage report
	$(PYTHON) -m pip install coverage
	$(PYTHON) -m coverage run -m pytest
	$(PYTHON) -m coverage report -m

lint:             ## Run linter checks
	$(RUFF) check qtop_py/ tests/

lint-fix:         ## Auto-fix lint issues
	$(RUFF) check --fix qtop_py/ tests/

format-check:     ## Check code formatting
	$(RUFF) format --check qtop_py/ tests/

format-fix:       ## Auto-format code
	$(RUFF) format qtop_py/ tests/

dist:             ## Build source and wheel distributions
	$(PYPROJECT_BUILD)

version:          ## Show package version
	@$(PYTHON) -c "import qtop_py; print(qtop_py.__version__)"

release: confirm lint test dist  ## Lint, test, then build (prepare for release)

fortifications:   ## Run codebase health checks
	@scripts/fortifications.sh

confirm:
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]

.PHONY: help install test coverage lint lint-fix format-check format-fix dist version release fortifications confirm
