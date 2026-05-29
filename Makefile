.PHONY: help test coverage lint lint-fix format-check format-fix dist confirm

PYTHON ?= python3

help:             ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help

test:             ## Run tests
	$(PYTHON) -m pytest

coverage:         ## Run tests with coverage
	$(PYTHON) -m pytest --cov=qtop_py --cov-report=term-missing

lint:             ## Run ruff linter
	ruff check .

lint-fix:         ## Auto-fix lint issues
	ruff check --fix .

format-check:     ## Check formatting
	ruff format --check .

format-fix:       ## Auto-format
	ruff format .

dist:             ## Build distribution
	pyproject-build

confirm:          ## Confirm action
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]
