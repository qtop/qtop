.DEFAULT_GOAL := help

PYTHON   ?= python3
PIP      ?= pip3
RUFF     ?= ruff
PYTEST   ?= python3 -m pytest
COVERAGE ?= python3 -m pytest --cov=qtop_py

help:             ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: test coverage lint lint-fix format-check format-fix dist fortifications all

test:             ## Run tests via pytest
	$(PYTEST)

coverage:         ## Run tests with coverage report
	$(COVERAGE)

lint:             ## Run ruff linter
	$(RUFF) check .

lint-fix:         ## Run ruff linter with automatic fixes
	$(RUFF) check --fix .

format-check:     ## Check code formatting with ruff
	$(RUFF) format --check .

format-fix:       ## Auto-format code with ruff
	$(RUFF) format .

dist:             ## Build source and wheel distributions
	python3 -m build

fortifications:   ## Run code health fortifications check
	@scripts/fortifications.sh

all: lint test    ## Run lint and test (default CI pipeline)

confirm:
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]
