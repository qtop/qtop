.DEFAULT_GOAL := help

PYTHON      ?= python3
PYTEST      ?= pytest
MAX_FAILURES ?= 0
SAMPLE_DIR  := qtop_py/contrib

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------

.PHONY: test
test:  ## Run unit tests with pytest
	$(PYTEST) tests/ -v

.PHONY: coverage
coverage:  ## Run tests and emit coverage report (XML + terminal)
	$(PYTEST) tests/ --cov=qtop_py --cov-report=term-missing --cov-report=xml:coverage.xml -v

.PHONY: sample-validate
sample-validate:  ## Run PBS/OAR/SGE sample gate (set MAX_FAILURES=0 to hard-fail)
	@echo "=== sample-validate: scheduler sample gate (max-failures=$(MAX_FAILURES)) ==="
	@mkdir -p sample-artifacts
	@cd $(SAMPLE_DIR) && bash func_tests.sh 2>&1 | tee ../../sample-artifacts/sample-run.log
	@echo "=== sample-validate: PASSED ==="

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

.PHONY: lint
lint:  ## Lint with ruff (errors only)
	ruff check qtop_py/ tests/

.PHONY: lint-fix
lint-fix:  ## Lint and auto-fix with ruff
	ruff check --fix qtop_py/ tests/

.PHONY: format-check
format-check:  ## Check formatting with ruff format
	ruff format --check qtop_py/ tests/

.PHONY: format-fix
format-fix:  ## Apply ruff formatting
	ruff format qtop_py/ tests/

.PHONY: fortify
fortify:  ## Run codebase security / health checks
	@bash scripts/fortify.sh

# ---------------------------------------------------------------------------
# Distribution
# ---------------------------------------------------------------------------

.PHONY: dist
dist:  ## Build sdist + wheel
	$(PYTHON) -m build

.PHONY: version
version:  ## Print the project version
	@$(PYTHON) -c "from qtop_py import __version__; print(__version__)"

# ---------------------------------------------------------------------------
# Release (interactive – requires human confirmation)
# ---------------------------------------------------------------------------

confirm:  ## Prompt for y/N confirmation before continuing
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]

.PHONY: release
release: confirm lint test dist  ## Full release pipeline: confirm → lint → test → dist
	@echo "Release artifacts ready in dist/. Tag and publish manually."

# ---------------------------------------------------------------------------
# CI entry-point: single command both GitHub Actions and GitLab CI call
# ---------------------------------------------------------------------------

.PHONY: ci
ci: lint test coverage sample-validate fortify  ## Full CI gate (lint + test + coverage + sample + fortify)
