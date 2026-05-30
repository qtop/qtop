PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered
SGE_SAMPLES_DIR ?= tests/plugins/sge_samples
SGE_OUTPUT_DIR ?= /tmp/qtop-sge-rendered
MAX_FAILURES ?= 0

.DEFAULT_GOAL := help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

confirm:
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]

test: ## Run all pytest tests
	$(PYTHON) -m pytest

coverage: ## Run tests with coverage report
	$(PYTHON) -m pytest --cov=qtop_py --cov-report=term-missing

lint: ## Run ruff linter
	$(PYTHON) -m ruff check qtop_py/ tests/ tools/

lint-fix: ## Auto-fix lint issues
	$(PYTHON) -m ruff check --fix qtop_py/ tests/ tools/

format-check: ## Check formatting with ruff
	$(PYTHON) -m ruff format --check qtop_py/ tests/ tools/

format-fix: ## Format code with ruff
	$(PYTHON) -m ruff format qtop_py/ tests/ tools/

fortify: ## Run fortifications check
	$(PYTHON) tools/fortifications.py

sample-gate: ## Run scheduler sample gates
	$(PYTHON) tools/sample_gate.py \
		--pbs-samples "$(PBS_SAMPLES_DIR)" --pbs-limit $(PBS_SAMPLE_LIMIT) --pbs-output "$(PBS_OUTPUT_DIR)" \
		--slurm-samples "$(SLURM_SAMPLES_DIR)" --slurm-output "$(SLURM_OUTPUT_DIR)" \
		--sge-samples "$(SGE_SAMPLES_DIR)" --sge-output "$(SGE_OUTPUT_DIR)" \
		--max-failures $(MAX_FAILURES)

test-pbs-samples: ## Validate PBS sample rendering
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples: ## Validate SLURM sample rendering
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)

pre-commit: ## Run pre-commit hooks on all files
	$(PYTHON) -m pre_commit run --all-files

dist: ## Build source & wheel distribution
	$(PYTHON) -m build

release: lint test fortify sample-gate dist ## Full release checklist
	@echo "Release checklist complete."

.PHONY: help confirm test coverage lint lint-fix format-check format-fix fortify sample-gate test-pbs-samples test-slurm-samples pre-commit dist release
