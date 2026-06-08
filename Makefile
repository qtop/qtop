.DEFAULT_GOAL := help

.PHONY: help all rerun clean ci-deps test coverage sample-gate backend-validation backend-colour-artifacts test-pbs-samples test-slurm-samples fortifications ruff-check lint lint-fix format-check format-fix compat-py36 ci github-ci gitlab-ci build github-build gitlab-build dist version confirm

PYTHON ?= python3
PIP ?= $(PYTHON) -m pip
SAMPLE_GATE_SCHEDULERS ?= pbs,sge,slurm,oar,demo
SAMPLE_GATE_MAX_FAILURES ?= 0
SAMPLE_GATE_ARTIFACT_DIR ?= artifacts/sample-gate
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 447
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered
FORTIFY_BASE_REF ?= origin/develop

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

all: help ci-deps test coverage sample-gate backend-validation backend-colour-artifacts test-pbs-samples test-slurm-samples fortifications ruff-check lint format-check compat-py36 ci github-ci gitlab-ci build github-build gitlab-build dist version ## Run every non-mutating validation and build target

rerun: ## Clean generated files, then run the complete validation path
	$(MAKE) clean
	$(MAKE) all

clean: confirm ## Remove generated validation and build output after confirmation
	rm -rf artifacts build dist .coverage .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete

ci-deps: ## Install pinned CI Python dependencies
	$(PIP) install -r requirements-ci.txt

test: ## Run the Python test suite
	$(PYTHON) -m pytest

coverage: ## Run tests with coverage.py and print a terminal report
	$(PYTHON) -m coverage run -m pytest
	$(PYTHON) -m coverage report

sample-gate: ## Run fast PBS/SGE/Slurm/OAR/demo sample gates
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers $(SAMPLE_GATE_SCHEDULERS) --max-failures $(SAMPLE_GATE_MAX_FAILURES) --artifact-dir $(SAMPLE_GATE_ARTIFACT_DIR)

backend-validation: sample-gate ## Validate PBS/SGE/Slurm/OAR/demo through one local-first path

backend-colour-artifacts: sample-gate ## Render ANSI-colour text and SVG review artifacts for every backend

test-pbs-samples: ## Run the larger archived PBS sample sweep
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples: ## Run Slurm parser tests and render committed Slurm samples
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)

fortifications: ## Check diff health and reject eval() call sites
	$(PYTHON) tools/fortifications.py --base-ref $(FORTIFY_BASE_REF)

ruff-check: ## Run ruff against the source tree
	$(PYTHON) -m ruff check .

lint: ruff-check fortifications ## Run dependency-light source and diff health checks

lint-fix: ## Auto-fix ruff lint issues where possible
	$(PYTHON) -m ruff check --fix .

format-check: ## Check the branch diff for whitespace errors
	git diff --check $(FORTIFY_BASE_REF)...HEAD

format-fix: ## Auto-format Python files with ruff
	$(PYTHON) -m ruff format .

compat-py36: ## Run dependency-light Python 3.6 compatibility checks
	find qtop_py tools -name '*.py' -print | xargs $(PYTHON) -m py_compile
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers $(SAMPLE_GATE_SCHEDULERS) --max-failures $(SAMPLE_GATE_MAX_FAILURES) --artifact-dir $(SAMPLE_GATE_ARTIFACT_DIR)-py36

ci: test backend-validation lint format-check ## Run the shared local/CI validation path

github-ci: ci ## GitHub Actions entry point for test validation

gitlab-ci: ci ## GitLab CI entry point for test validation

build: ci-deps ## Build source and wheel distributions with pinned CI build deps
	$(PYTHON) -m build --no-isolation

github-build: build ## GitHub Actions entry point for package builds

gitlab-build: build ## GitLab CI entry point for package builds

dist: build ## Alias for build, matching common release target names

version: ## Print the package version
	$(PYTHON) -c "import qtop_py; print(qtop_py.__version__)"

confirm: ## Ask for human confirmation before release-like tasks
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]
