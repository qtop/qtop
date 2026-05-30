.DEFAULT_GOAL := help

.PHONY: help ci-deps test sample-gate test-pbs-samples test-slurm-samples fortifications compat-py36 ci github-ci gitlab-ci build github-build gitlab-build confirm

PYTHON ?= python3
SAMPLE_GATE_SCHEDULERS ?= pbs,sge,slurm
SAMPLE_GATE_MAX_FAILURES ?= 0
SAMPLE_GATE_ARTIFACT_DIR ?= artifacts/sample-gate
SAMPLE_GATE_TIMEOUT ?= 20
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered
FORTIFY_BASE_REF ?= origin/develop

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

ci-deps: ## Install pinned CI dependencies
	$(PYTHON) -m pip install -r requirements-ci.txt

test: ## Run the Python test suite
	$(PYTHON) -m pytest

sample-gate: ## Run the fast committed scheduler sample gate
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers $(SAMPLE_GATE_SCHEDULERS) --max-failures $(SAMPLE_GATE_MAX_FAILURES) --timeout $(SAMPLE_GATE_TIMEOUT) --artifact-dir $(SAMPLE_GATE_ARTIFACT_DIR)

test-pbs-samples: ## Run the larger external PBS sample sweep
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples: ## Run Slurm unit tests and render committed Slurm samples
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)

fortifications: ## Check diff health and reject executable eval() call sites
	$(PYTHON) tools/fortifications.py --base-ref $(FORTIFY_BASE_REF)

compat-py36: ## Run dependency-light Python 3.6 compatibility checks
	find qtop_py tools -name '*.py' -print | xargs $(PYTHON) -m py_compile
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers $(SAMPLE_GATE_SCHEDULERS) --max-failures $(SAMPLE_GATE_MAX_FAILURES) --timeout $(SAMPLE_GATE_TIMEOUT) --artifact-dir $(SAMPLE_GATE_ARTIFACT_DIR)-py36

ci: test sample-gate fortifications ## Run the shared local/GitHub/GitLab validation path

github-ci gitlab-ci: ci ## Provider entry points for the shared CI path

build: ## Build source and wheel distributions
	$(PYTHON) -m build

github-build gitlab-build: build ## Provider entry points for package builds

confirm: ## Ask for human confirmation before release-like actions
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]
