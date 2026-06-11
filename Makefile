.DEFAULT_GOAL := help

.PHONY: help all rerun clean ci-deps test coverage coverage-xml sample-gate backend-validation backend-colour-artifacts render-backends trace-export-validation test-pbs-samples test-slurm-samples fortifications repo-sanity code-quality license-report ruff-check lint lint-fix format-check format-fix compat-py36 ci nightly-ci github-ci gitlab-ci build github-build gitlab-build dist version confirm

PYTHON ?= python3
PIP ?= $(PYTHON) -m pip
SAMPLE_GATE_SCHEDULERS ?= pbs,sge,slurm,oar,demo
SAMPLE_GATE_MAX_FAILURES ?= 0
SAMPLE_GATE_ARTIFACT_DIR ?= artifacts/sample-gate
BACKEND_COLOUR_ARTIFACT_DIR ?= artifacts/backend-colour
TRACE_JSON_B64 ?=
TRACE_FULLVIEW ?=
TRACE_ARTIFACT_DIR ?= artifacts/trace-export
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 447
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered
FORTIFY_BASE_REF ?= origin/develop
COVERAGE_XML ?= artifacts/coverage/coverage.xml
REPO_SANITY_DIR ?= artifacts/repo-sanity
CODE_QUALITY_JSON ?= artifacts/code-quality/gl-code-quality-report.json
LICENSE_DIR ?= artifacts/license

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

all: ci-deps test coverage backend-validation backend-colour-artifacts render-backends trace-export-validation test-pbs-samples test-slurm-samples fortifications ruff-check lint format-check compat-py36 ci github-ci gitlab-ci build github-build gitlab-build dist version ## Run every local validation and build target

rerun: clean all ## Clean generated files, then rerun the complete local validation path

clean: confirm ## Remove generated validation, build, and Python cache output
	rm -rf artifacts build dist .coverage .pytest_cache .ruff_cache qtop.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete

ci-deps: ## Install pinned CI Python dependencies
	$(PIP) install -r requirements-ci.txt

test: ## Run the Python test suite
	$(PYTHON) -m pytest

coverage: ## Run tests with coverage.py and print a terminal report
	$(PYTHON) -m coverage run -m pytest
	$(PYTHON) -m coverage report

coverage-xml: ## Run tests under coverage.py and write a Cobertura XML report (GitLab/Codecov ready)
	$(PYTHON) -m coverage run -m pytest
	$(PYTHON) -m coverage report
	@mkdir -p $(dir $(COVERAGE_XML))
	$(PYTHON) -m coverage xml -o $(COVERAGE_XML)
	@echo "wrote $(COVERAGE_XML)"

sample-gate: ## Run fast committed PBS/SGE/Slurm/OAR/demo qtop sample gates
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers $(SAMPLE_GATE_SCHEDULERS) --max-failures $(SAMPLE_GATE_MAX_FAILURES) --artifact-dir $(SAMPLE_GATE_ARTIFACT_DIR)

backend-validation: sample-gate ## Validate PBS/SGE/Slurm/OAR/demo through the shared local-first path

backend-colour-artifacts: ## Render ANSI-colour text and SVG review artifacts for every backend
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers $(SAMPLE_GATE_SCHEDULERS) --max-failures $(SAMPLE_GATE_MAX_FAILURES) --artifact-dir $(BACKEND_COLOUR_ARTIFACT_DIR)

render-backends: backend-colour-artifacts ## Alias for backend-colour-artifacts

trace-export-validation: ## Validate an exported qtop JSON trace when TRACE_JSON_B64 is provided
	@if [ -n "$(TRACE_JSON_B64)" ]; then \
		args="--artifact-dir $(TRACE_ARTIFACT_DIR)"; \
		if [ -n "$(TRACE_FULLVIEW)" ]; then args="$$args --fullview $(TRACE_FULLVIEW)"; fi; \
		$(PYTHON) tools/validate_trace_export.py "$(TRACE_JSON_B64)" $$args; \
	else \
		echo "Skipping exported trace validation: TRACE_JSON_B64 not set"; \
	fi

test-pbs-samples: ## Run the larger archived PBS sample sweep when the external corpus is available
	@if [ -d "$(PBS_SAMPLES_DIR)" ]; then \
		$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR); \
	else \
		echo "Skipping archived PBS sample sweep: $(PBS_SAMPLES_DIR) not found"; \
	fi

test-slurm-samples: ## Run Slurm parser tests and render committed Slurm samples
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)

fortifications: ## Check diff health and reject eval() call sites
	$(PYTHON) tools/fortifications.py --base-ref $(FORTIFY_BASE_REF)

repo-sanity: ## Audit the full tracked tree for bidi/invisible/confusable characters (text trust, qtop/qtop#488)
	$(PYTHON) tools/repo_sanity.py --report-dir $(REPO_SANITY_DIR)

code-quality: ## Emit GitLab Code Quality JSON from ruff (upstream CodeClimate template is deprecated)
	@mkdir -p $(dir $(CODE_QUALITY_JSON))
	$(PYTHON) -m ruff check --output-format gitlab --exit-zero . > $(CODE_QUALITY_JSON)
	@echo "wrote $(CODE_QUALITY_JSON)"

license-report: ## Report licenses of the pinned CI dependency set (pip-licenses, pinned in-job)
	$(PIP) install pip-licenses==5.0.0
	@mkdir -p $(LICENSE_DIR)
	$(PYTHON) -m piplicenses --format=markdown --with-urls > $(LICENSE_DIR)/ci-dependencies.md
	$(PYTHON) -m piplicenses --format=json > $(LICENSE_DIR)/ci-dependencies.json
	@echo "wrote $(LICENSE_DIR)"

ruff-check: ## Run ruff against the source tree
	$(PYTHON) -m ruff check .

lint: ruff-check fortifications ## Run dependency-light source and diff health checks

lint-fix: ## Auto-fix ruff lint issues where possible
	$(PYTHON) -m ruff check --fix .

format-check: ## Check ruff formatting and branch diff whitespace
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m ruff format --check --diff .
	git diff --check $(FORTIFY_BASE_REF)...HEAD

format-fix: ## Auto-format Python files with ruff
	$(PYTHON) -m ruff format .

compat-py36: ## Run dependency-light Python 3.6 compatibility checks
	find qtop_py tools -name '*.py' -print | xargs $(PYTHON) -m py_compile
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers $(SAMPLE_GATE_SCHEDULERS) --max-failures $(SAMPLE_GATE_MAX_FAILURES) --artifact-dir $(SAMPLE_GATE_ARTIFACT_DIR)-py36

ci: test backend-validation lint ruff-check format-check ## Run the shared local/CI validation path

github-ci: ci ## GitHub Actions entry point for test validation

gitlab-ci: ci ## GitLab CI entry point for test validation

nightly-ci: test backend-validation ## Git-free validation path for nightly matrix lanes (interpreter compatibility, qtop/qtop#488)

build: ci-deps ## Build source and wheel distributions with pinned CI build deps
	$(PYTHON) -m build --no-isolation

github-build: build ## GitHub Actions entry point for package builds

gitlab-build: build ## GitLab CI entry point for package builds

dist: build ## Alias for build, matching common release target names

version: ## Print the package version
	$(PYTHON) -c "import qtop_py; print(qtop_py.__version__)"

confirm: ## Ask for human confirmation before release-like tasks
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]
