## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT

.DEFAULT_GOAL := help

PYTHON ?= python3
PYTEST_XDIST_AUTO_NUM_WORKERS ?= 4
export PYTEST_XDIST_AUTO_NUM_WORKERS
COVERAGE_CORE ?= ctrace
export COVERAGE_CORE
PYTEST_ARGS ?= --durations=20 --durations-min=0.1
PYTEST_REPORT_DIR ?= artifacts/tests
# Build-and-install tests are slow (about a minute); test-packaging runs them, in the build jobs and nightly lanes.
PYTEST_MARKERS ?= not packaging
# Tests run on every CPU when pytest-xdist is installed (requirements-ci.txt); test_web.py forks a worker, so it runs alone.
PYTEST_WORKERS = $(shell $(PYTHON) -c "import xdist" >/dev/null 2>&1 && echo "-n auto --ignore=tests/test_web.py")
PIP ?= $(PYTHON) -m pip
DIST_DIR ?= dist
UVX ?= uvx
GO ?= go
BASE_SCHEMA ?= schema/cluster-state.cue
PROPOSED_SCHEMA ?=
SCHEMA_DEFINITION ?= \#ClusterState
# The zizmor of OSPS_TOOLS; tests/test_ci_requirements.py keeps the two equal.
ZIZMOR ?= $(UVX) --quiet zizmor@1.32.0
ZIZMOR_FLAGS ?= -qq --offline --no-progress --color never --render-links never --show-audit-urls never
# all: every backend the registry finds; or a comma-separated list such as pbs,slurm
SAMPLE_GATE_SCHEDULERS ?= all
AUTHORS_PATHS ?= qtop_py
SAMPLE_GATE_MAX_FAILURES ?= 0
SAMPLE_GATE_ARTIFACT_DIR ?= artifacts/sample-gate
BACKEND_COLOUR_ARTIFACT_DIR ?= artifacts/backend-colour
TRACE_JSON_B64 ?=
TRACE_FULLVIEW ?=
TRACE_ARTIFACT_DIR ?= artifacts/trace-export
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 447
PBS_OUTPUT_DIR ?= artifacts/pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= artifacts/slurm-rendered
FUZZ_SEED ?= 488
FUZZ_CASES ?= 5000
FUZZ_ARTIFACT_DIR ?= artifacts/fuzz
ATHERIS_ARGS ?=
FORTIFY_BASE_REF ?= origin/develop
COVERAGE_XML ?= artifacts/coverage/coverage.xml
COVERAGE_MIN ?= 95
COVERAGE_BRANCH_MIN ?= 93
COVERAGE_JSON ?= artifacts/coverage/details.json
REPO_SANITY_DIR ?= artifacts/repo-sanity
CODE_QUALITY_JSON ?= artifacts/code-quality/gl-code-quality-report.json
LICENSE_DIR ?= artifacts/license
SBOM_DIR ?= artifacts/sbom
SBOM_VENV ?= build/sbom-venv
SBOM_TOOL_VENV ?= build/sbom-tool
ATTEST_REPO ?= qtop/qtop
ATTEST_SIGNER ?= $(ATTEST_REPO)/.github/workflows/release.yml
CYCLONEDX_BOM ?= cyclonedx-bom==9.0.0
OSPS_DIR ?= artifacts/osps
OSPS_VENV ?= build/osps-venv
OSPS_TOOLS ?= darnit-mcp==0.1.1 zizmor==1.32.0

help:: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

all:: ## Run full verification and packaging without repeating tests or samples
all:: ci-deps pre-pr trace-export-validation test-pbs-samples compat-py36 packaging

validation-suite:: ## Validate scheduler samples, rendered output and trace export
validation-suite:: backend-validation backend-colour-artifacts render-backends trace-export-validation test-pbs-samples test-slurm-samples

ci-entrypoints:: ## Run the shared, GitHub and GitLab CI entry points
ci-entrypoints:: ci github-ci gitlab-ci

packaging:: ## Build and validate release artifacts, then print the version
packaging:: build github-build gitlab-build dist version

rerun:: clean all ## Clean generated files, then rerun the complete local validation path

clean:: confirm ## Remove generated validation, build and Python cache output
	rm -rf artifacts build dist .coverage .pytest_cache .ruff_cache qtop.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete

ci-deps:: ## Install pinned CI Python dependencies
	$(PIP) install -r requirements-ci.txt

test:: ## Run the Python test suite
	@mkdir -p "$(PYTEST_REPORT_DIR)"
	$(PYTHON) -m pytest $(PYTEST_WORKERS) $(PYTEST_ARGS) -m "$(PYTEST_MARKERS)" --junitxml="$(PYTEST_REPORT_DIR)/main.xml"
	$(if $(PYTEST_WORKERS),$(PYTHON) -m pytest tests/test_web.py $(PYTEST_ARGS) --junitxml="$(PYTEST_REPORT_DIR)/web.xml")

test-packaging:: ## Build, install and validate the sdist/wheel in pytest (the cases test and coverage skip)
	@mkdir -p "$(PYTEST_REPORT_DIR)"
	$(PYTHON) -m pytest $(PYTEST_WORKERS) $(PYTEST_ARGS) -m packaging tests/test_packaging.py --junitxml="$(PYTEST_REPORT_DIR)/packaging.xml"

test-fast:: ## Run parser/model/UI checks; pre-pr remains the full gate
	@mkdir -p "$(PYTEST_REPORT_DIR)"
	$(PYTHON) -m pytest $(PYTEST_WORKERS) $(PYTEST_ARGS) \
		tests/plugins tests/ui tests/test_mvu.py tests/test_cluster_state.py \
		tests/test_configuration.py tests/test_yaml_parser.py \
		--junitxml="$(PYTEST_REPORT_DIR)/fast.xml"

coverage:: ## Run tests with coverage.py and print a terminal report
	@mkdir -p "$(PYTEST_REPORT_DIR)"
	$(PYTHON) -m coverage erase
	$(PYTHON) -m coverage run -m pytest $(PYTEST_WORKERS) $(PYTEST_ARGS) -m "$(PYTEST_MARKERS)" --junitxml="$(PYTEST_REPORT_DIR)/main.xml"
	$(if $(PYTEST_WORKERS),$(PYTHON) -m coverage run -m pytest tests/test_web.py $(PYTEST_ARGS) --junitxml="$(PYTEST_REPORT_DIR)/web.xml")
	$(PYTHON) -m coverage combine
	@mkdir -p $(dir $(COVERAGE_JSON))
	$(PYTHON) -m coverage json -o "$(COVERAGE_JSON)"
	$(PYTHON) -m coverage report --include='tools/*'
	$(PYTHON) -m coverage report --include='qtop_py/*'
	$(PYTHON) tools/check_coverage.py "$(COVERAGE_JSON)" --statements $(COVERAGE_MIN) --branches $(COVERAGE_BRANCH_MIN)

coverage-xml:: coverage ## Write runtime Cobertura XML after the shared coverage gate
	@mkdir -p $(dir $(COVERAGE_XML))
	$(PYTHON) -m coverage xml --include='qtop_py/*' -o "$(COVERAGE_XML)"
	@echo "wrote $(COVERAGE_XML)"

sample-gate:: ## Run fast committed PBS/SGE/Slurm/OAR/demo qtop sample gates
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers $(SAMPLE_GATE_SCHEDULERS) --max-failures $(SAMPLE_GATE_MAX_FAILURES) --artifact-dir $(SAMPLE_GATE_ARTIFACT_DIR)

backend-validation:: sample-gate ## Validate PBS/SGE/Slurm/OAR/demo through the shared local-first path

backend-colour-artifacts:: ## Render ANSI-colour text and SVG review artifacts for every backend
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers $(SAMPLE_GATE_SCHEDULERS) --max-failures $(SAMPLE_GATE_MAX_FAILURES) --artifact-dir $(BACKEND_COLOUR_ARTIFACT_DIR)

render-backends:: backend-colour-artifacts ## Alias for backend-colour-artifacts

trace-export-validation:: ## Validate an exported qtop JSON trace when TRACE_JSON_B64 is provided
	@if [ -n "$(TRACE_JSON_B64)" ]; then \
		args="--artifact-dir $(TRACE_ARTIFACT_DIR)"; \
		if [ -n "$(TRACE_FULLVIEW)" ]; then args="$$args --fullview $(TRACE_FULLVIEW)"; fi; \
		$(PYTHON) -m tools.validate_trace_export "$(TRACE_JSON_B64)" $$args; \
	else \
		echo "Skipping exported trace validation: TRACE_JSON_B64 not set"; \
	fi

test-pbs-samples:: ## Run the larger archived PBS sample sweep when the external corpus is available
	@if [ -d "$(PBS_SAMPLES_DIR)" ]; then \
		$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR); \
	else \
		echo "Skipping archived PBS sample sweep: $(PBS_SAMPLES_DIR) not found"; \
	fi

test-slurm-samples:: ## Run Slurm parser tests and render committed Slurm samples
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers slurm --slurm-samples-dir $(SLURM_SAMPLES_DIR) --max-failures 0 --artifact-dir $(SLURM_OUTPUT_DIR)

deterministic-fuzz:: ## Reproducibly stress parser primitives with a fixed seed
	$(PYTHON) -m tools.deterministic_fuzz --seed $(FUZZ_SEED) --cases $(FUZZ_CASES)

coverage-fuzz:: ## Run bounded Atheris fuzzing (Linux CPython 3.12 CI dependencies)
	$(PYTHON) -m tools.atheris_fuzz --seed-corpus -runs=$(FUZZ_CASES) -seed=$(FUZZ_SEED) -max_total_time=120 -max_len=256 -timeout=10 -artifact_prefix="$(FUZZ_ARTIFACT_DIR)/" $(ATHERIS_ARGS) "$(FUZZ_ARTIFACT_DIR)/corpus"

fortifications:: ## Check added diff text and generated/binary paths
	$(PYTHON) tools/repo_sanity.py --base-ref "$(FORTIFY_BASE_REF)"

repo-sanity:: ## Audit tracked UTF-8 text for bidi/invisible/confusable characters (qtop/qtop#488)
	$(PYTHON) tools/repo_sanity.py --report-dir $(REPO_SANITY_DIR)

code-quality:: ## Emit GitLab Code Quality JSON from ruff (upstream CodeClimate template is deprecated)
	@mkdir -p $(dir $(CODE_QUALITY_JSON))
	$(PYTHON) -m ruff check --output-format gitlab --exit-zero . > $(CODE_QUALITY_JSON)
	@echo "wrote $(CODE_QUALITY_JSON)"

license-report:: ## Report licenses of the pinned CI dependency set (pip-licenses, pinned in-job)
	$(PIP) install pip-licenses==5.0.0
	@mkdir -p $(LICENSE_DIR)
	$(PYTHON) -m piplicenses --format=markdown --with-urls > $(LICENSE_DIR)/ci-dependencies.md
	$(PYTHON) -m piplicenses --format=json > $(LICENSE_DIR)/ci-dependencies.json
	@echo "wrote $(LICENSE_DIR)"

# Report-only OSPS Baseline audit; darnit's remediation tools are never called. It reads repository
# settings for the checked-out branch through gh/GITHUB_TOKEN, so run it on the default branch.
# Needs Python 3.11+ on Linux x86-64/aarch64, macOS arm64 or Windows (darnit's wheels).
# The raw audit holds command evidence (e.g. the caller's GitHub profile) and stays in build/.
osps-audit:: ## Report-only OSPS Baseline audit with pinned Darnit (needs gh auth or GITHUB_TOKEN)
	$(PYTHON) -m venv "$(OSPS_VENV)"
	"$(OSPS_VENV)/bin/python" -m pip install --disable-pip-version-check -q $(OSPS_TOOLS)
	@mkdir -p build
	PATH="$(abspath $(OSPS_VENV))/bin:$$PATH" "$(OSPS_VENV)/bin/darnit" audit --no-fail -o json . > build/osps-darnit.json
	$(PYTHON) tools/osps_report.py build/osps-darnit.json --output-dir "$(OSPS_DIR)"

# The generator lives in its own venv, so neither scanned environment lists it.
sbom-tool::
	$(PYTHON) -m venv "$(SBOM_TOOL_VENV)"
	"$(SBOM_TOOL_VENV)/bin/python" -m pip install --disable-pip-version-check -q $(CYCLONEDX_BOM)

# The release SBOM inventories the built wheel installed alone, as users get it.
# --no-index with --find-links keeps local paths out and yields the pkg:pypi purl.
# Not --output-reproducible: actions/attest-sbom accepts CycloneDX only with a serialNumber.
sbom:: sbom-tool ## Write a CycloneDX SBOM of the wheel in DIST_DIR (run build first)
	$(PYTHON) -m venv --clear "$(SBOM_VENV)"
	VERSION="$$($(PYTHON) -c 'from qtop_py import __version__; print(__version__)')"; \
	"$(SBOM_VENV)/bin/python" -m pip install --disable-pip-version-check --no-index --find-links "$(DIST_DIR)" "qtop==$$VERSION"
	"$(SBOM_VENV)/bin/python" -m pip uninstall --disable-pip-version-check -y pip
	@mkdir -p $(SBOM_DIR)
	"$(SBOM_TOOL_VENV)/bin/python" -m cyclonedx_py environment --mc-type application \
		--output-format JSON --output-file "$(SBOM_DIR)/qtop.cdx.json" "$(SBOM_VENV)/bin/python"
	@echo "wrote $(SBOM_DIR)/qtop.cdx.json"

sbom-ci:: sbom-tool ## Write a CycloneDX SBOM of the build/CI environment that runs PYTHON
	@mkdir -p $(SBOM_DIR)
	"$(SBOM_TOOL_VENV)/bin/python" -m cyclonedx_py environment --output-reproducible --output-format JSON \
		--output-file "$(SBOM_DIR)/ci.cdx.json" "$$($(PYTHON) -c 'import sys; print(sys.executable)')"
	@echo "wrote $(SBOM_DIR)/ci.cdx.json"

# Consumers run this on downloaded release files; the release workflow runs it right after attesting.
verify-attestations:: ## Verify GitHub provenance and SBOM attestations of each DIST_DIR wheel/sdist (needs gh)
	for artifact in "$(DIST_DIR)"/*.whl "$(DIST_DIR)"/*.tar.gz; do \
		gh attestation verify "$$artifact" --repo "$(ATTEST_REPO)" --signer-workflow "$(ATTEST_SIGNER)" || exit 1; \
		gh attestation verify "$$artifact" --repo "$(ATTEST_REPO)" --signer-workflow "$(ATTEST_SIGNER)" \
			--predicate-type https://cyclonedx.org/bom > /dev/null || exit 1; \
	done

# Air-gapped sites: DIST_DIR holds the release files, both .intoto.jsonl bundles and trusted_root.jsonl.
verify-attestations-offline:: ## Verify DIST_DIR wheel/sdist against release bundles and a saved trusted root, offline
	for artifact in "$(DIST_DIR)"/*.whl "$(DIST_DIR)"/*.tar.gz; do \
		gh attestation verify "$$artifact" --repo "$(ATTEST_REPO)" --signer-workflow "$(ATTEST_SIGNER)" \
			--bundle "$(DIST_DIR)/qtop-provenance.intoto.jsonl" --custom-trusted-root "$(DIST_DIR)/trusted_root.jsonl" || exit 1; \
		gh attestation verify "$$artifact" --repo "$(ATTEST_REPO)" --signer-workflow "$(ATTEST_SIGNER)" \
			--bundle "$(DIST_DIR)/qtop-sbom.intoto.jsonl" --custom-trusted-root "$(DIST_DIR)/trusted_root.jsonl" \
			--predicate-type https://cyclonedx.org/bom > /dev/null || exit 1; \
	done

type-contracts:: ## Check core contracts with mypy and Pyright (Python 3.12+ tooling)
	$(PYTHON) -m mypy
	$(PYTHON) -m pyright

property-test:: ## Run bounded Hypothesis properties (HYPOTHESIS_PROFILE=nightly for longer runs)
	@mkdir -p "$(PYTEST_REPORT_DIR)"
	$(PYTHON) -m pytest tests/test_properties.py $(PYTEST_ARGS) --junitxml="$(PYTEST_REPORT_DIR)/properties.xml"

type-check:: type-contracts ## Check core contracts and production/tooling with ty
	$(PYTHON) -m ty check --python "$$($(PYTHON) -c 'import sys; print(sys.executable)')"

ruff-check:: ## Run ruff against the source tree
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff check --select F403,S307 --ignore-noqa .

lint:: ruff-check fortifications repo-sanity ## Run dependency-light source and diff health checks

lint-fix:: ## Auto-fix ruff lint issues where possible
	$(PYTHON) -m ruff check --fix .

format-check:: ## Check ruff formatting and branch diff whitespace
	$(PYTHON) -m ruff format --check --diff .
	git diff --check "$$(git merge-base "$(FORTIFY_BASE_REF)" HEAD)" --

format-fix:: ## Auto-format Python files with ruff
	$(PYTHON) -m ruff format .

normalize-crlf:: ## Convert tracked text files to LF using /tmp, preserving file permissions (requires Bash)
	bash -o pipefail -c 'git grep -Ilz "" | while IFS= read -r -d "" file; do tmp=$$(mktemp /tmp/qtop-crlf.XXXXXX) || exit 1; sed "s/\r$$//" -- "$$file" > "$$tmp" && cat "$$tmp" > "$$file" || { rm -f -- "$$tmp"; exit 1; }; rm -f -- "$$tmp" || exit 1; done'

zizmor:: ## Audit GitHub Actions workflows independently
	@$(ZIZMOR) $(ZIZMOR_FLAGS) .github/

zizmor-fix:: ## Apply zizmor's safe workflow fixes independently
	@$(ZIZMOR) $(ZIZMOR_FLAGS) --fix=safe .github/

legacy-install-check:: ## Install the wheel built by a modern Python (make build) on Python 3.6-3.8, with no build tools
	$(PYTHON) -c "import sys; sys.exit(0 if (3, 6) <= sys.version_info[:2] < (3, 9) else 'legacy-install-check requires Python 3.6-3.8')"
	$(PYTHON) tools/validate_installation.py --artifacts-dir "$(DIST_DIR)" --wheel-only

install-check:: ## Build from an sdist and test a clean installation on this interpreter
	$(PYTHON) tools/validate_installation.py

release-check:: ## Validate the exact prebuilt sdist/wheel pair without replacing dist files
	$(PYTHON) tools/validate_installation.py --artifacts-dir "$(DIST_DIR)" --report artifacts/release/evidence.json

# Temporary until the 1.0 namespace change: a release tag must be v<__version__>,
# the form the EasyBuild recipe downloads. RELEASE_TAG comes from the environment.
release-tag-check:: ## Fail unless RELEASE_TAG is v<package version>
	@test -n "$$RELEASE_TAG" || { echo "RELEASE_TAG is not set"; exit 1; }
	@VERSION="$$($(PYTHON) -c 'from qtop_py import __version__; print(__version__)')"; \
	test "$$RELEASE_TAG" = "v$$VERSION" || { \
		echo "Release tag $$RELEASE_TAG does not match package version $$VERSION"; exit 1; }; \
	echo "Release tag $$RELEASE_TAG matches qtop $$VERSION"

# Non-blocking for now: reports CHANGELOG/tag drift from local tags (git fetch --tags first); not in pre-pr or CI.
release-audit:: ## Report CHANGELOG headings, tags and tagged versions that disagree
	$(PYTHON) tools/release_audit.py $(RELEASE_AUDIT_ARGS)

validated-build:: build ## Build once, validate those exact release artifacts and inventory them
	$(MAKE) release-check sbom sbom-ci


compat-py36:: ## Run dependency-light Python 3.6 compatibility checks
	find qtop_py tools -name '*.py' -print | xargs $(PYTHON) -m py_compile
	$(PYTHON) -m unittest discover -s tests -p test_legacy_helpers.py
	$(PYTHON) -m unittest discover -s tests -p test_legacy_contracts.py
	$(PYTHON) tools/validate_scheduler_samples.py --schedulers $(SAMPLE_GATE_SCHEDULERS) --max-failures $(SAMPLE_GATE_MAX_FAILURES) --artifact-dir $(SAMPLE_GATE_ARTIFACT_DIR)-py36

ci:: test backend-validation deterministic-fuzz lint format-check ## Run the shared local/CI validation path

# Coverage runs the full pytest suite, including the Hypothesis properties.
pre-pr:: lint format-check type-check coverage backend-validation deterministic-fuzz coverage-fuzz ## Run complete local PR gates (Linux x86-64, CPython 3.12)

github-ci:: ci ## GitHub Actions entry point for test validation

gitlab-ci:: ci ## GitLab CI entry point for test validation

nightly-ci:: test test-packaging backend-validation ## Git-free validation path for nightly matrix lanes (interpreter compatibility, qtop/qtop#488)

build:: ci-deps ## Build source and wheel distributions with pinned CI build deps
	$(PYTHON) -m build --no-isolation --outdir "$(DIST_DIR)"

github-build:: validated-build test-packaging ## GitHub Actions entry point for validated package builds

gitlab-build:: validated-build test-packaging ## GitLab CI entry point for validated package builds

dist:: build ## Alias for build, matching common release target names

authors-lines:: ## List authors by attributed Python lines, highest count first
	@git ls-files -z -- '*.py' | xargs -0 -n1 git blame --line-porcelain -- \
		| grep '^author ' | sed 's/^author //' | sort | uniq -c | sort -nr

authors:: ## Lines and % per author of non-blank, non-comment Python code (AUTHORS_PATHS="qtop_py tests tools")
	@$(PYTHON) tools/loc_authors.py $(AUTHORS_PATHS)

version:: ## Print the package version
	$(PYTHON) -c "import qtop_py; print(qtop_py.__version__)"

confirm:: ## Ask for human confirmation before release-like tasks
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]

scorecard-scan:: ## Scan an exact GitHub commit with Scorecard (requires Docker and GITHUB_AUTH_TOKEN)
	sh ci/scripts/scorecard-scan.sh

schema-compat:: ## Check proposed CUE schema accepts all baseline values (optional Go tooling)
	$(if $(strip $(PROPOSED_SCHEMA)),,$(error Set PROPOSED_SCHEMA to the candidate CUE file))
	$(GO) -C tools/schema-compat run -mod=readonly . -base "$(abspath $(BASE_SCHEMA))" -proposed "$(abspath $(PROPOSED_SCHEMA))" -definition '$(SCHEMA_DEFINITION)'

schema-compat-test:: ## Test CUE schema compatibility tooling
	$(GO) -C tools/schema-compat test -mod=readonly ./...


schema-ci:: schema-compat-test ## Test schema tooling and compare with the target branch contract
	@set -eu; base=$$(git merge-base "$(FORTIFY_BASE_REF)" HEAD); \
	if git cat-file -e "$$base:schema/cluster-state.cue" 2>/dev/null; then \
		baseline=$$(mktemp); trap 'rm -f -- "$$baseline"' EXIT HUP INT TERM; \
		git show "$$base:schema/cluster-state.cue" > "$$baseline"; \
		$(MAKE) schema-compat BASE_SCHEMA="$$baseline" PROPOSED_SCHEMA=schema/cluster-state.cue; \
	else \
		echo "schema-compat: baseline $$base has no contract; validating first introduction through schema-compat-test"; \
	fi


soak:: ## Exercise real watch terminal, capture and refresh budgets (SOAK_ARGS for live clusters)
	$(PYTHON) -m tools.terminal_probe $(SOAK_ARGS)


nightly-properties:: ## Run the extended 1,000-example Hypothesis profile
	HYPOTHESIS_PROFILE=nightly $(MAKE) property-test

ci-matrix-check:: ## Verify equivalent expanded nightly lanes on both providers
	$(PYTHON) -m pytest tests/test_ci_matrix.py $(PYTEST_ARGS)
