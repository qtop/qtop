.PHONY: help ci fortify test sample-gate test-pbs-samples test-slurm-samples

PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered
BASE_REF ?= origin/develop

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

ci: fortify test sample-gate ## Run the dependency-free CI checks used by GitHub and GitLab

fortify: ## Check diff-only unicode/control and generated/binary changes
	@echo "== weird unicode/control chars =="
	@if git rev-parse --verify "$(BASE_REF)" >/dev/null 2>&1; then \
		git diff -U0 "$(BASE_REF)...HEAD" \
			| grep -nE '[^	 -~]|(202A|202B|202C|202D|202E|2066|2067|2068|2069)' \
			&& exit 1 || true; \
	else \
		echo "Skipping diff fortification; $(BASE_REF) is not available."; \
	fi
	@echo "== unexpected binary/generated/build changes =="
	@if git rev-parse --verify "$(BASE_REF)" >/dev/null 2>&1; then \
		git diff --name-only "$(BASE_REF)...HEAD" \
			| grep -Ei '(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|\.xz$$|\.lzma$$|\.gz$$|\.bin$$|\.dat$$' \
			&& { echo "Manual review required"; exit 1; } || true; \
	fi
	@echo "OK"

test: ## Run unit tests
	$(PYTHON) -m pytest

sample-gate: test-slurm-samples ## Run fast in-repo scheduler sample checks

test-pbs-samples: ## Render archived PBS samples from qtop-test-repo
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples: ## Validate bundled Slurm command-trace samples
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)
