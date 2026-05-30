.PHONY: help confirm fortifications test test-ci sample-validation test-samples test-pbs-golden test-pbs-samples test-sge-samples test-slurm-samples

PYTHON ?= python3
PYTEST ?= $(PYTHON) -m pytest
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_GOLDEN_LIMIT ?= 10
PBS_SAMPLE_LIMIT ?= 100
SAMPLE_MAX_FAILURES ?= 0
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered

.DEFAULT_GOAL := help

help:             ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

confirm:          ## Ask for human confirmation before a sensitive target
	@echo "Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]

fortifications:   ## Inspect the pull-request diff for risky generated, binary, Unicode, or eval changes
	$(PYTHON) tools/fortify_diff.py

test:             ## Run the unit test suite
	$(PYTEST)

test-ci: fortifications test sample-validation ## Run the CI test and sample gate

sample-validation: test-sge-samples test-slurm-samples ## Run the fast SGE/Slurm gate and PBS gate when samples are available
	@if [ -d "$(PBS_SAMPLES_DIR)" ]; then \
		$(MAKE) test-pbs-golden; \
	else \
		echo "Skipping PBS golden samples; $(PBS_SAMPLES_DIR) is not available."; \
	fi

test-samples: test-sge-samples test-slurm-samples test-pbs-samples ## Run all sample validators

test-pbs-golden:  ## Render the curated PBS golden sample set
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_GOLDEN_LIMIT) --output $(PBS_OUTPUT_DIR) --max-failures $(SAMPLE_MAX_FAILURES)

test-pbs-samples: ## Render a larger PBS sample sweep
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR) --max-failures $(SAMPLE_MAX_FAILURES)

test-sge-samples: ## Validate the in-repository SGE sample
	$(PYTEST) tests/plugins/test_sge.py

test-slurm-samples: ## Validate the in-repository Slurm samples and rendered output
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR) --max-failures $(SAMPLE_MAX_FAILURES)
