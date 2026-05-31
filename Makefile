.PHONY: help test sample-gate fortifications test-pbs-samples test-slurm-samples

PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
SLURM_SAMPLES_DIR ?= tests/plugins/slurm_samples
SLURM_OUTPUT_DIR ?= /tmp/qtop-slurm-rendered
SAMPLE_GATE_OUTPUT_DIR ?= artifacts/sample-gate
SAMPLE_GATE_MAX_FAILURES ?= 0

.DEFAULT_GOAL := help

help: ## Show available Makefile targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run the complete pytest suite
	$(PYTHON) -m pytest

sample-gate: ## Run the fast PBS, SGE and Slurm PR sample gate
	$(PYTHON) tools/sample_gate.py --output $(SAMPLE_GATE_OUTPUT_DIR) --max-failures $(SAMPLE_GATE_MAX_FAILURES)

fortifications: ## Inspect changed files for unsafe control characters and generated artifacts
	$(PYTHON) tools/fortifications.py

test-pbs-samples: ## Render archived PBS samples from the external corpus
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR)

test-slurm-samples: ## Validate the bundled Slurm command-trace samples
	$(PYTHON) -m pytest tests/plugins/test_slurm.py
	$(PYTHON) tools/validate_slurm_samples.py $(SLURM_SAMPLES_DIR) --output $(SLURM_OUTPUT_DIR)
