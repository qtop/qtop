.PHONY: test test-pbs-samples

PYTHON ?= python3
PBS_SAMPLES_DIR ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_MAX_FAILURES ?=
PBS_SAMPLE_TIMEOUT ?= 8
PBS_OUTPUT_DIR ?= /tmp/qtop-pbs-rendered
PBS_MAX_FAILURES_ARG = $(if $(PBS_MAX_FAILURES),--max-failures $(PBS_MAX_FAILURES),)

test:
	$(PYTHON) -m pytest

test-pbs-samples:
	$(PYTHON) tools/validate_pbs_samples.py $(PBS_SAMPLES_DIR) --limit $(PBS_SAMPLE_LIMIT) --output $(PBS_OUTPUT_DIR) --timeout $(PBS_SAMPLE_TIMEOUT) $(PBS_MAX_FAILURES_ARG)
