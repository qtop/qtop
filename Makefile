PYTHON ?= python
PBS_SAMPLE_ROOT ?= ../qtop-test-repo/qtop5/results
PBS_SAMPLE_LIMIT ?= 100
PBS_OUTPUT_DIR ?= .work/pbs-samples

.PHONY: test test-unit test-pbs-samples

test: test-unit
	@if [ -d "$(PBS_SAMPLE_ROOT)" ]; then \
		$(MAKE) test-pbs-samples; \
	else \
		echo "Skipping PBS sample regression; set PBS_SAMPLE_ROOT to qtop-test-repo/qtop5/results."; \
	fi

test-unit:
	$(PYTHON) -m pytest

test-pbs-samples:
	$(PYTHON) tools/validate_pbs_samples.py "$(PBS_SAMPLE_ROOT)" --limit "$(PBS_SAMPLE_LIMIT)" --output "$(PBS_OUTPUT_DIR)"
