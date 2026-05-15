SHELL := /bin/bash
QTOP_SAMPLES_DIR ?= $(HOME)/qtop-test-repo/qtop5/results

.PHONY: help test test-quick test-pbs test-json test-regression clean

help:
	@echo "qtop development targets:"
	@echo "  make test        - Run all tests (unit + PBS regression)"
	@echo "  make test-quick  - Run unit tests only (no PBS samples)"
	@echo "  make test-pbs    - Run PBS regression test against $(QTOP_SAMPLES_DIR)"
	@echo "  make test-json   - Test JSON export structure"
	@echo "  make clean       - Clean up temporary files"

test: test-quick test-pbs test-json
	@echo "All tests passed."

test-quick:
	@echo "Running unit tests..."
	QTOP_SAMPLES_DIR="$(QTOP_SAMPLES_DIR)" \
		python3 -m pytest tests/test_qtop.py tests/plugins/test_pbs.py \
		tests/ui/test_viewport.py tests/test_regression_pbs.py \
		-v -k "not test_all_pbs_samples and not test_pbs_json_export" \
		--tb=short 2>&1 | tail -20

test-pbs:
	@echo "Running PBS regression test against $(QTOP_SAMPLES_DIR)..."
	@if [ ! -d "$(QTOP_SAMPLES_DIR)" ]; then \
		echo "ERROR: PBS samples not found at $(QTOP_SAMPLES_DIR)"; \
		echo "Clone them with:"; \
		echo "  git clone https://github.com/fgeorgatos/qtop-test-repo.git \$$HOME/qtop-test-repo"; \
		exit 1; \
	fi
	QTOP_SAMPLES_DIR="$(QTOP_SAMPLES_DIR)" \
		python3 -m pytest tests/test_regression_pbs.py::test_all_pbs_samples \
		-v --tb=short 2>&1 | tail -10

test-json:
	@echo "Testing JSON export..."
	QTOP_SAMPLES_DIR="$(QTOP_SAMPLES_DIR)" \
		python3 -m pytest tests/test_regression_pbs.py::test_pbs_json_export \
		-v --tb=short 2>&1 | tail -5

clean:
	@rm -rf /tmp/qtop_results_$$(whoami 2>/dev/null || echo "mac")/qtop_*.out
	@rm -rf /tmp/qtop_results_$$(whoami 2>/dev/null || echo "mac")/qtop_*.json
	@rm -rf /tmp/qtop_results_$$(whoami 2>/dev/null || echo "mac")/*.txt
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -type d -delete
	@echo "Cleaned up."