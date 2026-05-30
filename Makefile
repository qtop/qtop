PYTHON ?= python3
PYTEST ?= $(PYTHON) -m pytest
SAMPLE_MAX_FAILURES ?= 0

.PHONY: test sample-gate ci

test:
	$(PYTEST)

sample-gate:
	$(PYTHON) scripts/sample_gate.py --max-failures $(SAMPLE_MAX_FAILURES)

ci: test sample-gate
