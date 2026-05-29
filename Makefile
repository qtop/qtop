.PHONY: test test-slurm ruff ci

PYTHON ?= python3
PYTEST ?= pytest
RUFF ?= ruff
RUFF_PATHS ?= qtop_py/plugins/slurm.py tests/plugins/test_slurm.py

test:
	$(PYTHON) -m $(PYTEST)

test-slurm:
	$(PYTHON) -m $(PYTEST) tests/plugins/test_slurm.py

ruff:
	$(PYTHON) -m $(RUFF) check $(RUFF_PATHS)

ci: ruff test-slurm
