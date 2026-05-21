PYTHON ?= python

.PHONY: test test-slurm

test:
	$(PYTHON) -m pytest -q

test-slurm:
	$(PYTHON) -m pytest -q tests/plugins/test_slurm.py
