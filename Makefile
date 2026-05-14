.PHONY: test test-pbs-samples

test:
	python3 -m pytest

test-pbs-samples:
	test -n "$$QTOP_PBS_SAMPLE_DIR"
	python3 -m pytest tests/plugins/test_pbs_regression.py -q
