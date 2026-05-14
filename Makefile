.PHONY: test test-pbs-samples

test:
	python -m pytest

test-pbs-samples:
	@test -n "$$QTOP_PBS_SAMPLE_DIR" || (echo "Set QTOP_PBS_SAMPLE_DIR to qtop-test-repo/qtop5/results" && exit 2)
	python tests/validate_pbs_samples.py --samples-dir "$$QTOP_PBS_SAMPLE_DIR" --min-pass 100 --limit 100 --manifest .work/pbs_samples/manifest.tsv --output-dir .work/pbs_samples/rendered
