.PHONY: test coverage lint

test:
	pytest -q tests

coverage:
	pytest --cov=qtop --cov-report=term-missing tests

lint:
	ruff check .
