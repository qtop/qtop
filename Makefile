.PHONY: test lint coverage build clean install all

PYTHON := python
PYTEST := pytest
RUFF := ruff
BUILD := pyproject-build

test:
	$(PYTHON) -m $(PYTEST) -v

lint:
	$(RUFF) check qtop_py tests

coverage:
	$(PYTHON) -m $(PYTEST) --cov=qtop_py --cov-report=term-missing --cov-report=xml

build:
	$(BUILD)

clean:
	rm -rf dist build *.egg-info .coverage coverage.xml .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

install:
	pip install -e .

all: lint test build
