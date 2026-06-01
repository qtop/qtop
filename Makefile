PYTHON ?= python3
PYTEST ?= $(PYTHON) -m pytest
ARTIFACT_DIR ?= artifacts/sample-gate
MAX_FAILURES ?= 0

.PHONY: help test sample-gate sample-report ci clean-artifacts

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help

test: ## Run the pytest suite
	$(PYTEST)

sample-gate: ## Run fast scheduler sample validation and write artifacts
	ARTIFACT_DIR="$(ARTIFACT_DIR)" MAX_FAILURES="$(MAX_FAILURES)" PYTHON="$(PYTHON)" sh ./scripts/run_sample_gate.sh

sample-report: ## Run sample validation as an artifact-producing report
	ARTIFACT_DIR="$(ARTIFACT_DIR)" MAX_FAILURES=999 PYTHON="$(PYTHON)" sh ./scripts/run_sample_gate.sh

ci: test sample-report ## Run the shared CI entry point

clean-artifacts: ## Remove sample gate artifacts
	rm -rf artifacts
