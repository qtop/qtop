.DEFAULT_GOAL := help

.PHONY: help test sample-gate fortifications ci

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run the Python test suite
	python -m pytest

sample-gate: ## Validate committed PBS/OAR/SGE sample outputs
	python scripts/sample_gate.py --schedulers pbs,oar,sge --max-failures 0 --artifact-dir artifacts/sample-gate

fortifications: ## Check this diff for unsafe or generated-looking changes
	python scripts/fortifications.py --base-ref origin/main

ci: test sample-gate fortifications ## Run the shared local/CI validation path

