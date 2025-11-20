PIP_EXTRA_INDEX_URL ?= https://pypi.fury.io/lance-format/ https://pypi.fury.io/lancedb/

help:		## Show this help.
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)

.PHONY: develop
develop:	## Install the package in development mode.
	PIP_EXTRA_INDEX_URL=$(PIP_EXTRA_INDEX_URL) maturin develop --extras tests,dev,embeddings

.PHONY: format
format:		## Format the code.
	cargo fmt
	ruff format python

.PHONY: check
check:		## Check formatting and lints.
	cargo fmt --check
	ruff format --check python
	cargo clippy
	ruff check python

.PHONY: fix
fix:		## Fix python lints
	ruff check python --fix

.PHONY: typecheck
typecheck:	## Run type checking with pyright.
	pyright

.PHONY: doctest
doctest:	## Run documentation tests.
	pytest --doctest-modules python/lancedb

.PHONY: test
test:		## Run tests.
	pytest python/tests -vv --durations=10 -m "not slow and not s3_test"

.PHONY: clean
clean:
	rm -rf data
