#
# Makefile for running tests.
#

# Run all tests.
all-tests: feature-tests remote-tests

# Run tests for every feature. This requires using docker compose to set up
# the environment.
feature-tests:
	../../ci/run_with_docker_compose.sh \
		cargo test --all-features --tests --locked --examples $(CARGO_ARGS)
.PHONY: feature-tests

# Run tests against remote endpoints.
remote-tests:
	../../ci/run_with_test_connection.sh \
		cargo test --features remote --locked $(CARGO_ARGS)
.PHONY: remote-tests
