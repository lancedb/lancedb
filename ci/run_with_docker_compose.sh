#!/usr/bin/env bash

#
# A script for running the given command together with a docker compose environment.
#

# Bring down the docker setup once the command is done running.
tear_down() {
    docker compose -p fixture down
}
trap tear_down EXIT

set +xe

# Clean up any existing docker setup and bring up a new one.
docker compose -p fixture up --detach --wait || exit 1

"${@}"
