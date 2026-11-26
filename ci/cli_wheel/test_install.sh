#!/usr/bin/env bash
# Exercise resolver platform selection and both tools' installed entry points.
set -euo pipefail

tag=${1:?pass the CLI release tag}
wheels=${2:?pass the absolute wheel directory}
version=${tag#v}
spec="lancedb-tools==${version/-beta./b}"
test_env=$(mktemp -d)
trap 'rm -rf "$test_env"' EXIT
export XDG_CONFIG_HOME="$test_env/config" UV_CACHE_DIR="$test_env/uv"
export XDG_STATE_HOME="$test_env/state" XDG_DATA_HOME="$test_env/data"
export PIPX_HOME="$test_env/pipx" PIPX_BIN_DIR="$test_env/bin"
export PIPX_SHARED_LIBS="$test_env/shared" PIPX_MAN_DIR="$test_env/man"

check_cli() {
    "$@" --version | grep -Fx "lancedb $version"
    "$@" profile list | grep -w local
}

check_cli uvx --no-index --find-links "$wheels" --from "$spec" lancedb
python -m pip download --no-index --find-links "$wheels" --dest "$test_env/native" "$spec"
pipx install --backend pip "$test_env/native/"*.whl
check_cli "$PIPX_BIN_DIR/lancedb"
