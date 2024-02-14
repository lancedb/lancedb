#!/bin/bash
# Builds the node module for manylinux. Invoked by ci/build_linux_artifacts.sh.
set -e
ARCH=${1:-x86_64}
NUM_JOBS=$2

if [ -n "$NUM_JOBS" ]; then
    export CARGO_BUILD_JOBS=$NUM_JOBS
fi

if [ "$ARCH" = "x86_64" ]; then
    export OPENSSL_LIB_DIR=/usr/local/lib64/
else 
    export OPENSSL_LIB_DIR=/usr/local/lib/
fi
export OPENSSL_STATIC=1
export OPENSSL_INCLUDE_DIR=/usr/local/include/openssl

source $HOME/.bashrc

cd node
npm ci
npm run build-release
npm run pack-build
