#!/bin/bash
# Builds the node module for manylinux. Invoked by ci/build_linux_artifacts.sh.
set -e
ARCH=${1:-x86_64}
TARGET_TRIPLE=${2:-x86_64-unknown-linux-gnu}

#Alpine doesn't have .bashrc
FILE=$HOME/.bashrc && test -f $FILE && source $FILE

cd node
npm ci
npm run build-release
npm run pack-build -- -t $TARGET_TRIPLE
