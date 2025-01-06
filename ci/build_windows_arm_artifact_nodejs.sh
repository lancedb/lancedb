#!/bin/bash
set -e

pushd ci/windows_arm_node
docker build \
    -t lancedb-node-windows-arm \
    --progress=plain \
    .
popd

docker run \
    -v $(pwd):/io -w /io -it \
    lancedb-node-windows-arm \
    sh
    # bash ci/windows_arm_node/build_lancedb.sh
