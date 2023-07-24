#!/bin/bash
set -e
ARCH=${1:-x86_64}

# We pass down the current user so that when we later mount the local files 
# into the container, the files are accessible by the current user.
pushd ci/manylinux_node
docker build \
    -t lancedb-node-manylinux \
    --build-arg="ARCH=$ARCH" \
    --build-arg="DOCKER_USER=$(id -u)" \
    --progress=plain \
    .
popd

docker run \
    -v $(pwd):/io -w /io \
    lancedb-node-manylinux \
    bash ci/manylinux_node/build.sh $ARCH
