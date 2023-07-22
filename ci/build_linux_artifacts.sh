ARCH=${1:-x86_64}

pushd ci/manylinux_node
docker build \
    -t lancedb-node-manylinux \
    --build-arg="ARCH=$ARCH" \
    --build-arg="DOCKER_USER=will" \
    --progress=plain \
    .
popd

docker run \
    -v $(pwd):/io -w /io \
    lancedb-node-manylinux \
    bash ci/manylinux_node/build.sh $ARCH
