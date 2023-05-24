# Builds the Linux artifacts (node binaries).
# Usage: ./build_linux_artifacts.sh [target]
# Targets supported: 
# - x86_64-unknown-linux-gnu:centos
# - aarch64-unknown-linux-gnu:centos
# - aarch64-unknown-linux-musl
# - x86_64-unknown-linux-musl

# On MacOS, need to run in a linux container:
# docker run -v $(pwd):/io -w /io 

# Must run rustup toolchain install stable-x86_64-unknown-linux-gnu --force-non-host

set -e

build_node_binaries() {
    pushd node
    
    for target in $1
    do
        echo "Building node library for $target"
        # cross doesn't yet pass this down to Docker, so we do it ourselves.
        export CROSS_CONTAINER_OPTS="--platform linux/amd64"
        if [[ $target == *musl ]]; then
            # This is needed for cargo to allow build cdylibs with musl
            RUSTFLAGS="-C target-feature=-crt-static"
        fi
        npm run cross-release -- --target $target
        npm run pack-build -- --target $target
    done
    popd
}

if [ -n "$1" ]; then
    targets=$1
else
    # targets="x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-unknown-linux-musl x86_64-unknown-linux-musl"
    targets="aarch64-unknown-linux-gnu"
fi
build_node_binaries $targets