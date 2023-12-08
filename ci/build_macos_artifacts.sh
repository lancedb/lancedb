# Builds the macOS artifacts (node binaries).
# Usage: ./ci/build_macos_artifacts.sh [target]
# Targets supported: x86_64-apple-darwin aarch64-apple-darwin
set -e

prebuild_rust() {
    # Building here for the sake of easier debugging.
    pushd rust/ffi/node
    echo "Building rust library for $1"
    export RUST_BACKTRACE=1
    cargo build --release --target $1
    popd
}

build_node_binaries() {
    pushd node
    echo "Building node library for $1"
    npm run build-release -- --target $1
    npm run pack-build -- --target $1
    popd
}

if [ -n "$1" ]; then
    targets=$1
else
    targets="x86_64-apple-darwin aarch64-apple-darwin"
fi

echo "Building artifacts for targets: $targets"
for target in $targets
    do
    prebuild_rust $target
    build_node_binaries $target
done