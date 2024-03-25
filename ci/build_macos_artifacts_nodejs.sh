# Builds the macOS artifacts (nodejs binaries).
# Usage: ./ci/build_macos_artifacts_nodejs.sh [target]
# Targets supported: x86_64-apple-darwin aarch64-apple-darwin
set -e

prebuild_rust() {
    # Building here for the sake of easier debugging.
    pushd rust/lancedb
    echo "Building rust library for $1"
    export RUST_BACKTRACE=1
    cargo build --release --target $1
    popd
}

build_node_binaries() {
    pushd nodejs
    echo "Building nodejs library for $1"
    export RUST_TARGET=$1
    npm run build-release
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
