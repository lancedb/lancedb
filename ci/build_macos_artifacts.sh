# Builds the macOS artifacts (node binaries).
# Usage: ./build_macos_artifacts.sh [target]
# Targets supported: x86_64-apple-darwin aarch64-apple-darwin

build_node_binaries() {
    pushd node
    
    for target in $1
    do
        echo "Building node library for $target"
        npm run build-release -- --target $target
        npm run pack-build -- --target $target
    done
    popd
}

if [ -n "$1" ]; then
    targets=$1
else
    targets="x86_64-apple-darwin aarch64-apple-darwin"
fi
build_node_binaries $targets