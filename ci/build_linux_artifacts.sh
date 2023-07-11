#!/bin/bash
# Builds the Linux artifacts (node binaries).
# Usage: ./build_linux_artifacts.sh [target]
# Targets supported: 
# - x86_64-unknown-linux-gnu:centos
# - aarch64-unknown-linux-gnu:centos
# - aarch64-unknown-linux-musl
# - x86_64-unknown-linux-musl

# TODO: refactor this into a Docker container we can pull

set -e

setup_dependencies() {
    echo "Installing system dependencies..."
    if [[ $1 == *musl ]]; then
        # musllinux
        apk add openssl-dev
    else
        # rust / debian
        apt update
        apt install -y libssl-dev protobuf-compiler
    fi
}

install_node() {
    echo "Installing node..."
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
    source "$HOME"/.bashrc

    if [[ $1 == *musl ]]; then
        # This node version is 15, we need 16 or higher:
        # apk add nodejs-current npm 
        # So instead we install from source (nvm doesn't provide binaries for musl):
        nvm install -s --no-progress 17
    else
        nvm install --no-progress 17 # latest that supports glibc 2.17
    fi
}

build_node_binary() {
    echo "Building node library for $1..."
    pushd node

    npm ci
    
    if [[ $1 == *musl ]]; then
        # This is needed for cargo to allow build cdylibs with musl
        export RUSTFLAGS="-C target-feature=-crt-static"
    fi

    # Cargo can run out of memory while pulling dependencies, especially when running
    # in QEMU. This is a workaround for that.
    export CARGO_NET_GIT_FETCH_WITH_CLI=true

    # We don't pass in target, since the native target here already matches
    # We need to pass OPENSSL_LIB_DIR and OPENSSL_INCLUDE_DIR for static build to work https://github.com/sfackler/rust-openssl/issues/877
    OPENSSL_STATIC=1 OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include/openssl/ npm run build-release
    npm run pack-build

    popd
}

TARGET=${1:-x86_64-unknown-linux-gnu}
# Others:
# aarch64-unknown-linux-gnu
# x86_64-unknown-linux-musl
# aarch64-unknown-linux-musl

setup_dependencies $TARGET
install_node $TARGET
build_node_binary $TARGET
