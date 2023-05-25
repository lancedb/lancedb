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
        # manylinux2014
        yum install -y openssl-devel unzip   
    fi

    if [[ $1 == x86_64* ]]; then
        ARCH=x86_64
    else
        # gnu target
        ARCH=aarch_64
    fi
    
    # Install new enough protobuf (yum-provided is old)
    PB_REL=https://github.com/protocolbuffers/protobuf/releases
    PB_VERSION=23.1
    curl -LO $PB_REL/download/v$PB_VERSION/protoc-$PB_VERSION-linux-$ARCH.zip
    unzip protoc-$PB_VERSION-linux-$ARCH.zip -d /usr/local
}

install_node() {
    echo "Installing node..."
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
    source "$HOME"/.bashrc

    if [[ $1 == *musl ]]; then
        # This node version is 15, we need 16 or higher:
        # apk add nodejs-current npm 
        # So instead we install from source (nvm doesn't provide binaries for musl):
        nvm install -s 17
    else
        nvm install 17 # latest that supports glibc 2.17
    fi
}

install_rust() {
    echo "Installing rust..."
    curl https://sh.rustup.rs -sSf | bash -s -- -y
    export PATH="$PATH:/root/.cargo/bin"
}

build_node_binary() {
    echo "Building node library for $1..."
    pushd node
    
    if [[ $1 == *musl ]]; then
        # This is needed for cargo to allow build cdylibs with musl
        export RUSTFLAGS="-C target-feature=-crt-static"
    fi
    # We don't pass in target, since the native target here already matches
    # and openblas-src doesn't do well with cross-compilation.
    npm run build-release
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
install_rust
build_node_binary $TARGET
