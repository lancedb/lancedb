#!/bin/bash
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
    if [[ $1 == *musl ]]; then
        apk add nodejs-current npm
    else
        curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
        source "$HOME"/.nvm/nvm.sh
        nvm install 17 # latest that supports glibc 2.17
    fi
}

install_rust() {
    echo "Installing rust..."
    curl https://sh.rustup.rs -sSf | bash -s -- -y
    export PATH="$PATH:/root/.cargo/bin"
    rustup target add $1
}

build_node_binary() {
    echo "Building node library for $1..."
    pushd node
    
    if [[ $1 == *musl ]]; then
        # This is needed for cargo to allow build cdylibs with musl
        export RUSTFLAGS="-C target-feature=-crt-static"
    fi
    npm run build-release -- --target $1
    npm run pack-build -- --target $1

    popd
}

TARGET=${1:-x86_64-unknown-linux-gnu}
# Others:
# aarch64-unknown-linux-gnu
# x86_64-unknown-linux-musl
# aarch64-unknown-linux-musl

setup_dependencies $TARGET
install_node
install_rust $TARGET
build_node_binary $TARGET
