set -e

setup_dependencies() {
    echo "Installing system dependencies..."
    
    # manylinux2014
    yum install -y openssl-devel unzip

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

    nvm install --no-progress 16
}

install_rust() {
    echo "Installing rust..."
    curl https://sh.rustup.rs -sSf | bash -s -- -y
    export PATH="$PATH:/root/.cargo/bin"
}

TARGET=${1:-x86_64} # or aarch64

setup_dependencies $TARGET
install_node $TARGET
install_rust