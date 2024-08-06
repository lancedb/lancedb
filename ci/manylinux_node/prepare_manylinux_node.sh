#!/bin/bash
set -e

install_node() {
    echo "Installing node..."

    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash

    source "$HOME"/.bashrc

    nvm install --no-progress 18
}

install_rust() {
    echo "Installing rust..."
    curl https://sh.rustup.rs -sSf | bash -s -- -y
    export PATH="$PATH:/root/.cargo/bin"
}

install_node
install_rust