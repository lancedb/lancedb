#!/bin/bash
# Installs protobuf compiler. Should be run as root.
set -e

if [[ $1 == x86_64* ]]; then
    ARCH=x86_64
else
    # gnu target
    ARCH=aarch_64
fi

PB_REL=https://github.com/protocolbuffers/protobuf/releases
PB_VERSION=23.1
curl -LO $PB_REL/download/v$PB_VERSION/protoc-$PB_VERSION-linux-$ARCH.zip
unzip protoc-$PB_VERSION-linux-$ARCH.zip -d /usr/local
