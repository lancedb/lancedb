#!/bin/bash
# Builds openssl from source so we can statically link to it

# this is to avoid the error we get with the system installation:
# /usr/bin/ld: <library>: version node not found for symbol SSLeay@@OPENSSL_1.0.1
# /usr/bin/ld: failed to set dynamic section sizes: Bad value
set -e

git clone -b OpenSSL_1_1_1v \
    --single-branch \
    https://github.com/openssl/openssl.git

pushd openssl

if [[ $1 == x86_64* ]]; then
    ARCH=linux-x86_64
else
    # gnu target
    ARCH=linux-aarch64
fi

./Configure no-shared $ARCH

make

make install