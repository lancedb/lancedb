#!/usr/bin/env bash

#
# A script for running the given command together with the lancedb cli.
#

die() {
    echo $?
    exit 1
}

check_command_exists() {
    command="${1}"
    which ${command} &> /dev/null || \
        die "Unable to locate command: ${command}. Did you install it?"
}

if [[ ! -e ./lancedb ]]; then
    if [[ -v SOPHON_READ_TOKEN ]]; then
        INPUT="lancedb-linux-x64"
        gh release \
            --repo lancedb/lancedb \
            download ci-support-binaries \
            --pattern "${INPUT}" \
            || die "failed to fetch cli."
        check_command_exists openssl
        openssl enc -aes-256-cbc \
            -d -pbkdf2 \
            -pass "env:SOPHON_READ_TOKEN" \
            -in "${INPUT}" \
            -out ./lancedb-linux-x64.tar.gz \
            || die "openssl failed"
        TARGET="${INPUT}.tar.gz"
    else
        ARCH="x64"
        if [[ $OSTYPE == 'darwin'* ]]; then
            UNAME=$(uname -m)
            if [[ $UNAME == 'arm64' ]]; then
                ARCH='arm64'
            fi
            OSTYPE="macos"
        elif [[ $OSTYPE == 'linux'* ]]; then
            if [[ $UNAME == 'aarch64' ]]; then
                ARCH='arm64'
            fi
            OSTYPE="linux"
        else
            die "unknown OSTYPE: $OSTYPE"
        fi

        check_command_exists gh
        TARGET="lancedb-${OSTYPE}-${ARCH}.tar.gz"
        gh release \
            --repo lancedb/sophon \
            download lancedb-cli-v0.0.3 \
            --pattern "${TARGET}" \
            || die "failed to fetch cli."
    fi

    check_command_exists tar
    tar xvf "${TARGET}" || die "tar failed."
    [[ -e ./lancedb ]] || die "failed to extract lancedb."
fi

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
export CREATE_LANCEDB_TEST_CONNECTION_SCRIPT="${SCRIPT_DIR}/create_lancedb_test_connection.sh"

"${@}"
