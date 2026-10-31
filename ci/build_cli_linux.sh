#!/usr/bin/env bash
# Run inside a native-architecture manylinux_2_28 container. Cargo and rustup
# are mounted from the runner; keep the image's compiler/toolchain PATH intact.
set -euo pipefail

export PATH="/cargo/bin:$PATH"
# The manylinux images provide GCC, but do not guarantee Clang or LLD. Native
# dependencies use Clang, and the Arm linker wrapper below also requires LLD.
dnf install -y clang lld
export CC=clang CXX=clang++
"$CC" --version
"$CXX" --version
ld.lld --version

# Fail before Cargo downloads or compiles dependencies if either compiler or
# the linker cannot produce a runnable binary in this container.
printf 'int main(void) { return 0; }\n' > /tmp/cli-toolchain-probe.c
"$CC" -fuse-ld=lld /tmp/cli-toolchain-probe.c -o /tmp/cli-toolchain-probe
/tmp/cli-toolchain-probe
"$CXX" -fuse-ld=lld -x c++ /tmp/cli-toolchain-probe.c -o /tmp/cli-toolchain-probe
/tmp/cli-toolchain-probe

git config --global --add safe.directory /workspace

curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-${PROTOC_PLATFORM}.zip" -o /tmp/protoc.zip
echo "${PROTOC_SHA256}  /tmp/protoc.zip" | sha256sum -c -
unzip -q /tmp/protoc.zip -d /tmp/protoc
export PATH="/tmp/protoc/bin:$PATH"

# GNU ld on Arm has a much higher memory peak for Lance's final link. Use a
# linker wrapper without replacing the repository's CPU and feature rustflags.
if [[ "$CLI_TARGET" == aarch64-unknown-linux-gnu ]]; then
    cat > /tmp/cli-linker <<'LINKER'
#!/bin/sh
exec clang -fuse-ld=lld "$@"
LINKER
    chmod 755 /tmp/cli-linker
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/tmp/cli-linker
fi

cargo build --locked --release -p lancedb-cli --bin lancedb --target "$CLI_TARGET"
strip "target/$CLI_TARGET/release/lancedb"
