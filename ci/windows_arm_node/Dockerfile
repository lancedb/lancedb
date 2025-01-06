FROM alpine:edge

RUN apk add --update protobuf-dev curl clang lld llvm19 grep npm bash msitools sed

RUN curl --proto '=https' --tlsv1.3 -sSf https://raw.githubusercontent.com/rust-lang/rustup/refs/heads/master/rustup-init.sh | sh -s -- -y

COPY sysroot-aarch64-pc-windows-msvc.sh .

RUN source "$HOME/.cargo/env" \
    && rustup target add aarch64-pc-windows-msvc \
    && (mkdir -p sysroot && cd sysroot && sh ../sysroot-aarch64-pc-windows-msvc.sh) \
    && printf '#!/bin/sh\ncargo "$@"' > $HOME/.cargo/bin/cargo-xwin \
    && chmod u+x $HOME/.cargo/bin/cargo-xwin

ENV CC=clang \
    AR=llvm-ar \
    C_INCLUDE_PATH=/usr/aarch64-pc-windows-msvc/usr/include \
    CARGO_BUILD_TARGET=aarch64-pc-windows-msvc \
    RUSTFLAGS='-Clinker=lld -Clink-arg=/LIBPATH:/usr/aarch64-pc-windows-msvc/usr/lib -Clink-arg=arm64rt.lib'
