# On MacOS, need to run in a linux container:
# cat ci/ubuntu_build.dockerfile | docker build -t lancedb-node-build -
# docker run -v /var/run/docker.sock:/var/run/docker.sock -v $(pwd):/io -w /io lancedb-node-build bash ci/build_linux_artifacts.sh
FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Moscow

RUN apt update && apt install -y protobuf-compiler libssl-dev build-essential curl \
    software-properties-common npm docker.io

# Install rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# Install cross
# https://github.com/cross-rs/cross/issues/1257#issuecomment-1544553706
RUN cargo install cross --git https://github.com/cross-rs/cross

# Install additional build targets
RUN rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-unknown-linux-musl x86_64-unknown-linux-musl

# Install node
RUN npm install npm@latest -g && \
    npm install n -g && \
    n latest

# set CROSS_CONTAINER_IN_CONTAINER to inform `cross` that it is executed from within a container
ENV CROSS_CONTAINER_IN_CONTAINER=true
ENV CROSS_CONTAINER_ENGINE_NO_BUILDKIT=1
