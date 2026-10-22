# Simple base dockerfile that supports basic dependencies required to run lance with FTS and Hybrid Search
# Usage: docker build -t lancedb:latest -f Dockerfile .
FROM python:3.12-slim-bookworm

# Install build dependencies in a single layer
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    protobuf-compiler \
    git \
    ca-certificates && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Install Rust (pinned installer, non-interactive)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal

# Set the environment variable for Rust
ENV PATH="/root/.cargo/bin:${PATH}"

# Verify installations
RUN python --version && \
  rustc --version && \
  protoc --version

RUN pip install --no-cache-dir lancedb
