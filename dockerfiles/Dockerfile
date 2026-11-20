# Simple base dockerfile for running LanceDB, including FTS and Hybrid Search
# Usage: docker build -t lancedb:latest -f Dockerfile .
FROM python:3.12-slim-bookworm

# lancedb ships prebuilt manylinux wheels, so no Rust or C toolchain is needed.
RUN pip install --no-cache-dir lancedb

RUN useradd --create-home --uid 1000 lancedb
USER lancedb
WORKDIR /home/lancedb
