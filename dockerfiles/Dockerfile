#Simple base dockerfile that supports basic dependencies required to run lance with FTS and Hybrid Search
#Usage docker build -t lancedb:latest -f Dockerfile .
FROM python:3.10-slim-buster

# Install Rust
RUN apt-get update && apt-get install -y curl build-essential && \
  curl https://sh.rustup.rs -sSf | sh -s -- -y

# Set the environment variable for Rust
ENV PATH="/root/.cargo/bin:${PATH}"

# Install protobuf compiler
RUN apt-get install -y protobuf-compiler && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN apt-get -y update &&\
  apt-get -y upgrade && \
  apt-get -y install git


# Verify installations
RUN python --version && \
  rustc --version && \
  protoc --version

RUN pip install tantivy@git+https://github.com/quickwit-oss/tantivy-py#164adc87e1a033117001cf70e38c82a53014d985 lancedb
