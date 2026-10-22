#!/usr/bin/env bash

export RUST_LOG=info
exec ./lancedb server --port 0 --sql-port 0  --data-dir "${1}"
