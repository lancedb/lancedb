#!/bin/bash
set -e

rustup target add aarch64-pc-windows-msvc

cd nodejs
npm ci
npm run build-release
