#!/bin/bash
set -e

cd nodejs
npm ci
npm run build-release
