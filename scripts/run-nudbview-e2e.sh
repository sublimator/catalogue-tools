#!/bin/bash -u
set -ex

# Build e2e tests
echo "🔨 Building nudbview e2e tests..."
ninja -C build tests/nudbview/slice_e2e_gtest

# Run tests
echo ""
echo "🧪 Running nudbview e2e tests..."
./build/tests/nudbview/slice_e2e_gtest "$@"
