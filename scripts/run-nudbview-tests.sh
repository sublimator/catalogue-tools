#!/bin/bash -u
set -ex

# Build tests
echo "🔨 Building nudbview tests..."
ninja -C build tests/nudbview/nudbview_gtest

# Run tests
echo ""
echo "🧪 Running nudbview tests..."
./build/tests/nudbview/nudbview_gtest "$@"
