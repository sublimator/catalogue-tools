#!/usr/bin/env bash
# Run the executor-sensitive tests under Linux Clang without waiting for CI.
#
# Fast native-Linux gate (default on Apple Silicon):
#   scripts/test-linux-clang.sh
#
# Exact GitHub runner architecture and affected suites:
#   LINUX_CLANG_PLATFORM=linux/amd64 LINUX_CLANG_MODE=affected \
#     scripts/test-linux-clang.sh
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
IMAGE="${LINUX_CLANG_IMAGE:-catalogue-tools-linux-clang-20}"
PLATFORM="${LINUX_CLANG_PLATFORM:-linux/$(docker info --format '{{.Architecture}}')}"
ARCH="${PLATFORM#linux/}"
MODE="${LINUX_CLANG_MODE:-focused}"
REPEAT="${LINUX_CLANG_REPEAT:-100}"
BUILD_CORES="${BUILD_CORES:-$(sysctl -n hw.physicalcpu 2>/dev/null || nproc)}"
CONAN_VOLUME="catalogue-tools-linux-clang20-${ARCH}-conan"
BUILD_VOLUME="catalogue-tools-linux-clang20-${ARCH}-build"

docker build \
    --platform "$PLATFORM" \
    --file "$ROOT_DIR/builds/linux-clang/Dockerfile" \
    --tag "$IMAGE-$ARCH" \
    "$ROOT_DIR"

docker run --rm \
    --platform "$PLATFORM" \
    --mount "type=bind,source=$ROOT_DIR,target=/workspace,readonly" \
    --mount "type=volume,source=$CONAN_VOLUME,target=/root/.conan2" \
    --mount "type=volume,source=$BUILD_VOLUME,target=/build" \
    --env "BUILD_CORES=$BUILD_CORES" \
    --env "LINUX_CLANG_MODE=$MODE" \
    --env "LINUX_CLANG_REPEAT=$REPEAT" \
    "$IMAGE-$ARCH" \
    bash /workspace/builds/linux-clang/run.sh
