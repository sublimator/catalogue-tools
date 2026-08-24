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
if [ "${LINUX_CLANG_REPEAT+x}" = "x" ]; then
    REPEAT="$LINUX_CLANG_REPEAT"
elif [ "$MODE" = "vqt-tsan" ]; then
    REPEAT=1
else
    REPEAT=100
fi
BUILD_CORES="${BUILD_CORES:-$(sysctl -n hw.physicalcpu 2>/dev/null || nproc)}"
# Unset → default full. An explicitly empty or invalid token is refused so
# it cannot silently become a second cache identity.
if [ "${LINUX_CLANG_CACHE_GENERATION+x}" = "x" ]; then
    CACHE_GENERATION="$LINUX_CLANG_CACHE_GENERATION"
else
    CACHE_GENERATION="full"
fi
if [[ ! "$CACHE_GENERATION" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then
    echo "invalid LINUX_CLANG_CACHE_GENERATION=$CACHE_GENERATION" >&2
    exit 2
fi
# The first Clang-20 ARM experiment populated the old volume with Boost bytes
# built under temporary component exclusions while retaining a full-surface
# package identity. Never let that cache certify the corrected profile.
VOLUME_PREFIX="catalogue-tools-linux-clang20-${ARCH}"
CONAN_VOLUME="${VOLUME_PREFIX}-conan-${CACHE_GENERATION}"
BUILD_VARIANT=""
if [[ "$MODE" == *-tsan ]]; then
    BUILD_VARIANT="-tsan"
fi
BUILD_VOLUME="${VOLUME_PREFIX}-build-${CACHE_GENERATION}${BUILD_VARIANT}"

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
