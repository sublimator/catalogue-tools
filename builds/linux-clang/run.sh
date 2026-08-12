#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR=/workspace
BUILD_DIR=/build
MODE="${LINUX_CLANG_MODE:-focused}"
REPEAT="${LINUX_CLANG_REPEAT:-100}"
BUILD_CORES="${BUILD_CORES:-$(nproc)}"

mkdir -p "$BUILD_DIR" "$(conan config home)/profiles"

# Reproduce the profile construction used by the GitHub Clang matrix. The
# detected profile supplies the remaining host/build settings; these values
# establish the tested compiler, standard library, and build type.
conan profile detect --name detected --force
cat >"$(conan config home)/profiles/default" <<'EOF'
include(detected)
[settings]
compiler=clang
compiler.version=20
compiler.cppstd=20
compiler.libcxx=libstdc++
build_type=Release
[conf]
tools.build:compiler_executables={"c": "/usr/bin/clang-20", "cpp": "/usr/bin/clang++-20"}
tools.cmake.cmaketoolchain:user_presets=
EOF

conan export "$SOURCE_DIR/external/ftxui/all" \
    --name=ftxui --version=6.1.9 --user=catalogue-tools --channel=stable
conan export "$SOURCE_DIR/external/libsecp256k1/all" \
    --name=libsecp256k1 --version=0.6.0 --user=catalogue-tools --channel=stable
conan install "$SOURCE_DIR" \
    --output-folder "$BUILD_DIR" \
    --build missing \
    --settings build_type=Release

cmake -S "$SOURCE_DIR" -B "$BUILD_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=/usr/bin/clang-20 \
    -DCMAKE_CXX_COMPILER=/usr/bin/clang++-20 \
    -DCMAKE_TOOLCHAIN_FILE="$BUILD_DIR/conan_toolchain.cmake"

case "$MODE" in
    focused)
        cmake --build "$BUILD_DIR" \
            --target history-search-gtest peer-set-pruning-gtest \
            -j "$BUILD_CORES"
        XPRV_STRAND_FAIL_FAST=1 \
            "$BUILD_DIR/tests/peer-client/history-search-gtest" \
            --gtest_filter='PeerSetHistoryDriver.WaitReentersOwnerStrandAcrossConcurrentRunners:PeerSetHistoryDriver.AsioCancellationTerminatesBeforeAnotherGrant' \
            --gtest_repeat="$REPEAT" \
            --gtest_break_on_failure \
            --gtest_brief=1
        ;;
    affected)
        cmake --build "$BUILD_DIR" \
            --target history-search-gtest peer-set-pruning-gtest \
            -j "$BUILD_CORES"
        XPRV_STRAND_FAIL_FAST=1 \
            "$BUILD_DIR/tests/peer-client/history-search-gtest" \
            --gtest_brief=1
        XPRV_STRAND_FAIL_FAST=1 \
            "$BUILD_DIR/tests/peer-client/peer-set-pruning-gtest" \
            --gtest_brief=1
        ;;
    full)
        cmake --build "$BUILD_DIR" -j "$BUILD_CORES"
        XPRV_STRAND_FAIL_FAST=1 \
            ctest --test-dir "$BUILD_DIR" --verbose
        ;;
    *)
        echo "unknown LINUX_CLANG_MODE=$MODE (expected focused, affected, or full)" >&2
        exit 2
        ;;
esac
