#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR=/workspace
BUILD_DIR=/build
MODE="${LINUX_CLANG_MODE:-focused}"
REPEAT="${LINUX_CLANG_REPEAT:-100}"
BUILD_CORES="${BUILD_CORES:-$(nproc)}"
SANITIZER=""
if [[ "$MODE" == *-tsan ]]; then
    SANITIZER="thread"
fi

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

# Boost.Build spells the native AArch64 target as arm64-pc-linux. Clang does
# not associate that triple with Ubuntu's aarch64-linux-gnu GCC installation,
# so even standard C++ headers and crtbeginS.o disappear. B2 appends its target
# after ordinary profile flags, so a competing --target is ineffective. Pin
# the GCC installation directly for compilation and both link-probe classes;
# that survives argument order and is derived from the installed compiler
# rather than a filesystem guess. The exact CI-shaped amd64 lane needs no
# override.
case "$(uname -m)" in
    aarch64 | arm64)
        GCC_INSTALL_DIR="$(dirname "$(gcc-14 -print-libgcc-file-name)")"
        printf '%s\n' \
            "tools.build:cxxflags=[\"--gcc-install-dir=$GCC_INSTALL_DIR\"]" \
            "tools.build:exelinkflags=[\"--gcc-install-dir=$GCC_INSTALL_DIR\"]" \
            "tools.build:sharedlinkflags=[\"--gcc-install-dir=$GCC_INSTALL_DIR\"]" \
            >>"$(conan config home)/profiles/default"
        ;;
esac

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
    -DCMAKE_TOOLCHAIN_FILE="$BUILD_DIR/conan_toolchain.cmake" \
    -DCATL_SANITIZE="$SANITIZER"

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
    rpc)
        cmake --build "$BUILD_DIR" --target rpc-client-gtest -j "$BUILD_CORES"
        "$BUILD_DIR/tests/rpc-client/rpc-client-gtest" \
            --gtest_filter='RpcCallbackBridge0077.CompletionReturnsPublishedResult:RpcCallbackBridge0077.CallerCancellationReturnsEmptyRpcResult:RpcCallbackBridge0077.ForeignCompletionRacesCallerCancellation' \
            --gtest_repeat="$REPEAT" \
            --gtest_break_on_failure \
            --gtest_brief=1
        "$BUILD_DIR/tests/rpc-client/rpc-client-gtest" \
            --gtest_filter='RpcCallbackBridge0077.DetachedRequestDiscardsCallbackException' \
            --gtest_brief=1
        ;;
    rpc-tsan)
        # Issue 0077 witness protocol (three steps, mirrors vqt-tsan —
        # restructured in claude's adversarial review: the previous
        # halt_on_error=1 form made a FIRED witness kill the gate under
        # set -e, and a quiet run had no domination control, so it could
        # not distinguish "no race" from a null probe).
        cmake --build "$BUILD_DIR" --target rpc-client-gtest -j "$BUILD_CORES"
        # 1) The deterministic cells must PASS.
        "$BUILD_DIR/tests/rpc-client/rpc-client-gtest" \
            --gtest_filter='RpcCallbackBridge0077.CompletionReturnsPublishedResult:RpcCallbackBridge0077.CallerCancellationReturnsEmptyRpcResult:RpcCallbackBridge0077.DetachedRequestDiscardsCallbackException' \
            --gtest_repeat="$REPEAT" \
            --gtest_break_on_failure \
            --gtest_brief=1
        # 2) The seeded domination control MUST make TSan report (exit
        #    66); a quiet control is a null probe and refuses the run.
        set +e
        TSAN_OPTIONS="halt_on_error=0 exitcode=66" \
            "$BUILD_DIR/tests/rpc-client/rpc-client-gtest" \
            --gtest_filter='RpcCallbackBridge0077.TsanDominationControlSeededPair' \
            --gtest_brief=1
        control_rc=$?
        set -e
        if [[ "$control_rc" -ne 66 ]]; then
            echo "rpc-tsan: domination control did not report (rc=$control_rc) — null probe, refusing"
            exit 1
        fi
        # 3) The race cell: exit 66 = FIRED (data-race reports in the
        #    log); exit 0 = quiet, admissible DID-NOT-FIRE given step 2.
        set +e
        TSAN_OPTIONS="halt_on_error=0 exitcode=66" \
            "$BUILD_DIR/tests/rpc-client/rpc-client-gtest" \
            --gtest_filter='RpcCallbackBridge0077.ForeignCompletionRacesCallerCancellation' \
            --gtest_repeat="$REPEAT" \
            --gtest_brief=1
        race_rc=$?
        set -e
        if [[ "$race_rc" -ne 0 && "$race_rc" -ne 66 ]]; then
            echo "rpc-tsan: race cell failed outside the evidence protocol (rc=$race_rc)" >&2
            exit "$race_rc"
        fi
        echo "rpc-tsan: race cell exit=$race_rc (66=FIRED, 0=quiet)"
        ;;
    vqt-tsan)
        # Issue 0076 witness protocol (three steps, see
        # tests/xprv/validation-quorum-tracker-posthop-gtest.cpp):
        cmake --build "$BUILD_DIR" --target xprv-gtest -j "$BUILD_CORES"
        # 1) The deterministic post-hop escape witness must PASS.
        "$BUILD_DIR/tests/xprv/xprv-gtest" \
            --gtest_filter='ValidationQuorumTrackerPostHop0076.PostHopContinuationEscapesOwnerStrand' \
            --gtest_repeat="$REPEAT" \
            --gtest_break_on_failure \
            --gtest_brief=1
        # 2) The seeded domination control MUST make TSan report (exit
        #    66); a quiet control is a null probe and refuses the run.
        set +e
        TSAN_OPTIONS="halt_on_error=0 exitcode=66" \
            "$BUILD_DIR/tests/xprv/xprv-gtest" \
            --gtest_filter='ValidationQuorumTrackerPostHop0076.TsanDominationControlSeededPair' \
            --gtest_brief=1
        control_rc=$?
        set -e
        if [[ "$control_rc" -ne 66 ]]; then
            echo "vqt-tsan: domination control did not report (rc=$control_rc) — null probe, refusing"
            exit 1
        fi
        # 3) The workloads: exit 66 = FIRED (data-race reports in the
        #    log); exit 0 = quiet, admissible DID-NOT-FIRE given step 2.
        set +e
        TSAN_OPTIONS="halt_on_error=0 exitcode=66" \
            "$BUILD_DIR/tests/xprv/xprv-gtest" \
            --gtest_filter='ValidationQuorumTrackerPostHop0076.TsanWorkload*' \
            --gtest_repeat="$REPEAT" \
            --gtest_brief=1
        workload_rc=$?
        set -e
        if [[ "$workload_rc" -ne 0 && "$workload_rc" -ne 66 ]]; then
            echo "vqt-tsan: workload failed outside the evidence protocol (rc=$workload_rc)" >&2
            exit "$workload_rc"
        fi
        echo "vqt-tsan: workload exit=$workload_rc (66=FIRED, 0=quiet)"
        ;;
    full)
        cmake --build "$BUILD_DIR" -j "$BUILD_CORES"
        XPRV_STRAND_FAIL_FAST=1 \
            ctest --test-dir "$BUILD_DIR" --verbose
        ;;
    *)
        echo "unknown LINUX_CLANG_MODE=$MODE (expected focused, affected, rpc, rpc-tsan, vqt-tsan, or full)" >&2
        exit 2
        ;;
esac
