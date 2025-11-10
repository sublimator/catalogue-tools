#!/bin/bash -u
set -e

# Parse custom parameters
GTEST_ARGS=()
FUZZ_MODE=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --fuzz)
            FUZZ_MODE=1
            export FUZZ=1
            if [[ -n "${2:-}" && "$2" =~ ^[0-9]+$ ]]; then
                export FUZZ_ITERATIONS="$2"
                shift 2
            else
                shift 1
            fi
            ;;
        --iterations)
            export STRESS_ITERATIONS="$2"
            shift 2
            ;;
        --initial-records)
            export STRESS_INITIAL_RECORDS="$2"
            shift 2
            ;;
        --index-inserts)
            export STRESS_INDEX_INSERTS="$2"
            shift 2
            ;;
        --rekey-inserts)
            export STRESS_REKEY_INSERTS="$2"
            shift 2
            ;;
        --insert-delay-us)
            export STRESS_INSERT_DELAY_US="$2"
            shift 2
            ;;
        *)
            GTEST_ARGS+=("$1")
            shift
            ;;
    esac
done

# Build stress tests
echo "🔨 Building nudbview stress tests..."
set -x
ninja -C build tests/nudbview/slice_stress_gtest
set +x

# Run tests
echo ""
echo "🧪 Running nudbview stress tests..."
echo ""

if [[ $FUZZ_MODE -eq 1 ]]; then
    echo "🎲 FUZZ MODE ENABLED"
    echo "  FUZZ_ITERATIONS=${FUZZ_ITERATIONS:-100}"
    echo ""
    echo "Fuzzing generates random parameters for each test:"
    echo "  - Random initial DB size (100-5000 records)"
    echo "  - Random insert counts (10-500)"
    echo "  - Random insert rates (1-1000μs delay)"
    echo "  - Random index intervals (10-100)"
    echo "  - Random slice ranges (25-75% of DB)"
    echo ""
    # Auto-add fuzz filter if not already specified
    if [[ ${#GTEST_ARGS[@]} -eq 0 ]]; then
        GTEST_ARGS+=("--gtest_filter=Fuzz/*")
    elif ! printf '%s\n' "${GTEST_ARGS[@]}" | grep -q "gtest_filter"; then
        GTEST_ARGS+=("--gtest_filter=Fuzz/*")
    fi
else
    echo "Environment overrides:"
    [[ -n "${STRESS_ITERATIONS:-}" ]] && echo "  STRESS_ITERATIONS=$STRESS_ITERATIONS"
    [[ -n "${STRESS_INITIAL_RECORDS:-}" ]] && echo "  STRESS_INITIAL_RECORDS=$STRESS_INITIAL_RECORDS"
    [[ -n "${STRESS_INDEX_INSERTS:-}" ]] && echo "  STRESS_INDEX_INSERTS=$STRESS_INDEX_INSERTS"
    [[ -n "${STRESS_REKEY_INSERTS:-}" ]] && echo "  STRESS_REKEY_INSERTS=$STRESS_REKEY_INSERTS"
    [[ -n "${STRESS_INSERT_DELAY_US:-}" ]] && echo "  STRESS_INSERT_DELAY_US=$STRESS_INSERT_DELAY_US"
    echo ""
    echo "Test suite selection (use --gtest_filter):"
    echo "  Light/*          - Quick stress tests (default)"
    echo "  DISABLED_Heavy/* - Heavy stress tests (long running)"
    echo "  Fuzz/*           - Random fuzzing tests (use --fuzz)"
fi

echo ""
set -x
if [[ ${#GTEST_ARGS[@]} -gt 0 ]]; then
    ./build/tests/nudbview/slice_stress_gtest "${GTEST_ARGS[@]}"
else
    ./build/tests/nudbview/slice_stress_gtest
fi
