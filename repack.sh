#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build}"
CONVERTER="./${BUILD_DIR}/src/experiments/catl1-to-catl2"
INPUT="${INPUT:-$HOME/projects/xahaud-worktrees/xahaud/cat_all.bin}"
OUTPUT="${OUTPUT:-$HOME/projects/xahaud-worktrees/xahaud/cat_all.catl2}"
LOG_LEVEL="${LOG_LEVEL:-info}"

ninja -C "$BUILD_DIR"

# Pack
"$CONVERTER" --use-xrpl-defs --input "$INPUT" --output "$OUTPUT" --log-level "$LOG_LEVEL"

# Quick probe
"$CONVERTER" --input "$OUTPUT" \
  --get-ledger 97480076 \
  --get-key 0002DBD178A4A4033B016621EAA5F503DB8FA6C6B6DF0105FC5B34CC7790D07C
