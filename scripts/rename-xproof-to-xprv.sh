#!/usr/bin/env bash
# Rename xproof → xprv across the repo.
# Run with --dry-run (default) or --execute to actually do it.
#
# Comment out any lines you don't want to run.
set -euo pipefail

DRY_RUN=true
if [[ "${1:-}" == "--execute" ]]; then
  DRY_RUN=false
fi

run() {
  if $DRY_RUN; then
    echo "[dry-run] $*"
  else
    echo "[exec] $*"
    eval "$@"
  fi
}

# ─────────────────────────────────────────────
# 1. DIRECTORY RENAMES (order matters: deepest first)
# ─────────────────────────────────────────────

# Header include dir
run git mv src/xproof/includes/xproof src/xproof/includes/xprv

# Test dir + fixtures
run git mv tests/xproof tests/xprv

# Main source dir (do last since it contains the includes dir)
run git mv src/xproof src/xprv

# ─────────────────────────────────────────────
# 2. SCRIPT FILE RENAMES
# ─────────────────────────────────────────────

run git mv scripts/xproof-serve-smoke.py scripts/xprv-serve-smoke.py
run git mv scripts/xproof-serve-smoke-v2.py scripts/xprv-serve-smoke-v2.py
run git mv scripts/xproof-serve-check.py scripts/xprv-serve-check.py
run git mv scripts/xproof-tx-corpus.sample.json scripts/xprv-tx-corpus.sample.json
run git mv scripts/xproof-tx-corpus.generated.json scripts/xprv-tx-corpus.generated.json
run git mv scripts/verify-xproof.py scripts/verify-xprv.py

# ─────────────────────────────────────────────
# 3. CONTENT REPLACEMENTS (sed -i)
#    Each file listed individually so you can comment out any.
# ─────────────────────────────────────────────

# --- C++ source files ---
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/validation-collector.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/main.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/cmd-serve.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/cmd-prove.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/cmd-verify.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/cmd-dev.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/cmd-header.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/cmd-ping.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/proof-engine.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/proof-builder.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/proof-chain-json.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/proof-chain-binary.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/proof-resolver.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/proof-steps.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/http-server.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/config.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/network-config.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/validation-buffer.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/anchor-verifier.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/vl-cache.cpp"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/config.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/src/commands.h"

# --- C++ header files ---
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/proof-engine.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/proof-builder.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/proof-chain.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/proof-chain-json.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/proof-chain-binary.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/proof-resolver.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/proof-steps.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/http-server.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/network-config.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/hex-utils.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/skip-list.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/validation-collector.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/validation-buffer.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/vl-cache.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/anchor-verifier.h"
run "sed -i '' 's/xproof/xprv/g' src/xprv/includes/xprv/request-context.h"

# --- CMakeLists.txt files ---
run "sed -i '' 's/xproof/xprv/g' src/xprv/CMakeLists.txt"
run "sed -i '' 's/xproof/xprv/g' CMakeLists.txt"
run "sed -i '' 's/xproof/xprv/g' tests/xprv/CMakeLists.txt"
run "sed -i '' 's/xproof/xprv/g' tests/CMakeLists.txt"

# --- Test files ---
run "sed -i '' 's/xproof/xprv/g' tests/xprv/proof-chain-binary-gtest.cpp"
run "sed -i '' 's/xproof/xprv/g' tests/xprv/skip-list-gtest.cpp"

# --- Python scripts ---
run "sed -i '' 's/xproof/xprv/g' scripts/xprv-serve-smoke.py"
run "sed -i '' 's/xproof/xprv/g' scripts/xprv-serve-smoke-v2.py"
run "sed -i '' 's/xproof/xprv/g' scripts/xprv-serve-check.py"
run "sed -i '' 's/xproof/xprv/g' scripts/verify-xprv.py"
run "sed -i '' 's/xproof/xprv/g' scripts/prove-blast.py"
run "sed -i '' 's/xproof/xprv/g' scripts/log-filter.py"

# --- Other C++ files (outside src/xprv) ---
run "sed -i '' 's/xproof/xprv/g' src/peer-client/src/peer-crawl-client.cpp"
run "sed -i '' 's/xproof/xprv/g' src/peer-client/src/peer-set.cpp"
run "sed -i '' 's/xproof/xprv/g' src/peer-client/includes/catl/peer-client/peer-endpoint-cache.h"
run "sed -i '' 's/xproof/xprv/g' src/peer-client/includes/catl/peer-client/peer-set.h"

# --- Shell scripts ---
run "sed -i '' 's/xproof/xprv/g' scripts/build-hbb.sh"

# --- Comment in tests/shamap referencing xproof ---
run "sed -i '' 's/xproof/xprv/g' tests/shamap/CMakeLists.txt"

# ─────────────────────────────────────────────
# 4. UPPERCASE: XPROOF → XPRV (env vars etc.)
#    Done separately to avoid double-replacing.
# ─────────────────────────────────────────────

run "sed -i '' 's/XPROOF/XPRV/g' src/xprv/src/config.cpp"
run "sed -i '' 's/XPROOF/XPRV/g' src/xprv/src/config.h"
run "sed -i '' 's/XPROOF/XPRV/g' src/xprv/src/main.cpp"
run "sed -i '' 's/XPROOF/XPRV/g' scripts/xprv-serve-smoke.py"
run "sed -i '' 's/XPROOF/XPRV/g' scripts/xprv-serve-smoke-v2.py"
run "sed -i '' 's/XPROOF/XPRV/g' scripts/xprv-serve-check.py"
run "sed -i '' 's/XPROOF/XPRV/g' scripts/prove-blast.py"

# --- Other C++ files with XPROOF env vars ---
run "sed -i '' 's/XPROOF/XPRV/g' src/peer-client/src/peer-set.cpp"

# ─────────────────────────────────────────────
# 5. VERIFICATION
# ─────────────────────────────────────────────

if ! $DRY_RUN; then
  echo ""
  echo "=== Checking for any remaining 'xproof' references ==="
  grep -r "xproof" --include='*.cpp' --include='*.h' --include='*.py' \
       --include='*.sh' --include='*.txt' --include='*.json' \
       --include='*.toml' --include='*.cmake' \
       --exclude-dir=build --exclude-dir=build-tsan --exclude-dir=build-hbb \
       --exclude-dir=.ai-docs \
       . || echo "None found — clean rename!"
fi

# ─────────────────────────────────────────────
# 6. RENAME THIS SCRIPT ITSELF
# ─────────────────────────────────────────────

# run git mv scripts/rename-xproof-to-xprv.sh scripts/rename-xproof-to-xprv.sh.done

echo ""
echo "Done. (dry_run=$DRY_RUN)"
