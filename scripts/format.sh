#!/bin/bash -u
# We use:
#   bash -u to fail on unbound variables
#   set -e to fail on any command error
set -e

ROOT_DIR=`git rev-parse --show-toplevel`
cd $ROOT_DIR

FILES=($(find src -type f \( -name "*.h" -o -name "*.cpp" \)))

# clang-format -i "$FILES"

# Format each file individually
for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "Formatting $file"
        clang-format -i "$file"
    fi
done
