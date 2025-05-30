#!/bin/bash -u

set -ex

pushd build
ninja
popd

INPUT_FILE="$PWD/test-slice-4500000-5000000.catl"
OUTPUT_FILE="$INPUT_FILE.catl2"

# ./build/src/experiments/catl1-to-catl2 --input "$INPUT_FILE" --output "$OUTPUT_FILE" --max-ledgers 10000

# if GET_KEY is set, look up the key in the file
if [ -n "$GET_KEY" ]; then
  ./build/src/experiments/catl1-to-catl2 --help
  ./build/src/experiments/catl1-to-catl2 --input "$OUTPUT_FILE" --get-key "$GET_KEY" --get-ledger $GET_LEDGER
else
  echo "No key specified via GET_KEY and GET_LEDGER"
fi
