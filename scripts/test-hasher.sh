#!/bin/bash -u

set -ex

pushd build
ninja
popd

FIRST_LEDGER=1
SECOND_LEDGER=100
OUTPUT_FILE="/Users/nicholasdudfield/projects/xahau-history/cat.$FIRST_LEDGER-$SECOND_LEDGER.catl"

./build/src/hasher/catl-hasher \
  $HOME/projects/xahau-history/cat.1-5000000.dec --level=info \
  --first-ledger $FIRST_LEDGER --last-ledger $SECOND_LEDGER \
  --create-slice-file "$OUTPUT_FILE"

./build/src/hasher/catl-hasher \
  $OUTPUT_FILE --level=info

./build/src/utils/catl-validator $OUTPUT_FILE
