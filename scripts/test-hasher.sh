#!/bin/bash -u

set -ex

pushd build
ninja
popd

FIRST_LEDGER=1
SECOND_LEDGER=100
COMPRESSION=9
OUTPUT_FILE="$HOME/projects/xahau-history/cat.$FIRST_LEDGER-$SECOND_LEDGER.compression-$COMPRESSION.catl"

./build/src/hasher-v1/catl1-hasher \
  $HOME/projects/xahau-history/cat.1-5000000.dec --level=info \
  --first-ledger $FIRST_LEDGER --last-ledger $SECOND_LEDGER \
  --compression=$COMPRESSION \
  --create-slice-file "$OUTPUT_FILE"

if [ "$COMPRESSION" -gt "0" ]; then
  echo "Skipping catl1-hasher for compressed file"
else
  ./build/src/hasher-v1/catl1-hasher \
    $OUTPUT_FILE --level=info
fi

./build/src/utils-v1/catl1-validator $OUTPUT_FILE

echo "Done"
echo "Output file: $OUTPUT_FILE"

cp $OUTPUT_FILE tests/catalogue-v1/fixture
