#!/bin/bash -u

set -ex

pushd build
ninja
popd

# Test decompression from compressed fixture to uncompressed output
echo "Testing decompression from compressed fixture"
./build/src/utils-v1/catl1-decomp tests/catalogue-v1/fixture/cat.1-100.compression-9.catl ./test-decompressed.catl --force

# Validate the decompressed file
echo "Validating decompressed file"
./build/src/utils-v1/catl1-validator ./test-decompressed.catl

# Compare file sizes
COMPRESSED_SIZE=$(stat -f "%z" tests/catalogue-v1/fixture/cat.1-100.compression-9.catl)
DECOMPRESSED_SIZE=$(stat -f "%z" ./test-decompressed.catl)
UNCOMPRESSED_FIXTURE_SIZE=$(stat -f "%z" tests/catalogue-v1/fixture/cat.1-100.compression-0.catl)

echo "Compressed file size: $COMPRESSED_SIZE bytes"
echo "Decompressed file size: $DECOMPRESSED_SIZE bytes"
echo "Original uncompressed fixture size: $UNCOMPRESSED_FIXTURE_SIZE bytes"

# Calculate expansion ratio
RAW_RATIO=$(echo "scale=2; $DECOMPRESSED_SIZE / $COMPRESSED_SIZE" | bc)
echo "Expansion ratio: ${RAW_RATIO}x"

# Compare sizes with the uncompressed fixture
if [ "$DECOMPRESSED_SIZE" -eq "$UNCOMPRESSED_FIXTURE_SIZE" ]; then
  echo "✅ Success: Decompressed file size matches original uncompressed fixture size"
else
  echo "⚠️ Warning: Size mismatch between decompressed file ($DECOMPRESSED_SIZE) and uncompressed fixture ($UNCOMPRESSED_FIXTURE_SIZE)"
  exit 1
fi

echo "Test completed successfully!"
