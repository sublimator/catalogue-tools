#!/bin/bash -u
set -ex

# Build the project
ninja -C build src/nudbview/nudb-util/nudb-util

# Parameters for testing
NUDB_PATH=${NUDB_PATH:-"../test-nudb-key"}
INDEX_FILE=${INDEX_FILE:-"$NUDB_PATH/nudb.dat.index"}
INDEX_INTERVAL=${INDEX_INTERVAL:-10000}
LOG_LEVEL=${LOG_LEVEL:-"info"}
CLEAR_INDEX=${CLEAR_INDEX:-""}
SHOW_PROGRESS=${SHOW_PROGRESS:-"1"}

# Slice parameters
START_RECORD=${START_RECORD:-0}
EXCLUSIVE_END_RECORD=${EXCLUSIVE_END_RECORD:-100000}
SLICE_OUTPUT=${SLICE_OUTPUT:-"./test-slice"}

# Build progress flag
PROGRESS_FLAG=""
if [ -n "$SHOW_PROGRESS" ]; then
  PROGRESS_FLAG="--progress"
fi

echo "🔧 NuDB Index & Slice Testing"
echo "=============================="
echo "NuDB Path: $NUDB_PATH"
echo "Index File: $INDEX_FILE"
echo "Index Interval: $INDEX_INTERVAL"
echo "Log Level: $LOG_LEVEL"
echo ""

# Check if nudb path exists
if [ ! -d "$NUDB_PATH" ]; then
  echo "❌ Error: NuDB path does not exist: $NUDB_PATH"
  echo "Please create a NuDB database first using catl1-to-nudb"
  exit 1
fi

# Check if dat file exists
if [ ! -f "$NUDB_PATH/nudb.dat" ]; then
  echo "❌ Error: nudb.dat not found in $NUDB_PATH"
  exit 1
fi

# Clear index if requested
if [ -n "$CLEAR_INDEX" ]; then
  echo "🗑️  Clearing existing index..."
  rm -f "$INDEX_FILE"
  echo "✅ Index cleared"
fi

# Build or extend index
if [ -f "$INDEX_FILE" ]; then
  echo "📊 Index file exists - extending..."
  ./build/src/nudbview/nudb-util/nudb-util index-dat \
    --nudb-path "$NUDB_PATH" \
    --output "$INDEX_FILE" \
    --index-interval $INDEX_INTERVAL \
    --log-level "$LOG_LEVEL" \
    --extend \
    $PROGRESS_FLAG
  echo "✅ Index extended"
else
  echo "📊 Building new index..."
  ./build/src/nudbview/nudb-util/nudb-util index-dat \
    --nudb-path "$NUDB_PATH" \
    --output "$INDEX_FILE" \
    --index-interval $INDEX_INTERVAL \
    --log-level "$LOG_LEVEL" \
    $PROGRESS_FLAG
  echo "✅ Index built"
fi

# Show index stats
INDEX_SIZE=$(du -h "$INDEX_FILE" | cut -f1)
echo ""
echo "📈 Index Statistics:"
echo "  File size: $INDEX_SIZE"
echo "  Interval: $INDEX_INTERVAL records"
echo ""

# Create slice from records
echo "🔪 Creating slice from records $START_RECORD to $EXCLUSIVE_END_RECORD (exclusive)..."
echo "  Output: $SLICE_OUTPUT"

# Remove old slice files if they exist
rm -f "$SLICE_OUTPUT.key" "$SLICE_OUTPUT.meta"

./build/src/nudbview/nudb-util/nudb-util make-slice \
  --nudb-path "$NUDB_PATH" \
  --start $START_RECORD \
  --exclusive-end $EXCLUSIVE_END_RECORD \
  --index "$INDEX_FILE" \
  --output "$SLICE_OUTPUT" \
  --log-level "$LOG_LEVEL" \
  $PROGRESS_FLAG

echo "✅ Slice created"
echo ""

# Show slice stats
if [ -f "$SLICE_OUTPUT.key" ]; then
  KEY_SIZE=$(du -h "$SLICE_OUTPUT.key" | cut -f1)
  echo "📊 Slice Statistics:"
  echo "  Key file: $KEY_SIZE"
fi
if [ -f "$SLICE_OUTPUT.meta" ]; then
  META_SIZE=$(du -h "$SLICE_OUTPUT.meta" | cut -f1)
  echo "  Meta file: $META_SIZE"
fi

echo ""
echo "🎉 Done! Index and slice created successfully"
echo ""
echo "💡 Tips:"
echo "  - Set CLEAR_INDEX=1 to rebuild index from scratch"
echo "  - Set START_RECORD and EXCLUSIVE_END_RECORD to change slice range"
echo "  - Set INDEX_INTERVAL to change indexing granularity"
echo "  - Set SHOW_PROGRESS='' to disable progress output"
echo ""
echo "Examples:"
echo "  CLEAR_INDEX=1 ./scripts/test-nudb-index-slice.sh"
echo "  START_RECORD=0 EXCLUSIVE_END_RECORD=50000 ./scripts/test-nudb-index-slice.sh"
echo "  INDEX_INTERVAL=1000 CLEAR_INDEX=1 ./scripts/test-nudb-index-slice.sh"
