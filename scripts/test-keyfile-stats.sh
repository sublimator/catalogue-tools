#!/bin/bash -u
set -ex

# Build the project
ninja -C build nudb-util

# Parameters for testing
NUDB_PATH=${NUDB_PATH:-"./test-nudb"}
KEY_FILE=${KEY_FILE:-""} # Optional specific .key file path (default: nudb.key in NUDB_PATH)
LOG_LEVEL=${LOG_LEVEL:-"info"}
PROGRESS=${PROGRESS:-""}                   # Set to any value to show progress updates
HISTOGRAM=${HISTOGRAM:-"1"}                # Set to empty to disable histogram (default: enabled)
COLLISION_DETAILS=${COLLISION_DETAILS:-""} # Set to any value to show detailed collision info
DASHBOARD=${DASHBOARD:-""}                 # Set to any value to enable real-time FTXUI dashboard
JSON_OUTPUT=${JSON_OUTPUT:-""}             # Set to a file path to output JSON results

# Check if NuDB path exists
if [ ! -d "$NUDB_PATH" ]; then
  echo "❌ Error: NuDB database directory does not exist: $NUDB_PATH"
  echo "Please check the path and try again."
  echo ""
  echo "Example usage:"
  echo "  NUDB_PATH=./test-nudb ./scripts/test-keyfile-stats.sh"
  exit 1
fi

# Check if key file exists
KEY_FILE_PATH="${KEY_FILE:-$NUDB_PATH/nudb.key}"
if [ ! -f "$KEY_FILE_PATH" ]; then
  echo "❌ Error: Key file does not exist: $KEY_FILE_PATH"
  echo "Please check the path and try again."
  exit 1
fi

echo "Testing keyfile-stats tool"
echo "  NuDB path: $NUDB_PATH"
echo "  Key file: $KEY_FILE_PATH"

# Prepare command
CMD="./build/src/nudbview/nudb-util/nudb-util keyfile-stats"

# Build flags
FLAGS=""

# Always add nudb-path and log-level
FLAGS="$FLAGS --nudb-path $NUDB_PATH --log-level $LOG_LEVEL"

# Add key-file flag if specified
if [ -n "$KEY_FILE" ]; then
  FLAGS="$FLAGS --key-file $KEY_FILE"
fi

# Build progress flag if specified
if [ -n "$PROGRESS" ]; then
  echo "📊 Progress updates ENABLED"
  FLAGS="$FLAGS --progress"
fi

# Build histogram flag if enabled (default)
if [ -n "$HISTOGRAM" ]; then
  echo "📈 Detailed histogram ENABLED"
  FLAGS="$FLAGS --histogram"
fi

# Build collision-details flag if specified
if [ -n "$COLLISION_DETAILS" ]; then
  echo "🔍 Collision details ENABLED (will show top 20 buckets with collisions)"
  FLAGS="$FLAGS --collision-details"
fi

# Build dashboard flag if specified
if [ -n "$DASHBOARD" ]; then
  echo "🎨 Real-time dashboard ENABLED (press 'q' to quit)"
  echo "   Logs will be redirected to $NUDB_PATH/keyfile-stats.log"
  FLAGS="$FLAGS --dashboard"
fi

# Build JSON output flag if specified
if [ -n "$JSON_OUTPUT" ]; then
  echo "💾 JSON output will be written to: $JSON_OUTPUT"
  FLAGS="$FLAGS --json $JSON_OUTPUT"
fi

echo ""
echo "🚀 Running keyfile-stats..."
echo "Command: $CMD $FLAGS"
echo ""

# Execute the command
$CMD $FLAGS

# Show results
echo ""
echo "✅ Analysis complete!"

if [ -n "$JSON_OUTPUT" ] && [ -f "$JSON_OUTPUT" ]; then
  echo ""
  echo "JSON output saved to: $JSON_OUTPUT"
  echo "Preview:"
  head -20 "$JSON_OUTPUT"
fi

if [ -n "$DASHBOARD" ]; then
  LOG_FILE="$NUDB_PATH/keyfile-stats.log"
  if [ -f "$LOG_FILE" ]; then
    echo ""
    echo "Log file saved to: $LOG_FILE"
    echo "Log preview:"
    tail -30 "$LOG_FILE"
  fi
fi
