#!/bin/bash
# Attach lldb to a running peermon process
# Usage: ./scripts/peer-remote-lldb.sh [pid]

set -e

if [ -n "$1" ]; then
  PID="$1"
else
  # Find peermon process (exclude grep itself)
  PIDS=$(pgrep -f "peermon.*--dashboard" 2>/dev/null || pgrep -f "peermon" 2>/dev/null || true)

  if [ -z "$PIDS" ]; then
    echo "No peermon process found"
    echo ""
    echo "Start one with: NETWORK=xahau-local DASHBOARD=1 ./scripts/test-peer.sh"
    exit 1
  fi

  # Count processes
  NUM_PROCS=$(echo "$PIDS" | wc -l | tr -d ' ')

  if [ "$NUM_PROCS" -gt 1 ]; then
    echo "Multiple peermon processes found:"
    echo ""
    ps -p $(echo "$PIDS" | tr '\n' ',') -o pid,etime,command | head -20
    echo ""
    echo "Specify PID: ./scripts/peer-remote-lldb.sh <pid>"
    exit 1
  fi

  PID="$PIDS"
fi

echo "Attaching lldb to peermon (PID: $PID)..."
echo ""
echo "Commands:"
echo "  bt        - backtrace (run after crash/stop)"
echo "  c         - continue execution"
echo "  ctrl+c    - pause execution"
echo "  q         - quit lldb (detaches from process)"
echo ""

lldb -p "$PID"
