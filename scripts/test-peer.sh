#!/bin/bash -u
set -ex

# Build the peer monitor
echo "🔨 Building peer monitor..."
ninja -C build src/lesser-peer/peermon

# Default testnet peers (you can override these)
# Xahau Testnet specific peer
PEER_HOST=${PEER_HOST:-"79.110.60.125"}
PEER_PORT=${PEER_PORT:-21338}

# Alternative testnet options:
# XRPL Testnet: s.altnet.rippletest.net:51235
# XRPL Devnet: s.devnet.rippletest.net:51235
# Xahau Testnet: xahau-test.net:51235
# Specific Xahau node: 79.110.60.125:21338

# Configuration options
THREADS=${THREADS:-2}          # Number of IO threads
TIMEOUT=${TIMEOUT:-30}         # Connection timeout in seconds
LISTEN_MODE=${LISTEN_MODE:-""} # Set to any value to run in listen mode

# Display options
DASHBOARD=${DASHBOARD:-""}           # Enable FTXUI dashboard with log redirection
NO_CLS=${NO_CLS:-""}                 # Don't clear screen between updates
NO_DUMP=${NO_DUMP:-""}               # Don't dump packet contents
NO_STATS=${NO_STATS:-""}             # Don't show statistics
NO_HEX=${NO_HEX:-""}                 # Don't show hex dumps
NO_JSON=${NO_JSON:-""}               # Don't show JSON output
RAW_HEX=${RAW_HEX:-""}               # Show raw hex without formatting
SLOW=${SLOW:-""}                     # Update display at most once every 5 seconds
MANIFESTS_ONLY=${MANIFESTS_ONLY:-""} # Only collect manifests then exit

# Filter options - comma-separated packet types
# Available types: mtMANIFESTS, mtPING, mtTRANSACTION, mtVALIDATION, mtPROPOSE_LEDGER, etc.
SHOW_PACKETS=${SHOW_PACKETS:-""} # Show only these packet types
HIDE_PACKETS=${HIDE_PACKETS:-""} # Hide these packet types

# Transaction query options - comma-separated transaction hashes
QUERY_TX=${QUERY_TX:-""} # Transaction hashes to query from the peer (comma-separated)

# Logging controls (set to 0 to disable specific log partitions)
LOG_TXSET=${LOG_TXSET:-1}    # Transaction set acquisition logging
LOG_WIRE=${LOG_WIRE:-1}      # Wire format parsing logging
LOG_TX_JSON=${LOG_TX_JSON:-1} # Transaction JSON output
LOG_MANIFEST=${LOG_MANIFEST:-1} # Manifest tracking logging
export LOG_TXSET LOG_WIRE LOG_TX_JSON LOG_MANIFEST  # Export for the child process

# Protocol definitions path (should exist in your project)
# Use Xahau definitions for Xahau testnet
PROTOCOL_DEFS=${PROTOCOL_DEFS:-"tests/x-data/fixture/xahau_definitions.json"}

# Build command
CMD="./build/src/lesser-peer/peermon"

# Build command line arguments
ARGS="$PEER_HOST $PEER_PORT"
ARGS="$ARGS --threads $THREADS"
ARGS="$ARGS --timeout $TIMEOUT"
ARGS="$ARGS --protocol-definitions $PROTOCOL_DEFS"

# Add optional flags
[ -n "$LISTEN_MODE" ] && ARGS="$ARGS --listen"
[ -n "$DASHBOARD" ] && ARGS="$ARGS --dashboard"
[ -n "$NO_CLS" ] && ARGS="$ARGS --no-cls"
[ -n "$NO_DUMP" ] && ARGS="$ARGS --no-dump"
[ -n "$NO_STATS" ] && ARGS="$ARGS --no-stats"
[ -n "$NO_HEX" ] && ARGS="$ARGS --no-hex"
[ -n "$NO_JSON" ] && ARGS="$ARGS --no-json"
[ -n "$RAW_HEX" ] && ARGS="$ARGS --raw-hex"
[ -n "$SLOW" ] && ARGS="$ARGS --slow"
[ -n "$MANIFESTS_ONLY" ] && ARGS="$ARGS --manifests-only"

# Add packet filters
[ -n "$SHOW_PACKETS" ] && ARGS="$ARGS --show $SHOW_PACKETS"
[ -n "$HIDE_PACKETS" ] && ARGS="$ARGS --hide $HIDE_PACKETS"

# Add transaction queries (pass as single quoted argument to preserve commas)
[ -n "$QUERY_TX" ] && ARGS="$ARGS --query-tx \"$QUERY_TX\""

echo "🌐 Connecting to peer at $PEER_HOST:$PEER_PORT"
echo "📊 Configuration:"
echo "   - IO Threads: $THREADS"
echo "   - Timeout: $TIMEOUT seconds"
[ -n "$DASHBOARD" ] && echo "   - Dashboard UI: ENABLED (logs to peermon.log)"
[ -n "$SHOW_PACKETS" ] && echo "   - Showing only: $SHOW_PACKETS"
[ -n "$HIDE_PACKETS" ] && echo "   - Hiding: $HIDE_PACKETS"
[ -n "$QUERY_TX" ] && echo "   - Querying transactions: $QUERY_TX"

# Some useful presets as comments:
echo ""
echo "💡 Usage examples:"
echo "   # Monitor all traffic with dashboard UI:"
echo "   DASHBOARD=1 $0"
echo ""
echo "   # Monitor all traffic (console output):"
echo "   $0"
echo ""
echo "   # Focus on transactions and validations:"
echo "   SHOW_PACKETS=mtTRANSACTION,mtVALIDATION $0"
echo ""
echo "   # Track consensus and look for disputed transactions:"
echo "   SHOW_PACKETS=mtPROPOSE_LEDGER,mtVALIDATION,mtSTATUS_CHANGE $0"
echo ""
echo "   # Just watch proposals for transaction IDs (will also show ledger_data responses):"
echo "   SHOW_PACKETS=mtPROPOSE_LEDGER $0"
echo ""
echo "   # Just see transaction flow without hex dumps:"
echo "   SHOW_PACKETS=mtTRANSACTION NO_HEX=1 $0"
echo ""
echo "   # Connect to different Xahau testnet peer:"
echo "   PEER_HOST=xahau-test.net PEER_PORT=51235 $0"
echo ""
echo "   # Run in listen mode (act as server):"
echo "   LISTEN_MODE=1 PEER_HOST=0.0.0.0 $0"
echo ""
echo "   # Query specific transactions:"
echo "   QUERY_TX=\"93A8C30D8E380D8E3D78FBAF129F6A42A6F53F2178F0FCF7B1A6544A77BDC84C\" $0"
echo ""
echo "   # Query multiple transactions (comma-separated):"
echo "   QUERY_TX=\"93A8C30D8E380D8E3D78FBAF129F6A42A6F53F2178F0FCF7B1A6544A77BDC84C,5697CC215A76AC664C3D39948DAE3DF606F4E2F6246E29369509D5F20BC3CB56\" $0"
echo ""
echo "   # Disable transaction set acquisition logging:"
echo "   LOG_TXSET=0 $0"
echo ""
echo "   # Watch proposals without txset/wire logging:"
echo "   LOG_TXSET=0 LOG_WIRE=0 SHOW_PACKETS=mtPROPOSE_LEDGER $0"
echo ""
if [ -n "$DASHBOARD" ]; then
  echo "Press 'q' in dashboard to quit"
else
  echo "Press Ctrl+C to stop monitoring"
fi
echo "========================================="
echo ""

# Run the peer monitor (use eval to handle quoted arguments properly)
eval "$CMD $ARGS"
