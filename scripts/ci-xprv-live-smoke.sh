#!/usr/bin/env bash
#
# Live xprv smoke test: boot `xprv serve`, then prove + verify real
# transactions end-to-end against live network peers.
#
# It fetches a *recent* transaction via RPC at run time rather than pinning a
# historical hash, because peers do not retain arbitrarily old ledgers — a
# hard-coded old hash would rot into spurious failures. It deliberately targets
# a recent flag ledger (index % 256 == 0) so the era-direct skip-list path is
# exercised, plus a non-flag target for the common case.
#
# Exits non-zero on the first real failure. Intended for CI on a single
# compiler; also runnable locally:
#
#   XPRV_BIN=build/src/xprv/xprv scripts/ci-xprv-live-smoke.sh
#
set -euo pipefail

XPRV_BIN="${XPRV_BIN:-build/src/xprv/xprv}"
BIND="${XPRV_BIND:-127.0.0.1:18099}"
NETWORK="${XPRV_NETWORK:-0}"
RPC="${XPRV_RPC:-https://s1.ripple.com:51234}"
HEALTH_TIMEOUT="${XPRV_HEALTH_TIMEOUT:-150}"
PROVE_TIMEOUT="${XPRV_PROVE_TIMEOUT:-120}"

BASE="http://${BIND}"
WORK="$(mktemp -d)"
SERVER_PID=""

log()  { echo "[live-smoke] $*"; }
fail() { echo "[live-smoke] FAIL: $*" >&2; exit 1; }

cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

[ -x "$XPRV_BIN" ] || fail "xprv binary not found/executable at: $XPRV_BIN"

# ── Boot server ─────────────────────────────────────────────────────
log "starting: $XPRV_BIN serve --bind $BIND --network $NETWORK --threads 1"
"$XPRV_BIN" serve --bind "$BIND" --network "$NETWORK" --threads 1 \
  > "$WORK/server.log" 2>&1 &
SERVER_PID=$!

# ── Wait for peers + VL (quorum-capable) ────────────────────────────
log "waiting up to ${HEALTH_TIMEOUT}s for peers + VL ..."
ready=""
deadline=$(( $(date +%s) + HEALTH_TIMEOUT ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    tail -n 40 "$WORK/server.log" >&2 || true
    fail "server process exited during startup"
  fi
  h="$(curl -s --max-time 5 "$BASE/health" 2>/dev/null || true)"
  if [ -n "$h" ] && printf '%s' "$h" | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
sys.exit(0 if (d.get("vl_loaded") and d.get("peer_count", 0) > 0) else 1)
'; then
    ready=1
    log "healthy: $(printf '%s' "$h" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("peers=%d vl=%s quorum_seq=%s" % (d.get("peer_count",0), d.get("vl_loaded"), d.get("latest_quorum_seq")))')"
    break
  fi
  sleep 3
done
[ -n "$ready" ] || { tail -n 40 "$WORK/server.log" >&2 || true; fail "server never reached peers+VL within ${HEALTH_TIMEOUT}s"; }

# ── RPC helpers ─────────────────────────────────────────────────────
rpc() {
  curl -s --max-time 20 -X POST "$RPC" -H 'Content-Type: application/json' -d "$1"
}

TIP="$(rpc '{"method":"server_info","params":[{}]}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["info"]["validated_ledger"]["seq"])' || true)"
[ -n "$TIP" ] || fail "could not read validated ledger tip from RPC"
log "validated tip: $TIP"

# first tx hash in a given ledger (empty if none)
tx_in_ledger() {
  rpc "{\"method\":\"ledger\",\"params\":[{\"ledger_index\":$1,\"transactions\":true}]}" \
    | python3 -c 'import sys,json; t=json.load(sys.stdin)["result"]["ledger"].get("transactions",[]); print(t[0] if t else "")' \
    || true
}

# ── Pick targets: a recent flag ledger + a recent non-flag ledger ───
FLAG=$(( ((TIP - 32) / 256) * 256 ))   # most recent flag ledger >= 32 back
FLAG_TX=""
for _ in 1 2 3 4; do
  FLAG_TX="$(tx_in_ledger "$FLAG")"
  [ -n "$FLAG_TX" ] && break
  FLAG=$(( FLAG - 256 ))
done
[ -n "$FLAG_TX" ] || fail "no transactions found in recent flag ledgers"
log "flag target: ledger=$FLAG (%256=$((FLAG % 256))) tx=$FLAG_TX"

NONFLAG=$(( TIP - 12 )); [ $((NONFLAG % 256)) -eq 0 ] && NONFLAG=$((NONFLAG - 1))
NONFLAG_TX="$(tx_in_ledger "$NONFLAG")"
[ -n "$NONFLAG_TX" ] || log "note: no tx in non-flag ledger $NONFLAG, skipping control"

# ── prove + verify helpers ──────────────────────────────────────────
prove() {  # $1=tx $2=format(json|bin) $3=outfile
  local code
  code="$(curl -s --max-time "$PROVE_TIMEOUT" -o "$3" -w '%{http_code}' \
    "$BASE/prove?tx=$1&format=$2" || true)"
  [ "$code" = "200" ] || { tail -n 30 "$WORK/server.log" >&2 || true; fail "prove($2) HTTP ${code:-none} for $1"; }
}

verify() {  # $1=infile $2=content-type -> asserts verified:true
  local out
  out="$(curl -s --max-time 30 -X POST "$BASE/verify" -H "Content-Type: $2" --data-binary "@$1" || true)"
  printf '%s' "$out" | python3 -c 'import sys,json; sys.exit(0 if json.load(sys.stdin).get("verified") is True else 1)' \
    || fail "verify returned: ${out:-<empty>}"
}

# ── Flag target: JSON form, assert era-direct 5-step shape + verifies ─
log "proving flag target (JSON) ..."
prove "$FLAG_TX" json "$WORK/flag.json"
python3 - "$WORK/flag.json" "$FLAG" <<'PY' || fail "flag-target proof is not the expected era-direct shape"
import sys, json
d = json.load(open(sys.argv[1])); flag = int(sys.argv[2])
steps = d["steps"]
types = [s["type"] for s in steps]
hdrs  = [s["header"]["ledger_index"] for s in steps if s["type"] == "ledger_header"]
states = sum(1 for s in steps if s["type"] == "map_proof" and s.get("tree") == "state")
# era-direct: anchor, anchor-header, ONE state proof, target header, tx proof
ok = (len(steps) == 5
      and types == ["anchor","ledger_header","map_proof","ledger_header","map_proof"]
      and states == 1
      and flag in hdrs and flag % 256 == 0
      and not any(h % 256 == 0 and h != flag for h in hdrs))  # no intermediate flag header
print("[live-smoke] flag proof steps=%d headers=%s state_proofs=%d format_version=%d"
      % (len(steps), hdrs, states, d.get("format_version")))
sys.exit(0 if ok else 1)
PY
verify "$WORK/flag.json" application/json
log "flag target JSON: era-direct shape OK + verified"

# ── Flag target: binary form, assert header bytes + verifies ────────
log "proving flag target (binary) ..."
prove "$FLAG_TX" bin "$WORK/flag.bin"
python3 - "$WORK/flag.bin" "$NETWORK" src/xprv/VERSION <<'PY' || fail "binary header bytes wrong"
import sys
d = open(sys.argv[1], "rb").read(); net = int(sys.argv[2])
version = int(open(sys.argv[3]).read().strip())
assert d[:4] == b"XPRV", "bad magic"
assert d[4] == version, "version byte != %d (got %d)" % (version, d[4])
assert int.from_bytes(d[6:10], "big") == net, "network_id not big-endian %s" % net
print("[live-smoke] binary header OK: magic=XPRV version=%d net(BE)=%d size=%dB"
      % (d[4], int.from_bytes(d[6:10], "big"), len(d)))
PY
verify "$WORK/flag.bin" application/octet-stream
log "flag target binary: header OK + verified"

# ── Non-flag control: common short-list path verifies ───────────────
if [ -n "$NONFLAG_TX" ]; then
  log "proving non-flag control (JSON) ..."
  prove "$NONFLAG_TX" json "$WORK/nonflag.json"
  verify "$WORK/nonflag.json" application/json
  log "non-flag control: verified"
fi

log "PASS — live prove+verify succeeded (flag JSON+binary, non-flag control)"
