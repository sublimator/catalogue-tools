#!/usr/bin/env python3
"""
Generate codec test fixtures using rippled's xrpl-codec as ground truth.
Outputs a JSON file with encode/decode pairs for all supported types.
"""

import json
import subprocess
import sys

CODEC = "/Users/nicholasdudfield/projects/ripple-deps/rippled-develop/build/build/Release/xrpl-codec"

def encode(json_val, codec_type="stobject"):
    """Encode a value using xrpl-codec, return hex string.

    For stobject: pass JSON string as argument.
    For primitive types (amount, currency, issue): pipe raw value via stdin.
    Objects (like IOU amounts) are passed as JSON via stdin.
    """
    if codec_type == "stobject":
        cmd = [CODEC, "encode", "--codec-type", codec_type, json.dumps(json_val)]
        result = subprocess.run(cmd, capture_output=True, text=True)
    else:
        # Primitive types: raw value or JSON object via stdin
        if isinstance(json_val, dict):
            stdin_val = json.dumps(json_val)
        else:
            stdin_val = str(json_val)
        cmd = [CODEC, "encode", "--codec-type", codec_type]
        result = subprocess.run(cmd, capture_output=True, text=True, input=stdin_val)

    if result.returncode != 0:
        desc = json.dumps(json_val) if isinstance(json_val, dict) else str(json_val)
        print(f"WARN: encode({codec_type}) failed for {desc[:60]}: {result.stderr.strip()}", file=sys.stderr)
        return None
    return result.stdout.strip()

def decode(hex_str, codec_type="stobject"):
    """Decode a hex string using xrpl-codec, return JSON or raw string."""
    cmd = [CODEC, "decode", "--codec-type", codec_type, hex_str]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"WARN: decode({codec_type}) failed for {hex_str[:40]}: {result.stderr.strip()}", file=sys.stderr)
        return None
    out = result.stdout.strip()
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return out  # raw string for primitives

# Test accounts (genesis + a few well-known)
GENESIS = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"
ALICE = "rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe"
BOB = "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"

fixtures = {"stobject": [], "amount": [], "currency": [], "issue": []}

# ---------------------------------------------------------------------------
# STObject fixtures — various transaction types
# ---------------------------------------------------------------------------
stobject_cases = [
    {
        "name": "Payment_native",
        "json": {
            "TransactionType": "Payment",
            "Flags": 0,
            "Sequence": 1,
            "Fee": "12",
            "Account": GENESIS,
            "Destination": ALICE,
            "Amount": "1000000",
        }
    },
    {
        "name": "Payment_IOU",
        "json": {
            "TransactionType": "Payment",
            "Flags": 0,
            "Sequence": 42,
            "Fee": "15",
            "Account": GENESIS,
            "Destination": ALICE,
            "Amount": {
                "value": "100.50",
                "currency": "USD",
                "issuer": BOB,
            },
        }
    },
    {
        "name": "TrustSet",
        "json": {
            "TransactionType": "TrustSet",
            "Flags": 131072,
            "Sequence": 5,
            "Fee": "12",
            "Account": GENESIS,
            "LimitAmount": {
                "value": "1000000",
                "currency": "USD",
                "issuer": ALICE,
            },
        }
    },
    {
        "name": "OfferCreate",
        "json": {
            "TransactionType": "OfferCreate",
            "Flags": 0,
            "Sequence": 10,
            "Fee": "12",
            "Account": GENESIS,
            "TakerPays": "5000000",
            "TakerGets": {
                "value": "1",
                "currency": "USD",
                "issuer": ALICE,
            },
        }
    },
    {
        "name": "AccountSet_minimal",
        "json": {
            "TransactionType": "AccountSet",
            "Flags": 0,
            "Sequence": 3,
            "Fee": "10",
            "Account": GENESIS,
        }
    },
    {
        "name": "Payment_with_memos",
        "json": {
            "TransactionType": "Payment",
            "Flags": 0,
            "Sequence": 100,
            "Fee": "12",
            "Account": GENESIS,
            "Destination": ALICE,
            "Amount": "500000",
            "Memos": [
                {
                    "Memo": {
                        "MemoType": "746578742F706C61696E",
                        "MemoData": "48656C6C6F",
                    }
                }
            ],
        }
    },
    {
        "name": "Payment_with_destination_tag",
        "json": {
            "TransactionType": "Payment",
            "Flags": 0,
            "Sequence": 7,
            "Fee": "12",
            "Account": GENESIS,
            "Destination": ALICE,
            "DestinationTag": 12345,
            "Amount": "1000000",
        }
    },
    {
        "name": "empty_object",
        "json": {}
    },
    {
        "name": "UInt64_field",
        "json": {
            "TransactionType": "AccountSet",
            "Flags": 0,
            "Sequence": 1,
            "Fee": "10",
            "Account": GENESIS,
        }
    },
]

for case in stobject_cases:
    hex_val = encode(case["json"])
    if hex_val:
        decoded = decode(hex_val)
        fixtures["stobject"].append({
            "name": case["name"],
            "json": case["json"],
            "hex": hex_val,
            "decoded": decoded,
        })

# ---------------------------------------------------------------------------
# Amount fixtures
# ---------------------------------------------------------------------------
amount_cases = [
    {"name": "zero_drops", "json": "0"},
    {"name": "one_drop", "json": "1"},
    {"name": "one_xrp", "json": "1000000"},
    {"name": "large_native", "json": "100000000000"},
    {"name": "negative_native", "json": "-1000000"},
    {"name": "iou_simple", "json": {"value": "1", "currency": "USD", "issuer": ALICE}},
    {"name": "iou_decimal", "json": {"value": "1.5", "currency": "USD", "issuer": ALICE}},
    {"name": "iou_large", "json": {"value": "1000000", "currency": "USD", "issuer": ALICE}},
    {"name": "iou_small", "json": {"value": "0.001", "currency": "EUR", "issuer": BOB}},
    {"name": "iou_negative", "json": {"value": "-50.25", "currency": "USD", "issuer": ALICE}},
    {"name": "iou_zero", "json": {"value": "0", "currency": "USD", "issuer": ALICE}},
]

for case in amount_cases:
    hex_val = encode(case["json"], "amount")
    if hex_val:
        fixtures["amount"].append({
            "name": case["name"],
            "json": case["json"],
            "hex": hex_val,
        })

# ---------------------------------------------------------------------------
# Currency fixtures
# ---------------------------------------------------------------------------
currency_cases = [
    {"name": "XRP", "json": "XRP"},
    {"name": "USD", "json": "USD"},
    {"name": "EUR", "json": "EUR"},
    {"name": "BTC", "json": "BTC"},
]

for case in currency_cases:
    hex_val = encode(case["json"], "currency")
    if hex_val:
        fixtures["currency"].append({
            "name": case["name"],
            "json": case["json"],
            "hex": hex_val,
        })

# ---------------------------------------------------------------------------
# Issue fixtures
# ---------------------------------------------------------------------------
issue_cases = [
    {"name": "XRP_issue", "json": {"currency": "XRP"}},
    {"name": "USD_issue", "json": {"currency": "USD", "issuer": ALICE}},
]

for case in issue_cases:
    hex_val = encode(case["json"], "issue")
    if hex_val:
        fixtures["issue"].append({
            "name": case["name"],
            "json": case["json"],
            "hex": hex_val,
        })

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
output_path = sys.argv[1] if len(sys.argv) > 1 else "tests/x-data/fixture/codec-fixtures.json"
with open(output_path, "w") as f:
    json.dump(fixtures, f, indent=2)

total = sum(len(v) for v in fixtures.values())
print(f"Generated {total} fixtures → {output_path}")
for k, v in fixtures.items():
    print(f"  {k}: {len(v)}")
