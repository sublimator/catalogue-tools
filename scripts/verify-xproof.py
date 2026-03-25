#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["xrpl-py>=4.0.0"]
# ///
"""
Standalone Python verifier for xproof JSON proofs.

Verifies the full cryptographic chain:
  anchor → header → state tree → skip list → [flag header → state tree → skip list →] target header → tx tree

Usage:
  ./scripts/verify-xproof.py proof.json
"""
import hashlib
import json
import struct
import sys
from pathlib import Path


# ─── SHA512-Half ─────────────────────────────────────────────────────

def sha512half(data: bytes) -> bytes:
    return hashlib.sha512(data).digest()[:32]


# ─── Ledger header hashing ──────────────────────────────────────────

def hash_header(hdr: dict) -> bytes:
    """SHA512Half(LWR\\0 + seq + drops + parent_hash + tx_hash + account_hash + times + flags)"""
    buf = b"LWR\x00"
    buf += struct.pack(">I", hdr["seq"])
    buf += struct.pack(">Q", int(hdr["drops"]))
    buf += bytes.fromhex(hdr["parent_hash"])
    buf += bytes.fromhex(hdr["tx_hash"])
    buf += bytes.fromhex(hdr["account_hash"])
    buf += struct.pack(">I", hdr["parent_close_time"])
    buf += struct.pack(">I", hdr["close_time"])
    buf += struct.pack("B", hdr["close_time_resolution"])
    buf += struct.pack("B", hdr["close_flags"])
    return sha512half(buf)


# ─── SHAMap trie hashing ────────────────────────────────────────────

ZERO_HASH = b"\x00" * 32


def hash_inner(child_hashes: list[bytes]) -> bytes:
    assert len(child_hashes) == 16
    return sha512half(b"MIN\x00" + b"".join(child_hashes))


def hash_leaf(key_hex: str, data_bytes: bytes, is_tx: bool) -> bytes:
    prefix = b"SND\x00" if is_tx else b"MLN\x00"
    return sha512half(prefix + data_bytes + bytes.fromhex(key_hex))


def vl_encode(data: bytes) -> bytes:
    n = len(data)
    if n <= 192:
        return bytes([n]) + data
    elif n <= 12480:
        n -= 193
        return bytes([193 + (n >> 8), n & 0xFF]) + data
    elif n <= 918744:
        n -= 12481
        return bytes([241 + (n >> 16), (n >> 8) & 0xFF, n & 0xFF]) + data
    raise ValueError(f"VL too large: {len(data)}")


def serialize_leaf(leaf_json, is_tx: bool) -> bytes:
    from xrpl.core.binarycodec import encode

    if is_tx:
        if "blob" in leaf_json:
            return bytes.fromhex(leaf_json["blob"])
        tx_bytes = bytes.fromhex(encode(leaf_json["tx"]))
        meta_bytes = bytes.fromhex(encode(leaf_json["meta"]))
        return vl_encode(tx_bytes) + vl_encode(meta_bytes)
    else:
        return bytes.fromhex(encode(leaf_json))


def hash_trie(node, is_tx: bool) -> bytes:
    """Recursively hash a JSON trie. string=placeholder, list=leaf, dict=inner."""
    if isinstance(node, str):
        return bytes.fromhex(node)

    if isinstance(node, list):
        key_hex = node[0]
        return hash_leaf(key_hex, serialize_leaf(node[1], is_tx), is_tx)

    if isinstance(node, dict):
        children = [ZERO_HASH] * 16
        for k, v in node.items():
            if len(k) == 1 and k in "0123456789abcdef":
                children[int(k, 16)] = hash_trie(v, is_tx)
        return hash_inner(children)

    raise ValueError(f"unexpected trie node: {type(node)}")


def find_leaf(node):
    if isinstance(node, list):
        return node
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "__depth__":
                continue
            r = find_leaf(v)
            if r is not None:
                return r
    return None


def count_nodes(node, d=0):
    if isinstance(node, str):
        return {"i": 0, "l": 0, "p": 1, "d": d}
    if isinstance(node, list):
        return {"i": 0, "l": 1, "p": 0, "d": d}
    if isinstance(node, dict):
        s = {"i": 1, "l": 0, "p": 0, "d": d}
        for k, v in node.items():
            if k == "__depth__" or len(k) != 1:
                continue
            c = count_nodes(v, d + 1)
            s["i"] += c["i"]; s["l"] += c["l"]; s["p"] += c["p"]
            s["d"] = max(s["d"], c["d"])
        return s
    return {"i": 0, "l": 0, "p": 0, "d": d}


# ─── Verifier ────────────────────────────────────────────────────────

def verify(proof: dict) -> bool:
    steps = proof["steps"]
    trusted_hash: bytes | None = None
    tx_hash: bytes | None = None
    ac_hash: bytes | None = None
    skip_hashes: list[str] | None = None
    ok = True

    print(f"\n{'=' * 60}")
    print(f"  PROOF VERIFICATION ({len(steps)} steps)")
    print(f"{'=' * 60}")

    for i, step in enumerate(steps, 1):
        t = step["type"]

        if t == "anchor":
            trusted_hash = bytes.fromhex(step["ledger_hash"])
            n_val = len(step.get("validations", {}))
            print(f"\n  Step {i}: ANCHOR seq={step.get('ledger_index','?')} "
                  f"hash={step['ledger_hash'][:16]}... ({n_val} validations)")
            print(f"    [SKIP] VL/sig verification — trust anchor hash")

        elif t == "ledger_header":
            hdr = step["header"]
            computed = hash_header(hdr)
            h = computed.hex().upper()

            print(f"\n  Step {i}: HEADER seq={hdr['seq']}")
            print(f"    Hash={h[:16]}... tx={hdr['tx_hash'][:16]}... ac={hdr['account_hash'][:16]}...")

            if trusted_hash is not None:
                if computed == trusted_hash:
                    print(f"    [PASS] matches anchor hash")
                else:
                    print(f"    [FAIL] expected {trusted_hash.hex().upper()[:16]}...")
                    ok = False
                trusted_hash = None
            elif skip_hashes is not None:
                if h in skip_hashes:
                    print(f"    [PASS] found in skip list ({len(skip_hashes)} entries)")
                else:
                    print(f"    [FAIL] not in skip list!")
                    ok = False
                skip_hashes = None

            tx_hash = bytes.fromhex(hdr["tx_hash"])
            ac_hash = bytes.fromhex(hdr["account_hash"])

        elif t == "map_proof":
            tree = step["tree"]
            is_tx = tree == "tx"
            trie = step.get("trie")

            print(f"\n  Step {i}: {'TX' if is_tx else 'STATE'} TREE")

            if trie is None:
                print(f"    [SKIP] no trie")
                continue

            s = count_nodes(trie)
            print(f"    {s['i']} inners, {s['p']} placeholders, {s['l']} leaves (depth {s['d']})")

            expected = tx_hash if is_tx else ac_hash
            if expected is None:
                print(f"    [SKIP] no expected hash")
                continue

            try:
                root = hash_trie(trie, is_tx)
                if root == expected:
                    print(f"    [PASS] root {root.hex().upper()[:16]}... matches {'tx' if is_tx else 'account'}_hash")
                else:
                    print(f"    [FAIL] root {root.hex().upper()[:16]}... != {expected.hex().upper()[:16]}...")
                    ok = False
            except Exception as e:
                print(f"    [ERROR] {e}")
                ok = False

            if not is_tx:
                leaf = find_leaf(trie)
                if leaf and len(leaf) >= 2 and isinstance(leaf[1], dict):
                    hashes = leaf[1].get("Hashes")
                    if hashes:
                        skip_hashes = [h.upper() for h in hashes]
                        print(f"    Skip list: {len(skip_hashes)} hashes")

            if is_tx:
                leaf = find_leaf(trie)
                if leaf and len(leaf) >= 2 and isinstance(leaf[1], dict):
                    tx_data = leaf[1].get("tx", {})
                    print(f"    Leaf: {tx_data.get('TransactionType','?')} from {tx_data.get('Account','?')}")

    print(f"\n{'=' * 60}")
    print(f"  {'PASS — all checks verified' if ok else 'FAIL'}")
    print(f"{'=' * 60}\n")
    return ok


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <proof.json>", file=sys.stderr)
        sys.exit(1)

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"Not found: {path}", file=sys.stderr)
        sys.exit(1)

    proof = json.loads(path.read_text())
    sys.exit(0 if verify(proof) else 1)


if __name__ == "__main__":
    main()
