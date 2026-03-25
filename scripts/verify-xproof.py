#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["xrpl-py>=4.0.0"]
# ///
"""
Standalone Python verifier for xproof JSON proofs.

Verifies the full cryptographic chain:
  anchor (VL sig + validation sigs + quorum) →
  header → state tree → skip list →
  [flag header → state tree → skip list →]
  target header → tx tree

Usage:
  ./scripts/verify-xproof.py proof.json
  ./scripts/verify-xproof.py proof.json --trusted-key ED2677AB...
"""
import base64
import hashlib
import json
import math
import struct
import sys
from binascii import hexlify, unhexlify
from pathlib import Path

import xrpl.core.binarycodec
import xrpl.core.keypairs
from xrpl.core.binarycodec import decode, encode, encode_for_signing


# ─── SHA512-Half ─────────────────────────────────────────────────────

def sha512half(data: bytes) -> bytes:
    return hashlib.sha512(data).digest()[:32]


# ─── Ledger header hashing ──────────────────────────────────────────

def hash_header(hdr: dict) -> bytes:
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
HEX_CHARS = set("0123456789abcdefABCDEF")


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
    if is_tx:
        tx_bytes = bytes.fromhex(encode(leaf_json["tx"]))
        meta_bytes = bytes.fromhex(encode(leaf_json["meta"]))
        return vl_encode(tx_bytes) + vl_encode(meta_bytes)
    else:
        clean = {k: v for k, v in leaf_json.items() if k not in ("blob", "index")}
        return bytes.fromhex(encode(clean))


def hash_trie(node, is_tx: bool) -> bytes:
    if isinstance(node, str):
        return bytes.fromhex(node)
    if isinstance(node, list):
        key_hex = node[0]
        return hash_leaf(key_hex, serialize_leaf(node[1], is_tx), is_tx)
    if isinstance(node, dict):
        children = [ZERO_HASH] * 16
        for k, v in node.items():
            if len(k) == 1 and k in HEX_CHARS:
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
            if len(k) != 1 or k not in HEX_CHARS:
                continue
            c = count_nodes(v, d + 1)
            s["i"] += c["i"]; s["l"] += c["l"]; s["p"] += c["p"]
            s["d"] = max(s["d"], c["d"])
        return s
    return {"i": 0, "l": 0, "p": 0, "d": d}


# ─── STValidation parsing ───────────────────────────────────────────

def parse_validation(val_hex: str) -> dict | None:
    """Decode an STValidation, return signing data with VAL\\0 prefix."""
    try:
        decoded = decode(val_hex)
    except Exception:
        return None

    signing_key = decoded.get("SigningPubKey", "")
    signature = decoded.get("Signature", "")
    ledger_hash = decoded.get("LedgerHash", "")
    if not signing_key or not signature or not ledger_hash:
        return None

    # encode_for_signing strips sfSignature and prefixes with STX\0.
    # Swap to VAL\0 for validation signing data.
    signing_hex = encode_for_signing(decoded)
    signing_bytes = b"VAL\x00" + unhexlify(signing_hex)[4:]

    return {
        "ledger_hash": ledger_hash.upper(),
        "signing_key": signing_key.upper(),
        "signature": unhexlify(signature),
        "signing_data": signing_bytes,
    }


# ─── Anchor verification ────────────────────────────────────────────

XRPL_VL_KEY = "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734"


def verify_anchor(anchor: dict, trusted_key: str) -> tuple[bool, str]:
    """Verify anchor: VL signature + validation signatures + quorum.
    Returns (ok, message)."""
    unl = anchor.get("unl")
    if not unl:
        return False, "no UNL data"

    # Step 1: publisher key matches trusted key
    proof_key = unl.get("public_key", "")
    if proof_key.lower() != trusted_key.lower():
        return False, f"publisher key mismatch: {proof_key[:16]}... != {trusted_key[:16]}..."
    print(f"    A1: Publisher key {proof_key[:16]}... matches trusted key")

    # Step 2: parse publisher manifest → get signing key
    manifest_hex = unl.get("manifest", "")
    if not manifest_hex:
        return False, "no manifest"
    manifest_bytes = unhexlify(manifest_hex)
    manifest = decode(hexlify(manifest_bytes).decode())
    signing_key = manifest.get("SigningPubKey", "")
    if not signing_key:
        return False, "manifest has no SigningPubKey"
    print(f"    A2: Publisher manifest → signing key {signing_key[:16]}...")

    # Step 3: verify blob signature (raw bytes, signed by publisher signing key)
    blob_hex = unl.get("blob", "")
    sig_hex = unl.get("signature", "")
    if not blob_hex or not sig_hex:
        return False, "no blob or signature"
    blob_bytes = unhexlify(blob_hex)
    sig_bytes = unhexlify(sig_hex)

    if not xrpl.core.keypairs.is_valid_message(blob_bytes, sig_bytes, signing_key):
        return False, "blob signature FAILED"
    print(f"    A3: Blob signature VERIFIED ({len(blob_bytes)} bytes)")

    # Step 4: parse validator manifests from blob → build signing→master key map
    blob_json = json.loads(blob_bytes)
    validators = blob_json.get("validators", [])
    signing_to_master: dict[str, str] = {}  # signing_key_hex → master_key_hex

    for v in validators:
        m_b64 = v.get("manifest", "")
        if not m_b64:
            continue
        m_bytes = base64.b64decode(m_b64)
        m = decode(hexlify(m_bytes).decode())
        sk = m.get("SigningPubKey", "").upper()
        mk = m.get("PublicKey", "").upper()
        if sk:
            signing_to_master[sk] = mk

    unl_size = len(signing_to_master)
    print(f"    A4: {unl_size} validator manifests parsed from UNL blob")

    # Step 5: verify each STValidation
    validations = anchor.get("validations", {})
    anchor_hash = anchor.get("ledger_hash", "").upper()

    verified = 0
    matched_unl = 0
    counted_keys: set[str] = set()

    for _key_hex, val_hex in validations.items():
        parsed = parse_validation(val_hex)
        if not parsed:
            continue

        if parsed["ledger_hash"] != anchor_hash:
            continue

        try:
            if not xrpl.core.keypairs.is_valid_message(
                parsed["signing_data"], parsed["signature"], parsed["signing_key"]
            ):
                continue
        except Exception:
            continue

        verified += 1

        sk = parsed["signing_key"].upper()
        if sk in signing_to_master and sk not in counted_keys:
            counted_keys.add(sk)
            matched_unl += 1

    print(f"    A5: {verified} sigs verified, {matched_unl}/{unl_size} UNL validators")

    # Step 6: quorum check (80%)
    quorum = math.ceil(unl_size * 0.8)
    if matched_unl >= quorum:
        print(f"    A6: QUORUM — {matched_unl}/{unl_size} (>= 80%)")
        return True, f"{matched_unl}/{unl_size} validators"
    else:
        return False, f"quorum not met: {matched_unl}/{unl_size} (need {quorum})"


# ─── Verifier ────────────────────────────────────────────────────────

def verify(proof: dict, trusted_key: str | None = None) -> bool:
    steps = proof["steps"]
    trusted_hash: bytes | None = None
    tx_hash: bytes | None = None
    ac_hash: bytes | None = None
    skip_hashes: list[str] | None = None
    ok = True

    if not trusted_key:
        trusted_key = XRPL_VL_KEY

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

            anchor_ok, anchor_msg = verify_anchor(step, trusted_key)
            if anchor_ok:
                print(f"    [PASS] anchor verified ({anchor_msg})")
            else:
                print(f"    [FAIL] {anchor_msg}")
                ok = False

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
    import argparse
    ap = argparse.ArgumentParser(description="Verify an xproof JSON proof")
    ap.add_argument("proof", help="Path to proof JSON file")
    ap.add_argument("--trusted-key", default=None, help="VL publisher key (hex)")
    args = ap.parse_args()

    path = Path(args.proof)
    if not path.exists():
        print(f"Not found: {path}", file=sys.stderr)
        sys.exit(1)

    proof = json.loads(path.read_text())
    sys.exit(0 if verify(proof, args.trusted_key) else 1)


if __name__ == "__main__":
    main()
