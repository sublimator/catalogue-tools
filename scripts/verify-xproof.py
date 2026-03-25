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

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import struct
import sys
from binascii import hexlify, unhexlify
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import xrpl.core.keypairs
from xrpl.core.binarycodec import decode, encode, encode_for_signing

# ─── Types ───────────────────────────────────────────────────────────

# A 32-byte hash (SHA512-Half result, ledger hash, tree root, etc.)
Hash256 = bytes

# JSON trie node: str (placeholder hash), list (leaf [key, data]), or dict (inner)
TrieNode = str | list[Any] | dict[str, Any]


@dataclass
class ParsedValidation:
    """Decoded STValidation with signing data ready for verification."""

    ledger_hash: str  # uppercase hex
    signing_key: str  # uppercase hex
    signature: bytes  # raw DER signature
    signing_data: bytes  # VAL\0 + serialized fields (sans sfSignature)


@dataclass
class TrieStats:
    """Node counts from a JSON trie."""

    inners: int = 0
    leaves: int = 0
    placeholders: int = 0
    max_depth: int = 0


@dataclass
class AnchorResult:
    """Result of anchor verification."""

    ok: bool
    message: str
    unl_size: int = 0
    matched_unl: int = 0
    verified_sigs: int = 0


@dataclass
class VerifyResult:
    """Overall proof verification result."""

    ok: bool = True
    steps_checked: int = 0
    errors: list[str] = field(default_factory=list)


# ─── Constants ───────────────────────────────────────────────────────

ZERO_HASH: Hash256 = b"\x00" * 32
HEX_BRANCH = set("0123456789abcdefABCDEF")

# Hash prefixes (XRPL protocol)
PREFIX_HEADER = b"LWR\x00"  # ledger header
PREFIX_INNER = b"MIN\x00"  # SHAMap inner node
PREFIX_TX_LEAF = b"SND\x00"  # transaction + metadata leaf
PREFIX_STATE_LEAF = b"MLN\x00"  # account state (SLE) leaf
PREFIX_VALIDATION = b"VAL\x00"  # STValidation signing data
PREFIX_TX_SIGNING = b"STX\x00"  # STObject signing (swapped to VAL for validations)

# Default XRPL mainnet VL publisher key
XRPL_VL_KEY = "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734"


# ─── SHA512-Half ─────────────────────────────────────────────────────


def sha512half(data: bytes) -> Hash256:
    """SHA-512, truncated to first 32 bytes (256 bits)."""
    return hashlib.sha512(data).digest()[:32]


# ─── Ledger header hashing ──────────────────────────────────────────


def hash_header(hdr: dict[str, Any]) -> Hash256:
    """Hash a ledger header: SHA512Half(LWR\\0 + canonical fields)."""
    buf = PREFIX_HEADER
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


def hash_inner(child_hashes: list[Hash256]) -> Hash256:
    """Inner node: SHA512Half(MIN\\0 + 16 × 32-byte child hashes)."""
    assert len(child_hashes) == 16
    return sha512half(PREFIX_INNER + b"".join(child_hashes))


def hash_leaf(key_hex: str, data_bytes: bytes, *, is_tx: bool) -> Hash256:
    """Leaf node: SHA512Half(prefix + serialized_data + 32-byte key)."""
    prefix = PREFIX_TX_LEAF if is_tx else PREFIX_STATE_LEAF
    return sha512half(prefix + data_bytes + bytes.fromhex(key_hex))


def vl_encode(data: bytes) -> bytes:
    """XRPL variable-length prefix encoding."""
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


def serialize_leaf(leaf_json: dict[str, Any], *, is_tx: bool) -> bytes:
    """Serialize a leaf's JSON data to XRPL canonical binary."""
    if is_tx:
        tx_bytes = bytes.fromhex(encode(leaf_json["tx"]))
        meta_bytes = bytes.fromhex(encode(leaf_json["meta"]))
        return vl_encode(tx_bytes) + vl_encode(meta_bytes)
    else:
        clean = {k: v for k, v in leaf_json.items() if k not in ("blob", "index")}
        return bytes.fromhex(encode(clean))


def hash_trie(node: TrieNode, *, is_tx: bool) -> Hash256:
    """Recursively hash a JSON trie node (placeholder / leaf / inner)."""
    if isinstance(node, str):
        return bytes.fromhex(node)
    if isinstance(node, list):
        key_hex: str = node[0]
        return hash_leaf(key_hex, serialize_leaf(node[1], is_tx=is_tx), is_tx=is_tx)
    if isinstance(node, dict):
        children: list[Hash256] = [ZERO_HASH] * 16
        for k, v in node.items():
            if len(k) == 1 and k in HEX_BRANCH:
                children[int(k, 16)] = hash_trie(v, is_tx=is_tx)
        return hash_inner(children)
    raise ValueError(f"unexpected trie node: {type(node)}")


def find_leaf(node: TrieNode) -> list[Any] | None:
    """Find the first leaf (array) in a trie."""
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


def count_nodes(node: TrieNode, depth: int = 0) -> TrieStats:
    """Count trie node types recursively."""
    if isinstance(node, str):
        return TrieStats(placeholders=1, max_depth=depth)
    if isinstance(node, list):
        return TrieStats(leaves=1, max_depth=depth)
    if isinstance(node, dict):
        s = TrieStats(inners=1, max_depth=depth)
        for k, v in node.items():
            if len(k) != 1 or k not in HEX_BRANCH:
                continue
            c = count_nodes(v, depth + 1)
            s.inners += c.inners
            s.leaves += c.leaves
            s.placeholders += c.placeholders
            s.max_depth = max(s.max_depth, c.max_depth)
        return s
    return TrieStats()


# ─── STValidation parsing ───────────────────────────────────────────


def parse_validation(val_hex: str) -> ParsedValidation | None:
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
    signing_bytes = PREFIX_VALIDATION + unhexlify(signing_hex)[4:]

    return ParsedValidation(
        ledger_hash=ledger_hash.upper(),
        signing_key=signing_key.upper(),
        signature=unhexlify(signature),
        signing_data=signing_bytes,
    )


# ─── Anchor verification ────────────────────────────────────────────


def verify_anchor(anchor: dict[str, Any], trusted_key: str) -> AnchorResult:
    """Verify anchor: VL signature + validation signatures + quorum."""
    unl = anchor.get("unl")
    if not unl:
        return AnchorResult(ok=False, message="no UNL data")

    # Step 1: publisher key matches trusted key
    proof_key: str = unl.get("public_key", "")
    if proof_key.lower() != trusted_key.lower():
        return AnchorResult(
            ok=False,
            message=f"publisher key mismatch: {proof_key[:16]}... != {trusted_key[:16]}...",
        )
    print(f"    A1: Publisher key {proof_key[:16]}... matches trusted key")

    # Step 2: parse publisher manifest → get signing key
    manifest_hex: str = unl.get("manifest", "")
    if not manifest_hex:
        return AnchorResult(ok=False, message="no manifest")
    manifest = decode(hexlify(unhexlify(manifest_hex)).decode())
    signing_key: str = manifest.get("SigningPubKey", "")
    if not signing_key:
        return AnchorResult(ok=False, message="manifest has no SigningPubKey")
    print(f"    A2: Publisher manifest → signing key {signing_key[:16]}...")

    # Step 3: verify blob signature (raw bytes, signed by publisher signing key)
    blob_hex: str = unl.get("blob", "")
    sig_hex: str = unl.get("signature", "")
    if not blob_hex or not sig_hex:
        return AnchorResult(ok=False, message="no blob or signature")
    blob_bytes = unhexlify(blob_hex)
    sig_bytes = unhexlify(sig_hex)

    if not xrpl.core.keypairs.is_valid_message(blob_bytes, sig_bytes, signing_key):
        return AnchorResult(ok=False, message="blob signature FAILED")
    print(f"    A3: Blob signature VERIFIED ({len(blob_bytes)} bytes)")

    # Step 4: parse validator manifests from blob → build signing→master key map
    blob_json = json.loads(blob_bytes)
    validators: list[dict[str, Any]] = blob_json.get("validators", [])
    signing_to_master: dict[str, str] = {}

    for v in validators:
        m_b64: str = v.get("manifest", "")
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
    validations: dict[str, str] = anchor.get("validations", {})
    anchor_hash = anchor.get("ledger_hash", "").upper()

    verified = 0
    matched_unl = 0
    counted_keys: set[str] = set()

    for _key_hex, val_hex in validations.items():
        parsed = parse_validation(val_hex)
        if not parsed:
            continue
        if parsed.ledger_hash != anchor_hash:
            continue
        try:
            if not xrpl.core.keypairs.is_valid_message(
                parsed.signing_data, parsed.signature, parsed.signing_key
            ):
                continue
        except Exception:
            continue

        verified += 1
        if (
            parsed.signing_key in signing_to_master
            and parsed.signing_key not in counted_keys
        ):
            counted_keys.add(parsed.signing_key)
            matched_unl += 1

    print(f"    A5: {verified} sigs verified, {matched_unl}/{unl_size} UNL validators")

    # Step 6: quorum check (80%)
    quorum = math.ceil(unl_size * 0.8)
    if matched_unl >= quorum:
        print(f"    A6: QUORUM — {matched_unl}/{unl_size} (>= 80%)")
        return AnchorResult(
            ok=True,
            message=f"{matched_unl}/{unl_size} validators",
            unl_size=unl_size,
            matched_unl=matched_unl,
            verified_sigs=verified,
        )
    else:
        return AnchorResult(
            ok=False,
            message=f"quorum not met: {matched_unl}/{unl_size} (need {quorum})",
            unl_size=unl_size,
            matched_unl=matched_unl,
            verified_sigs=verified,
        )


# ─── Main verifier ──────────────────────────────────────────────────


def verify(proof: dict[str, Any], trusted_key: str | None = None) -> VerifyResult:
    """Verify all steps in a proof chain. Returns structured result."""
    steps: list[dict[str, Any]] = proof["steps"]
    result = VerifyResult()

    trusted_hash: Hash256 | None = None
    tx_hash: Hash256 | None = None
    ac_hash: Hash256 | None = None
    skip_hashes: list[str] | None = None

    if not trusted_key:
        trusted_key = XRPL_VL_KEY

    print(f"\n{'=' * 60}")
    print(f"  PROOF VERIFICATION ({len(steps)} steps)")
    print(f"{'=' * 60}")

    for i, step in enumerate(steps, 1):
        result.steps_checked += 1
        t: str = step["type"]

        if t == "anchor":
            trusted_hash = bytes.fromhex(step["ledger_hash"])
            n_val = len(step.get("validations", {}))
            print(
                f"\n  Step {i}: ANCHOR seq={step.get('ledger_index', '?')} "
                f"hash={step['ledger_hash'][:16]}... ({n_val} validations)"
            )

            ar = verify_anchor(step, trusted_key)
            if ar.ok:
                print(f"    [PASS] anchor verified ({ar.message})")
            else:
                print(f"    [FAIL] {ar.message}")
                result.ok = False
                result.errors.append(f"step {i}: anchor: {ar.message}")

        elif t == "ledger_header":
            hdr: dict[str, Any] = step["header"]
            computed = hash_header(hdr)
            h = computed.hex().upper()

            print(f"\n  Step {i}: HEADER seq={hdr['seq']}")
            print(
                f"    Hash={h[:16]}... tx={hdr['tx_hash'][:16]}... "
                f"ac={hdr['account_hash'][:16]}..."
            )

            if trusted_hash is not None:
                if computed == trusted_hash:
                    print("    [PASS] matches anchor hash")
                else:
                    print(f"    [FAIL] expected {trusted_hash.hex().upper()[:16]}...")
                    result.ok = False
                    result.errors.append(f"step {i}: header hash mismatch")
                trusted_hash = None
            elif skip_hashes is not None:
                if h in skip_hashes:
                    print(f"    [PASS] found in skip list ({len(skip_hashes)} entries)")
                else:
                    print("    [FAIL] not in skip list!")
                    result.ok = False
                    result.errors.append(f"step {i}: not in skip list")
                skip_hashes = None

            tx_hash = bytes.fromhex(hdr["tx_hash"])
            ac_hash = bytes.fromhex(hdr["account_hash"])

        elif t == "map_proof":
            tree: str = step["tree"]
            is_tx = tree == "tx"
            trie: TrieNode | None = step.get("trie")

            print(f"\n  Step {i}: {'TX' if is_tx else 'STATE'} TREE")

            if trie is None:
                print("    [SKIP] no trie")
                continue

            stats = count_nodes(trie)
            print(
                f"    {stats.inners} inners, {stats.placeholders} placeholders, "
                f"{stats.leaves} leaves (depth {stats.max_depth})"
            )

            expected = tx_hash if is_tx else ac_hash
            if expected is None:
                print("    [SKIP] no expected hash")
                continue

            try:
                root = hash_trie(trie, is_tx=is_tx)
                if root == expected:
                    print(
                        f"    [PASS] root {root.hex().upper()[:16]}... "
                        f"matches {'tx' if is_tx else 'account'}_hash"
                    )
                else:
                    print(
                        f"    [FAIL] root {root.hex().upper()[:16]}... "
                        f"!= {expected.hex().upper()[:16]}..."
                    )
                    result.ok = False
                    result.errors.append(f"step {i}: {tree} tree root mismatch")
            except Exception as e:
                print(f"    [ERROR] {e}")
                result.ok = False
                result.errors.append(f"step {i}: {tree} tree error: {e}")

            # Extract skip list from state tree leaf
            if not is_tx:
                leaf = find_leaf(trie)
                if leaf and len(leaf) >= 2 and isinstance(leaf[1], dict):
                    hashes = leaf[1].get("Hashes")
                    if hashes:
                        skip_hashes = [h.upper() for h in hashes]
                        print(f"    Skip list: {len(skip_hashes)} hashes")

            # Extract tx info from tx tree leaf
            if is_tx:
                leaf = find_leaf(trie)
                if leaf and len(leaf) >= 2 and isinstance(leaf[1], dict):
                    tx_data: dict[str, Any] = leaf[1].get("tx", {})
                    print(
                        f"    Leaf: {tx_data.get('TransactionType', '?')} "
                        f"from {tx_data.get('Account', '?')}"
                    )

    print(f"\n{'=' * 60}")
    print(f"  {'PASS — all checks verified' if result.ok else 'FAIL'}")
    if result.errors:
        for err in result.errors:
            print(f"    - {err}")
    print(f"{'=' * 60}\n")
    return result


# ─── CLI ─────────────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(description="Verify an xproof JSON proof")
    ap.add_argument("proof", help="Path to proof JSON file")
    ap.add_argument("--trusted-key", default=None, help="VL publisher key (hex)")
    args = ap.parse_args()

    path = Path(args.proof)
    if not path.exists():
        print(f"Not found: {path}", file=sys.stderr)
        sys.exit(1)

    proof = json.loads(path.read_text())
    r = verify(proof, args.trusted_key)
    sys.exit(0 if r.ok else 1)


if __name__ == "__main__":
    main()
