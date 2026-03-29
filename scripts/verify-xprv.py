#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["xrpl-py>=4.0.0"]
# ///
"""
Standalone Python verifier for xprv proofs (JSON and binary).

Verifies the full cryptographic chain:
  anchor (VL sig + validation sigs + quorum) →
  header → state tree → skip list →
  [flag header → state tree → skip list →]
  target header → tx tree

Supports both formats:
  - JSON (.json, .xprv.json): {"network_id": N, "steps": [...]}
  - Binary (.bin, .xprv.bin): XPRV header + TLV body (optionally zlib)

Usage:
  ./scripts/verify-xprv.py proof.json
  ./scripts/verify-xprv.py proof.xprv.bin
  ./scripts/verify-xprv.py proof.json --trusted-key ED2677AB...
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
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, TypedDict

import zlib

import xrpl.core.keypairs
from xrpl.core.binarycodec import decode, encode, encode_for_signing

# Monkey-patch xrpl-py's broken _calculate_precision (XRPLF/xrpl-py#923).
# It expands e-notation to full decimal form then counts ALL digits instead
# of significant figures, rejecting valid values like 9999999999999999e79.
import xrpl.core.binarycodec.types.amount as _amt
from decimal import Decimal as _Decimal

_amt._calculate_precision = (
    lambda v: len(_Decimal(v).as_tuple().digits) if not _Decimal(v).is_zero() else 0
)

# ─── JSON proof shape ────────────────────────────────────────────────
#
# A proof JSON has the structure:
#   { "network_id": 0, "steps": [AnchorStep, HeaderStep, MapProofStep, ...] }
#
# Steps are discriminated by "type" field.


class UNLData(TypedDict, total=False):
    """Validator list data embedded in the anchor."""

    public_key: str  # publisher master key (hex)
    manifest: str  # publisher manifest (hex)
    blob: str  # signed JSON blob containing validator list (hex)
    signature: str  # blob signature (hex)


# A 32-byte hash (SHA512-Half result, ledger hash, tree root, etc.)
Hash256 = bytes

# JSON trie: placeholder (hex hash), leaf [key_hex, data], or inner {branch: child}
TriePlaceholder = str
TrieLeaf = list[Any]  # [key_hex: str, leaf_data: dict]
TrieInner = dict[str, Any]  # {"0": child, ..., "F": child, "__depth__": int}
TrieNode = TriePlaceholder | TrieLeaf | TrieInner


class AnchorStep(TypedDict, total=False):
    """Anchor step: trusted starting point with UNL + validations."""

    type: Literal["anchor"]
    ledger_hash: str  # trusted ledger hash (hex)
    ledger_index: int
    unl: UNLData
    validations: dict[str, str]  # signing_key_hex → raw STValidation hex


class LedgerHeader(TypedDict):
    """Ledger header fields (nested under HeaderStep.header)."""

    seq: int
    drops: str  # string representation of uint64
    parent_hash: str  # 64-char hex
    tx_hash: str  # 64-char hex
    account_hash: str  # 64-char hex
    parent_close_time: int
    close_time: int
    close_time_resolution: int
    close_flags: int


class HeaderStep(TypedDict):
    """Ledger header step: provides tx_hash and account_hash."""

    type: Literal["ledger_header"]
    header: LedgerHeader


class MapProofStep(TypedDict, total=False):
    """SHAMap trie proof step: state or tx tree."""

    type: Literal["map_proof"]
    tree: Literal["state", "tx"]
    trie: TrieNode


class Proof(TypedDict):
    """Top-level proof document."""

    network_id: int
    steps: list[AnchorStep | HeaderStep | MapProofStep]


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
    warnings: list[str] = field(default_factory=list)


# ─── Constants ───────────────────────────────────────────────────────

ZERO_HASH: Hash256 = b"\x00" * 32
HEX_BRANCH = set("0123456789abcdefABCDEF")

# Hash prefixes (XRPL protocol)
PREFIX_HEADER = b"LWR\x00"  # ledger header
PREFIX_INNER = b"MIN\x00"  # SHAMap inner node
PREFIX_TX_LEAF = b"SND\x00"  # transaction + metadata leaf
PREFIX_STATE_LEAF = b"MLN\x00"  # account state (SLE) leaf
PREFIX_VALIDATION = b"VAL\x00"  # STValidation signing data
PREFIX_MANIFEST = b"MAN\x00"  # manifest signing data
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
        clean = {k: v for k, v in leaf_json.items() if k not in ("index",)}
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


def add_capability(store: dict[str, list[str]], key_hex: str, source: str) -> None:
    """Record that a hash/root is authenticated by some earlier step."""
    if source not in store[key_hex]:
        store[key_hex].append(source)


def first_source(store: dict[str, list[str]], key_hex: str) -> str | None:
    """Return the first supporting source for an authenticated value."""
    sources = store.get(key_hex)
    return sources[0] if sources else None


def collect_json_leaves(node: Any) -> list[tuple[str, dict[str, Any]]]:
    """Collect all leaves from a JSON trie."""
    leaves: list[tuple[str, dict[str, Any]]] = []

    def walk(cur: Any) -> None:
        if isinstance(cur, list) and len(cur) >= 2:
            key_hex = cur[0]
            leaf_data = cur[1]
            if isinstance(key_hex, str) and isinstance(leaf_data, dict):
                leaves.append((key_hex, leaf_data))
            return
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k == "__depth__":
                    continue
                if len(k) == 1 and k in HEX_BRANCH:
                    walk(v)

    walk(node)
    return leaves


def decode_binary_leaf(leaf_bytes: bytes, *, is_tx: bool) -> dict[str, Any] | None:
    """Decode a binary trie leaf back to JSON form."""
    try:
        if is_tx:
            pos = 0
            tx_len, pos = _vl_decode(leaf_bytes, pos)
            tx_hex = leaf_bytes[pos : pos + tx_len].hex()
            pos += tx_len
            meta_len, pos = _vl_decode(leaf_bytes, pos)
            meta_hex = leaf_bytes[pos : pos + meta_len].hex()
            return {"tx": decode(tx_hex), "meta": decode(meta_hex)}
        return decode(leaf_bytes.hex())
    except Exception:
        return None


def collect_binary_leaves(
    data: bytes, *, is_tx: bool
) -> list[tuple[str, dict[str, Any]]]:
    """Collect all decodable leaves from a binary trie."""
    leaves: list[tuple[str, dict[str, Any]]] = []

    def walk(pos: int) -> int:
        header = struct.unpack_from("<I", data, pos)[0]
        pos += 4

        for branch in range(16):
            branch_type = (header >> (branch * 2)) & 0x03
            if branch_type == BR_EMPTY:
                continue
            if branch_type == BR_LEAF:
                key_bytes = data[pos : pos + 32]
                pos += 32
                data_len, pos = leb128_read(data, pos)
                leaf_bytes = data[pos : pos + data_len]
                pos += data_len
                leaf_data = decode_binary_leaf(leaf_bytes, is_tx=is_tx)
                if leaf_data is not None:
                    leaves.append((key_bytes.hex().upper(), leaf_data))
                continue
            if branch_type == BR_INNER:
                pos = walk(pos)
                continue
            if branch_type == BR_HASH:
                pos += 32

        return pos

    walk(0)
    return leaves


def extract_state_child_hashes(
    leaves: list[tuple[str, dict[str, Any]]],
) -> list[str]:
    """Extract ledger hashes from authenticated state leaves (skip lists)."""
    hashes: list[str] = []
    for _key_hex, leaf_data in leaves:
        raw_hashes = leaf_data.get("Hashes")
        if not isinstance(raw_hashes, list):
            continue
        for item in raw_hashes:
            if isinstance(item, str):
                hashes.append(item.upper())
    return hashes


def describe_tx_leaves(leaves: list[tuple[str, dict[str, Any]]]) -> str:
    """Produce a short human-readable summary of tx leaves."""
    if not leaves:
        return "0 tx leaves"
    first = leaves[0][1]
    tx = first.get("tx", {}) if isinstance(first, dict) else {}
    tx_type = tx.get("TransactionType", "?")
    account = tx.get("Account", "?")
    if len(leaves) == 1:
        return f"1 tx leaf ({tx_type} from {account})"
    return f"{len(leaves)} tx leaves (first: {tx_type} from {account})"


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


def parse_manifest(manifest_bytes: bytes) -> dict[str, Any]:
    """Parse a raw validator/publisher manifest STObject."""
    return decode(hexlify(manifest_bytes).decode())


def manifest_signing_data(manifest_dict: dict[str, Any]) -> bytes:
    """Build MAN\\0 + manifest fields with both signature fields stripped."""
    signing_hex = encode_for_signing(manifest_dict)
    signing_bytes = unhexlify(signing_hex)
    return PREFIX_MANIFEST + signing_bytes[4:]


def verify_manifest(manifest_bytes: bytes) -> tuple[bool, str, dict[str, Any]]:
    """Verify a manifest's signing-key and master-key signatures."""
    parsed = parse_manifest(manifest_bytes)

    master_key = parsed.get("PublicKey", "").upper()
    signing_key = parsed.get("SigningPubKey", "").upper()
    signature_hex = parsed.get("Signature", "")
    master_sig_hex = parsed.get("MasterSignature", "")
    sequence = parsed.get("Sequence")

    if not master_key:
        return False, "manifest missing PublicKey", parsed
    if not master_sig_hex:
        return False, "manifest missing MasterSignature", parsed

    is_revocation = sequence == 0xFFFFFFFF
    if not is_revocation and not signing_key:
        return False, "manifest missing SigningPubKey", parsed

    signing_data = manifest_signing_data(parsed)

    if signature_hex:
        sig_bytes = unhexlify(signature_hex)
        if not signing_key:
            return False, "manifest has Signature but no SigningPubKey", parsed
        if not xrpl.core.keypairs.is_valid_message(signing_data, sig_bytes, signing_key):
            return False, "signing key signature invalid", parsed

    master_sig_bytes = unhexlify(master_sig_hex)
    if not xrpl.core.keypairs.is_valid_message(
        signing_data, master_sig_bytes, master_key
    ):
        return False, "master key signature invalid", parsed

    return True, "ok", parsed


# ─── Anchor verification ────────────────────────────────────────────


def verify_anchor(anchor: dict[str, Any], trusted_key: str) -> AnchorResult:
    """Verify anchor: manifest chain + VL blob + validations + quorum."""
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

    # Step 2: verify publisher manifest → get signing key
    manifest_hex: str = unl.get("manifest", "")
    if not manifest_hex:
        return AnchorResult(ok=False, message="no manifest")
    manifest_bytes = unhexlify(manifest_hex)
    ok, err, manifest = verify_manifest(manifest_bytes)
    if not ok:
        return AnchorResult(ok=False, message=f"publisher manifest: {err}")
    signing_key: str = manifest.get("SigningPubKey", "").upper()
    if not signing_key:
        return AnchorResult(ok=False, message="manifest has no SigningPubKey")
    print(f"    A2: Publisher manifest VERIFIED, signing key {signing_key[:16]}...")

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

    # Step 4: verify validator manifests from blob → build signing→master key map
    blob_json = json.loads(blob_bytes)
    validators: list[dict[str, Any]] = blob_json.get("validators", [])
    signing_to_master: dict[str, str] = {}
    manifest_warnings = 0

    for v in validators:
        m_b64: str = v.get("manifest", "")
        if not m_b64:
            continue
        m_bytes = base64.b64decode(m_b64)
        ok, err, m = verify_manifest(m_bytes)
        if not ok:
            manifest_warnings += 1
            continue
        sk = m.get("SigningPubKey", "").upper()
        mk = m.get("PublicKey", "").upper()
        if sk:
            signing_to_master[sk] = mk

    unl_size = len(signing_to_master)
    if manifest_warnings:
        print(
            f"    A4: {unl_size} validator manifests verified, {manifest_warnings} ignored"
        )
    else:
        print(f"    A4: {unl_size} validator manifests verified from UNL blob")

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


def verify(
    proof: dict[str, Any] | list[Any], trusted_key: str | None = None
) -> VerifyResult:
    """Verify a proof DAG serialized as a parent-first step list."""
    if isinstance(proof, list):
        steps: list[dict[str, Any]] = proof
    else:
        steps = proof.get("steps", [])
    result = VerifyResult()

    if not steps:
        result.ok = False
        result.errors.append("proof has no steps")
        return result

    if not trusted_key:
        trusted_key = XRPL_VL_KEY

    anchor_count = sum(1 for step in steps if step.get("type") == "anchor")
    if anchor_count != 1:
        result.ok = False
        result.errors.append(f"proof must contain exactly one anchor, found {anchor_count}")

    trusted_ledger_hashes: dict[str, list[str]] = defaultdict(list)
    trusted_tx_roots: dict[str, list[str]] = defaultdict(list)
    trusted_state_roots: dict[str, list[str]] = defaultdict(list)
    seen_header_hashes: set[str] = set()
    seen_map_roots: set[tuple[str, str]] = set()
    skip_source_last_seq: dict[str, int] = {}
    anchor_seen = False

    print(f"\n{'=' * 60}")
    print(f"  PROOF VERIFICATION ({len(steps)} steps)")
    print(f"{'=' * 60}")

    for i, step in enumerate(steps, 1):
        result.steps_checked += 1
        step_type = step.get("type")

        if step_type == "anchor":
            if i != 1:
                msg = "anchor must be the first serialized step"
                print(f"\n  Step {i}: ANCHOR\n    [FAIL] {msg}")
                result.ok = False
                result.errors.append(f"step {i}: {msg}")
                continue
            if anchor_seen:
                msg = "duplicate anchor"
                print(f"\n  Step {i}: ANCHOR\n    [FAIL] {msg}")
                result.ok = False
                result.errors.append(f"step {i}: {msg}")
                continue

            anchor_seen = True
            n_val = len(step.get("validations", {}))
            print(
                f"\n  Step {i}: ANCHOR seq={step.get('ledger_index', '?')} "
                f"hash={step.get('ledger_hash', '')[:16]}... ({n_val} validations)"
            )

            ar = verify_anchor(step, trusted_key)
            if ar.ok:
                ledger_hash = step["ledger_hash"].upper()
                add_capability(trusted_ledger_hashes, ledger_hash, "anchor")
                print(f"    [PASS] anchor verified ({ar.message})")
            else:
                print(f"    [FAIL] {ar.message}")
                result.ok = False
                result.errors.append(f"step {i}: anchor: {ar.message}")

        elif step_type == "ledger_header":
            hdr: dict[str, Any] = step.get("header", {})
            computed = hash_header(hdr)
            computed_hex = computed.hex().upper()

            print(f"\n  Step {i}: HEADER seq={hdr['seq']}")
            print(
                f"    Hash={computed_hex[:16]}... tx={hdr['tx_hash'][:16]}... "
                f"ac={hdr['account_hash'][:16]}..."
            )

            source = first_source(trusted_ledger_hashes, computed_hex)
            if source is None:
                msg = "header hash is not supported by any earlier authenticated ancestor"
                print(f"    [FAIL] {msg}")
                result.ok = False
                result.errors.append(f"step {i}: {msg}")
                continue

            if source == "anchor":
                print("    [PASS] matches anchor hash")
            elif source.startswith("skip list from step "):
                print(f"    [PASS] found in {source}")
            else:
                print(f"    [PASS] supported by {source}")

            if computed_hex in seen_header_hashes:
                warn = f"step {i}: duplicate ledger_header {computed_hex[:16]}... is non-canonical"
                print(f"    [WARN] {warn}")
                result.warnings.append(warn)
            seen_header_hashes.add(computed_hex)

            if source.startswith("skip list from step "):
                prev_seq = skip_source_last_seq.get(source)
                seq = int(hdr["seq"])
                if prev_seq is not None and seq > prev_seq:
                    warn = (
                        f"step {i}: headers from {source} are not in descending seq order "
                        f"({seq} after {prev_seq})"
                    )
                    print(f"    [WARN] {warn}")
                    result.warnings.append(warn)
                skip_source_last_seq[source] = seq

            parent_hash = hdr["parent_hash"].upper()
            tx_root = hdr["tx_hash"].upper()
            state_root = hdr["account_hash"].upper()

            add_capability(
                trusted_ledger_hashes,
                parent_hash,
                f"parent hash of header seq={hdr['seq']}",
            )
            add_capability(trusted_tx_roots, tx_root, f"header seq={hdr['seq']}")
            add_capability(
                trusted_state_roots, state_root, f"header seq={hdr['seq']}"
            )

        elif step_type == "map_proof":
            tree: str = step["tree"]
            is_tx = tree == "tx"
            trie: TrieNode | None = step.get("trie")
            binary_root_hex: str | None = step.get("__binary_root__")
            binary_payload: bytes = step.get("__binary_payload__", b"")

            print(f"\n  Step {i}: {'TX' if is_tx else 'STATE'} TREE")

            try:
                if binary_root_hex:
                    root_hex = binary_root_hex.upper()
                    leaves = collect_binary_leaves(binary_payload, is_tx=is_tx)
                    print(f"    Binary trie, root={root_hex[:16]}...")
                else:
                    if trie is None:
                        raise ValueError("no trie payload")
                    stats = count_nodes(trie)
                    print(
                        f"    {stats.inners} inners, {stats.placeholders} placeholders, "
                        f"{stats.leaves} leaves (depth {stats.max_depth})"
                    )
                    root_hex = hash_trie(trie, is_tx=is_tx).hex().upper()
                    leaves = collect_json_leaves(trie)
            except Exception as exc:
                print(f"    [ERROR] {exc}")
                result.ok = False
                result.errors.append(f"step {i}: {tree} tree error: {exc}")
                continue

            capability_store = trusted_tx_roots if is_tx else trusted_state_roots
            root_source = first_source(capability_store, root_hex)
            if root_source is None:
                msg = f"{tree} root {root_hex[:16]}... is not supported by any earlier header"
                print(f"    [FAIL] {msg}")
                result.ok = False
                result.errors.append(f"step {i}: {msg}")
                continue

            if not leaves:
                msg = f"{tree} map proof contains no decodable leaves"
                print(f"    [FAIL] {msg}")
                result.ok = False
                result.errors.append(f"step {i}: {msg}")
                continue

            print(f"    [PASS] root {root_hex[:16]}... supported by {root_source}")
            print(f"    leaves: {len(leaves)}")

            root_key = (tree, root_hex)
            if root_key in seen_map_roots:
                warn = (
                    f"step {i}: repeated {tree} map_proof for root {root_hex[:16]}... "
                    "is non-canonical; merge leaves into one proof"
                )
                print(f"    [WARN] {warn}")
                result.warnings.append(warn)
            seen_map_roots.add(root_key)

            if is_tx:
                print(f"    {describe_tx_leaves(leaves)}")
            else:
                child_hashes = extract_state_child_hashes(leaves)
                if child_hashes:
                    for child_hash in child_hashes:
                        add_capability(
                            trusted_ledger_hashes,
                            child_hash,
                            f"skip list from step {i}",
                        )
                    print(
                        f"    capability: {len(child_hashes)} child ledger hashes "
                        f"from authenticated state leaf/leaves"
                    )
                else:
                    print("    state proof contains no Hashes arrays")

        else:
            msg = f"unknown step type: {step_type!r}"
            print(f"\n  Step {i}: UNKNOWN\n    [FAIL] {msg}")
            result.ok = False
            result.errors.append(f"step {i}: {msg}")

    print(f"\n{'=' * 60}")
    print(f"  {'PASS — all checks verified' if result.ok else 'FAIL'}")
    if result.errors:
        for err in result.errors:
            print(f"    - {err}")
    if result.warnings:
        print("  WARNINGS")
        for warn in result.warnings:
            print(f"    - {warn}")
    print(f"{'=' * 60}\n")
    return result


# ─── Binary format (XPRV) ────────────────────────────────────────────
#
# File: XPRV(4) + version(1) + flags(1) [+ network_id(4 LE if v2)]
# Body: TLV records (type(1) + length(LEB128) + payload)
# TLV types: 0x01=anchor, 0x02=header, 0x03=tx_map, 0x04=state_map, 0x05=unl

MAGIC = b"XPRV"
FORMAT_VERSION = int((Path(__file__).parent.parent / "src/xprv/VERSION").read_text().strip())
FLAG_ZLIB = 0x01

# TLV types
TLV_ANCHOR = 0x01
TLV_HEADER = 0x02
TLV_MAP_TX = 0x03
TLV_MAP_STATE = 0x04
TLV_UNL = 0x05

# Binary trie branch types (2 bits each in a uint32 LE header)
BR_EMPTY = 0
BR_LEAF = 1
BR_INNER = 2
BR_HASH = 3

# Blob JSON key permutations (for exact byte reconstitution)
BLOB_KEYS = ["sequence", "expiration", "validators"]
VAL_KEYS = ["validation_public_key", "manifest"]
PERMS_3 = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]]
PERMS_2 = [[0, 1], [1, 0]]


def leb128_read(data: bytes, pos: int) -> tuple[int, int]:
    """Read a LEB128 varint. Returns (value, new_pos)."""
    value = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        value |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return value, pos
        shift += 7
    raise ValueError("LEB128: unexpected end")


def read_vl(data: bytes, pos: int) -> tuple[bytes, int]:
    """Read a VL-prefixed blob (LEB128 length + bytes)."""
    length, pos = leb128_read(data, pos)
    return data[pos : pos + length], pos + length


def is_binary_proof(data: bytes) -> bool:
    """Check if data starts with XPRV magic."""
    return len(data) >= 4 and data[:4] == MAGIC


def parse_binary_header(data: bytes) -> tuple[int, int, int, int]:
    """Parse XPRV file header. Returns (version, flags, network_id, body_offset)."""
    if len(data) < 6:
        raise ValueError("binary proof: too short")
    if data[:4] != MAGIC:
        raise ValueError("binary proof: bad magic")
    version = data[4]
    flags = data[5]
    if version != FORMAT_VERSION:
        raise ValueError(
            f"binary proof: version {version} != expected {FORMAT_VERSION}")
    if len(data) < 10:
        raise ValueError("binary proof: header too short")
    net = struct.unpack_from("<I", data, 6)[0]
    return version, flags, net, 10


def parse_tlv_records(body: bytes) -> list[tuple[int, bytes]]:
    """Parse TLV body into list of (type, payload)."""
    records: list[tuple[int, bytes]] = []
    pos = 0
    while pos < len(body):
        tlv_type = body[pos]
        pos += 1
        length, pos = leb128_read(body, pos)
        payload = body[pos : pos + length]
        pos += length
        records.append((tlv_type, payload))
    return records


def decode_bin_header(payload: bytes) -> dict[str, Any]:
    """Decode a 118-byte ledger header TLV payload to dict."""
    if len(payload) < 118:
        raise ValueError(f"header payload too short: {len(payload)}")
    seq = struct.unpack_from(">I", payload, 0)[0]
    drops = struct.unpack_from(">Q", payload, 4)[0]
    parent_hash = hexlify(payload[12:44]).decode().upper()
    tx_hash = hexlify(payload[44:76]).decode().upper()
    account_hash = hexlify(payload[76:108]).decode().upper()
    parent_close_time = struct.unpack_from(">I", payload, 108)[0]
    close_time = struct.unpack_from(">I", payload, 112)[0]
    close_time_resolution = payload[116]
    close_flags = payload[117]
    return {
        "seq": seq,
        "drops": str(drops),
        "parent_hash": parent_hash,
        "tx_hash": tx_hash,
        "account_hash": account_hash,
        "parent_close_time": parent_close_time,
        "close_time": close_time,
        "close_time_resolution": close_time_resolution,
        "close_flags": close_flags,
    }


def decode_anchor_core(payload: bytes) -> dict[str, Any]:
    """Decode anchor core TLV: hash + ledger_index + publisher key + validations."""
    if len(payload) < 69:
        raise ValueError(f"anchor core payload too short: {len(payload)}")

    # Must match xprv::encode_anchor_core():
    #   ledger_hash[32] + ledger_index[4] + publisher_key[33] + ...
    ledger_hash = hexlify(payload[0:32]).decode().upper()
    ledger_index = struct.unpack_from(">I", payload, 32)[0]
    publisher_key = hexlify(payload[36:69]).decode().upper()
    pos = 69
    val_count, pos = leb128_read(payload, pos)

    validations: dict[str, str] = {}
    if val_count == 0:
        return {
            "type": "anchor",
            "ledger_hash": ledger_hash,
            "ledger_index": ledger_index,
            "unl": {"public_key": publisher_key},
            "validations": validations,
        }

    # Read common fields (XRPL binary → hex for decode())
    common_bytes, pos = read_vl(payload, pos)
    common_hex = hexlify(common_bytes).decode()
    common_fields = decode(common_hex) if common_hex else {}

    # Read unique field names
    num_unique, pos = leb128_read(payload, pos)
    unique_names: list[str] = []
    for _ in range(num_unique):
        name_len, pos = leb128_read(payload, pos)
        unique_names.append(payload[pos : pos + name_len].decode("utf-8"))
        pos += name_len

    # Read each validator's key + unique fields, merge with common
    for _ in range(val_count):
        vkey = hexlify(payload[pos : pos + 33]).decode().upper()
        pos += 33
        unique_bytes, pos = read_vl(payload, pos)
        unique_hex = hexlify(unique_bytes).decode()
        unique_fields = decode(unique_hex) if unique_hex else {}

        merged = dict(common_fields)
        merged.update(unique_fields)
        full_hex = encode(merged)
        validations[vkey] = full_hex

    return {
        "type": "anchor",
        "ledger_hash": ledger_hash,
        "ledger_index": ledger_index,
        "unl": {"public_key": publisher_key},
        "validations": validations,
    }


def decode_anchor_unl(payload: bytes, anchor: dict[str, Any]) -> None:
    """Decode anchor UNL TLV and merge into existing anchor dict."""
    pos = 0
    manifest_bytes, pos = read_vl(payload, pos)
    sig_bytes, pos = read_vl(payload, pos)

    anchor["unl"]["manifest"] = hexlify(manifest_bytes).decode().upper()
    anchor["unl"]["signature"] = hexlify(sig_bytes).decode().upper()

    if pos >= len(payload):
        return

    # Reconstruct blob JSON with exact key ordering
    shape = payload[pos]
    pos += 1
    top_perm = PERMS_3[shape & 0x07]
    val_perm = PERMS_2[(shape >> 3) & 0x01]

    sequence = struct.unpack_from(">I", payload, pos)[0]
    pos += 4
    expiration = struct.unpack_from(">I", payload, pos)[0]
    pos += 4
    validator_count, pos = leb128_read(payload, pos)

    validators: list[tuple[str, str]] = []
    for _ in range(validator_count):
        vpk = hexlify(payload[pos : pos + 33]).decode().upper()
        pos += 33
        m_bytes, pos = read_vl(payload, pos)
        m_b64 = base64.b64encode(m_bytes).decode()
        validators.append((vpk, m_b64))

    # Build blob JSON string with exact key ordering for sig verification
    values = {
        0: str(sequence),
        1: str(expiration),
        2: None,  # validators array
    }

    parts: list[str] = []
    for ki in top_perm:
        key = BLOB_KEYS[ki]
        if ki == 2:  # validators
            val_parts: list[str] = []
            for vpk, m_b64 in validators:
                vobj_parts: list[str] = []
                for vki in val_perm:
                    vkey = VAL_KEYS[vki]
                    vval = vpk if vki == 0 else m_b64
                    vobj_parts.append(f'"{vkey}":"{vval}"')
                val_parts.append("{" + ",".join(vobj_parts) + "}")
            parts.append(f'"{key}":[' + ",".join(val_parts) + "]")
        else:
            parts.append(f'"{key}":{values[ki]}')

    blob_str = "{" + ",".join(parts) + "}"
    anchor["unl"]["blob"] = hexlify(blob_str.encode()).decode().upper()


def hash_binary_trie(data: bytes, pos: int, *, is_tx: bool) -> tuple[Hash256, int]:
    """Recursively hash a binary trie. Returns (hash, new_pos).

    Binary trie format: uint32 LE header (16 branches × 2 bits),
    then depth-first children. Branch types: 00=empty, 01=leaf,
    10=inner (recurse), 11=hash (32-byte placeholder).
    """
    if pos + 4 > len(data):
        raise ValueError("binary trie: unexpected end reading header")
    header = struct.unpack_from("<I", data, pos)[0]
    pos += 4

    children: list[Hash256] = [ZERO_HASH] * 16
    for branch in range(16):
        btype = (header >> (branch * 2)) & 0x03
        if btype == BR_EMPTY:
            continue
        elif btype == BR_LEAF:
            key = data[pos : pos + 32]
            pos += 32
            data_len, pos = leb128_read(data, pos)
            leaf_data = data[pos : pos + data_len]
            pos += data_len
            prefix = PREFIX_TX_LEAF if is_tx else PREFIX_STATE_LEAF
            children[branch] = sha512half(prefix + leaf_data + key)
        elif btype == BR_INNER:
            children[branch], pos = hash_binary_trie(data, pos, is_tx=is_tx)
        elif btype == BR_HASH:
            children[branch] = data[pos : pos + 32]
            pos += 32

    return hash_inner(children), pos


def find_binary_trie_leaf(data: bytes, *, is_tx: bool) -> dict[str, Any] | None:
    """Find and decode the first leaf in a binary trie. Returns decoded JSON or None."""
    try:
        return _scan_binary_trie_leaf(data, 0, is_tx=is_tx)[0]
    except Exception:
        return None


def _scan_binary_trie_leaf(
    data: bytes, pos: int, *, is_tx: bool
) -> tuple[dict[str, Any] | None, int]:
    """Recursively scan a binary trie for the first leaf."""
    header = struct.unpack_from("<I", data, pos)[0]
    pos += 4
    for branch in range(16):
        btype = (header >> (branch * 2)) & 0x03
        if btype == BR_EMPTY:
            continue
        elif btype == BR_LEAF:
            _key = data[pos : pos + 32]
            pos += 32
            data_len, pos = leb128_read(data, pos)
            leaf_bytes = data[pos : pos + data_len]
            pos += data_len
            # Decode the canonical binary leaf data
            try:
                if is_tx:
                    # TX leaf: vl_encode(tx_bytes) + vl_encode(meta_bytes)
                    lpos = 0
                    tx_len, lpos = _vl_decode(leaf_bytes, lpos)
                    tx_hex = hexlify(leaf_bytes[lpos : lpos + tx_len]).decode()
                    lpos += tx_len
                    meta_len, lpos = _vl_decode(leaf_bytes, lpos)
                    meta_hex = hexlify(leaf_bytes[lpos : lpos + meta_len]).decode()
                    return {"tx": decode(tx_hex), "meta": decode(meta_hex)}, pos
                else:
                    # State leaf: raw SLE binary
                    sle_hex = hexlify(leaf_bytes).decode()
                    return decode(sle_hex), pos
            except Exception:
                return None, pos
        elif btype == BR_INNER:
            found, pos = _scan_binary_trie_leaf(data, pos, is_tx=is_tx)
            if found is not None:
                return found, pos
        elif btype == BR_HASH:
            pos += 32
    return None, pos


def _vl_decode(data: bytes, pos: int) -> tuple[int, int]:
    """Decode XRPL variable-length prefix. Returns (length, new_pos)."""
    b0 = data[pos]
    if b0 <= 192:
        return b0, pos + 1
    elif b0 <= 240:
        b1 = data[pos + 1]
        return 193 + ((b0 - 193) << 8) + b1, pos + 2
    else:
        b1, b2 = data[pos + 1], data[pos + 2]
        return 12481 + ((b0 - 241) << 16) + (b1 << 8) + b2, pos + 3


def binary_to_proof(data: bytes) -> dict[str, Any]:
    """Parse a binary XPRV file into a proof dict compatible with verify()."""
    version, flags, network_id, body_offset = parse_binary_header(data)

    body = data[body_offset:]
    if flags & FLAG_ZLIB:
        body = zlib.decompress(body)

    records = parse_tlv_records(body)

    steps: list[dict[str, Any]] = []
    anchor_idx = -1

    for tlv_type, payload in records:
        if tlv_type == TLV_ANCHOR:
            anchor_idx = len(steps)
            steps.append(decode_anchor_core(payload))
        elif tlv_type == TLV_UNL:
            if anchor_idx >= 0:
                decode_anchor_unl(payload, steps[anchor_idx])
        elif tlv_type == TLV_HEADER:
            steps.append(
                {"type": "ledger_header", "header": decode_bin_header(payload)}
            )
        elif tlv_type in (TLV_MAP_TX, TLV_MAP_STATE):
            tree = "tx" if tlv_type == TLV_MAP_TX else "state"
            is_tx = tree == "tx"
            root_hash, _ = hash_binary_trie(payload, 0, is_tx=is_tx)
            # Store the computed root hash and tree type.
            # We can't convert binary trie to JSON trie easily, so we
            # store __binary_root__ for the verifier to check directly.
            steps.append(
                {
                    "type": "map_proof",
                    "tree": tree,
                    "__binary_root__": root_hash.hex().upper(),
                    "__binary_payload__": payload,
                }
            )

    return {"network_id": network_id, "steps": steps}


# ─── CLI ─────────────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(description="Verify an xprv proof (JSON or binary)")
    ap.add_argument("proof", help="Path to proof file (.json or .bin)")
    ap.add_argument("--trusted-key", default=None, help="VL publisher key (hex)")
    args = ap.parse_args()

    path = Path(args.proof)
    if not path.exists():
        print(f"Not found: {path}", file=sys.stderr)
        sys.exit(1)

    raw = path.read_bytes()
    if is_binary_proof(raw):
        print("Format: binary (XPRV)")
        proof = binary_to_proof(raw)
    else:
        print("Format: JSON")
        proof = json.loads(raw)

    r = verify(proof, args.trusted_key)
    sys.exit(0 if r.ok else 1)


if __name__ == "__main__":
    main()
