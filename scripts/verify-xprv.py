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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, TypedDict

import zlib

import xrpl.core.keypairs
from xrpl.core.binarycodec import decode, encode, encode_for_signing

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
            binary_root_hex: str | None = step.get("__binary_root__")

            print(f"\n  Step {i}: {'TX' if is_tx else 'STATE'} TREE")

            if trie is None and not binary_root_hex:
                print("    [SKIP] no trie")
                continue

            if trie:
                stats = count_nodes(trie)
                print(
                    f"    {stats.inners} inners, {stats.placeholders} placeholders, "
                    f"{stats.leaves} leaves (depth {stats.max_depth})"
                )
            else:
                print(f"    Binary trie, root={binary_root_hex[:16]}...")

            expected = tx_hash if is_tx else ac_hash
            if expected is None:
                print("    [SKIP] no expected hash")
                continue

            # Binary proofs have pre-computed root hash
            binary_root = step.get("__binary_root__")
            try:
                if binary_root:
                    root = bytes.fromhex(binary_root)
                else:
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
                if binary_root:
                    leaf_data = find_binary_trie_leaf(
                        step.get("__binary_payload__", b""), is_tx=False
                    )
                    if leaf_data:
                        hashes = leaf_data.get("Hashes")
                        if hashes:
                            skip_hashes = [h.upper() for h in hashes]
                            print(f"    Skip list: {len(skip_hashes)} hashes")
                else:
                    leaf = find_leaf(trie)
                    if leaf and len(leaf) >= 2 and isinstance(leaf[1], dict):
                        hashes = leaf[1].get("Hashes")
                        if hashes:
                            skip_hashes = [h.upper() for h in hashes]
                            print(f"    Skip list: {len(skip_hashes)} hashes")

            # Extract tx info from tx tree leaf
            if is_tx:
                if binary_root:
                    leaf_data = find_binary_trie_leaf(
                        step.get("__binary_payload__", b""), is_tx=True
                    )
                    if leaf_data and "tx" in leaf_data:
                        tx_data = leaf_data["tx"]
                        print(
                            f"    Leaf: {tx_data.get('TransactionType', '?')} "
                            f"from {tx_data.get('Account', '?')}"
                        )
                else:
                    leaf = find_leaf(trie)
                    if leaf and len(leaf) >= 2 and isinstance(leaf[1], dict):
                        tx_data_j: dict[str, Any] = leaf[1].get("tx", {})
                        print(
                            f"    Leaf: {tx_data_j.get('TransactionType', '?')} "
                            f"from {tx_data_j.get('Account', '?')}"
                        )

    print(f"\n{'=' * 60}")
    print(f"  {'PASS — all checks verified' if result.ok else 'FAIL'}")
    if result.errors:
        for err in result.errors:
            print(f"    - {err}")
    print(f"{'=' * 60}\n")
    return result


# ─── Binary format (XPRV) ────────────────────────────────────────────
#
# File: XPRV(4) + version(1) + flags(1) [+ network_id(4 LE if v2)]
# Body: TLV records (type(1) + length(LEB128) + payload)
# TLV types: 0x01=anchor, 0x02=header, 0x03=tx_map, 0x04=state_map, 0x05=unl

MAGIC = b"XPRV"
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
    if version == 0x01:
        return version, flags, 0, 6
    if version == 0x02:
        if len(data) < 10:
            raise ValueError("binary proof: v2 header too short")
        net = struct.unpack_from("<I", data, 6)[0]
        return version, flags, net, 10
    raise ValueError(f"binary proof: unsupported version {version}")


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
    """Decode anchor core TLV: hash + publisher key + decomposed validations."""
    pos = 0
    ledger_hash = hexlify(payload[pos : pos + 32]).decode().upper()
    pos += 32
    publisher_key = hexlify(payload[pos : pos + 33]).decode().upper()
    pos += 33
    val_count, pos = leb128_read(payload, pos)

    validations: dict[str, str] = {}
    if val_count == 0:
        return {
            "type": "anchor",
            "ledger_hash": ledger_hash,
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
