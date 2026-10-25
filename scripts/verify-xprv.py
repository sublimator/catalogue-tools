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
  - JSON (.json, .xprv.json): {"format_version": N, "network_id": N, "steps": [...]}
  - Binary (.bin, .xprv.bin): XPRV header + TLV body (optionally zlib)

Usage:
  ./scripts/verify-xprv.py proof.json
  ./scripts/verify-xprv.py proof.xprv.bin
  ./scripts/verify-xprv.py proof.json --trusted-key ED2677AB...
  ./scripts/verify-xprv.py xahau-proof.json --network 21337
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
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
#   {
#     "format_version": 3,
#     "network_id": 0,
#     "steps": [AnchorStep, HeaderStep, MapProofStep, ...]
#   }
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

    ledger_index: int
    total_coins: str  # string representation of uint64
    parent_hash: str  # 64-char hex
    transaction_hash: str  # 64-char hex
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
    full: bool  # vfFullValidation set; only full validations count to quorum


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
    vl_sequence: int = 0
    vl_effective: int = 0
    vl_expiration: int = 0


@dataclass
class VerifyResult:
    """Overall proof verification result."""

    ok: bool = True
    steps_checked: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class LedgerHashesLeaf:
    """A structurally recognized LedgerHashes state leaf."""

    key_hex: str
    hashes: tuple[str, ...]
    last_ledger_sequence: int | None


@dataclass(frozen=True)
class LedgerHashesSupport:
    """Constraints carried with one candidate child ledger hash."""

    leaf_key_hex: str
    containing_ledger_sequence: int
    last_ledger_sequence: int | None
    hashes_count: int
    source: str


@dataclass(frozen=True)
class ResolvedLedgerHashesSupport:
    """A candidate that is valid for a specific child header."""

    list_type: Literal["recent", "flag"]
    hashes_count: int
    source: str


# ─── Constants ───────────────────────────────────────────────────────

ZERO_HASH: Hash256 = b"\x00" * 32
HEX_BRANCH = set("0123456789abcdefABCDEF")
# Branch keys are uppercase only (spec §2.3). No case normalization:
# accepting both `a` and `A` puts two members on one branch, and only
# one of them gets hashed into the root.
BRANCH_KEYS = set("0123456789ABCDEF")

# Hash prefixes (XRPL protocol)
PREFIX_HEADER = b"LWR\x00"  # ledger header
PREFIX_INNER = b"MIN\x00"  # SHAMap inner node
PREFIX_TX_LEAF = b"SND\x00"  # transaction + metadata leaf
PREFIX_STATE_LEAF = b"MLN\x00"  # account state (SLE) leaf
PREFIX_VALIDATION = b"VAL\x00"  # STValidation signing data
PREFIX_MANIFEST = b"MAN\x00"  # manifest signing data
FULL_VALIDATION_FLAG = 0x00000001  # rippled vfFullValidation
PREFIX_TX_SIGNING = b"STX\x00"  # STObject signing (swapped to VAL for validations)

# Default XRPL mainnet VL publisher key
XRPL_VL_KEY = "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734"
XAHAU_VL_KEY = "EDA46E9C39B1389894E690E58914DC1029602870370A0993E5B87C4A24EAF4A8E8"

# Spec §8: network_id is bound to the configured publisher key set.
XAHAU_NETWORK_ID = 21337
PUBLISHER_KEY_BY_NETWORK: dict[int, str] = {0: XRPL_VL_KEY, XAHAU_NETWORK_ID: XAHAU_VL_KEY}
NETWORK_BY_PUBLISHER_KEY: dict[str, int] = {
    key: network for network, key in PUBLISHER_KEY_BY_NETWORK.items()
}


# ─── SHA512-Half ─────────────────────────────────────────────────────


def sha512half(data: bytes) -> Hash256:
    """SHA-512, truncated to first 32 bytes (256 bits)."""
    return hashlib.sha512(data).digest()[:32]


def short_skip_key_hex() -> str:
    """Key of the recent-history LedgerHashes entry."""
    return sha512half(struct.pack(">H", 0x0073)).hex().upper()


def long_skip_key_hex(ledger_sequence: int) -> str:
    """Key of the LedgerHashes era containing ledger_sequence."""
    return sha512half(
        struct.pack(">HI", 0x0073, ledger_sequence >> 16)
    ).hex().upper()


def classify_ledger_hashes_link(
    leaf_key_hex: str,
    containing_ledger_sequence: int,
    child_ledger_sequence: int,
) -> Literal["recent", "flag"] | None:
    """Return the list type only when key and child range are valid."""
    if child_ledger_sequence <= 0 or child_ledger_sequence >= containing_ledger_sequence:
        return None
    if leaf_key_hex == short_skip_key_hex():
        if containing_ledger_sequence - child_ledger_sequence <= 256:
            return "recent"
        return None
    if (
        child_ledger_sequence % 256 == 0
        and leaf_key_hex == long_skip_key_hex(child_ledger_sequence)
    ):
        return "flag"
    return None


def ledger_hashes_last_sequence_consistent(
    list_type: Literal["recent", "flag"],
    containing_ledger_sequence: int,
    child_ledger_sequence: int,
    last_ledger_sequence: int | None,
) -> bool:
    """Cross-check LastLedgerSequence when the SLE includes it."""
    if last_ledger_sequence is None:
        return True
    if list_type == "recent":
        return last_ledger_sequence == containing_ledger_sequence - 1
    return (
        last_ledger_sequence < containing_ledger_sequence
        and last_ledger_sequence % 256 == 0
        and last_ledger_sequence >> 16 == child_ledger_sequence >> 16
        and child_ledger_sequence <= last_ledger_sequence
    )


# ─── Ledger header hashing ──────────────────────────────────────────


def hash_header(hdr: dict[str, Any]) -> Hash256:
    """Hash a ledger header: SHA512Half(LWR\\0 + canonical fields)."""
    buf = PREFIX_HEADER
    buf += struct.pack(">I", hdr["ledger_index"])
    buf += struct.pack(">Q", int(hdr["total_coins"]))
    buf += bytes.fromhex(hdr["parent_hash"])
    buf += bytes.fromhex(hdr["transaction_hash"])
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
        # Hash the DECODED content, not a sibling `blob`. What you see must be
        # what you hash: if the verifier hashed `blob` instead, then `blob` would
        # be the authenticated datum and the decoded members displayed to a human
        # would be unauthenticated assertions that could disagree with it while
        # the proof still verified. Re-serialising is what binds the legible form
        # to the authenticated one.
        #
        # `index`/`hash` duplicate the trie leaf key and `blob` duplicates these
        # very bytes; none are XRPL fields, so they are dropped before encoding.
        # Passing them through was the KeyError('blob') that made every
        # hybrid-leaf proof fail here while its binary form verified.
        clean = {
            k: v
            for k, v in leaf_json.items()
            if k not in ("index", "blob", "hash")
        }
        return bytes.fromhex(encode(clean))


def trie_branches(node: dict[str, Any], depth: int) -> dict[int, Any]:
    """Parse an inner node's branches ONCE, strictly. Shared by every consumer.

    SOUNDNESS-CRITICAL. This exists because two walks over the same JSON
    disagreed about which members were real:

        hash_trie:            children[int(k,16)] = ...   -> last writer wins
        collect_json_leaves:  iterates members            -> keeps them all

    `"A"` and `"a"` are both single valid nibbles and both survived the
    len/charset filter, so `{"A": <real leaf>, "a": <forged leaf>}` hashed to the
    same root as the real trie while the leaf collector returned BOTH — handing a
    forged LedgerHashes leaf to the skip-list resolver. That authenticated a
    fabricated header, whose fabricated transaction_hash authenticated a
    transaction that never existed. Both reference verifiers returned exit 0 on a
    forged ~1e9 XRP Payment.

    Two rules close it. Normalize the nibble (spec §2: verifiers MAY accept
    lowercase but MUST normalize before comparing), and REFUSE a duplicate rather
    than picking a winner — an ambiguous trie has no single meaning, so there is
    no correct winner to pick. Unknown members are refused for the same reason:
    a member no consumer agrees about is exactly the hiding place.

    `depth` is this node's nesting depth, 0 at the root. `__depth__`, where present,
    MUST equal it (spec §2.3). Nothing hashes a depth, so a `__depth__` that
    disagrees with the structure it describes is either a producer bug or a trap for
    a reader that believes it over the nesting.
    """
    branches: dict[int, Any] = {}
    for k, v in node.items():
        if k == "__depth__":
            # bool is an int in Python and True == 1, so exclude it explicitly.
            if isinstance(v, bool) or not isinstance(v, int) or v != depth:
                raise ValueError(
                    f"trie inner node __depth__ is {v!r} but its nesting depth is "
                    f"{depth} (spec §2.3)"
                )
            continue
        if len(k) != 1 or k not in BRANCH_KEYS:
            raise ValueError(
                f"trie inner node has non-branch member {k!r} — branch keys are "
                f"uppercase 0-9A-F (spec §2.3)"
            )
        idx = int(k, 16)
        if idx in branches:
            raise ValueError(
                f"trie inner node has duplicate branch {idx:X} (member {k!r}) — "
                f"ambiguous after nibble normalization"
            )
        branches[idx] = v
    return branches


def hash_trie(node: TrieNode, *, is_tx: bool, depth: int = 0) -> Hash256:
    """Recursively hash a JSON trie node (placeholder / leaf / inner)."""
    if depth == 0:
        validate_json_trie(node)
    if isinstance(node, str):
        return bytes.fromhex(node)
    if isinstance(node, list):
        key_hex: str = node[0]
        return hash_leaf(key_hex, serialize_leaf(node[1], is_tx=is_tx), is_tx=is_tx)
    if isinstance(node, dict):
        children: list[Hash256] = [ZERO_HASH] * 16
        for idx, v in trie_branches(node, depth).items():
            children[idx] = hash_trie(v, is_tx=is_tx, depth=depth + 1)
        return hash_inner(children)
    raise ValueError(f"unexpected trie node: {type(node)}")


def count_nodes(node: TrieNode, depth: int = 0) -> TrieStats:
    """Count trie node types recursively."""
    if depth == 0:
        validate_json_trie(node)
    if isinstance(node, str):
        return TrieStats(placeholders=1, max_depth=depth)
    if isinstance(node, list):
        return TrieStats(leaves=1, max_depth=depth)
    if isinstance(node, dict):
        s = TrieStats(inners=1, max_depth=depth)
        # Diagnostic, but routed through the strict parse: spec §2.3 requires the
        # branch set be derived once and used for every purpose, display included.
        # Reported shape must describe the trie that was hashed, not a laxer reading.
        for _idx, v in trie_branches(node, depth).items():
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
    validate_json_trie(node)
    leaves: list[tuple[str, dict[str, Any]]] = []

    def walk(cur: Any, depth: int) -> None:
        if isinstance(cur, list) and len(cur) >= 2:
            key_hex = cur[0]
            leaf_data = cur[1]
            if isinstance(key_hex, str) and isinstance(leaf_data, dict):
                leaves.append((key_hex, leaf_data))
            return
        if isinstance(cur, dict):
            # Same strict parse as the hash walk: if the two disagree about which
            # members exist, a leaf can be collected without being hashed.
            for _idx, v in sorted(trie_branches(cur, depth).items()):
                walk(v, depth + 1)

    walk(node, 0)
    return leaves


def decode_binary_leaf(leaf_bytes: bytes, *, is_tx: bool) -> dict[str, Any] | None:
    """Decode a binary trie leaf for display. Failure returns None.

    Authentication does not depend on this. A caller that needs a skip link
    must ignore None: unknown bytes are not a LedgerHashes leaf.
    """
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
) -> list[tuple[str, dict[str, Any] | None]]:
    """Collect every structural leaf. The dict is display-only and may be None."""
    leaves: list[tuple[str, dict[str, Any] | None]] = []

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
                # Decode is display. The leaf is authenticated either way.
                leaf_data = decode_binary_leaf(leaf_bytes, is_tx=is_tx)
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


def is_hash_hex(value: Any) -> bool:
    """Return whether value is exactly one 32-byte hexadecimal string."""
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(c in "0123456789abcdefABCDEF" for c in value)
    )


def extract_ledger_hashes_leaves(
    leaves: list[tuple[str, dict[str, Any] | None]],
) -> list[LedgerHashesLeaf]:
    """Recognize well-formed LedgerHashes leaves without granting authority."""
    result: list[LedgerHashesLeaf] = []
    for key_hex, leaf_data in leaves:
        # Undecoded bytes are not a skip link, whatever their key is.
        if not isinstance(leaf_data, dict):
            continue
        if not is_hash_hex(key_hex):
            continue
        if leaf_data.get("LedgerEntryType") != "LedgerHashes":
            continue
        raw_hashes = leaf_data.get("Hashes")
        if not isinstance(raw_hashes, list) or len(raw_hashes) > 256:
            continue
        if not all(is_hash_hex(item) for item in raw_hashes):
            continue

        last_sequence = leaf_data.get("LastLedgerSequence")
        if last_sequence is not None and (
            isinstance(last_sequence, bool)
            or not isinstance(last_sequence, int)
            or not 0 <= last_sequence <= 0xFFFFFFFF
        ):
            continue

        result.append(
            LedgerHashesLeaf(
                key_hex=key_hex.upper(),
                hashes=tuple(item.upper() for item in raw_hashes),
                last_ledger_sequence=last_sequence,
            )
        )
    return result


def find_ledger_hashes_support(
    candidates: dict[str, list[LedgerHashesSupport]],
    header_hash: str,
    header_sequence: int,
) -> ResolvedLedgerHashesSupport | None:
    """Find a LedgerHashes candidate whose key and range bind this header."""
    for candidate in candidates.get(header_hash, []):
        list_type = classify_ledger_hashes_link(
            candidate.leaf_key_hex,
            candidate.containing_ledger_sequence,
            header_sequence,
        )
        if list_type is None or not ledger_hashes_last_sequence_consistent(
            list_type,
            candidate.containing_ledger_sequence,
            header_sequence,
            candidate.last_ledger_sequence,
        ):
            continue
        return ResolvedLedgerHashesSupport(
            list_type=list_type,
            hashes_count=candidate.hashes_count,
            source=candidate.source,
        )
    return None


def describe_tx_leaves(
    leaves: list[tuple[str, dict[str, Any] | None]],
) -> str:
    """Produce a short human-readable summary of tx leaves."""
    if not leaves:
        return "0 tx leaves"
    first = leaves[0][1]
    if not isinstance(first, dict):
        if len(leaves) == 1:
            return "1 tx leaf (authenticated, not decoded)"
        return f"{len(leaves)} tx leaves (first: authenticated, not decoded)"
    tx = first.get("tx", {})
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
        full=isinstance(decoded.get("Flags"), int)
        and bool(decoded["Flags"] & FULL_VALIDATION_FLAG),
    )


def parse_manifest(manifest_bytes: bytes) -> dict[str, Any]:
    """Parse a raw validator/publisher manifest STObject."""
    return decode(hexlify(manifest_bytes).decode())


def manifest_signing_data(manifest_dict: dict[str, Any]) -> bytes:
    """Build MAN\\0 + manifest fields with both signature fields stripped."""
    signing_hex = encode_for_signing(manifest_dict)
    signing_bytes = unhexlify(signing_hex)
    return PREFIX_MANIFEST + signing_bytes[4:]


MANIFEST_FIELDS = {
    "Version",
    "Sequence",
    "PublicKey",
    "SigningPubKey",
    "Signature",
    "Domain",
    "MasterSignature",
}
MANIFEST_REVOKED = 0xFFFFFFFF


def is_properly_formed_toml_domain(domain: bytes) -> bool:
    """Port of catl::vl::is_properly_formed_toml_domain (vl-client.cpp)."""
    if not 4 <= len(domain) <= 128:
        return False
    labels = domain.split(b".")
    if len(labels) < 2:
        return False
    for i, label in enumerate(labels):
        if not 1 <= len(label) <= 63 or label[:1] == b"-" or label[-1:] == b"-":
            return False
        is_tld = i == len(labels) - 1
        for c in label:
            alpha = 65 <= c <= 90 or 97 <= c <= 122
            if is_tld and not alpha:
                return False
            if not is_tld and not (alpha or 48 <= c <= 57 or c == 45):
                return False
    return len(labels[-1]) >= 2


SECP256K1_ORDER = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
ED25519_ORDER = 2**252 + 27742317777372353535851937790883648493  # L


def _der_integer(sig: bytes, pos: int) -> tuple[int, int] | None:
    """Parse one strict-DER INTEGER at pos; return (value, next_pos)."""
    if pos + 2 > len(sig) or sig[pos] != 0x02:
        return None
    length = sig[pos + 1]
    body = sig[pos + 2 : pos + 2 + length]
    if length == 0 or len(body) != length or body[0] & 0x80:
        return None
    if length > 1 and body[0] == 0 and not body[1] & 0x80:
        return None  # unnecessary leading zero
    return int.from_bytes(body, "big"), pos + 2 + length


def secp256k1_signature_is_canonical(sig: bytes) -> bool:
    """Strict DER with 0 < r < n and 0 < s <= n/2 (low-S).

    The C++ verifier gets this from libsecp256k1, which parses strict DER and
    refuses high-S signatures. Without it the two verifiers disagree on the
    same bytes, and a high-S copy is a second valid encoding of one signature.
    """
    if not 8 <= len(sig) <= 72 or sig[0] != 0x30 or sig[1] != len(sig) - 2:
        return False
    r = _der_integer(sig, 2)
    if r is None:
        return False
    s_ = _der_integer(sig, r[1])
    if s_ is None or s_[1] != len(sig):
        return False
    return 0 < r[0] < SECP256K1_ORDER and 0 < s_[0] <= SECP256K1_ORDER // 2


def ed25519_signature_is_canonical(sig: bytes) -> bool:
    """64 bytes with S < L, as libsodium requires.

    S and S + L verify identically under the group law, so without this the
    same signature has two encodings and Python accepts one C++ rejects.
    """
    return len(sig) == 64 and int.from_bytes(sig[32:], "little") < ED25519_ORDER


def verify_signature(data: bytes, signature: bytes, key_hex: str) -> bool:
    """The one signature check for every XRPL signature this verifier sees."""
    try:
        canonical = (
            ed25519_signature_is_canonical(signature)
            if key_hex.upper().startswith("ED")
            else secp256k1_signature_is_canonical(signature)
        )
        if not canonical:
            return False
        return xrpl.core.keypairs.is_valid_message(data, signature, key_hex)
    except Exception:
        return False


def _signature_ok(data: bytes, signature_hex: str, key_hex: str) -> bool:
    try:
        return verify_signature(data, unhexlify(signature_hex), key_hex)
    except Exception:
        return False


def verify_manifest(manifest_bytes: bytes) -> tuple[bool, str, dict[str, Any]]:
    """Verify a manifest's template and signatures.

    Mirrors catl::vl::parse_manifest plus xprv::verify_manifest: only the
    version-zero manifest fields, canonical encoding, a required Sequence, and
    for a revocation no signing key or signature at all.
    """
    parsed = parse_manifest(manifest_bytes)

    extra = set(parsed) - MANIFEST_FIELDS
    if extra:
        return False, f"manifest has fields outside the template: {sorted(extra)}", parsed
    if encode(parsed).upper() != hexlify(manifest_bytes).decode().upper():
        return False, "manifest encoding is not canonical", parsed

    sequence = parsed.get("Sequence")
    version = parsed.get("Version")
    master_key = _upper_hex(parsed.get("PublicKey"))
    signing_key = _upper_hex(parsed.get("SigningPubKey"))
    signature_hex = parsed.get("Signature", "")
    master_sig_hex = parsed.get("MasterSignature", "")

    if not isinstance(sequence, int):
        return False, "manifest missing Sequence", parsed
    if version is not None and version != 0:
        return False, f"unsupported manifest Version {version}", parsed
    if len(master_key) != 66:
        return False, "manifest PublicKey is not 33 bytes", parsed
    if not master_sig_hex:
        return False, "manifest missing MasterSignature", parsed
    if "Domain" in parsed and not is_properly_formed_toml_domain(
        bytes.fromhex(parsed["Domain"])
    ):
        return False, "manifest Domain is not a well-formed domain", parsed

    signing_data = manifest_signing_data(parsed)
    if not _signature_ok(signing_data, master_sig_hex, master_key):
        return False, "master key signature invalid", parsed

    if sequence == MANIFEST_REVOKED:
        # Presence, not value, as in C++: even an empty field is refused.
        if "SigningPubKey" in parsed or "Signature" in parsed:
            return False, "revocation carries a signing key or signature", parsed
        return True, "ok", parsed

    if len(signing_key) != 66:
        return False, "manifest SigningPubKey is not 33 bytes", parsed
    if not signature_hex:
        return False, "manifest missing Signature", parsed
    if signing_key == master_key:
        return False, "manifest signing key equals its master key", parsed
    if not _signature_ok(signing_data, signature_hex, signing_key):
        return False, "signing key signature invalid", parsed

    return True, "ok", parsed


# ─── Anchor verification ────────────────────────────────────────────


def validator_list_active_at(effective: int, expiration: int, close_time: int) -> bool:
    """Whether a ledger closed inside a signed list's validity interval."""
    return expiration > effective and effective <= close_time < expiration


def _upper_hex(value: Any) -> str:
    return value.upper() if isinstance(value, str) else ""


def _blob_uint32(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 <= value <= 0xFFFFFFFF else None


def verify_anchor(anchor: dict[str, Any], trusted_key: str) -> AnchorResult:
    """Verify anchor: manifest chain + VL blob + validations + quorum.

    Mirrors AnchorVerifier::verify (src/xprv/src/anchor-verifier.cpp): a
    malformed or unbound input fails the anchor rather than being skipped.
    """
    unl = anchor.get("unl")
    if not isinstance(unl, dict):
        return AnchorResult(ok=False, message="no UNL data")
    # §2.1: public_key always; manifest, blob and signature together or not at all.
    if set(unl) - {"public_key", "manifest", "blob", "signature"}:
        return AnchorResult(ok=False, message="unknown member in unl")
    if len({"manifest", "blob", "signature"} & set(unl)) not in (0, 3):
        return AnchorResult(
            ok=False,
            message="unl must carry manifest, blob and signature together, or none of them",
        )
    trusted = trusted_key.upper()

    # Step 1: publisher key matches trusted key
    proof_key = _upper_hex(unl.get("public_key"))
    if proof_key != trusted:
        return AnchorResult(
            ok=False,
            message=f"publisher key mismatch: {proof_key[:16]}... != {trusted[:16]}...",
        )
    print(f"    A1: Publisher key {proof_key[:16]}... matches trusted key")

    # A list-less anchor (§2.1, §3.3) needs the verifier's own copy of the
    # list; this verifier has no external list source yet (§1.3, D4).
    if set(unl) == {"public_key"}:
        return AnchorResult(
            ok=False, message="validator list not bundled and no external list"
        )

    # Step 2: verify publisher manifest, bound to the trusted key → signing key
    manifest_hex = unl.get("manifest")
    if not isinstance(manifest_hex, str) or not manifest_hex:
        return AnchorResult(ok=False, message="no manifest")
    try:
        ok, err, manifest = verify_manifest(unhexlify(manifest_hex))
    except Exception as exc:
        ok, err, manifest = False, str(exc), {}
    if not ok:
        return AnchorResult(ok=False, message=f"publisher manifest: {err}")
    if _upper_hex(manifest.get("PublicKey")) != trusted:
        return AnchorResult(
            ok=False, message="publisher manifest master key does not match trusted key"
        )
    signing_key = _upper_hex(manifest.get("SigningPubKey"))
    if not signing_key:
        return AnchorResult(ok=False, message="manifest has no SigningPubKey")
    print(f"    A2: Publisher manifest VERIFIED, signing key {signing_key[:16]}...")

    # Step 3: verify blob signature (raw bytes, signed by publisher signing key)
    blob_hex = unl.get("blob")
    sig_hex = unl.get("signature")
    if not isinstance(blob_hex, str) or not isinstance(sig_hex, str) or not blob_hex or not sig_hex:
        return AnchorResult(ok=False, message="no blob or signature")
    try:
        blob_bytes = unhexlify(blob_hex)
        blob_ok = verify_signature(blob_bytes, unhexlify(sig_hex), signing_key)
    except Exception:
        blob_ok = False
    if not blob_ok:
        return AnchorResult(ok=False, message="blob signature FAILED")
    print(f"    A3: Blob signature VERIFIED ({len(blob_bytes)} bytes)")

    # Step 4: verify validator manifests from blob → build signing→master key map
    try:
        blob_json = json.loads(blob_bytes)
    except ValueError:
        return AnchorResult(ok=False, message="invalid blob JSON")
    if not isinstance(blob_json, dict):
        return AnchorResult(ok=False, message="UNL blob is not an object")
    validators = blob_json.get("validators")
    if (
        "sequence" not in blob_json
        or "expiration" not in blob_json
        or not isinstance(validators, list)
    ):
        return AnchorResult(ok=False, message="UNL blob is missing required fields")
    vl_sequence = _blob_uint32(blob_json["sequence"])
    vl_expiration = _blob_uint32(blob_json["expiration"])
    vl_effective = _blob_uint32(blob_json.get("effective", 0))
    if vl_sequence is None or vl_expiration is None or vl_effective is None:
        return AnchorResult(ok=False, message="UNL blob validity fields are malformed")
    if vl_expiration <= vl_effective:
        return AnchorResult(ok=False, message="UNL blob validity interval is invalid")

    signing_to_master: dict[str, str] = {}
    masters: set[str] = set()
    for entry in validators:
        if not isinstance(entry, dict):
            return AnchorResult(ok=False, message="malformed validator entry in UNL blob")
        m_b64 = entry.get("manifest")
        advertised = entry.get("validation_public_key")
        if not isinstance(m_b64, str) or not isinstance(advertised, str):
            return AnchorResult(ok=False, message="validator entry missing key or manifest")
        try:
            ok, err, m = verify_manifest(base64.b64decode(m_b64, validate=True))
        except Exception as exc:
            ok, err, m = False, str(exc), {}
        revoked = ok and m.get("Sequence") == MANIFEST_REVOKED
        sk = _upper_hex(m.get("SigningPubKey"))
        if not ok or (not sk and not revoked):
            return AnchorResult(
                ok=False, message=f"invalid validator manifest: {err if not ok else 'no signing key'}"
            )
        mk = _upper_hex(m.get("PublicKey"))
        if advertised.upper() != mk:
            return AnchorResult(ok=False, message="validator manifest master key mismatch")
        if mk in masters:
            return AnchorResult(ok=False, message="duplicate validator master key")
        if revoked:
            # A revoked master stays in N and never votes (§1.3): it has no
            # signing key, so no validation can map to it.
            masters.add(mk)
            continue
        if sk in signing_to_master:
            return AnchorResult(ok=False, message="duplicate validator signing key")
        masters.add(mk)
        signing_to_master[sk] = mk

    if masters & signing_to_master.keys():
        return AnchorResult(ok=False, message="validator key reused across roles")
    unl_size = len(masters)
    if unl_size == 0:
        return AnchorResult(ok=False, message="UNL contains no verified validators")
    print(f"    A4: {unl_size} validator manifests verified from UNL blob")

    # Step 5: verify each STValidation; one vote per validator master key
    validations = anchor.get("validations")
    if not isinstance(validations, dict):
        return AnchorResult(ok=False, message="no validations in anchor")
    anchor_hash = _upper_hex(anchor.get("ledger_hash"))

    verified = 0
    partial = 0
    matched_unl = 0
    counted_masters: set[str] = set()

    for key_hex, val_hex in validations.items():
        try:
            parsed = parse_validation(val_hex) if isinstance(val_hex, str) else None
        except Exception:
            parsed = None
        if not parsed:
            continue
        if _upper_hex(key_hex) != parsed.signing_key:
            return AnchorResult(
                ok=False, message="validation map key does not match SigningPubKey"
            )
        if parsed.ledger_hash != anchor_hash:
            continue
        if not verify_signature(parsed.signing_data, parsed.signature, parsed.signing_key):
            continue

        verified += 1
        # rippled counts only full validations toward quorum (§5).
        if not parsed.full:
            partial += 1
            continue
        master = signing_to_master.get(parsed.signing_key)
        if master is not None and master not in counted_masters:
            counted_masters.add(master)
            matched_unl += 1

    partial_note = f", {partial} partial (not counted)" if partial else ""
    print(
        f"    A5: {verified} sigs verified{partial_note}, "
        f"{matched_unl}/{unl_size} UNL validators"
    )

    # Step 6: quorum check, ceil(80%) as in the C++ verifier
    quorum = (unl_size * 4 + 4) // 5
    result = AnchorResult(
        ok=matched_unl >= quorum,
        message=f"{matched_unl}/{unl_size} validators",
        unl_size=unl_size,
        matched_unl=matched_unl,
        verified_sigs=verified,
        vl_sequence=vl_sequence,
        vl_effective=vl_effective,
        vl_expiration=vl_expiration,
    )
    if result.ok:
        print(f"    A6: QUORUM — {matched_unl}/{unl_size} (>= 80%)")
    else:
        result.message = f"quorum not met: {matched_unl}/{unl_size} (need {quorum})"
    return result


# ─── Main verifier ──────────────────────────────────────────────────


def validate_json_trie(node: Any, depth: int = 0) -> None:
    """Enforce the same node grammar before hashing, enumeration or display."""
    if isinstance(node, str):
        if len(node) != 64 or any(c not in "0123456789abcdefABCDEF" for c in node):
            raise ValueError("trie placeholder must be a 32-byte hex hash")
    elif isinstance(node, list):
        if len(node) != 2 or not isinstance(node[0], str):
            raise ValueError("trie leaf must be exactly [key, data]")
        if len(node[0]) != 64 or any(c not in "0123456789abcdefABCDEF" for c in node[0]):
            raise ValueError("trie leaf key must be a 32-byte hex hash")
        if not isinstance(node[1], (dict, str)):
            raise ValueError("trie leaf data must be an object or hex string")
    elif isinstance(node, dict):
        if depth >= 64:
            raise ValueError("trie depth exceeds maximum (64)")
        for child in trie_branches(node, depth).values():
            validate_json_trie(child, depth + 1)
    else:
        raise ValueError(f"unexpected trie node: {type(node)}")


def load_json_proof(raw: str | bytes) -> dict[str, Any] | list[Any]:
    """Reject repeated members before a normal dict could discard their tokens."""
    def object_members(pairs):
        obj = {}
        for key, value in pairs:
            if key in obj:
                raise ValueError(f"duplicate JSON member {key!r}")
            obj[key] = value
        return obj
    return json.loads(raw, object_pairs_hook=object_members)


def validate_json_steps(steps: list[Any]) -> None:
    for step in steps:
        if step.get("type") == "anchor":
            for key in step.get("validations", {}):
                if len(key) != 66 or any(c not in "0123456789ABCDEF" for c in key):
                    raise ValueError("validation map keys must be uppercase 33-byte hex")
        elif step.get("type") == "map_proof":
            if step.get("tree") not in ("state", "tx"):
                raise ValueError("unknown map tree type")
            if "trie" in step:
                validate_json_trie(step["trie"])


def verify(
    proof: Proof | dict[str, Any] | list[Any],
    trusted_key: str | None = None,
    network_id: int | None = None,
) -> VerifyResult:
    """Verify a proof DAG serialized as a parent-first step list.

    `network_id` is the configured network. It defaults to the network of a
    known `trusted_key`, else to XRPL mainnet (0), and the proof's own
    `network_id` must match it (spec §8). A bare step list is the legacy
    form and, as in the C++ loader, belongs to network 0.
    """
    result = VerifyResult()
    error = None
    if isinstance(proof, list):
        steps: list[dict[str, Any]] = proof
        proof_network: Any = 0
    else:
        steps = proof.get("steps", [])
        proof_network = proof.get("network_id")
        version = proof.get("format_version")
        if isinstance(version, bool) or version != FORMAT_VERSION:
            error = f"unsupported format_version {version!r} (expected {FORMAT_VERSION})"
        elif isinstance(proof_network, bool) or not isinstance(proof_network, int):
            error = f"proof network_id must be an integer, got {proof_network!r}"

    if network_id is None:
        network_id = NETWORK_BY_PUBLISHER_KEY.get(trusted_key.upper(), 0) if trusted_key else 0
    if not trusted_key:
        trusted_key = PUBLISHER_KEY_BY_NETWORK.get(network_id)
    key_network = NETWORK_BY_PUBLISHER_KEY.get(trusted_key.upper()) if trusted_key else None

    if error:
        pass
    elif not trusted_key:
        error = f"no trusted publisher key configured for network {network_id}"
    elif key_network is not None and key_network != network_id:
        error = f"trusted key belongs to network {key_network}, not configured network {network_id}"
    elif proof_network != network_id:
        error = f"proof network_id {proof_network} does not match configured network {network_id}"
    if not error:
        try:
            validate_json_steps(steps)
        except ValueError as exc:
            error = str(exc)
    if error:
        print(f"\n  FAIL: {error}")
        result.ok = False
        result.errors.append(error)
        return result

    if not steps:
        result.ok = False
        result.errors.append("proof has no steps")
        return result

    anchor_count = sum(1 for step in steps if step.get("type") == "anchor")
    if anchor_count != 1:
        result.ok = False
        result.errors.append(f"proof must contain exactly one anchor, found {anchor_count}")

    trusted_ledger_hashes: dict[str, list[str]] = defaultdict(list)
    trusted_tx_roots: dict[str, list[str]] = defaultdict(list)
    trusted_state_roots: dict[str, list[str]] = defaultdict(list)
    trusted_state_root_sequences: dict[str, set[int]] = defaultdict(set)
    ledger_hashes_candidates: dict[str, list[LedgerHashesSupport]] = defaultdict(list)
    seen_header_hashes: set[str] = set()
    seen_map_roots: set[tuple[str, str]] = set()
    skip_source_last_seq: dict[str, int] = {}
    anchor_seen = False
    anchor_vl_window: tuple[int, int] | None = None

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
                anchor_vl_window = (ar.vl_effective, ar.vl_expiration)
                print(f"    [PASS] anchor verified ({ar.message})")
            else:
                print(f"    [FAIL] {ar.message}")
                result.ok = False
                result.errors.append(f"step {i}: anchor: {ar.message}")

        elif step_type == "ledger_header":
            hdr: dict[str, Any] = step.get("header", {})
            computed = hash_header(hdr)
            computed_hex = computed.hex().upper()

            print(f"\n  Step {i}: HEADER seq={hdr['ledger_index']}")
            print(
                f"    Hash={computed_hex[:16]}... tx={hdr['transaction_hash'][:16]}... "
                f"ac={hdr['account_hash'][:16]}..."
            )

            direct_source = first_source(trusted_ledger_hashes, computed_hex)
            skip_support = (
                None
                if direct_source is not None
                else find_ledger_hashes_support(
                    ledger_hashes_candidates, computed_hex, int(hdr["ledger_index"])
                )
            )
            if direct_source is None and skip_support is None:
                msg = (
                    "header hash is not supported by any earlier authenticated "
                    "ancestor or recognized LedgerHashes leaf"
                )
                print(f"    [FAIL] {msg}")
                result.ok = False
                result.errors.append(f"step {i}: {msg}")
                continue
            if (
                direct_source == "anchor"
                and anchor_vl_window is not None
                and not validator_list_active_at(*anchor_vl_window, int(hdr["close_time"]))
            ):
                msg = "anchor close time is outside the signed list interval"
                print(f"    [FAIL] {msg}")
                result.ok = False
                result.errors.append(f"step {i}: {msg}")
                continue

            if direct_source == "anchor":
                print("    [PASS] matches anchor hash")
            elif skip_support is not None:
                print(f"    [PASS] found in {skip_support.source}")
            else:
                print(f"    [PASS] supported by {direct_source}")

            if computed_hex in seen_header_hashes:
                warn = f"step {i}: duplicate ledger_header {computed_hex[:16]}... is non-canonical"
                print(f"    [WARN] {warn}")
                result.warnings.append(warn)
            seen_header_hashes.add(computed_hex)

            if skip_support is not None:
                prev_seq = skip_source_last_seq.get(skip_support.source)
                seq = int(hdr["ledger_index"])
                if prev_seq is not None and seq > prev_seq:
                    warn = (
                        f"step {i}: headers from {skip_support.source} are not in "
                        f"descending seq order ({seq} after {prev_seq})"
                    )
                    print(f"    [WARN] {warn}")
                    result.warnings.append(warn)
                skip_source_last_seq[skip_support.source] = seq

            parent_hash = hdr["parent_hash"].upper()
            tx_root = hdr["transaction_hash"].upper()
            state_root = hdr["account_hash"].upper()

            add_capability(
                trusted_ledger_hashes,
                parent_hash,
                f"parent hash of header ledger_index={hdr['ledger_index']}",
            )
            add_capability(trusted_tx_roots, tx_root, f"header ledger_index={hdr['ledger_index']}")
            add_capability(
                trusted_state_roots, state_root, f"header seq={hdr['ledger_index']}"
            )
            trusted_state_root_sequences[state_root].add(int(hdr["ledger_index"]))

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
                detail = str(exc)
                if network_id == XAHAU_NETWORK_ID and isinstance(exc, KeyError):
                    # Known limitation: xrpl-py ships XRPL definitions only, so
                    # it can't re-serialize Xahau fields in a decoded JSON leaf.
                    detail = (
                        f"xrpl-py has no Xahau definition for {exc}; this verifier "
                        "can't check Xahau JSON leaves, verify the binary proof instead"
                    )
                print(f"    [ERROR] {detail}")
                result.ok = False
                result.errors.append(f"step {i}: {tree} tree error: {detail}")
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
                msg = f"{tree} map proof contains no leaves"
                print(f"    [FAIL] {msg}")
                result.ok = False
                result.errors.append(f"step {i}: {msg}")
                continue

            print(f"    [PASS] root {root_hex[:16]}... supported by {root_source}")
            print(f"    leaves: {len(leaves)}")
            for key_hex, leaf_data in leaves:
                if not isinstance(leaf_data, dict):
                    print(f"    leaf {key_hex[:16]}... authenticated, decode failed")

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
                ledger_hashes_leaves = extract_ledger_hashes_leaves(leaves)
                if ledger_hashes_leaves:
                    candidate_count = sum(
                        len(leaf.hashes) for leaf in ledger_hashes_leaves
                    )
                    print(
                        f"    LedgerHashes: {candidate_count} candidate child hashes "
                        f"in {len(ledger_hashes_leaves)} structurally valid leaf/leaves"
                    )

                    containing_sequences = trusted_state_root_sequences.get(root_hex)
                    if not containing_sequences:
                        msg = "state root has no authenticated containing ledger sequence"
                        print(f"    [FAIL] {msg}")
                        result.ok = False
                        result.errors.append(f"step {i}: {msg}")
                        continue

                    for leaf in ledger_hashes_leaves:
                        source = f"LedgerHashes leaf {leaf.key_hex[:16]}... from step {i}"
                        for containing_sequence in containing_sequences:
                            support = LedgerHashesSupport(
                                leaf_key_hex=leaf.key_hex,
                                containing_ledger_sequence=containing_sequence,
                                last_ledger_sequence=leaf.last_ledger_sequence,
                                hashes_count=len(leaf.hashes),
                                source=source,
                            )
                            for child_hash in leaf.hashes:
                                ledger_hashes_candidates[child_hash].append(support)
                else:
                    print("    state proof contains no structurally valid LedgerHashes leaves")

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
# File: XPRV(4) + version(1) + flags(1) + network_id(4 BE)
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

# Binary trie branch types (2 bits each; header byte k holds branches
# 4k..4k+3, low bits first — equivalent to reading a uint32 LE)
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
    """Read a LEB128 varint. Returns (value, new_pos).

    Strict canonical decoding: rejects non-minimal encodings, varints
    longer than 5 bytes, and values that exceed uint32.
    """
    value = 0
    shift = 0
    count = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        count += 1
        if count > 5:
            raise ValueError("LEB128: varint longer than 5 bytes")
        value |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            if count > 1 and b == 0:
                raise ValueError("LEB128: non-minimal encoding")
            if value > 0xFFFFFFFF:
                raise ValueError("LEB128: value exceeds uint32")
            return value, pos
        shift += 7
    raise ValueError("LEB128: unexpected end")


def read_vl(data: bytes, pos: int) -> tuple[bytes, int]:
    """Read a VL-prefixed blob (LEB128 length + bytes)."""
    length, pos = leb128_read(data, pos)
    if length > len(data) - pos:
        raise ValueError("binary proof: truncated VL data")
    return data[pos : pos + length], pos + length


def read_fixed(data: bytes, pos: int, length: int) -> tuple[bytes, int]:
    if pos > len(data) or length > len(data) - pos:
        raise ValueError("binary proof: truncated fixed-size data")
    return data[pos:pos + length], pos + length


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
    # Spec §3.1/§3.8: flag bits 1-7 are reserved and MUST be zero; a verifier
    # MUST reject a proof with any of them set. This check was missing, so a
    # proof with reserved bits set was accepted here while the C++ verifier
    # rejected it — a silent divergence between the two reference verifiers.
    if flags & ~FLAG_ZLIB:
        raise ValueError(
            f"binary proof: reserved flag bits are set (flags=0x{flags:02X})")
    if len(data) < 10:
        raise ValueError("binary proof: header too short")
    net = struct.unpack_from(">I", data, 6)[0]
    return version, flags, net, 10


KNOWN_TLV_TYPES = frozenset(
    {TLV_ANCHOR, TLV_HEADER, TLV_MAP_TX, TLV_MAP_STATE, TLV_UNL}
)


def parse_tlv_records(body: bytes) -> list[tuple[int, bytes]]:
    """Parse TLV body into list of (type, payload), enforcing spec §3.8.

    Previously this accepted anything: an unknown type byte was returned and
    then silently ignored by binary_to_proof's if/elif chain, and a length
    exceeding the remaining bytes was quietly truncated by Python's forgiving
    slice. Both are MUST-reject conditions, and both were rejected by the C++
    parser — so a malformed proof could verify here and fail there.
    """
    records: list[tuple[int, bytes]] = []
    pos = 0
    seen_anchor = False
    seen_unl = False
    while pos < len(body):
        if seen_unl:
            raise ValueError("binary proof: record found after anchor UNL")
        tlv_type = body[pos]
        pos += 1
        length, pos = leb128_read(body, pos)
        # Subtraction form: pos + length could overflow on a hostile varint.
        if length > len(body) - pos:
            raise ValueError(
                "binary proof: TLV length exceeds remaining body bytes")
        if tlv_type not in KNOWN_TLV_TYPES:
            raise ValueError(f"binary proof: unknown TLV type {tlv_type}")
        if tlv_type == TLV_ANCHOR:
            if seen_anchor:
                raise ValueError("binary proof: duplicate anchor core")
            seen_anchor = True
        if tlv_type == TLV_UNL:
            if not seen_anchor:
                raise ValueError("binary proof: anchor UNL precedes anchor core")
            seen_unl = True
        payload = body[pos : pos + length]
        pos += length
        records.append((tlv_type, payload))

    # 0x01 is required; 0x05 is OPTIONAL as of format v6 (spec §3.3) — omitting
    # it is how a verifier with cached or on-chain-pinned trust avoids the blob.
    if not seen_anchor:
        raise ValueError("binary proof: missing anchor core record")
    return records


def decode_bin_header(payload: bytes) -> dict[str, Any]:
    """Decode a 118-byte ledger header TLV payload to dict."""
    if len(payload) != 118:
        raise ValueError("ledger header payload must be exactly 118 bytes")
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
        "ledger_index": seq,
        "total_coins": str(drops),
        "parent_hash": parent_hash,
        "transaction_hash": tx_hash,
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
        if pos != len(payload):
            raise ValueError("trailing bytes in anchor core payload")
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
    if bytes.fromhex(encode(common_fields)) != common_bytes:
        raise ValueError("non-canonical common STObject")

    # Read unique field names
    num_unique, pos = leb128_read(payload, pos)
    unique_names: list[str] = []
    for _ in range(num_unique):
        name_len, pos = leb128_read(payload, pos)
        name, pos = read_fixed(payload, pos, name_len)
        unique_names.append(name.decode("utf-8"))
    if unique_names != sorted(set(unique_names)):
        raise ValueError("non-canonical anchor core field names")

    # Read each validator's key + unique fields, merge with common
    all_fields = []
    previous_key = b""
    for _ in range(val_count):
        key_bytes, pos = read_fixed(payload, pos, 33)
        if key_bytes <= previous_key:
            raise ValueError("anchor core validation keys must be distinct and sorted")
        previous_key = key_bytes
        vkey = key_bytes.hex().upper()
        unique_bytes, pos = read_vl(payload, pos)
        unique_hex = hexlify(unique_bytes).decode()
        unique_fields = decode(unique_hex) if unique_hex else {}
        if bytes.fromhex(encode(unique_fields)) != unique_bytes:
            raise ValueError("non-canonical unique STObject")
        if common_fields.keys() & unique_fields.keys():
            raise ValueError("common and unique validation fields overlap")

        merged = dict(common_fields)
        merged.update(unique_fields)
        if merged.get("SigningPubKey", "").upper() != vkey:
            raise ValueError("binary validation key does not match SigningPubKey")
        all_fields.append(merged)
        full_hex = encode(merged)
        validations[vkey] = full_hex

    if pos != len(payload):
        raise ValueError("trailing bytes in anchor core payload")
    union = set().union(*(fields.keys() for fields in all_fields))
    expected_common = {
        key: all_fields[0][key] for key in union
        if key in all_fields[0] and all(key in fields and fields[key] == all_fields[0][key] for fields in all_fields)
    }
    if common_fields != expected_common or unique_names != sorted(union - expected_common.keys()):
        raise ValueError("non-canonical anchor core decomposition")

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
        raise ValueError("missing anchor UNL shape")

    # Reconstruct blob JSON with exact key ordering
    shape = payload[pos]
    pos += 1
    if shape & 0xF0 or (shape & 7) > 5:
        raise ValueError("invalid anchor UNL shape")
    top_perm = PERMS_3[shape & 0x07]
    val_perm = PERMS_2[(shape >> 3) & 0x01]

    sequence = struct.unpack_from(">I", payload, pos)[0]
    pos += 4
    expiration = struct.unpack_from(">I", payload, pos)[0]
    pos += 4
    validator_count, pos = leb128_read(payload, pos)

    validators: list[tuple[str, str]] = []
    for _ in range(validator_count):
        key_bytes, pos = read_fixed(payload, pos, 33)
        vpk = key_bytes.hex().upper()
        m_bytes, pos = read_vl(payload, pos)
        m_b64 = base64.b64encode(m_bytes).decode()
        validators.append((vpk, m_b64))
    if pos != len(payload):
        raise ValueError("trailing bytes in anchor UNL payload")

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


def hash_binary_trie(data: bytes, pos: int, *, is_tx: bool, depth: int = 0) -> tuple[Hash256, int]:
    """Recursively hash a binary trie. Returns (hash, new_pos).

    Binary trie format: uint32 LE header (16 branches × 2 bits),
    then depth-first children. Branch types: 00=empty, 01=leaf,
    10=inner (recurse), 11=hash (32-byte placeholder).
    """
    if depth >= 64:
        raise ValueError("binary trie: depth exceeds maximum (64)")
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
            key, pos = read_fixed(data, pos, 32)
            data_len, pos = leb128_read(data, pos)
            leaf_data, pos = read_fixed(data, pos, data_len)
            prefix = PREFIX_TX_LEAF if is_tx else PREFIX_STATE_LEAF
            children[branch] = sha512half(prefix + leaf_data + key)
        elif btype == BR_INNER:
            children[branch], pos = hash_binary_trie(data, pos, is_tx=is_tx, depth=depth + 1)
        elif btype == BR_HASH:
            children[branch], pos = read_fixed(data, pos, 32)

    return hash_inner(children), pos


# NB: a `find_binary_trie_leaf` / `_scan_binary_trie_leaf` pair used to live here —
# a third walk over the binary trie, for display, with no call site. Deleted rather
# than kept: an independent display walk that disagrees with the hash walk about
# which nodes exist is exactly the shape the JSON forgery exploited (see
# trie_branches). Leaf enumeration goes through collect_binary_leaves only.


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
            root_hash, end = hash_binary_trie(payload, 0, is_tx=is_tx)
            if end != len(payload):
                raise ValueError("binary trie: trailing bytes")
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

    return {"format_version": version, "network_id": network_id, "steps": steps}


# ─── CLI ─────────────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(description="Verify an xprv proof (JSON or binary)")
    ap.add_argument("proof", help="Path to proof file (.json or .bin)")
    ap.add_argument("--trusted-key", default=None, help="VL publisher key (hex)")
    ap.add_argument(
        "--network",
        type=int,
        default=None,
        help="configured network id (0 = XRPL, 21337 = Xahau); the proof must match",
    )
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
        proof = load_json_proof(raw)

    r = verify(proof, args.trusted_key, args.network)
    sys.exit(0 if r.ok else 1)


if __name__ == "__main__":
    main()
