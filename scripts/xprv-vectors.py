#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["xrpl-py==5.2.0"]
# ///
"""XPRV known-answer test vectors: generate, mutate, and check.

Fills spec Appendix B, which currently says: "Independent implementations cannot
be fully cross-validated until these exist."

Division of labour, and why
---------------------------
C++ owns the canonical binary ENCODER (`to_binary`), so vectors are encoded
there — `xprv convert` — rather than reimplemented here. A second encoder would
be a second thing to keep in sync, and a divergence between them would look
exactly like a divergence between implementations.

This tool independently DERIVES the expected intermediate values using the Python
verifier's own primitives, records them in the vector, and checks them. The C++
gtest consumes the same corpus. So the corpus is a contract neither implementation
authored alone: Python states the intermediates, C++ must reproduce them, and
disagreement fails a build. A generator that also served as its own oracle would
prove nothing.

Vector shapes
-------------
Positive vectors carry a proof plus every intermediate a third-party
implementation needs to localise a bug: the `LWR\\0` header preimage and its
hash, each leaf's hashed bytes and leaf hash, recomputed trie roots, and the
derived skip-list keys.

Negative vectors are stored as a base reference plus explicit byte mutations,
not as whole copies. That keeps the corpus small, but more importantly it makes
each negative self-documenting: the vector states precisely which byte it
corrupts and which rule that violates. Each negative is a landed mutation whose
REJECTION is the assertion — a verifier that accepts it has a hole, and one that
rejects it for the wrong reason is also a finding, which is why `reason_contains`
is recorded.

Accept vectors are the same shape with the opposite assertion: an edit of a
positive vector that a verifier MUST still accept.

The edits. `json_patch` is a list of {op, path, ...} on the base's JSON form:
`set` and `remove` at a JSON pointer, `select` (the value at a pointer becomes
the document) and `duplicate` (the object at `path` gets a second member named
`key`, with `value`; only text can say that, so it is applied to the text).
`mutations` is a list of byte edits on the base's binary form: {at, set_hex},
{insert_at, insert_hex}, {delete_at, delete_len}, {truncate_to}, {append_hex},
{append_zeros: N}, {compress_body: level}, {append_zlib_zeros: N} (one whole
zlib stream of N zero bytes), {drop_last: N} and {xor_last: hex}.

Trust-layer rules cannot be edits of a captured proof: every edit breaks a
real signature. Those vectors are inline. Each carries its whole proof under
`form` (`json`, and `binary_hex` where the proof has a binary form), built by
scripts/xprv_trust_world.py from test keys, and its own `trust`. A runner
checks every form a vector carries. `trust` is {network_id, publisher_key} and
may list `manifests`: base64 raw manifests the verifier holds from outside the
proof (spec §1.3). A derived vector is checked under its base's trust unless
it names its own; no vector does yet.

`reason_contains` is the longest phrase both verifiers give for the refusal.
Where they share no phrase it is the narrowest word they share, and the rule
is then carried by the verdict alone.

A vector may carry `known_failing`, an object naming the verifiers ("cpp",
"python") that do not yet meet its expectation. Each entry says what the
verifier `does` instead (accept, reject, wrong-reason, abnormal or timeout)
and `because` of what. It records a known defect as a test instead of leaving
it out of the corpus.
`mutate` writes such a vector only while the Python verifier really does miss
it, `check` and the C++ gtest skip a vector tagged for their own verifier, and
scripts/xprv-conformance.py holds the tag to account: a tagged verifier that
starts meeting the expectation fails the run until the tag is removed.

Commands
--------
annotate  derive intermediates from a proof and write a positive vector
mutate    generate the accept and negative families from a positive vector
trust     generate the inline trust-world vectors (accept-trust-*, negative-trust-*)
check     verify every vector; assert verdicts AND recorded intermediates

`mutate` and `trust` are deterministic: a second run changes no file. Each
removes a vector it no longer generates.
"""
from __future__ import annotations

import argparse
import base64
import copy
import importlib.util
import io
import json
import os
import logging
import struct
import subprocess
import sys
import tempfile
import zlib
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

LOG = logging.getLogger("xprv-vectors")

REPO = Path(__file__).resolve().parent.parent
VERIFIER = REPO / "scripts" / "verify-xprv.py"
VECTOR_DIR = REPO / "tests" / "xprv" / "vectors"
# The encoder to shell out to; XPRV_BIN overrides the default build tree.
XPRV_BIN = Path(os.environ.get("XPRV_BIN", REPO / "build" / "src" / "xprv" / "xprv"))


def load_verifier():
    """Import the reference Python verifier as a module.

    Hyphenated filename, so importlib rather than a plain import — the same
    pattern authgraph uses for the analyzer.
    """
    spec = importlib.util.spec_from_file_location("xprv_verify", VERIFIER)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot import verifier at {VERIFIER}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["xprv_verify"] = mod
    spec.loader.exec_module(mod)
    return mod


V = None  # set in main() after logging is configured


# ─── intermediates ───────────────────────────────────────────────────


def header_preimage(hdr: dict[str, Any]) -> bytes:
    """LWR\\0 || canonical 118-byte header. Mirrors spec §5."""
    buf = bytes.fromhex("4C575200")
    buf += struct.pack(">I", hdr["ledger_index"])
    buf += struct.pack(">Q", int(hdr["total_coins"]))
    buf += bytes.fromhex(hdr["parent_hash"])
    buf += bytes.fromhex(hdr["transaction_hash"])
    buf += bytes.fromhex(hdr["account_hash"])
    buf += struct.pack(">I", hdr["parent_close_time"])
    buf += struct.pack(">I", hdr["close_time"])
    buf += struct.pack("B", hdr["close_time_resolution"])
    buf += struct.pack("B", hdr["close_flags"])
    return buf


def derive_intermediates(proof: dict[str, Any]) -> dict[str, Any]:
    """Every value a third-party implementation needs to localise a mismatch."""
    out: dict[str, Any] = {"headers": [], "map_proofs": [], "skip_keys": {}}

    for idx, step in enumerate(proof.get("steps", [])):
        kind = step.get("type")

        if kind == "ledger_header":
            hdr = step["header"]
            pre = header_preimage(hdr)
            computed = V.hash_header(hdr)
            assert len(pre) == 118 + 4, f"preimage is {len(pre)} bytes"
            out["headers"].append(
                {
                    "step": idx,
                    "ledger_index": hdr["ledger_index"],
                    "preimage_hex": pre.hex().upper(),
                    "preimage_len": len(pre),
                    "ledger_hash": computed.hex().upper(),
                }
            )

        elif kind == "map_proof":
            is_tx = step.get("tree") == "tx"
            trie = step["trie"]
            root = V.hash_trie(trie, is_tx=is_tx)
            leaves = []
            for key_hex, leaf in V.collect_json_leaves(trie):
                data = V.serialize_leaf(leaf, is_tx=is_tx)
                leaves.append(
                    {
                        "key": key_hex.upper(),
                        "hashed_bytes_hex": data.hex().upper(),
                        "leaf_hash": V.hash_leaf(
                            key_hex, data, is_tx=is_tx
                        ).hex().upper(),
                    }
                )
            out["map_proofs"].append(
                {
                    "step": idx,
                    "tree": step.get("tree"),
                    "trie_root": root.hex().upper(),
                    "leaves": leaves,
                }
            )

    out["skip_keys"]["short"] = V.short_skip_key_hex()
    for h in out["headers"]:
        seq = h["ledger_index"]
        out["skip_keys"][f"long_for_{seq}"] = V.long_skip_key_hex(seq)
    return out


# ─── verdicts ────────────────────────────────────────────────────────


def python_verdict(
    raw: bytes, *, binary: bool = False, trust: dict[str, Any] | None = None
) -> tuple[bool, str]:
    """Run the reference Python verifier, capturing its narrative output.

    `trust` is the vector's configured trust ({network_id, publisher_key}),
    never the key a proof names for itself.
    """
    buf = io.StringIO()
    trust = trust or {}
    # Validator and publisher manifests the verifier holds from outside the
    # proof (§1.3), as base64 raw manifests.
    held = [base64.b64decode(m) for m in trust.get("manifests", [])]
    try:
        with redirect_stdout(buf):
            proof = V.binary_to_proof(raw) if binary or V.is_binary_proof(raw) else V.load_json_proof(raw)
            res = V.verify(proof, trust.get("publisher_key"), trust.get("network_id"), held)
        return bool(res.ok), "; ".join(res.errors) or "ok"
    except Exception as exc:  # a parse refusal is a legitimate reject
        return False, f"{type(exc).__name__}: {exc}"


# ─── negative-vector family ──────────────────────────────────────────


def negative_family(raw: bytes) -> list[dict[str, Any]]:
    """Byte mutations of a valid binary proof, each violating a stated rule.

    Offsets are computed from the real file rather than hardcoded, so the family
    survives a re-capture of the base proof.
    """
    fam: list[dict[str, Any]] = []

    def mut(name: str, rule: str, edits: list[dict[str, Any]], reason: str, layer: str = "parse") -> None:
        fam.append(
            {
                "name": name,
                "violates": rule,
                "layer": layer,
                "mutations": edits,
                "expect": {"verdict": "reject", "reason_contains": reason},
            }
        )

    mut(
        "bad-version",
        f"spec §3.8: a file header version other than 0x{V.FORMAT_VERSION:02X}",
        [{"at": 4, "set_hex": "05"}],
        "version",
    )
    mut(
        "reserved-flag-bit",
        "spec §3.1/§3.8: reserved flag bits MUST be zero",
        [{"at": 5, "set_hex": "02"}],
        "flag",
    )
    # This is a binary-form negative: force binary parsing so the magic check,
    # rather than an incidental JSON/UTF-8 refusal, is the tested oracle.
    mut(
        "bad-magic",
        "spec §3.1: magic identifies the binary container",
        [{"at": 0, "set_hex": "59"}],
        "magic",
    )

    # Offsets below depend on the first TLV record, which starts at byte 10.
    first_type_at = 10
    mut(
        "unknown-tlv-type",
        "spec §3.2/§3.8: unknown TLV types MUST be rejected",
        [{"at": first_type_at, "set_hex": "7F"}],
        "TLV",
    )
    mut(
        "anchor-unl-before-core",
        "spec §3.8: a 0x05 record that precedes 0x01",
        [{"at": first_type_at, "set_hex": "05"}],
        "anchor",
    )
    mut(
        "truncated-body",
        "spec §3.8: a TLV length exceeding the remaining bytes",
        [{"truncate_to": len(raw) - 64}],
        "exceeds",
    )

    # The first record's length, re-encoded one byte longer than it needs.
    length = _leb128(len(V.parse_tlv_records(raw[10:])[0][1]))
    last = first_type_at + len(length)
    mut(
        "nonminimal-leb128",
        "spec §4: decoders MUST reject non-minimal LEB128",
        [
            {"at": last, "set_hex": f"{length[-1] | 0x80:02X}"},
            {"insert_at": last + 1, "insert_hex": "00"},
        ],
        "non-minimal",
    )
    # Without its 0x05 record the anchor has no list. The reference verifiers
    # hold no external one, so this is a missing-trust refusal (§3.3), not a
    # malformed proof.
    mut(
        "missing-list",
        "spec §3.3: a verifier without cached trust MUST reject a 0x05-less proof",
        [{"truncate_to": _record_offsets(raw)[-1][1]}],
        "no external list",
        layer="trust",
    )
    # 21337, big-endian, in the header's network field.
    mut(
        "network-mismatch-binary",
        "spec §8: a proof whose network_id is not the configured network MUST be rejected",
        [{"at": 6, "set_hex": "00005359"}],
        "does not match configured network",
        layer="trust",
    )
    mut(
        "zlib-trailer",
        "spec §3.1: a compressed body is one RFC 1950 zlib stream",
        [{"compress_body": 9}, {"append_hex": "00"}],
        "bytes after the zlib stream",
    )

    # The anchor core (§3.4): 32-byte hash, 33-byte publisher key, a count,
    # then each validation whole, in ascending order of its SigningPubKey.
    records = V.parse_tlv_records(raw[10:])
    core_at = next(i for i, (t, _) in enumerate(records) if t == V.TLV_ANCHOR)
    head, vals = _core_parts(records[core_at][1])
    assert len(vals) >= 2, "the base needs two validations to reorder"

    def core(validations: list[bytes], tail: bytes = b"") -> list[dict[str, Any]]:
        payload = head + _leb128(len(validations)) + b"".join(validations) + tail
        return _replace_payload(raw, core_at, payload)

    order_rule = "spec §3.4: validations are in strictly ascending order of their SigningPubKey"
    mut("anchor-core-unsorted-validations", order_rule,
        core([vals[1], vals[0]] + vals[2:]), "distinct and sorted")
    mut("anchor-core-duplicate-validation", order_rule,
        core([vals[0]] + vals), "distinct and sorted")
    mut("anchor-core-empty-validation",
        "spec §2.1, §3.4: every validation record is a complete canonical STValidation",
        core([b"\x00"] + vals), "canonical STValidation")
    mut("anchor-core-trailing-byte",
        "spec §3.8: a TLV payload is consumed exactly",
        core(vals, b"\x00"), "trailing bytes in anchor core")
    # A core with no validations is well formed: 66 bytes. It fails later,
    # at quorum, which is a trust refusal and not a parse one.
    mut("anchor-core-no-validations",
        "spec §1.3 rule 6, §3.4: an anchor with no validations parses and fails quorum",
        core([]), "quorum not met", layer="trust")

    # The same tree-shape rules in the binary trie (§3.7), on the first map
    # proof: the payload is rebuilt with one node changed and its length fixed.
    map_at = next(i for i, (t, _) in enumerate(records) if t in (V.TLV_MAP_TX, V.TLV_MAP_STATE))
    tree = _trie_parse(records[map_at][1])

    def trie(edit: Any) -> list[dict[str, Any]]:
        changed = copy.deepcopy(tree)
        edit(changed)
        return _replace_payload(raw, map_at, _trie_bytes(changed))

    def first_inner_with_gap(node: list[Any]) -> tuple[list[Any], int]:
        for branch, child in enumerate(node):
            if child is None:
                return node, branch
        for child in node:
            if isinstance(child, list):
                try:
                    return first_inner_with_gap(child)
                except LookupError:
                    pass
        raise LookupError("no inner node has an empty branch")

    def set_gap(value: Any) -> Any:
        def edit(root: list[Any]) -> None:
            node, branch = first_inner_with_gap(root)
            node[branch] = value
        return edit

    def first_of(kind: str, node: list[Any]) -> tuple[list[Any], int]:
        for branch, child in enumerate(node):
            if isinstance(child, tuple) and child[0] == kind:
                return node, branch
        for child in node:
            if isinstance(child, list):
                try:
                    return first_of(kind, child)
                except LookupError:
                    pass
        raise LookupError(kind)

    def hash_to_inner(root: list[Any]) -> None:
        node, branch = first_of("hash", root)
        node[branch] = [("hash", b"\xab" * 32)] + [None] * 15

    def move_leaf_key(root: list[Any]) -> None:
        node, branch = first_of("leaf", root)
        _, key, data = node[branch]
        node[branch] = ("leaf", bytes([key[0] ^ 0x10]) + key[1:], data)

    def drop_leaves(node: list[Any]) -> None:
        for branch, child in enumerate(node):
            if isinstance(child, list):
                drop_leaves(child)
            elif isinstance(child, tuple) and child[0] == "leaf":
                node[branch] = ("hash", b"\xab" * 32)

    shape_rule = "spec §2.3, §3.7: an inner node is present only because a leaf lies beneath it"
    mut("empty-inner-node-binary", shape_rule, trie(set_gap([None] * 16)), "contains no leaves")
    mut("leafless-inner-node-binary", shape_rule, trie(hash_to_inner), "contains no leaves")
    mut("leafless-map-binary", "spec §3.8: a map proof MUST contain at least one authenticated leaf",
        trie(drop_leaves), "contains no leaves")
    mut("zero-hash-placeholder-binary", "spec §2.3, §3.7: an empty branch is absent, never a zero hash",
        trie(set_gap(("hash", bytes(32)))), "zero hash")
    mut("leaf-key-off-its-path-binary", "spec §2.3, §3.7: a leaf sits where its key puts it",
        trie(move_leaf_key), "does not match its trie path")

    # The anchor list (§3.5): manifest, signature and the exact signed list,
    # each length-prefixed and none empty.
    unl_at = next(i for i, (t, _) in enumerate(records) if t == V.TLV_UNL)
    unl = records[unl_at][1]
    manifest, pos = V.read_vl(unl, 0)
    signature, pos = V.read_vl(unl, pos)
    assert V.read_vl(unl, pos)[1] == len(unl)
    mut("anchor-unl-trailing-byte",
        "spec §3.8: a TLV payload is consumed exactly",
        _replace_payload(raw, unl_at, unl + b"\x00"), "trailing bytes in anchor UNL")
    mut("anchor-unl-empty-list",
        "spec §3.5: the manifest, the signature and the list are each non-empty",
        _replace_payload(raw, unl_at, unl[:pos] + b"\x00"),
        "non-empty manifest, list and signature")

    # Record order and count (§3.3, §3.8).
    offsets = _record_offsets(raw)
    assert records[core_at][0] == V.TLV_ANCHOR and core_at == 0 and records[1][0] == V.TLV_HEADER
    anchor_record = raw[offsets[0][1] : offsets[1][1]]
    header_record_len = offsets[2][1] - offsets[1][1]
    mut("anchor-core-not-first", "spec §3.8: the first record is the anchor core",
        [{"delete_at": offsets[0][1], "delete_len": len(anchor_record)},
         {"insert_at": offsets[0][1] + header_record_len, "insert_hex": anchor_record.hex().upper()}],
        "first record must be the anchor core")
    mut("no-records", "spec §3.8: a body carries exactly one anchor core",
        [{"truncate_to": 10}], "missing anchor core record")
    mut("duplicate-anchor-core", "spec §3.8: a body carries exactly one anchor core",
        [{"insert_at": offsets[1][1], "insert_hex": anchor_record.hex().upper()}], "duplicate anchor core")
    core_payload_at = offsets[0][1] + 1 + len(_leb128(len(records[0][1])))
    mut("publisher-key-unknown-prefix-binary", "spec §5.1: a public key is 33 bytes beginning ED, 02 or 03",
        [{"at": core_payload_at + 32, "set_hex": "04"}], "must begin with ED, 02 or 03")

    # Every payload is consumed exactly (§3.8). The anchor records are above.
    mut("header-trailing-byte", "spec §3.6: a ledger header payload is exactly 118 bytes",
        _replace_payload(raw, 1, records[1][1] + b"\x00"), "ledger header payload must be exactly 118 bytes")
    mut("trie-trailing-byte", "spec §3.8: a TLV payload is consumed exactly",
        _replace_payload(raw, map_at, records[map_at][1] + b"\x00"), "binary trie: trailing bytes")

    # The compressed body is one complete zlib stream, bounded while it
    # inflates (§3.1, §8).
    one_stream = "spec §3.1: a compressed body is one complete RFC 1950 zlib stream"
    mut("zlib-truncated", one_stream, [{"compress_body": 9}, {"drop_last": 1}], "truncated zlib body")
    mut("zlib-second-stream", one_stream, [{"compress_body": 9}, {"append_zlib_zeros": 16}],
        "bytes after the zlib stream")
    mut("zlib-bad-checksum", one_stream, [{"compress_body": 9}, {"xor_last": "01"}], "invalid zlib body")
    cap = V.MAX_DECOMPRESSED_PROOF_SIZE
    mut("zlib-over-the-cap", "spec §8: a compressed body is bounded while it inflates",
        [{"truncate_to": 10}, {"at": 5, "set_hex": "01"}, {"append_zlib_zeros": cap + 1}],
        "decompressed size exceeds cap")
    mut("plain-body-over-the-cap", "spec §8: the cap holds before parsing, compressed or not",
        [{"truncate_to": 10}, {"append_zeros": cap + 1}], "body exceeds")
    # The cap is inclusive: a body of exactly the cap is read, and refused for
    # what it holds. Zeros are no record, so the reason is the parser's.
    inclusive = "spec §8: a body of exactly the cap is within it"
    mut("zlib-body-at-the-cap", inclusive,
        [{"truncate_to": 10}, {"at": 5, "set_hex": "01"}, {"append_zlib_zeros": cap}],
        "unknown TLV type 0")
    mut("plain-body-at-the-cap", inclusive, [{"truncate_to": 10}, {"append_zeros": cap}],
        "unknown TLV type 0")
    return fam


def mutation_accept_family(raw: bytes) -> list[dict[str, Any]]:
    """Byte edits of a valid binary proof that a verifier MUST still accept."""
    return [
        {
            "name": f"compressed-level-{level}",
            "states": "spec §3.1: compression is a container option; the same proof verifies at any level",
            "mutations": [{"compress_body": level}],
            "expect": {"verdict": "accept"},
        }
        for level in (1, 9)
    ]


def _leb128(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _core_parts(payload: bytes) -> tuple[bytes, list[bytes]]:
    """An anchor core's fixed head and its validation records, lengths included."""
    count, pos = V.leb128_read(payload, 65)
    records = []
    for _ in range(count):
        size, start = V.leb128_read(payload, pos)
        records.append(payload[pos : start + size])
        pos = start + size
    assert pos == len(payload)
    return payload[:65], records


def _trie_parse(data: bytes, pos: int = 0) -> Any:
    """A binary trie as nested lists of 16 branches.

    A branch is None, ("leaf", key, bytes), ("hash", hash) or another list.
    Used only to build malformed tries for negative vectors; the verifiers
    never see this model.
    """
    def node(pos: int) -> tuple[list[Any], int]:
        header = int.from_bytes(data[pos : pos + 4], "little")
        pos += 4
        branches: list[Any] = [None] * 16
        for branch in range(16):
            code = (header >> (2 * branch)) & 3
            if code == V.BR_LEAF:
                size, start = V.leb128_read(data, pos + 32)
                branches[branch] = ("leaf", data[pos : pos + 32], data[start : start + size])
                pos = start + size
            elif code == V.BR_INNER:
                branches[branch], pos = node(pos)
            elif code == V.BR_HASH:
                branches[branch] = ("hash", data[pos : pos + 32])
                pos += 32
        return branches, pos

    root, end = node(pos)
    assert end == len(data)
    return root


def _trie_bytes(node: list[Any]) -> bytes:
    header, body = 0, bytearray()
    for branch, child in enumerate(node):
        if child is None:
            continue
        if isinstance(child, list):
            header |= V.BR_INNER << (2 * branch)
            body += _trie_bytes(child)
        elif child[0] == "leaf":
            header |= V.BR_LEAF << (2 * branch)
            body += child[1] + _leb128(len(child[2])) + child[2]
        else:
            header |= V.BR_HASH << (2 * branch)
            body += child[1]
    return header.to_bytes(4, "little") + bytes(body)


def _splice(old: bytes, new: bytes, at: int) -> list[dict[str, Any]]:
    """The smallest delete and insert that turn `old`, at file offset `at`, into `new`."""
    lead = 0
    while lead < min(len(old), len(new)) and old[lead] == new[lead]:
        lead += 1
    tail = 0
    while tail < min(len(old), len(new)) - lead and old[-1 - tail] == new[-1 - tail]:
        tail += 1
    gone, came = old[lead : len(old) - tail], new[lead : len(new) - tail]
    ops: list[dict[str, Any]] = []
    if gone:
        ops.append({"delete_at": at + lead, "delete_len": len(gone)})
    if came:
        ops.append({"insert_at": at + lead, "insert_hex": came.hex().upper()})
    return ops


def _replace_payload(raw: bytes, index: int, payload: bytes) -> list[dict[str, Any]]:
    """Mutations that give TLV record `index` a new payload and a matching length."""
    at = _record_offsets(raw)[index][1] + 1
    old = V.parse_tlv_records(raw[10:])[index][1]
    old_len, new_len = _leb128(len(old)), _leb128(len(payload))
    # The payload first: it follows the length, so editing the length
    # afterwards cannot move it.
    return _splice(old, payload, at + len(old_len)) + _splice(old_len, new_len, at)


def _record_offsets(raw: bytes) -> list[tuple[int, int]]:
    """(type, offset) of each TLV record in an uncompressed binary proof."""
    offsets, at = [], 10
    for tlv_type, payload in V.parse_tlv_records(raw[10:]):
        offsets.append((tlv_type, at))
        at += 1 + len(_leb128(len(payload))) + len(payload)
    return offsets


def _leaf_pointer(proof: dict[str, Any], tree: str) -> str:
    """JSON pointer to the first leaf of the last map proof of `tree`."""
    index = max(i for i, s in enumerate(proof["steps"]) if s["type"] == "map_proof" and s["tree"] == tree)
    path, node = f"/steps/{index}/trie", proof["steps"][index]["trie"]
    while isinstance(node, dict):
        key = next(k for k, c in node.items() if k != "__depth__" and not isinstance(c, str))
        path, node = f"{path}/{key}", node[key]
    return path


def _first_where(node: Any, pointer: str, wanted: Any) -> str:
    """JSON pointer to the first trie node, in branch order, that `wanted` accepts."""
    if wanted(node):
        return pointer
    if isinstance(node, dict):
        for key in sorted(k for k in node if k != "__depth__"):
            try:
                return _first_where(node[key], f"{pointer}/{key}", wanted)
            except LookupError:
                pass
    raise LookupError("no such trie node")


def _first_empty_branch(node: dict[str, Any], pointer: str) -> tuple[str, str]:
    """(pointer to an inner node, a branch key it does not have)."""
    for key in "0123456789ABCDEF":
        if key not in node:
            return pointer, key
    for key in "0123456789ABCDEF":
        if isinstance(node[key], dict):
            try:
                return _first_empty_branch(node[key], f"{pointer}/{key}")
            except LookupError:
                pass
    raise LookupError("no inner node has an empty branch")


def _pointer_tokens(pointer: str) -> list[str]:
    return [t.replace("~1", "/").replace("~0", "~") for t in pointer.split("/")[1:]] if pointer else []


def _step(node: Any, token: str) -> Any:
    return node[int(token)] if isinstance(node, list) else node[token]


def apply_json_patch(doc: Any, ops: list[dict[str, Any]]) -> Any:
    """Apply a negative's edits to its base's JSON form.

    `set` writes a value at a JSON pointer (RFC 6901), creating an object member
    if needed; `remove` deletes the member or element at a pointer, which must
    exist; `select` replaces the whole document with the value at a pointer;
    `duplicate` repeats a member name (see patched_text).
    """
    doc = copy.deepcopy(doc)
    duplicates = 0
    for op in ops:
        tokens = _pointer_tokens(op["path"])
        if op["op"] == "select":
            for token in tokens:
                doc = _step(doc, token)
        elif op["op"] == "set":
            parent = doc
            for token in tokens[:-1]:
                parent = _step(parent, token)
            if isinstance(parent, list):
                parent[int(tokens[-1])] = op["value"]
            else:
                parent[tokens[-1]] = op["value"]
        elif op["op"] == "remove":
            parent = doc
            for token in tokens[:-1]:
                parent = _step(parent, token)
            del parent[int(tokens[-1]) if isinstance(parent, list) else tokens[-1]]
        elif op["op"] == "duplicate":
            # A parsed document cannot hold one member twice, so the second
            # copy goes in under a placeholder name that patched_text renames.
            parent = doc
            for token in tokens:
                parent = _step(parent, token)
            if op["key"] not in parent:
                raise SystemExit(f"json_patch duplicate: {op['path']} has no member {op['key']!r}")
            parent[_duplicate_placeholder(duplicates)] = op["value"]
            duplicates += 1
        else:
            raise SystemExit(f"unknown json_patch op {op['op']!r}")
    return doc


def _duplicate_placeholder(n: int) -> str:
    return f"__xprv_duplicate_member_{n}__"


def patched_text(doc: Any, ops: list[dict[str, Any]]) -> str:
    """The JSON text a patched vector hands a verifier.

    Everything but `duplicate` is an edit of the parsed document. `duplicate`
    writes a member name a second time, which only text can carry: the object
    at `path` gets a second member named `key`, with `value`.
    """
    text = json.dumps(apply_json_patch(doc, ops))
    for n, op in enumerate(o for o in ops if o["op"] == "duplicate"):
        placeholder = json.dumps(_duplicate_placeholder(n))
        if text.count(placeholder) != 1:
            raise SystemExit("json_patch duplicate: placeholder not found exactly once")
        text = text.replace(placeholder, json.dumps(op["key"]))
    return text


def patch_negative_family(base: dict[str, Any], base_name: str) -> list[dict[str, Any]]:
    """Negatives stated as edits of a positive vector's JSON form.

    These attacks live in JSON structure or in the anchor's contents, so no
    byte patch of the binary form expresses them. Like a byte mutation, each
    names exactly what it changes.
    """
    proof = base["form"]["json"]
    fam: list[dict[str, Any]] = []

    def neg(name: str, rule: str, layer: str, reason: str, ops: list[dict[str, Any]],
            known_failing: dict[str, dict[str, str]] | None = None) -> None:
        entry = {
            "name": name,
            "violates": rule,
            "layer": layer,
            "base": base_name,
            "json_patch": ops,
            "expect": {"verdict": "reject", "reason_contains": reason},
        }
        if known_failing:
            entry["known_failing"] = known_failing
        fam.append(entry)

    def at(path: str, value: Any) -> dict[str, Any]:
        return {"op": "set", "path": path, "value": value}

    first_map = next(i for i, s in enumerate(proof["steps"]) if s["type"] == "map_proof")
    state_leaf, tx_leaf = _leaf_pointer(proof, "state"), _leaf_pointer(proof, "tx")
    validations = proof["steps"][0]["validations"]

    neg("bare-list", "spec §2: a proof is the versioned object; a steps array alone has no version",
        "parse", "object", [{"op": "select", "path": "/steps"}])
    neg("unknown-member", "D12: the JSON form is closed; every member is part of the proof",
        "parse", "unknown member note", [at("/note", "x")])
    neg("integral-float-version", "D12: integer fields are integer tokens",
        "parse", "format_version", [at("/format_version", float(proof["format_version"]))])
    neg("malformed-validation-value", "spec §2.1: validations map signing keys to hex STValidation bytes",
        "parse", "validation value must be uppercase hex", [at("/steps/0/validations/ED" + "00" * 32, "ZZ")])
    neg("wrong-depth", "spec §2.3: __depth__ MUST equal the node's nesting depth",
        "parse", "__depth__", [at(f"/steps/{first_map}/trie/__depth__", 17)])
    neg("unknown-leaf-member", "spec §2.3: leaf data is the decoded content and nothing else",
        "graph", "not a serialized field", [at(f"{state_leaf}/1/DisplayedButNotHashed", 1)])

    recorded = base["intermediates"]["map_proofs"][-1]
    assert recorded["tree"] == "tx" and len(recorded["leaves"]) == 1
    neg("leafless-map", "spec §3.8: a map proof MUST contain at least one authenticated leaf",
        "parse", "contains no leaves", [at(tx_leaf, recorded["leaves"][0]["leaf_hash"])])

    neg("failed-quorum", "spec §1.3 rule 6: quorum is ceil(0.8 x N) masters",
        "trust", "quorum not met", [at("/steps/0/validations", dict(list(validations.items())[:5]))])

    # Enough corrupted signatures to drop the votes below quorum: a verifier
    # that skipped signature checks would still count them and accept.
    forged = []
    for key in list(validations)[:5]:
        fields = V.decode(validations[key])
        sig = fields["Signature"]
        fields["Signature"] = sig[:-2] + f"{int(sig[-2:], 16) ^ 1:02X}"
        forged.append(at(f"/steps/0/validations/{key}", V.encode(fields).upper()))
    neg("forged-validation-signatures", "spec §5: every counted validation's signature MUST verify",
        "trust", "quorum not met", forged)

    other_network = next(n for n in V.PUBLISHER_KEY_BY_NETWORK if n != base["trust"]["network_id"])
    neg("network-mismatch", "spec §8: a proof whose network_id is not the configured network MUST be rejected",
        "trust", "does not match configured network", [at("/network_id", other_network)])
    other = next(k for k in V.PUBLISHER_KEY_BY_NETWORK.values() if k != base["trust"]["publisher_key"])
    neg("publisher-key-mismatch", "spec §8: the anchor's publisher key MUST be the configured one",
        "trust", "publisher key mismatch", [at("/steps/0/unl/public_key", other)])

    neg("incomplete-validation", "spec §2.1: every validations entry is a canonical STValidation",
        "parse", "canonical STValidation",
        [at("/steps/0/validations/" + "00" * 33, "00")])
    neg("trie-root-leaf", "spec §2.3: a JSON map_proof root is an inner node",
        "parse", "inner node",
        [at(f"/steps/{first_map}/trie", ["A" * 64, {}])])

    # A field the transaction helper would drop must stay in the validation
    # preimage. Enough injected signatures fail to put the vote below quorum.
    injected = []
    for key in list(validations)[:5]:
        fields = V.decode(validations[key])
        fields["TxnSignature"] = "DEADBEEF"
        injected.append(at(f"/steps/0/validations/{key}", V.encode(fields).upper()))
    neg("validation-unsigned-field",
        "spec §5.1: validation signing data keeps every field except Signature",
        "trust", "quorum not met", injected)

    neg("anchor-ledger-index", "spec §2, §2.1: the anchor is a hash; it carries no ledger index",
        "parse", "unknown member", [at("/steps/0/ledger_index", 1)])
    neg("no-validations", "spec §1.3 rule 6: an anchor with no validations fails quorum",
        "trust", "quorum not met", [at("/steps/0/validations", {})])

    # §2: hex has one spelling, uppercase, in every member of the proof's own.
    def lowered(pointer: str) -> dict[str, Any]:
        node: Any = proof
        for token in _pointer_tokens(pointer):
            node = _step(node, token)
        assert node != node.lower(), f"{pointer} has no letter to lowercase"
        return at(pointer, node.lower())

    header_at = next(i for i, s in enumerate(proof["steps"]) if s["type"] == "ledger_header")
    some_validation = next(k for k, val in validations.items() if val != val.lower())
    hash_at = _first_where(proof["steps"][first_map]["trie"], f"/steps/{first_map}/trie",
                           lambda n: isinstance(n, str))
    hex_rule = "spec §2: hex in the JSON form is uppercase"
    neg("lowercase-anchor-hash", hex_rule, "parse", "uppercase hex", [lowered("/steps/0/ledger_hash")])
    neg("lowercase-publisher-key", hex_rule, "parse", "uppercase hex", [lowered("/steps/0/unl/public_key")])
    neg("lowercase-list-signature", hex_rule, "parse", "uppercase hex", [lowered("/steps/0/unl/signature")])
    neg("lowercase-validation", hex_rule, "parse", "uppercase hex",
        [lowered(f"/steps/0/validations/{some_validation}")])
    neg("lowercase-header-hash", hex_rule, "parse", "uppercase hex",
        [lowered(f"/steps/{header_at}/header/account_hash")])
    neg("lowercase-trie-hash", hex_rule, "parse", "uppercase hex", [lowered(hash_at)])
    neg("lowercase-leaf-key", hex_rule, "parse", "uppercase hex", [lowered(f"{state_leaf}/0")])

    neg("total-coins-leading-zero",
        "spec §2.2: total_coins is the decimal text of a uint64 with no leading zeros",
        "parse", "total_coins must be",
        [at(f"/steps/{header_at}/header/total_coins", "0" + proof["steps"][header_at]["header"]["total_coins"])])

    # §2.3: a tree has one shape. Each of these keeps the root and changes
    # only how the same tree is written.
    inner_at, free = _first_empty_branch(proof["steps"][first_map]["trie"], f"/steps/{first_map}/trie")
    shape_rule = "spec §2.3: an inner node is present only because a leaf lies beneath it"
    neg("empty-inner-node", shape_rule, "parse", "contains no leaves", [at(f"{inner_at}/{free}", {})])
    neg("leafless-inner-node", shape_rule, "parse", "contains no leaves",
        [at(hash_at, {"0": "AB" * 32})])
    neg("zero-hash-placeholder", "spec §2.3: an empty branch is absent, never a zero hash",
        "parse", "zero hash", [at(f"{inner_at}/{free}", "0" * 64)])
    state_key = proof
    for token in _pointer_tokens(f"{state_leaf}/0"):
        state_key = _step(state_key, token)
    moved = ("1" if state_key[0] == "0" else "0") + state_key[1:]
    neg("leaf-key-off-its-path", "spec §2.3: a leaf sits where its key puts it",
        "parse", "does not match its trie path", [at(f"{state_leaf}/0", moved)])

    # §2.3 gives a plain field's value two JSON types. A codec that skips a
    # null member hashes the leaf as if it were absent, so the reader sees a
    # member nothing authenticated.
    value_rule = "spec §2.3: a leaf field that is not structured has a string or a number as its value"
    neg("null-leaf-value", value_rule, "graph", "must be a string or a number",
        [at(f"{tx_leaf}/1/tx/InvoiceID", None)],
        known_failing={
            "python": {"does": "accept", "because": "xrpl-py drops a null member, so the leaf hashes as if it were absent"},
            "cpp": {"does": "wrong-reason", "because": "refuses through a codec type error, not through the §2.3 value rule"},
        })
    neg("bool-leaf-value", value_rule, "graph", "must be a string or a number",
        [at(f"{state_leaf}/1/Flags", False)],
        known_failing={
            "python": {"does": "accept", "because": "false and true serialize as 0 and 1"},
            "cpp": {"does": "wrong-reason", "because": "refuses through a codec type error, not through the §2.3 value rule"},
        })
    neg("tx-not-an-object", "spec §2.3: a transaction leaf's tx and meta are the decoded objects",
        "graph", "tx must be an object", [at(f"{tx_leaf}/1/tx", 123)])
    # A verifier that describes a leaf it has just refused can trip over it.
    neg("tx-type-not-a-string", "spec §2.3: a leaf is refused, never fatal, when a member has the wrong JSON type",
        "graph", "", [at(f"{tx_leaf}/1/tx/TransactionType", [])])

    def value_at(pointer: str) -> Any:
        node: Any = proof
        for token in _pointer_tokens(pointer):
            node = _step(node, token)
        return node

    def remove(path: str) -> dict[str, Any]:
        return {"op": "remove", "path": path}

    # ── the closed form, at every level (§2, D12) ────────────────────
    closed = "spec §2: the JSON form is closed; an object carries its listed members and no other"
    for what, pointer in (("anchor", "/steps/0"), ("ledger_header step", f"/steps/{header_at}"),
                          ("header", f"/steps/{header_at}/header"), ("map_proof step", f"/steps/{first_map}")):
        neg("unknown-member-in-" + what.replace(" ", "-").replace("_", "-"), closed, "parse",
            f"unknown member note in {what}", [at(f"{pointer}/note", "x")])
    # The Python verifier names this member as its own (it carries a binary
    # payload under it internally); to the C++ one it is any unknown member.
    neg("reserved-binary-member", closed, "parse", "map_proof",
        [at(f"/steps/{first_map}/__binary_root__", "00" * 32)])

    # ── the envelope and the steps (§2) ──────────────────────────────
    envelope = "spec §2: a proof is an object with format_version, network_id and steps"
    neg("missing-network-id", envelope, "parse", "network_id", [remove("/network_id")])
    neg("boolean-network-id", "spec §2: integer fields are integer tokens", "parse",
        "proof network_id must be an integer",
        [at("/network_id", False)])
    neg("missing-format-version", envelope, "parse", "format_version", [remove("/format_version")])
    neg("unknown-format-version", "spec §9.1: an unrecognized version is refused", "parse", "format_version 999",
        [at("/format_version", 999)])
    neg("unknown-step-type", "spec §2.4: an unknown step type", "parse", "unknown step type",
        [at(f"/steps/{header_at}/type", "banana")])
    neg("step-not-an-object", "spec §2: a step is an object", "parse", "step must be an object",
        [at(f"/steps/{header_at}", 5)])
    neg("unknown-tree", "spec §2.3: tree is tx or state", "parse", "unknown map tree type",
        [at(f"/steps/{first_map}/tree", "banana")])
    one_anchor = "spec §1: a proof contains exactly one anchor, and it is the first step"
    anchor_step, header_step = value_at("/steps/0"), value_at(f"/steps/{header_at}")
    neg("no-anchor", one_anchor, "graph", "exactly one anchor", [remove("/steps/0")])
    neg("two-anchors", one_anchor, "graph", "exactly one anchor", [at(f"/steps/{header_at}", anchor_step)])
    neg("anchor-not-first", one_anchor, "graph", "anchor must be the first",
        [at("/steps/0", header_step), at(f"/steps/{header_at}", anchor_step)])

    # ── header fields (§2.2) ─────────────────────────────────────────
    header_ptr = f"/steps/{header_at}/header"
    header = value_at(header_ptr)
    numbers = "spec §2: integer fields are integer tokens in range"
    neg("total-coins-a-number", "spec §2.2: total_coins is decimal text", "parse", "total_coins must be",
        [at(f"{header_ptr}/total_coins", int(header["total_coins"]))])
    neg("total-coins-with-junk", "spec §2.2: total_coins is decimal text", "parse", "total_coins must be",
        [at(f"{header_ptr}/total_coins", "100junk")])
    neg("close-flags-out-of-range", numbers, "parse", "header close_flags must be",
        [at(f"{header_ptr}/close_flags", 256)])
    neg("close-time-resolution-out-of-range", numbers, "parse", "header close_time_resolution must be",
        [at(f"{header_ptr}/close_time_resolution", 256)])
    neg("ledger-index-a-float", numbers, "parse", "header ledger_index must be",
        [at(f"{header_ptr}/ledger_index", float(header["ledger_index"]))])
    neg("header-hash-with-a-space", "spec §2: a hash is 64 uppercase hex digits", "parse",
        "header parent_hash must be uppercase hex",
        [at(f"{header_ptr}/parent_hash", "AB CD")])

    # ── the anchor's members (§2.1) ──────────────────────────────────
    publisher_key = value_at("/steps/0/unl/public_key")
    neg("publisher-key-unknown-prefix", "spec §5.1: a public key is 33 bytes beginning ED, 02 or 03",
        "parse", "must begin with ED, 02 or 03", [at("/steps/0/unl/public_key", "04" + publisher_key[2:])])
    # "malformed validation key or value" in one verifier and "validation
    # value must be a hex string" in the other: they share only the noun.
    neg("null-validation-value", "spec §2.1: a validation value is hex text", "parse", "validation",
        [at("/steps/0/validations/ED" + "00" * 32, None)])
    moved_key = next(iter(validations))
    neg("validation-under-a-foreign-key", "spec §2.1: a validation sits under its own SigningPubKey",
        "parse", "does not match SigningPubKey",
        [remove(f"/steps/0/validations/{moved_key}"),
         at("/steps/0/validations/ED" + "AB" * 32, validations[moved_key])])
    # The verifiers word this one differently; they share only the noun.
    neg("lowercase-validation-keys", "spec §2.1: a validations key is uppercase hex", "parse", "validation",
        [at("/steps/0/validations", {key.lower(): value for key, value in validations.items()})])

    # ── trie node grammar (§2.3) ─────────────────────────────────────
    node_rule = "spec §2.3: a trie node is a hash string, a [key, data] leaf or an inner-node object"
    trie_at = f"/steps/{first_map}/trie"
    neg("null-trie-branch", node_rule, "parse", "trie", [at(f"{inner_at}/{free}", None)])
    neg("numeric-trie-branch", node_rule, "parse", "trie", [at(f"{inner_at}/{free}", 17)])
    neg("malformed-trie-placeholder", node_rule, "parse", "placeholder must be a 32-byte uppercase hex hash",
        [at(f"{inner_at}/{free}", "BAD")])
    neg("leaf-with-three-elements", node_rule, "parse", "leaf must be exactly [key, data]",
        [at(state_leaf, value_at(state_leaf) + ["ignored"])])
    neg("non-nibble-trie-member", "spec §2.3: an inner node's members are its branch keys", "parse", "branch keys",
        [at(f"{trie_at}/10", "AB" * 32)])
    neg("trie-root-hash", "spec §2.3: a JSON map_proof root is an inner node", "parse", "root must be an inner node",
        [at(trie_at, "AB" * 32)])
    neg("hex-leaf-data", "spec §2.3: JSON leaf data is the decoded object, never hex", "parse",
        "leaf data must be a decoded object, not hex",
        [at(f"{tx_leaf}/1", "ABCDEF")])

    # ── members a codec would display and not hash (§2.3) ────────────
    unhashed = "spec §2.3: leaf data is the decoded content and nothing else"
    tx_content = value_at(f"{tx_leaf}/1")
    wrapper = next(iter(tx_content["meta"]["AffectedNodes"][0]))
    neg("leaf-index-member", unhashed, "graph", "not a serialized field",
        [at(f"{state_leaf}/1/index", value_at(f"{state_leaf}/0"))])
    neg("tx-hash-member", unhashed, "graph", "not a serialized field",
        [at(f"{tx_leaf}/1/tx/hash", value_at(f"{tx_leaf}/0"))])
    neg("meta-unknown-member", unhashed, "graph", "not a serialized field",
        [at(f"{tx_leaf}/1/meta/not_a_field", "1")])
    neg("nested-unknown-member", unhashed, "graph", "not a serialized field",
        [at(f"{tx_leaf}/1/meta/AffectedNodes/0/{wrapper}/DisplayedButNotHashed", 1)])
    neg("tx-leaf-wrapper-member", "spec §2.3: a transaction leaf has exactly the members tx and meta",
        "graph", "a tx leaf must be exactly {tx, meta}", [at(f"{tx_leaf}/1/hash", value_at(f"{tx_leaf}/0"))])

    # ── a member name written twice (§2.4): only the text can say it ──
    twice = "spec §2.4: no object in a proof repeats a member"
    neg("duplicate-validation-member", twice, "parse", "duplicate",
        [{"op": "duplicate", "path": "/steps/0/validations", "key": moved_key, "value": "00"}])
    branch = next(key for key in value_at(trie_at) if key != "__depth__")
    neg("duplicate-trie-branch", twice, "parse", "duplicate",
        [{"op": "duplicate", "path": trie_at, "key": branch, "value": "AB" * 32}])
    return fam


def patch_accept_family(base: dict[str, Any], base_name: str) -> list[dict[str, Any]]:
    """Edits of a positive vector's JSON form that a verifier MUST still accept."""
    proof = base["form"]["json"]

    def depth_pointers(node: Any, pointer: str) -> list[str]:
        found = [f"{pointer}/__depth__"] if "__depth__" in node else []
        for key, child in node.items():
            if isinstance(child, dict):
                found += depth_pointers(child, f"{pointer}/{key}")
        return found

    diagnostics = [
        pointer
        for i, step in enumerate(proof["steps"]) if step["type"] == "map_proof"
        for pointer in depth_pointers(step["trie"], f"/steps/{i}/trie")
    ]
    assert diagnostics, "the base carries no __depth__ to remove"
    return [{
        "name": "no-depth-diagnostics",
        "states": "spec §2.3: __depth__ is an optional diagnostic; a trie without any verifies",
        "base": base_name,
        "json_patch": [{"op": "remove", "path": pointer} for pointer in diagnostics],
        "expect": {"verdict": "accept"},
    }]


def apply_mutations(raw: bytes, muts: list[dict[str, Any]]) -> bytes:
    buf = bytearray(raw)
    for m in muts:
        for name in ("at", "insert_at", "delete_at", "delete_len", "truncate_to"):
            if name in m and int(m[name]) < 0:
                raise SystemExit(f"mutation {name} is negative")
        if "compress_body" in m:
            # Any valid stream will do: what is tested is what follows it.
            buf = buf[:5] + bytes([buf[5] | 1]) + buf[6:10] + zlib.compress(bytes(buf[10:]), int(m["compress_body"]))
            continue
        if "append_hex" in m:
            buf += bytes.fromhex(m["append_hex"])
            continue
        if "append_zeros" in m:
            # This many zero bytes: a body too large to store in a vector.
            buf += bytes(int(m["append_zeros"]))
            continue
        if "append_zlib_zeros" in m:
            # A whole RFC 1950 stream that inflates to this many zero bytes:
            # too large an input to store, and any valid stream will do.
            buf += zlib.compress(bytes(int(m["append_zlib_zeros"])), 9)
            continue
        if "drop_last" in m:
            count = int(m["drop_last"])
            if count < 0 or count > len(buf):
                raise SystemExit(f"drop_last {count} exceeds file length {len(buf)}")
            del buf[len(buf) - count :]
            continue
        if "xor_last" in m:
            if not buf:
                raise SystemExit("xor_last on an empty file")
            buf[-1] ^= int(m["xor_last"], 16)
            continue
        if "truncate_to" in m:
            buf = buf[: int(m["truncate_to"])]
            continue
        if "insert_at" in m:
            at = int(m["insert_at"])
            if at > len(buf):
                raise SystemExit(f"insertion at {at} exceeds file length {len(buf)}")
            buf[at:at] = bytes.fromhex(m["insert_hex"])
            continue
        if "delete_at" in m:
            at, count = int(m["delete_at"]), int(m["delete_len"])
            if at + count > len(buf):
                raise SystemExit(f"deletion at {at} exceeds file length {len(buf)}")
            del buf[at : at + count]
            continue
        at = int(m["at"])
        patch = bytes.fromhex(m["set_hex"])
        if at + len(patch) > len(buf):
            raise SystemExit(f"mutation at {at} exceeds file length {len(buf)}")
        buf[at : at + len(patch)] = patch
    return bytes(buf)


# ─── commands ────────────────────────────────────────────────────────


def xprv_convert(src: Path, dst: Path, *, gzip: bool = False) -> None:
    """Shell out to the C++ encoder. It owns the canonical binary form."""
    if not XPRV_BIN.exists():
        raise SystemExit(
            f"{XPRV_BIN} not built — run: cmake --build build --target xprv"
        )
    cmd = [str(XPRV_BIN), "convert", str(src), str(dst)]
    if gzip:
        cmd.append("--gzip")
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0 or not dst.exists():
        raise SystemExit(f"xprv convert failed: {r.stdout.strip()} {r.stderr.strip()}")


def cmd_annotate(args: argparse.Namespace) -> int:
    src = Path(args.proof)
    raw = src.read_bytes()

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        # Intermediates need the JSON form: the binary trie decoder yields only a
        # precomputed root, not the decoded leaves a third-party implementation
        # must be able to reproduce. Convert if we were handed binary.
        if V.is_binary_proof(raw):
            json_path = tmp / "proof.json"
            xprv_convert(src, json_path)
        else:
            json_path = src
        proof = json.loads(json_path.read_text())

        # Canonical binary comes from the C++ encoder, never from here.
        bin_path = tmp / "proof.bin"
        xprv_convert(json_path, bin_path)
        bin_raw = bin_path.read_bytes()

        trust = {
            "network_id": proof.get("network_id"),
            "publisher_key": V.PUBLISHER_KEY_BY_NETWORK[proof.get("network_id")],
        }
        json_ok, json_reason = python_verdict(json_path.read_bytes(), trust=trust)
        bin_ok, bin_reason = python_verdict(bin_raw, trust=trust)

    if not (json_ok and bin_ok) and not args.allow_failing:
        LOG.error("refusing to record a positive vector that does not verify "
                  "(json=%s, binary=%s). Use --allow-failing to override.",
                  "ok" if json_ok else json_reason,
                  "ok" if bin_ok else bin_reason)
        return 1
    # Spec §8: both forms MUST verify identically. Assert it here, since this is
    # the one place both forms of the same proof exist side by side.
    if json_ok != bin_ok:
        LOG.error("FORM DIVERGENCE: json=%s binary=%s — spec §8 requires both "
                  "forms of one proof to verify identically", json_ok, bin_ok)
        return 1

    vec = {
        "name": args.name,
        "description": args.description,
        "spec_version": proof.get("format_version"),
        "network_id": proof.get("network_id"),
        "expect": {"verdict": "accept"},
        # Configured trust for checking this vector: the network's publisher
        # key from the verifier's own table, not the key the proof names.
        "trust": {
            "network_id": proof.get("network_id"),
            "publisher_key": V.PUBLISHER_KEY_BY_NETWORK[proof.get("network_id")],
        },
        "form": {},
        "intermediates": derive_intermediates(proof),
        "provenance": {
            "derived_by": "scripts/xprv-vectors.py (Python primitives)",
            "note": "intermediates derived independently of the C++ encoder; "
                    "the C++ gtest must reproduce them",
        },
    }
    vec["form"]["json"] = proof
    vec["form"]["binary_hex"] = bin_raw.hex().upper()
    vec["form"]["binary_len"] = len(bin_raw)
    vec["expect"]["both_forms_verify"] = True

    VECTOR_DIR.mkdir(parents=True, exist_ok=True)
    out = VECTOR_DIR / f"{args.name}.json"
    out.write_text(json.dumps(vec, indent=2, sort_keys=False) + "\n")
    h = vec["intermediates"]["headers"]
    m = vec["intermediates"]["map_proofs"]
    LOG.info("wrote %s (%d headers, %d map proofs, %d leaves)",
             out.relative_to(REPO), len(h), len(m),
             sum(len(x["leaves"]) for x in m))
    return 0


def cmd_mutate(args: argparse.Namespace) -> int:
    base_path = VECTOR_DIR / f"{args.base}.json"
    base = json.loads(base_path.read_text())
    bin_hex = base.get("form", {}).get("binary_hex")
    if not bin_hex:
        LOG.error("base vector %s has no binary form to mutate", args.base)
        return 1
    raw = bytes.fromhex(bin_hex)

    written = 0
    for derived in patch_accept_family(base, args.base) + patch_negative_family(base, args.base):
        # Compared as text: 10 and 10.0 are equal numbers and different tokens.
        if patched_text(base["form"]["json"], derived["json_patch"]) == json.dumps(base["form"]["json"]):
            LOG.error("vector %r patches its base into itself; it would assert nothing", derived["name"])
            return 1
    for acc in patch_accept_family(base, args.base):
        data = patched_text(base["form"]["json"], acc["json_patch"]).encode()
        ok, reason = python_verdict(data, trust=base["trust"])
        if not ok:
            LOG.error("accept vector %r was REFUSED by the reference verifier (%s); "
                      "not writing a vector that asserts a false claim", acc["name"], reason)
            return 1
        out = VECTOR_DIR / f"accept-{acc['name']}.json"
        out.write_text(json.dumps(acc, indent=2) + "\n")
        LOG.info("wrote %s (accepted)", out.name)
        written += 1
    for neg in patch_negative_family(base, args.base):
        data = patched_text(base["form"]["json"], neg["json_patch"]).encode()
        ok, reason = python_verdict(data, trust=base["trust"])
        tagged = "python" in neg.get("known_failing", {})
        want = neg["expect"]["reason_contains"].lower()
        met = not ok and want in reason.lower()
        if tagged and met:
            LOG.error("negative %r is tagged known_failing for python, but the reference "
                      "verifier now refuses it for its reason; remove the tag", neg["name"])
            return 1
        if ok and not tagged:
            LOG.error("inline negative %r was ACCEPTED by the reference verifier; "
                      "not writing a vector that asserts a false claim", neg["name"])
            return 1
        neg["observed"] = {"python_reason": "accepted" if ok else reason}
        out = VECTOR_DIR / f"negative-{neg['name']}.json"
        out.write_text(json.dumps(neg, indent=2) + "\n")
        LOG.info("wrote %s (rejected: %s)", out.name, reason[:60])
        written += 1
    for acc in mutation_accept_family(raw):
        mutated = apply_mutations(raw, acc["mutations"])
        ok, reason = python_verdict(mutated, binary=True, trust=base["trust"])
        if mutated == raw or not ok:
            LOG.error("accept vector %r %s; not writing a vector that asserts a false claim",
                      acc["name"], "changes nothing" if mutated == raw else f"was REFUSED ({reason})")
            return 1
        acc["base"] = args.base
        out = VECTOR_DIR / f"accept-{acc['name']}.json"
        out.write_text(json.dumps(acc, indent=2) + "\n")
        LOG.info("wrote %s (accepted)", out.name)
        written += 1
    for neg in negative_family(raw):
        mutated = apply_mutations(raw, neg["mutations"])
        ok, reason = python_verdict(mutated, binary=True, trust=base["trust"])
        if ok:
            LOG.error("mutation %r was ACCEPTED by the reference verifier — "
                      "either the mutation does not land or there is a real "
                      "hole; not writing a vector that asserts a false claim",
                      neg["name"])
            return 1
        neg["base"] = args.base
        neg["observed"] = {"python_reason": reason}
        out = VECTOR_DIR / f"negative-{neg['name']}.json"
        out.write_text(json.dumps(neg, indent=2) + "\n")
        LOG.info("wrote %s (rejected: %s)", out.name, reason[:60])
        written += 1
    # A vector this base no longer generates would keep asserting a rule
    # nobody maintains.
    expected = {
        f"accept-{v['name']}.json"
        for v in patch_accept_family(base, args.base) + mutation_accept_family(raw)
    } | {
        f"negative-{v['name']}.json"
        for v in patch_negative_family(base, args.base) + negative_family(raw)
    }
    for stale in sorted(VECTOR_DIR.glob("*.json")):
        if stale.name not in expected and json.loads(stale.read_text()).get("base") == args.base:
            stale.unlink()
            LOG.info("removed %s (no longer generated)", stale.name)
    LOG.info("%d derived vectors written", written)
    return 0


def load_trust_world() -> Any:
    """scripts/xprv_trust_world.py, bound to the verifier already loaded."""
    path = REPO / "scripts" / "xprv_trust_world.py"
    spec = importlib.util.spec_from_file_location("xprv_trust_world", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot import {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["xprv_trust_world"] = mod
    spec.loader.exec_module(mod)
    mod.use(V)
    return mod


def cmd_trust(args: argparse.Namespace) -> int:
    """Write the synthetic trust world as inline vectors.

    Each carries its whole proof and its own trust: the test publisher key,
    and any manifests the verifier is to hold from outside the proof. A proof
    that parses is written in both forms; the binary one comes from the
    world's own writer, checked here against `xprv convert` wherever the C++
    encoder can be asked for the same proof.
    """
    world = load_trust_world()
    wanted: dict[str, dict[str, Any]] = {}
    compared = 0
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        for vec in world.cases():
            name = vec["name"]
            accept = vec["expect"]["verdict"] == "accept"
            form = vec["form"]
            # A JSON trie has decoded leaves; only the C++ encoder turns those
            # into a binary trie, so such a proof stays JSON here.
            plain = "json" in form and all(s["type"] != "map_proof" for s in form["json"]["steps"])
            if plain and (accept or vec["layer"] != "parse"):
                form["binary_hex"] = world.binary_proof(form["json"]).hex().upper()
                if accept and XPRV_BIN.exists():
                    src, dst = tmp / "proof.json", tmp / "proof.bin"
                    src.write_text(json.dumps(form["json"]))
                    xprv_convert(src, dst)
                    if dst.read_bytes().hex().upper() != form["binary_hex"]:
                        LOG.error("%s: the world's binary writer and xprv convert disagree", name)
                        return 1
                    dst.unlink()
                    compared += 1

            reasons = []
            met = True
            for which in ("json", "binary_hex"):
                if which not in form:
                    continue
                raw = bytes.fromhex(form[which]) if which == "binary_hex" else json.dumps(form[which]).encode()
                ok, reason = python_verdict(raw, binary=which == "binary_hex", trust=vec["trust"])
                reasons.append("accepted" if ok else reason)
                if accept:
                    met = met and ok
                else:
                    met = met and not ok and vec["expect"]["reason_contains"].lower() in reason.lower()
            tagged = "python" in vec.get("known_failing", {})
            if tagged and met:
                LOG.error("%s is tagged known_failing for python, but the reference verifier "
                          "now meets it; remove the tag", name)
                return 1
            if not tagged and not met:
                LOG.error("%s: the reference verifier does not meet this vector (%s); "
                          "not writing a vector that asserts a false claim",
                          name, " | ".join(r[:120] for r in reasons))
                return 1
            if not accept:
                vec["observed"] = {"python_reason": reasons[0]}
            wanted[f"{'accept' if accept else 'negative'}-{name}.json"] = vec

    for stale in sorted(VECTOR_DIR.glob("*-trust-*.json")):
        if stale.name not in wanted:
            stale.unlink()
            LOG.info("removed %s (no longer generated)", stale.name)
    for filename, vec in wanted.items():
        (VECTOR_DIR / filename).write_text(json.dumps(vec, indent=2) + "\n")
    LOG.info("%d trust-world vectors written; %d binary forms compared with xprv convert",
             len(wanted), compared)
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    vectors = sorted(VECTOR_DIR.glob("*.json"))
    if not vectors:
        LOG.error("no vectors in %s", VECTOR_DIR)
        return 2

    failures: list[str] = []
    positives = negatives = accepts = known = 0

    for path in vectors:
        vec = json.loads(path.read_text())
        name = vec.get("name", path.stem)
        expect = vec.get("expect", {})

        if "python" in vec.get("known_failing", {}):
            # scripts/xprv-conformance.py holds the tag to account.
            known += 1
            LOG.info("known failing, not checked here: %s (%s)", name, vec["known_failing"]["python"]["because"])
            continue

        if "form" in vec and "intermediates" not in vec:
            # An inline vector: the whole proof and its own trust, with no
            # recorded intermediates. It is verified, in each form it carries,
            # and nothing more. The trust-world vectors are all of this kind,
            # and so is the one whole-proof JSON forgery.
            form = vec["form"]
            forms = []
            if "json" in form:
                forms.append(("json", json.dumps(form["json"]).encode()))
            if "binary_hex" in form:
                forms.append(("binary", bytes.fromhex(form["binary_hex"])))
            if expect.get("verdict") == "accept":
                accepts += 1
            else:
                negatives += 1
            for which, raw in forms:
                ok, reason = python_verdict(raw, binary=which == "binary", trust=vec.get("trust"))
                if expect.get("verdict") == "accept":
                    if not ok:
                        failures.append(f"{name}: expected accept, {which} form rejected ({reason[:90]})")
                elif ok:
                    failures.append(
                        f"{name}: expected reject, got ACCEPT — this is a hole, "
                        f"not a test failure"
                    )
                elif (want := expect.get("reason_contains")) and \
                        want.lower() not in reason.lower():
                    failures.append(
                        f"{name}: rejected for the wrong reason "
                        f"(want ~{want!r}, got {reason[:70]!r})"
                    )
            continue

        if "base" in vec:
            base = json.loads((VECTOR_DIR / f"{vec['base']}.json").read_text())
            # A derived vector is checked under its base's trust unless it
            # names its own, as scripts/xprv-conformance.py reads it.
            trust = vec.get("trust") or base.get("trust")
            if "json_patch" in vec:
                raw = patched_text(base["form"]["json"], vec["json_patch"]).encode()
                ok, reason = python_verdict(raw, trust=trust)
            else:
                raw = apply_mutations(
                    bytes.fromhex(base["form"]["binary_hex"]), vec["mutations"]
                )
                ok, reason = python_verdict(raw, binary=True, trust=trust)
            if expect.get("verdict") == "accept":
                accepts += 1
                if not ok:
                    failures.append(f"{name}: expected accept, got reject ({reason[:70]})")
                continue
            negatives += 1
            if ok:
                failures.append(f"{name}: expected reject, got accept")
            elif (want := expect.get("reason_contains")) and \
                    want.lower() not in reason.lower():
                failures.append(
                    f"{name}: rejected for the wrong reason "
                    f"(want ~{want!r}, got {reason[:70]!r})"
                )
            continue

        positives += 1
        form = vec.get("form", {})

        # Spec §8: both forms of one proof MUST verify identically. Check each
        # independently rather than picking one, since a divergence between them
        # is exactly the class of bug this corpus exists to surface.
        verdicts: dict[str, tuple[bool, str]] = {}
        trust = vec.get("trust")
        if not trust:
            failures.append(f"{name}: positive vector has no configured trust")
        if "json" in form:
            verdicts["json"] = python_verdict(json.dumps(form["json"]).encode(), trust=trust)
        if "binary_hex" in form:
            verdicts["binary"] = python_verdict(bytes.fromhex(form["binary_hex"]), trust=trust)

        if expect.get("verdict") == "accept":
            for which, (ok, reason) in verdicts.items():
                if not ok:
                    failures.append(
                        f"{name}: expected accept, {which} form rejected ({reason})")
        if len({ok for ok, _ in verdicts.values()}) > 1:
            failures.append(
                f"{name}: FORM DIVERGENCE — "
                + ", ".join(f"{w}={'accept' if ok else 'reject'}"
                            for w, (ok, _) in verdicts.items())
            )

        # Intermediates are defined over the JSON representation: the binary trie
        # decoder yields only a precomputed root, not the decoded leaves a
        # third-party implementation must reproduce.
        if "json" not in form:
            failures.append(f"{name}: positive vector has no JSON form to "
                            f"re-derive intermediates from")
            continue
        fresh = derive_intermediates(form["json"])
        recorded = vec.get("intermediates", {})
        for section in ("headers", "map_proofs"):
            if fresh.get(section) != recorded.get(section):
                failures.append(
                    f"{name}: recorded {section} do not match freshly derived "
                    f"values — the vector and the verifier disagree"
                )
        if fresh.get("skip_keys") != recorded.get("skip_keys"):
            failures.append(f"{name}: recorded skip_keys mismatch")

    for f in failures:
        LOG.error("FAIL %s", f)
    LOG.info("checked %d positive, %d accept and %d negative vector(s), %d known failing "
             "left to xprv-conformance.py: %d failure(s)",
             positives, accepts, negatives, known, len(failures))
    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--log-level", default="INFO",
                    choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("annotate", help="write a positive vector from a proof")
    a.add_argument("proof")
    a.add_argument("--name", required=True)
    a.add_argument("--description", default="")
    a.add_argument("--allow-failing", action="store_true")
    a.set_defaults(fn=cmd_annotate)

    m = sub.add_parser("mutate", help="write the accept and negative families")
    m.add_argument("--base", required=True)
    m.set_defaults(fn=cmd_mutate)

    t = sub.add_parser("trust", help="write the synthetic trust-world vectors")
    t.set_defaults(fn=cmd_trust)

    c = sub.add_parser("check", help="check every vector in the corpus")
    c.set_defaults(fn=cmd_check)

    args = ap.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s %(name)s: %(message)s",
    )

    global V
    V = load_verifier()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
