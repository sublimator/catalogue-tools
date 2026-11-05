#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["xrpl-py>=4.0.0"]
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

Commands
--------
annotate  derive intermediates from a proof and write a positive vector
mutate    generate the negative-vector family from a positive vector
check     verify every vector; assert verdicts AND recorded intermediates
"""
from __future__ import annotations

import argparse
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
    try:
        with redirect_stdout(buf):
            proof = V.binary_to_proof(raw) if binary or V.is_binary_proof(raw) else V.load_json_proof(raw)
            res = V.verify(proof, trust.get("publisher_key"), trust.get("network_id"))
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
    mut(
        "zlib-trailer",
        "spec §3.1: a compressed body is one RFC 1950 zlib stream",
        [{"compress_body": 9}, {"append_hex": "00"}],
        "bytes after the zlib stream",
    )
    return fam


def _leb128(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


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


def _pointer_tokens(pointer: str) -> list[str]:
    return [t.replace("~1", "/").replace("~0", "~") for t in pointer.split("/")[1:]] if pointer else []


def _step(node: Any, token: str) -> Any:
    return node[int(token)] if isinstance(node, list) else node[token]


def apply_json_patch(doc: Any, ops: list[dict[str, Any]]) -> Any:
    """Apply a negative's edits to its base's JSON form.

    `set` writes a value at a JSON pointer (RFC 6901), creating an object member
    if needed; `select` replaces the whole document with the value at a pointer.
    """
    doc = copy.deepcopy(doc)
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
        else:
            raise SystemExit(f"unknown json_patch op {op['op']!r}")
    return doc


def patch_negative_family(base: dict[str, Any], base_name: str) -> list[dict[str, Any]]:
    """Negatives stated as edits of a positive vector's JSON form.

    These attacks live in JSON structure or in the anchor's contents, so no
    byte patch of the binary form expresses them. Like a byte mutation, each
    names exactly what it changes.
    """
    proof = base["form"]["json"]
    fam: list[dict[str, Any]] = []

    def neg(name: str, rule: str, layer: str, reason: str, ops: list[dict[str, Any]]) -> None:
        fam.append({
            "name": name,
            "violates": rule,
            "layer": layer,
            "base": base_name,
            "json_patch": ops,
            "expect": {"verdict": "reject", "reason_contains": reason},
        })

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
        "parse", "validation value must be a hex string", [at("/steps/0/validations/ED" + "00" * 32, "ZZ")])
    neg("wrong-depth", "spec §2.3: __depth__ MUST equal the node's nesting depth",
        "parse", "__depth__", [at(f"/steps/{first_map}/trie/__depth__", 17)])
    neg("unknown-leaf-member", "spec §2.3: leaf data is the decoded content and nothing else",
        "graph", "not a serialized field", [at(f"{state_leaf}/1/DisplayedButNotHashed", 1)])

    recorded = base["intermediates"]["map_proofs"][-1]
    assert recorded["tree"] == "tx" and len(recorded["leaves"]) == 1
    neg("leafless-map", "spec §3.8: a map proof MUST contain at least one authenticated leaf",
        "graph", "contains no leaves", [at(tx_leaf, recorded["leaves"][0]["leaf_hash"])])

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

    other = next(k for k in V.PUBLISHER_KEY_BY_NETWORK.values() if k != base["trust"]["publisher_key"])
    neg("publisher-key-mismatch", "spec §8: the anchor's publisher key MUST be the configured one",
        "trust", "publisher key mismatch", [at("/steps/0/unl/public_key", other)])
    return fam


def apply_mutations(raw: bytes, muts: list[dict[str, Any]]) -> bytes:
    buf = bytearray(raw)
    for m in muts:
        if "compress_body" in m:
            # Any valid stream will do: what is tested is what follows it.
            buf = buf[:5] + bytes([buf[5] | 1]) + buf[6:10] + zlib.compress(bytes(buf[10:]), int(m["compress_body"]))
            continue
        if "append_hex" in m:
            buf += bytes.fromhex(m["append_hex"])
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
    for neg in patch_negative_family(base, args.base):
        data = json.dumps(apply_json_patch(base["form"]["json"], neg["json_patch"])).encode()
        ok, reason = python_verdict(data, trust=base["trust"])
        if ok:
            LOG.error("inline negative %r was ACCEPTED by the reference verifier; "
                      "not writing a vector that asserts a false claim", neg["name"])
            return 1
        neg["observed"] = {"python_reason": reason}
        out = VECTOR_DIR / f"negative-{neg['name']}.json"
        out.write_text(json.dumps(neg, indent=2) + "\n")
        LOG.info("wrote %s (rejected: %s)", out.name, reason[:60])
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
    LOG.info("%d negative vectors written", written)
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    vectors = sorted(VECTOR_DIR.glob("*.json"))
    if not vectors:
        LOG.error("no vectors in %s", VECTOR_DIR)
        return 2

    failures: list[str] = []
    positives = negatives = 0

    for path in vectors:
        vec = json.loads(path.read_text())
        name = vec.get("name", path.stem)
        expect = vec.get("expect", {})

        if "form" in vec and expect.get("verdict") == "reject":
            # JSON-form negative: the whole malformed proof is carried inline,
            # because the attack lives in JSON trie structure rather than in
            # binary framing and cannot be expressed as a byte patch.
            negatives += 1
            form = vec["form"]
            raw = (
                bytes.fromhex(form["binary_hex"])
                if "binary_hex" in form
                else json.dumps(form["json"]).encode()
            )
            ok, reason = python_verdict(raw, trust=vec.get("trust"))
            if ok:
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
            negatives += 1
            base = json.loads((VECTOR_DIR / f"{vec['base']}.json").read_text())
            if "json_patch" in vec:
                raw = json.dumps(apply_json_patch(base["form"]["json"], vec["json_patch"])).encode()
                ok, reason = python_verdict(raw, trust=base.get("trust"))
            else:
                raw = apply_mutations(
                    bytes.fromhex(base["form"]["binary_hex"]), vec["mutations"]
                )
                ok, reason = python_verdict(raw, binary=True, trust=base.get("trust"))
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
    LOG.info("checked %d positive, %d negative vector(s): %d failure(s)",
             positives, negatives, len(failures))
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

    m = sub.add_parser("mutate", help="write the negative-vector family")
    m.add_argument("--base", required=True)
    m.set_defaults(fn=cmd_mutate)

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
