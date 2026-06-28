#!/usr/bin/env python3
"""Audit XRPL/Xahau protocol-definition JSON files in the repo.

Finds every *definitions*.json (and server-definitions.json), then for each
file computes:

  - a byte-level SHA-256 (exact-duplicate detection), and
  - a *semantic* SHA-256 over the protocol-relevant content only (FIELDS,
    TYPES, LEDGER_ENTRY_TYPES, TRANSACTION_TYPES, TRANSACTION_RESULTS), with
    keys sorted and volatile metadata (status/hash/features) ignored — so two
    files that mean the same thing but differ in whitespace/key-order/metadata
    still group together.

It then classifies each file as XRPL vs Xahau by marker fields, groups files
into byte- and semantic-equivalence classes, and prints a report so we can
settle on exactly one canonical XRPL and one canonical Xahau file.

Usage:
  scripts/audit-definitions.py [--root DIR] [--json]

Exit status is 0 always (it's a report); use --json for machine output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

# Directories that hold vendored/generated copies we don't canonicalize.
EXCLUDE_PARTS = {
    ".git", "build", "build-ubuntu", "build-hbb", "build-tsan",
    "node_modules", ".venv", "catenv", ".conan2", "xprv-py",
    ".mypy_cache", ".pytest_cache", ".specstory", ".idea",
    ".ai-docs", ".reviews", "worktrees",
}

# Fields unique to each network — used to classify a file. A file is whichever
# network it has markers for; having both (or neither) is reported as such.
# NB: HookGrants is NOT a discriminator — it appears in XRPL definitions too.
XAHAU_MARKERS = {
    "ImportVLKey", "GenesisMint", "GovernanceFlags", "EmittedTxnID",
    "ActiveValidator",
}
XRPL_MARKERS = {
    "AMMID", "Asset2", "AuctionSlot", "AcceptedCredentials",
    "AssetPrice", "AdditionalBooks",
}


def iter_definition_files(root: Path):
    for p in sorted(root.rglob("*.json")):
        if any(part in EXCLUDE_PARTS for part in p.parts):
            continue
        name = p.name.lower()
        if "definition" not in name:
            continue
        yield p


def unwrap(doc: Any) -> dict:
    """server_definitions RPC nests the payload under result/'.'; unwrap it."""
    if isinstance(doc, dict):
        if "FIELDS" in doc or "TYPES" in doc:
            return doc
        for key in ("result", "."):
            inner = doc.get(key)
            if isinstance(inner, dict) and ("FIELDS" in inner or "TYPES" in inner):
                return inner
    return doc if isinstance(doc, dict) else {}


def field_codes(doc: dict) -> dict[str, int]:
    types = dict(doc.get("TYPES", {}))
    out: dict[str, int] = {}
    for entry in doc.get("FIELDS", []):
        try:
            name, info = entry
            tc = types.get(info["type"])
            if tc is None:
                continue
            out[name] = (int(tc) << 16) | int(info["nth"])
        except (ValueError, KeyError, TypeError):
            continue
    return out


def semantic_fingerprint(doc: dict) -> str:
    """Stable hash over the protocol-meaningful content only."""
    payload = {
        "FIELDS": sorted(field_codes(doc).items()),
        "TYPES": sorted(doc.get("TYPES", {}).items()),
        "LEDGER_ENTRY_TYPES": sorted(doc.get("LEDGER_ENTRY_TYPES", {}).items()),
        "TRANSACTION_TYPES": sorted(doc.get("TRANSACTION_TYPES", {}).items()),
        "TRANSACTION_RESULTS": sorted(doc.get("TRANSACTION_RESULTS", {}).items()),
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


def classify(codes: dict[str, int]) -> str:
    names = set(codes)
    xah = bool(names & XAHAU_MARKERS)
    xrpl = bool(names & XRPL_MARKERS)
    if xah and not xrpl:
        return "xahau"
    if xrpl and not xah:
        return "xrpl"
    if xah and xrpl:
        return "both?"
    return "unknown"


def audit(root: Path) -> list[dict]:
    results = []
    for path in iter_definition_files(root):
        raw = path.read_bytes()
        byte_hash = hashlib.sha256(raw).hexdigest()
        rec: dict[str, Any] = {
            "path": str(path.relative_to(root)),
            "bytes": len(raw),
            "byte_sha": byte_hash[:12],
        }
        try:
            doc = unwrap(json.loads(raw))
            codes = field_codes(doc)
            rec["network"] = classify(codes)
            rec["fields"] = len(codes)
            rec["txn_types"] = len(doc.get("TRANSACTION_TYPES", {}))
            rec["semantic_sha"] = semantic_fingerprint(doc)[:12]
        except json.JSONDecodeError as exc:
            rec["network"] = "PARSE-ERROR"
            rec["error"] = str(exc)
            rec["semantic_sha"] = "-"
        results.append(rec)
    return results


def group_by(results: list[dict], key: str) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for r in results:
        groups.setdefault(r.get(key, "-"), []).append(r["path"])
    return groups


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=".", type=Path, help="repo root")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    root = args.root.resolve()
    results = audit(root)

    if args.json:
        json.dump(results, sys.stdout, indent=2)
        print()
        return 0

    print(f"Definition JSONs under {root} (excluding vendored/generated):\n")
    hdr = f"{'network':9} {'fields':>6} {'txns':>5} {'bytes':>8}  {'byte_sha':12} {'semantic':12}  path"
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(results, key=lambda x: (x.get("network", ""), x["path"])):
        print(
            f"{r.get('network','?'):9} {r.get('fields','-'):>6} "
            f"{r.get('txn_types','-'):>5} {r['bytes']:>8}  "
            f"{r['byte_sha']:12} {r.get('semantic_sha','-'):12}  {r['path']}"
        )

    print("\n== byte-identical groups (exact duplicates) ==")
    for sha, paths in group_by(results, "byte_sha").items():
        if len(paths) > 1:
            print(f"  {sha}: {len(paths)} copies")
            for p in paths:
                print(f"      {p}")

    print("\n== semantically-identical groups (same protocol content) ==")
    for sha, paths in group_by(results, "semantic_sha").items():
        if sha != "-" and len(paths) > 1:
            print(f"  {sha}: {len(paths)} files")
            for p in paths:
                print(f"      {p}")

    print("\n== canonical candidates (one per network) ==")
    by_net: dict[str, list[dict]] = {}
    for r in results:
        by_net.setdefault(r.get("network", "?"), []).append(r)
    for net in ("xrpl", "xahau"):
        files = by_net.get(net, [])
        sems = {f.get("semantic_sha") for f in files}
        flag = "OK (single version)" if len(sems) <= 1 else f"WARNING: {len(sems)} distinct versions!"
        print(f"  {net}: {len(files)} file(s), {len(sems)} distinct content — {flag}")
        for f in files:
            print(f"      {f['path']}  ({f.get('fields','-')} fields, sem {f.get('semantic_sha','-')})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
