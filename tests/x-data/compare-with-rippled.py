#!/usr/bin/env python3
"""Compare x-data's JSON for ledger objects with rippled's own, field by field.

Each object is decoded twice: by catl_xdata_cli (x-data) and by xrpl-codec
(rippled's libxrpl on the command line). Every place the two JSON documents
differ is attributed to the serialized field it sits in, and the differences
are grouped by that field's type.

    tests/x-data/compare-with-rippled.py --xdata BUILD/src/x-data/catl_xdata_cli \\
        [--codec PATH/xrpl-codec] [--fixtures] [LEDGER.json ...]

--fixtures adds every object in fixture/codec-roundtrip-fixtures.json.
LEDGER.json is a rippled `ledger` reply with binary transactions and metadata
(`"transactions": true, "expand": true, "binary": true`); each transaction and
each metadata blob is one object.

x-data's lowercase `*_ascii` hints are a display convenience of the command
line tool, not part of a decoded object, and are left out.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DEFAULT_CODEC = "/Users/nicholasdudfield/projects/ripple-deps/rippled-xrpl-codec/build/build/Release/xrpl-codec"


def field_types() -> dict[str, str]:
    defs = json.loads((REPO / "src/x-data/definitions/xrpl_definitions.json").read_text())
    return {name: meta["type"] for name, meta in defs["FIELDS"]}


def run(cmd: list[str]) -> tuple[bool, str]:
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.returncode == 0, (r.stdout if r.returncode == 0 else (r.stdout + r.stderr)).strip()


def strip_hints(node):
    if isinstance(node, dict):
        return {k: strip_hints(v) for k, v in node.items() if not k.endswith("_ascii")}
    if isinstance(node, list):
        return [strip_hints(v) for v in node]
    return node


def kind_of(value) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "null"


def diff(x, r, types, field, inner, out):
    """x is x-data's value, r is rippled's. `field` is the enclosing serialized
    field; `inner` is the path from that field down to here."""
    if isinstance(x, dict) and isinstance(r, dict):
        for key in sorted(set(x) | set(r)):
            if key in types:
                sub_field, sub_inner = key, ""
            else:
                sub_field, sub_inner = field, f"{inner}.{key}"
            if key not in r:
                out.append((sub_field, sub_inner, "only x-data writes it", x[key], None))
            elif key not in x:
                out.append((sub_field, sub_inner, "only rippled writes it", None, r[key]))
            else:
                diff(x[key], r[key], types, sub_field, sub_inner, out)
        return
    if isinstance(x, list) and isinstance(r, list):
        if len(x) != len(r):
            out.append((field, inner + "[]", "array length differs", len(x), len(r)))
            return
        for a, b in zip(x, r):
            diff(a, b, types, field, inner + "[]", out)
        return
    if kind_of(x) != kind_of(r):
        out.append((field, inner, f"x-data {kind_of(x)}, rippled {kind_of(r)}", x, r))
    elif x != r:
        out.append((field, inner, "text differs" if isinstance(x, str) else "value differs", x, r))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--xdata", required=True, help="path to catl_xdata_cli")
    ap.add_argument("--codec", default=DEFAULT_CODEC, help="path to rippled's xrpl-codec")
    ap.add_argument("--fixtures", action="store_true", help="include codec-roundtrip-fixtures.json")
    ap.add_argument("--examples", type=int, default=1, help="examples per group")
    ap.add_argument("--fields", type=int, default=40, help="field names listed per group")
    ap.add_argument("ledgers", nargs="*")
    args = ap.parse_args()

    types = field_types()
    objects: list[tuple[str, str, str]] = []  # (source, label, hex)
    if args.fixtures:
        fx = json.loads((REPO / "tests/x-data/fixture/codec-roundtrip-fixtures.json").read_text())
        for i, e in enumerate(fx["accountState"]):
            objects.append(("fixtures", f"ledger entry {i} {e['json'].get('LedgerEntryType')}", e["binary"]))
        for i, e in enumerate(fx["transactions"]):
            objects.append(("fixtures", f"transaction {i} {e['json'].get('TransactionType')}", e["binary"]))
    for path in args.ledgers:
        ledger = json.loads(Path(path).read_text()).get("result", {}).get("ledger")
        if not ledger:
            continue
        name = Path(path).stem
        for i, t in enumerate(ledger.get("transactions", [])):
            objects.append(("live", f"{name} tx {i}", t["tx_blob"]))
            objects.append(("live", f"{name} meta {i}", t["meta"]))
    if not objects:
        sys.exit("nothing to compare: pass --fixtures and/or ledger files")

    def decode(item):
        _, _, hex_text = item
        return (run([args.xdata, "--decode", hex_text, "-e", "0"]),
                run([args.codec, "decode", hex_text]))

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(decode, objects))

    per_source = Counter()
    identical = Counter()
    failures = defaultdict(list)
    groups: dict[tuple, list] = defaultdict(list)
    names: dict[tuple, Counter] = defaultdict(Counter)
    objects_hit: dict[tuple, set] = defaultdict(set)

    for index, ((source, label, _), ((x_ok, x_text), (r_ok, r_text))) in enumerate(zip(objects, results)):
        per_source[source] += 1
        if not x_ok or not r_ok:
            who = "x-data" if not x_ok else "rippled"
            failures[(source, who)].append((label, (x_text if not x_ok else r_text)[-160:]))
            continue
        x = strip_hints(json.loads(x_text))
        r = json.loads(r_text)
        found: list = []
        diff(x, r, types, "(object)", "", found)
        if not found:
            identical[source] += 1
        for field, inner, what, xv, rv in found:
            key = (types.get(field, "?"), inner, what)
            groups[key].append((source, label, field, xv, rv))
            names[key][field] += 1
            objects_hit[key].add(index)

    for source in per_source:
        print(f"{source}: {per_source[source]} objects, {identical[source]} decode identically in x-data and rippled")
    for (source, who), items in failures.items():
        print(f"{source}: {who} could not decode {len(items)}; e.g. {items[0][0]}: {items[0][1]}")

    print()
    print("| Field type | Where in the value | Difference | Occurrences | Objects | Fields | Example: x-data | Example: rippled |")
    print("|---|---|---|---:|---:|---|---|---|")
    for key in sorted(groups, key=lambda k: (-len(groups[k]), k)):
        ftype, inner, what = key
        rows = groups[key]
        fields = ", ".join(f"{n} ({c})" for n, c in names[key].most_common(args.fields))
        if len(names[key]) > args.fields:
            fields += f", and {len(names[key]) - args.fields} more"
        for source, label, field, xv, rv in rows[: args.examples]:
            show = lambda v: "(absent)" if v is None else "`" + json.dumps(v)[:60] + "`"
            print(f"| {ftype} | {inner or '(the value)'} | {what} | {len(rows)} | {len(objects_hit[key])} | {fields} "
                  f"| {field}: {show(xv)} | {show(rv)} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
