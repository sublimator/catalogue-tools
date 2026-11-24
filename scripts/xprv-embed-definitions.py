#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Write the Xahau definitions into scripts/verify-xprv.py.

The Python verifier is handed out as one file, and it checks a decoded leaf by
serializing it with its network's definitions. xrpl-py brings XRPL's. Xahau's
are carried inside the script, as a compressed copy of the parts a codec uses
from src/x-data/definitions/xahau_definitions.json, the same file the C++
verifier embeds. This writes that copy between the two marker lines.

Definitions decide which bytes a JSON leaf stands for, so they are part of what
the verifier trusts. They are embedded and fingerprinted, never fetched when a
proof is checked.

    scripts/xprv-embed-definitions.py           rewrite the embedded copy
    scripts/xprv-embed-definitions.py --check   exit 1 if it is out of date

Run it after refreshing xahau_definitions.json. A unit test in
scripts/test-verify-xprv.py fails while the two differ.
"""
from __future__ import annotations

import argparse
import base64
import json
import sys
import zlib
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SOURCE = REPO / "src" / "x-data" / "definitions" / "xahau_definitions.json"
TARGET = REPO / "scripts" / "verify-xprv.py"
BEGIN = "# >>> xahau definitions (written by scripts/xprv-embed-definitions.py; do not edit)\n"
END = "# <<< xahau definitions\n"
FIELD_MEMBERS = ("isSerialized", "isSigningField", "isVLEncoded", "nth", "type")


def codec_table(definitions: dict) -> dict:
    """The parts of a definitions file that decide how an object is serialized."""
    return {
        "FIELDS": [[name, {k: entry[k] for k in FIELD_MEMBERS}] for name, entry in definitions["FIELDS"]],
        "LEDGER_ENTRY_TYPES": definitions["LEDGER_ENTRY_TYPES"],
        "TRANSACTION_RESULTS": definitions["TRANSACTION_RESULTS"],
        "TRANSACTION_TYPES": definitions["TRANSACTION_TYPES"],
        "TYPES": definitions["TYPES"],
        "native_currency_code": definitions["native_currency_code"],
    }


def canonical_text(table: dict) -> str:
    """One text for one table: sorted members, no spaces. The fingerprint is of this."""
    return json.dumps(table, sort_keys=True, separators=(",", ":"))


def embedded_block(text: str) -> str:
    blob = base64.b64encode(zlib.compress(text.encode(), 9)).decode()
    lines = [blob[i : i + 76] for i in range(0, len(blob), 76)]
    body = "".join(f'    "{line}"\n' for line in lines)
    return f"{BEGIN}_XAHAU_DEFINITIONS_ZB64 = (\n{body})\n{END}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="change nothing; exit 1 if out of date")
    args = ap.parse_args()

    block = embedded_block(canonical_text(codec_table(json.loads(SOURCE.read_text()))))
    script = TARGET.read_text()
    if script.count(BEGIN) != 1 or script.count(END) != 1:
        sys.exit(f"{TARGET.name}: expected exactly one pair of marker lines")
    start, end = script.index(BEGIN), script.index(END) + len(END)
    if script[start:end] == block:
        print(f"{TARGET.name}: the embedded Xahau definitions are up to date")
        return 0
    if args.check:
        print(f"{TARGET.name}: the embedded Xahau definitions differ from {SOURCE.name}; "
              "run scripts/xprv-embed-definitions.py")
        return 1
    TARGET.write_text(script[:start] + block + script[end:])
    print(f"{TARGET.name}: embedded Xahau definitions rewritten from {SOURCE.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
