#!/usr/bin/env python3
"""Generate fixture/codec-roundtrip-rippled-decode.json from rippled's own codec.

fixture/codec-roundtrip-fixtures.json pairs each object's bytes with the JSON
xrpl.js writes for them. That JSON is what a client hands the encoder, and the
round-trip tests keep using it for that. It is not what rippled writes when it
decodes: rippled writes an ordinary UInt64 field as lowercase hex without
padding and adds `type` to each path step. x-data's decoder follows rippled, so
its decode test needs rippled's JSON for the same bytes. This writes it.

Every value in the output is what xrpl-codec (rippled's libxrpl on the command
line) printed for the entry's `binary`; none is derived by hand or from x-data.
The gtest CodecRoundtrip*.DecodeMatchesJson holds x-data to the file.

    tests/x-data/generate-rippled-decode-fixture.py [--codec PATH]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_CODEC = "/Users/nicholasdudfield/projects/ripple-deps/rippled-xrpl-codec/build/build/Release/xrpl-codec"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--codec", default=DEFAULT_CODEC, help="path to rippled's xrpl-codec")
    args = ap.parse_args()
    if not Path(args.codec).exists():
        sys.exit(f"xrpl-codec not found at {args.codec}; pass --codec PATH")
    source = json.loads((HERE / "fixture" / "codec-roundtrip-fixtures.json").read_text())

    def decode(entry: dict) -> object:
        r = subprocess.run([args.codec, "decode", entry["binary"]], capture_output=True, text=True)
        if r.returncode != 0:
            return {"refused": (r.stderr or r.stdout).strip().splitlines()[-1]}
        return json.loads(r.stdout)

    out: dict[str, object] = {
        "generated_by": "tests/x-data/generate-rippled-decode-fixture.py",
        "oracle": "xrpl-codec, rippled libxrpl (branch xrpl-codec, cd322b9811). XRPL only.",
        "about": "rippled's JSON for the `binary` of each entry of codec-roundtrip-fixtures.json, in the same order",
    }
    refused = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        for section in ("transactions", "accountState"):
            decoded = list(pool.map(decode, source[section]))
            refused += sum(1 for d in decoded if isinstance(d, dict) and set(d) == {"refused"})
            out[section] = decoded
    target = HERE / "fixture" / "codec-roundtrip-rippled-decode.json"
    target.write_text(json.dumps(out, indent=1) + "\n")
    print(f"{target.name}: {len(out['transactions'])} transactions, {len(out['accountState'])} ledger entries, "
          f"{refused} refused by rippled")
    return 0


if __name__ == "__main__":
    sys.exit(main())
