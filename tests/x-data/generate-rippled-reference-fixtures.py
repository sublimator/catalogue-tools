#!/usr/bin/env python3
"""Generate fixture/rippled-reference-fixtures.json from rippled's own codec.

The oracle is xrpl-codec: rippled's libxrpl on the command line. Every expected
value in the fixture is what that binary printed; none is derived by hand or
from x-data. The gtest RippledReference.* holds x-data to the file.

    tests/x-data/generate-rippled-reference-fixtures.py [--codec PATH]

What it covers, and why:
  currency   to_string(Currency) / toCurrency: the native code, the
             three-character form and its character set, hex.
  issue      Issue::setJson / issueFromJson: a native issue is an object; a
             currency without an issuer is native only as the native code.
  amount     STAmount::getText / amountFromJson: issued value text across
             every exponent and both notation cutovers; number grammar.
  number     to_string(Number) / numberFromJson: normalisation, both notation
             cutovers, trailing-zero mantissas, values that need rounding.
  pathset    a step's currency through the same currency rules.
  bridge     both issues of an XChainBridge.
  account    a field never set ("" and an empty field) against one set to the
             zero account (its address and twenty zero bytes).
  uint64     which UInt64 fields rippled treats as base-ten, and their decimal
             text at 0, 1, 255, 2^63 - 1, 2^63 and 2^64 - 1, with what it
             refuses. Then every other UInt64 field as hex: x-data reads those
             as rippled does and writes them differently, which is an open
             question, so that half's decode test is DISABLED_.

An entry rippled refuses carries "refused" with rippled's message. A few
entries carry "xdata": x-data deliberately does something else there, and the
entry says what and why (see DEVIATIONS).

The oracle is XRPL only. The Xahau rules (XAH native) are tested separately
from the xahaud source; there is no Xahau oracle build.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

DEFAULT_CODEC = "/Users/nicholasdudfield/projects/ripple-deps/rippled-xrpl-codec/build/build/Release/xrpl-codec"
OUT = Path(__file__).resolve().parent / "fixture" / "rippled-reference-fixtures.json"

ALICE = "rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe"
ALICE_HEX = "F667B0CA50CC7709A220B0561B85E53A48461FA8"
XAH_ISSUER = "rswh1fvyLqHizBS2awu1vs6QcmwTBd9qiv"
ZERO_ACCOUNT = "rrrrrrrrrrrrrrrrrrrrrhoLvTp"
NO_ACCOUNT = "rrrrrrrrrrrrrrrrrrrrBZbvji"
MPT_ID = "00000001" + ALICE_HEX
Z20 = "00" * 20
USD_HEX = "0000000000000000000000005553440000000000"
XAH_HEX = "0000000000000000000000005841480000000000"
XRP_ISO_HEX = "0000000000000000000000005852500000000000"

# Where x-data does not do what rippled does, on purpose. Keyed by section and
# the entry's input as JSON text.
DEVIATIONS = {
    ("amount_encode", json.dumps({"value": 5, "currency": "USD", "issuer": ALICE})): (
        "refuses",
        "x-data takes an amount value as a string only; rippled also takes a JSON integer",
    ),
    ("amount_encode", json.dumps({"value": "1", "currency": "USD", "issuer": ZERO_ACCOUNT})): (
        "refuses",
        "rippled writes these bytes and then cannot read them back (invalid native account)",
    ),
    ("amount_encode", json.dumps({"value": "1e-82", "currency": "USD", "issuer": ALICE})): (
        "refuses",
        "rippled writes zero for a value below its range; x-data refuses a value it would have to turn into zero",
    ),
    ("pathset_encode", json.dumps([[{"account": ZERO_ACCOUNT}]])): (
        "01" + Z20 + "00",
        "rippled derives a step's type from which parts are non-zero and drops a zero account; "
        "x-data keeps the member the JSON has, so bytes with the bit set and a zero account round-trip",
    ),
}


class Oracle:
    def __init__(self, path: str) -> None:
        self.path = path
        if not Path(path).exists():
            sys.exit(f"xrpl-codec not found at {path}; pass --codec PATH")

    def run(self, args: list[str]) -> tuple[bool, str]:
        r = subprocess.run([self.path, *args], capture_output=True, text=True)
        return r.returncode == 0, (r.stdout if r.returncode == 0 else r.stdout + r.stderr).strip()

    def decode(self, kind: str, hex_text: str) -> tuple[bool, str]:
        r = subprocess.run([self.path, "decode", "--codec-type", kind, hex_text], capture_output=True, text=True)
        return r.returncode == 0, (r.stdout if r.returncode == 0 else r.stdout + r.stderr).strip()

    def encode(self, kind: str, text: str) -> tuple[bool, str]:
        r = subprocess.run([self.path, "encode", "--codec-type", kind], input=text, capture_output=True, text=True)
        return r.returncode == 0, (r.stdout if r.returncode == 0 else r.stdout + r.stderr).strip()


def reason(message: str) -> str:
    for prefix in ("Error encoding: ", "Error decoding: ", "Error: "):
        if message.startswith(prefix):
            message = message[len(prefix):]
            break
    # rippled's JSON parser reports a field error as an RPC error object.
    start = message.find("{")
    if start >= 0:
        try:
            return message[:start] + json.loads(message[start:])["error_message"]
        except (ValueError, KeyError, TypeError):
            pass
    return message


def decode_section(oracle: Oracle, kind: str, hexes: list[str], *, as_json: bool) -> list[dict]:
    """Entries rippled decodes. One it refuses is left out: x-data's decoders are lenient."""
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(lambda h: oracle.decode(kind, h), hexes))
    out = []
    for hex_text, (ok, text) in zip(hexes, results):
        if not ok:
            continue
        if as_json:
            # The oracle prints a scalar bare: a native amount is a string.
            value = json.loads(text) if text[:1] in "{[" else text
            out.append({"hex": hex_text, "json": value})
        else:
            out.append({"hex": hex_text, "text": text})
    return out


def encode_section(oracle: Oracle, kind: str, section: str, inputs: list, *, as_json: bool) -> list[dict]:
    """A string input goes to the oracle verbatim, as the field's string value;
    anything else goes as JSON. The oracle trims surrounding whitespace and
    takes no empty input, so neither is asked of it at the top level."""
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(lambda v: oracle.encode(kind, v if isinstance(v, str) else json.dumps(v)), inputs))
    out = []
    for value, (ok, text) in zip(inputs, results):
        entry: dict = {"json" if as_json else "text": value}
        if ok:
            entry["hex"] = text.upper()
        else:
            entry["refused"] = reason(text)
        deviation = DEVIATIONS.get((section, json.dumps(value)))
        if deviation is None and ok and section == "amount_encode" and wraps_in_rippled(value):
            deviation = (
                "refuses",
                "rippled reads the digits as unsigned and then as signed, so digits above "
                "2^63 - 1 are written as a negative amount",
            )
        if deviation:
            entry["xdata"], entry["why"] = deviation
        out.append(entry)
    return out


def wraps_in_rippled(amount) -> bool:
    """An issued value whose digits, read as one integer, exceed 2^63 - 1."""
    if not isinstance(amount, dict) or "currency" not in amount or not isinstance(amount.get("value"), str):
        return False
    digits = "".join(c for c in amount["value"].split("e")[0].split("E")[0] if c.isdigit())
    return bool(digits) and int(digits) > 2 ** 63 - 1


def iou_raw(mantissa: int, offset: int, negative: bool = False) -> str:
    if mantissa == 0:
        return "8000000000000000"
    raw = (1 << 63) | (0 if negative else 1 << 62) | ((offset + 97) << 54) | mantissa
    return f"{raw:016X}"


def number_raw(mantissa: int, exponent: int) -> str:
    return f"{mantissa & 0xFFFFFFFFFFFFFFFF:016X}{exponent & 0xFFFFFFFF:08X}"


def build(oracle: Oracle) -> dict:
    fx: dict = {
        "generated_by": "tests/x-data/generate-rippled-reference-fixtures.py",
        "oracle": "xrpl-codec, rippled libxrpl (branch xrpl-codec, cd322b9811). XRPL only.",
    }

    # ── currency ──────────────────────────────────────────────────────
    cur = [Z20, "00" * 19 + "01", USD_HEX, XAH_HEX, XRP_ISO_HEX,
           "0000000000000000000000007872700000000000",  # "xrp" lowercase
           "524C555344000000000000000000000000000000",
           "0000000000000000000000005553440000000001",  # ISO pattern broken at the tail
           "0100000000000000000000005553440000000000"]  # ISO pattern broken at the head
    # Every byte value in each of the three code positions.
    for position in range(3):
        for byte in range(256):
            code = [0x55, 0x53, 0x44]
            code[position] = byte
            cur.append("00" * 12 + bytes(code).hex().upper() + "00" * 5)
    fx["currency_decode"] = decode_section(oracle, "currency", sorted(set(cur)), as_json=False)

    texts = ["XRP", "XAH", "USD", "usd", "xrp", "Xrp", "U D", "1", "USDX", "US", "~US", "<>(",
             "524C555344000000000000000000000000000000", "524c555344000000000000000000000000000000",
             Z20, XRP_ISO_HEX, "00" * 19 + "01", "ZZ" * 20, "00" * 19, "00" * 21]
    for byte in range(0x21, 0x7F):  # each printable character in the middle
        texts.append("U" + chr(byte) + "D")
    fx["currency_encode"] = encode_section(oracle, "currency", "currency_encode", sorted(set(texts)), as_json=False)

    # ── issue ─────────────────────────────────────────────────────────
    fx["issue_decode"] = decode_section(oracle, "issue", [
        Z20,
        USD_HEX + ALICE_HEX,
        XAH_HEX + ALICE_HEX,
        "524C555344000000000000000000000000000000" + ALICE_HEX,
        XRP_ISO_HEX + ALICE_HEX,
        ALICE_HEX + "00" * 19 + "01" + "01000000",  # MPT
    ], as_json=True)
    fx["issue_encode"] = encode_section(oracle, "issue", "issue_encode", [
        {"currency": "XRP"},
        {"currency": ""},
        {"currency": "XAH"},
        {"currency": "BTC"},
        {"currency": "USD"},
        {"currency": "XRP", "issuer": ALICE},
        {"currency": "", "issuer": ALICE},
        {"currency": "XRP", "issuer": None},
        {"currency": "XAH", "issuer": XAH_ISSUER},
        {"currency": "USD", "issuer": ALICE},
        {"currency": "usd", "issuer": ALICE},
        {"currency": "524C555344000000000000000000000000000000", "issuer": ALICE},
        {"currency": Z20, "issuer": ALICE},
        {"currency": XRP_ISO_HEX, "issuer": ALICE},
        {"currency": "1", "issuer": ALICE},
        {"currency": "U D", "issuer": ALICE},
        {"currency": "USD", "issuer": ZERO_ACCOUNT},
        {"currency": "USD", "issuer": NO_ACCOUNT},
        {"currency": "USD", "issuer": "not an account"},
        {"currency": 5, "issuer": ALICE},
        {"issuer": ALICE},
        {"mpt_issuance_id": MPT_ID},
        {"mpt_issuance_id": MPT_ID, "currency": "USD"},
        "XRP",
        "USD",
    ], as_json=True)

    # ── amount ────────────────────────────────────────────────────────
    amounts = ["4000000000000000", "4000000000000001", "40000000000F4240", "00000000000F4240",
               "416345785D8A0000",  # 1e17 drops
               "60" + "0000000000000064" + MPT_ID, "20" + "0000000000000064" + MPT_ID,
               "20" + "0000000000000000" + MPT_ID, "60" + "7FFFFFFFFFFFFFFF" + MPT_ID]
    tail = USD_HEX + ALICE_HEX
    amounts.append(iou_raw(0, 0) + tail)
    # Every exponent, with a mantissa that has trailing zeros, one with none
    # and one in between; a few negative.
    for offset in range(-96, 81):
        for mantissa in (1000000000000000, 9999999999999999, 1234567890120000):
            amounts.append(iou_raw(mantissa, offset) + tail)
        if offset % 8 == 0:
            amounts.append(iou_raw(1234567890123456, offset, negative=True) + tail)
    amounts.append(iou_raw(1300000000000000, -10) + XAH_HEX + ALICE_HEX)
    amounts.append(iou_raw(1300000000000000, -10) + XRP_ISO_HEX + ALICE_HEX)
    fx["amount_decode"] = decode_section(oracle, "amount", amounts, as_json=True)
    # rippled reads a few byte strings it would not write (an MPT zero with
    # the sign bit clear). Record what it writes for the JSON it read.
    for entry in fx["amount_decode"]:
        value = entry["json"]
        ok, text = oracle.encode("amount", value if isinstance(value, str) else json.dumps(value))
        if ok and text.upper() != entry["hex"]:
            entry["rippled_writes"] = text.upper()

    def iou(value, currency="USD", issuer=ALICE):
        return {"value": value, "currency": currency, "issuer": issuer}

    amount_inputs: list = [
        "0", "1", "1000000", "-1000000", "100000000000000000", "100000000000000001", "1e3", "1E+3", "+5",
        "1.5", "1.0", "10e-1", "007", "abc", "100 GARBAGE", "0x10",
        iou("130000", "XAH", XAH_ISSUER),
        iou("1", "XRP"), iou("1", ""), iou("1", Z20), iou("1", XRP_ISO_HEX),
        iou("1", "USD", ZERO_ACCOUNT), iou("1", "USD", NO_ACCOUNT), iou("1", "USD", "not an account"),
        {"value": "1", "currency": "USD"},
        {"value": "1", "issuer": ALICE},
        {"currency": "USD", "issuer": ALICE},
        iou(5),
        {"value": "100", "mpt_issuance_id": MPT_ID},
        {"value": "-100", "mpt_issuance_id": MPT_ID},
        {"value": "0", "mpt_issuance_id": MPT_ID},
        {"value": "-0", "mpt_issuance_id": MPT_ID},
        {"value": "1e2", "mpt_issuance_id": MPT_ID},
        {"value": "1.5", "mpt_issuance_id": MPT_ID},
        {"value": "0x64", "mpt_issuance_id": MPT_ID},
        {"value": "9223372036854775807", "mpt_issuance_id": MPT_ID},
        {"value": "9223372036854775808", "mpt_issuance_id": MPT_ID},
        {"value": "100", "mpt_issuance_id": MPT_ID, "currency": "USD"},
        {"value": "100", "mpt_issuance_id": MPT_ID, "issuer": ALICE},
    ]
    values = ["0", "0.0", "-0", "1", "-1", "+5", "1.5", "0.001", "1e3", "1E+3", "1e-3", "1E-3", "1e007",
              "007", "1.", ".5", "", "abc", "100 GARBAGE", "1 ", " 1", "1e", "1e+", "--1", "1.5.5", "0x10",
              "1,5", "NaN", "Infinity",
              # the notation cutovers, as text
              "1e-5", "1e-6", "1e10", "1e11", "1e12", "0.000001", "0.0000001", "1000000000000000e-3",
              "1000000000000", "1407563260269.962", "1407563260269962e-3",
              # range
              "9999999999999999e80", "1e-81", "1e-82", "1e95", "1e96", "1e97",
              # more digits than the 16 kept: nearest, ties to even
              "12345678901234567", "12345678901234565", "12345678901234575", "12345678901234564",
              "12345678901234566", "123456789012345650", "123456789012345651", "123456789012345649",
              "1234567890123456500", "1234567890123456501", "12345678901234565001", "12345678901234565000",
              "12345678901234564999", "99999999999999995", "99999999999999994", "9999999999999999500",
              "18446744073709551615", "18446744073709551616",
              "10000000000000000000000000000000000000000"]
    amount_inputs += [iou(v) for v in values]
    fx["amount_encode"] = encode_section(oracle, "amount", "amount_encode", amount_inputs, as_json=True)

    # ── number ────────────────────────────────────────────────────────
    numbers = [number_raw(0, -(2 ** 31)), number_raw(0, 0),
               number_raw(-(2 ** 63), 0), number_raw(2 ** 63 - 1, 0), number_raw(-(2 ** 63) + 1, 0),
               number_raw(1, -32768), number_raw(1, -32751), number_raw(1, -32750), number_raw(1, 32750),
               number_raw(1, 32768), number_raw(10 ** 18, -32768), number_raw(10 ** 18, -32769),
               number_raw(10 ** 18, 32767), number_raw(10 ** 18, 32768)]
    mantissas = [1, 12, 1000, 123456789, 9999999999999999, 10 ** 17, 10 ** 18, 999999999999999999,
                 1234567890123456789, 5 * 10 ** 18, 922337203685477581, -1, -1234567890123456789]
    for mantissa in mantissas:
        for exponent in range(-52, 16):
            numbers.append(number_raw(mantissa, exponent))
    fx["number_decode"] = decode_section(oracle, "number", sorted(set(numbers)), as_json=False)

    number_texts = ["0", "0.0", "-0", "1", "-1", "+5", "1.5", "100", "0.001", "1e3", "1E+3", "1e-3", "1e007",
                    "007", "1.", ".5", "abc", "100 GARBAGE", "1 2", "1e", "--1", "0x10", "NaN",
                    "1e10", "1e11", "1e-10", "1e-11", "0.0000000001", "0.00000000001", "10000000000",
                    "100000000000", "1e18", "1e19", "2e18", "123e9", "1000000000000000000",
                    "9999999999999999e80", "9999999999999999e-1", "999999999999999900e78",
                    "9223372036854775807", "9223372036854775808", "9223372036854775809", "9223372036854775810",
                    "-9223372036854775807", "-9223372036854775808", "9999999999999999999",
                    "9999999999999999990", "10000000000000000000", "12345678901234567890",
                    "12345678901234567891", "18446744073709551615", "18446744073709551616",
                    "123456789012345678901", "1.2345678901234567891", "0.12345678901234567891",
                    "1e32750", "1e32768", "1e32786", "1e32787", "1e-32750", "1e-32768", "1e-32769",
                    "1e-32786", "1e99999999999"]
    fx["number_encode"] = encode_section(oracle, "number", "number_encode", number_texts, as_json=False)

    # ── pathset ───────────────────────────────────────────────────────
    path_inputs = [
        [[{"currency": "XAH", "issuer": XAH_ISSUER}]],
        [[{"currency": "XRP"}]],
        [[{"currency": "USD", "issuer": ALICE}]],
        [[{"account": ALICE}, {"currency": "USD", "issuer": ALICE}], [{"currency": "XRP"}]],
        [[{"currency": "524C555344000000000000000000000000000000", "issuer": ALICE}]],
        [[{"currency": "U D", "issuer": ALICE}]],
        [[{"currency": XRP_ISO_HEX, "issuer": ALICE}]],
        [[{"currency": "00" * 19 + "01", "issuer": ALICE}]],
        [[{"account": ZERO_ACCOUNT}]],
    ]
    fx["pathset_encode"] = encode_section(oracle, "pathset", "pathset_encode", path_inputs, as_json=True)
    fx["pathset_decode"] = decode_section(
        oracle, "pathset",
        [e["hex"] for e in fx["pathset_encode"] if "hex" in e and "xdata" not in e], as_json=True)

    # ── bridge ────────────────────────────────────────────────────────
    def bridge(locking, issuing):
        return {"LockingChainDoor": ALICE, "LockingChainIssue": locking,
                "IssuingChainDoor": ALICE, "IssuingChainIssue": issuing}

    bridge_inputs = [
        bridge({"currency": "XRP"}, {"currency": "XRP"}),
        bridge({"currency": "USD", "issuer": ALICE}, {"currency": "XAH", "issuer": XAH_ISSUER}),
        bridge("XRP", "XRP"),
        bridge({"currency": "BTC"}, {"currency": "XRP"}),
    ]
    # A door is an account field: one never set is an empty field and "", and
    # one set to the zero account is twenty zero bytes and its address.
    def doors(locking, issuing):
        return {"LockingChainDoor": locking, "LockingChainIssue": {"currency": "XRP"},
                "IssuingChainDoor": issuing, "IssuingChainIssue": {"currency": "XRP"}}

    def door_hex(door):
        return {"": "00", ZERO_ACCOUNT: "14" + Z20, ALICE: "14" + ALICE_HEX}[door]

    unset_doors = [doors("", ALICE), doors(ALICE, ""), doors("", "")]
    for value in unset_doors:
        DEVIATIONS[("bridge_encode", json.dumps(value))] = (
            door_hex(value["LockingChainDoor"]) + Z20 + door_hex(value["IssuingChainDoor"]) + Z20,
            "rippled prints a door that was never set as \"\" and will not read that back; "
            "x-data writes the empty field it decodes from, so the pair round-trips",
        )
    bridge_inputs += [doors(ZERO_ACCOUNT, ALICE), doors(ALICE, ZERO_ACCOUNT), *unset_doors]
    fx["bridge_encode"] = encode_section(oracle, "bridge", "bridge_encode", bridge_inputs, as_json=True)
    fx["bridge_decode"] = decode_section(
        oracle, "bridge",
        [e["hex"] for e in fx["bridge_encode"] if "hex" in e] + [e["xdata"] for e in fx["bridge_encode"] if "why" in e],
        as_json=True)

    # ── UInt64 ────────────────────────────────────────────────────────
    # rippled marks a few UInt64 fields base-ten: amounts, read and written
    # as decimal text. Every other UInt64 is hex. Which is which is asked of
    # the oracle, field by field, over the oracle's own field table: a field
    # is base-ten when rippled reads "255" as 255 and refuses "ff".
    ok, text = oracle.run(["server-definitions"])
    if not ok:
        sys.exit("xrpl-codec server-definitions failed: " + text)
    oracle_uint64 = sorted(n for n, m in json.loads(text)["FIELDS"] if m["type"] == "UInt64" and m.get("isSerialized"))
    base_ten = []
    hexadecimal = []
    for name in oracle_uint64:
        as_decimal, out_d = oracle.encode("stobject", json.dumps({name: "255"}))
        as_hex, _ = oracle.encode("stobject", json.dumps({name: "ff"}))
        if as_decimal and not as_hex and out_d.upper().endswith("00000000000000FF"):
            base_ten.append(name)
        elif as_hex:
            hexadecimal.append(name)
    fx["uint64_fields"] = [{"field": n, "base_ten": n in base_ten} for n in sorted(base_ten + hexadecimal)]

    # x-data can only be asked about the fields its own definitions hold.
    defs = json.loads((Path(__file__).resolve().parents[2] / "src/x-data/definitions/xrpl_definitions.json").read_text())
    known = {n for n, m in defs["FIELDS"] if m["type"] == "UInt64" and m.get("isSerialized")}

    decimal_inputs: list = []
    for name in base_ten:
        if name in known:
            for value in ("0", "1", "255", str(2 ** 63 - 1), str(2 ** 63), str(2 ** 64 - 1)):
                decimal_inputs.append({name: value})
    probe = next(n for n in base_ten if n in known)
    for value in (str(2 ** 64), "ff", "FF", "0xff", "00000000000000FF", "0255", "00", "-1", "-0", "+5",
                  " 5", "5 ", "1e3", "1.0", "", "12a", 255, -1, True, None):
        decimal_inputs.append({probe: value})
    DEVIATIONS[("uint64_decimal_encode", json.dumps({probe: 255}))] = (
        "refuses",
        "x-data takes a UInt64 as a string only; rippled also takes a small JSON integer",
    )
    fx["uint64_decimal_encode"] = encode_section(
        oracle, "stobject", "uint64_decimal_encode", decimal_inputs, as_json=True)
    fx["uint64_decimal_decode"] = decode_section(
        oracle, "stobject",
        sorted({e["hex"] for e in fx["uint64_decimal_encode"] if "hex" in e}), as_json=True)

    # The hex fields. x-data reads these as rippled does and writes them
    # differently (16 uppercase digits against lowercase without padding).
    # That is an open question, so the decode half of this is DISABLED_.
    hex_inputs: list = [{name: "ff"} for name in hexadecimal if name in known]
    hex_inputs.append({"ExchangeRate": "4f0415eb4ea0c727"})
    hex_inputs.append({"OwnerNode": "0"})
    fx["uint64_hex_encode"] = encode_section(oracle, "stobject", "uint64_hex_encode", hex_inputs, as_json=True)
    fx["uint64_hex_decode"] = decode_section(
        oracle, "stobject", [e["hex"] for e in fx["uint64_hex_encode"] if "hex" in e], as_json=True)

    # ── account fields: one never set, and one set to the zero account ──
    # STAccount writes a field that was never set as an empty field and
    # prints it as "", and writes the zero account, once set, as twenty zero
    # bytes and prints its address. 0x81 is Account, 0x83 Destination.
    fx["account_decode"] = decode_section(
        oracle, "stobject",
        ["8100", "8114" + Z20, "8300", "8314" + Z20, "8114" + ALICE_HEX], as_json=True)
    for name, header in (("Account", "81"), ("Destination", "83")):
        DEVIATIONS[("account_encode", json.dumps({name: ""}))] = (
            header + "00",
            "rippled prints an account field that was never set as \"\" and will not read "
            "that back; x-data writes the empty field it decodes from, so the pair round-trips",
        )
    fx["account_encode"] = encode_section(
        oracle, "stobject", "account_encode",
        [{"Account": ZERO_ACCOUNT}, {"Destination": ZERO_ACCOUNT}, {"Account": ALICE},
         {"Account": ""}, {"Destination": ""}],
        as_json=True)
    return fx


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--codec", default=DEFAULT_CODEC, help="path to rippled's xrpl-codec")
    ap.add_argument("--out", default=str(OUT))
    args = ap.parse_args()

    fx = build(Oracle(args.codec))
    lines = ["{"]
    keys = list(fx)
    for k in keys:
        value = fx[k]
        last = "" if k == keys[-1] else ","
        if isinstance(value, list):
            lines.append(f"  {json.dumps(k)}: [")
            lines += ["    " + json.dumps(e, ensure_ascii=True) + ("," if i + 1 < len(value) else "")
                      for i, e in enumerate(value)]
            lines.append("  ]" + last)
        else:
            lines.append(f"  {json.dumps(k)}: {json.dumps(value)}{last}")
    lines.append("}")
    Path(args.out).write_text("\n".join(lines) + "\n")
    for k in keys:
        if isinstance(fx[k], list):
            refused = sum("refused" in e for e in fx[k])
            print(f"{k}: {len(fx[k])} entries" + (f", {refused} refused by rippled" if refused else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
