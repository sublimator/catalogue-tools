#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Run the XPRV vector corpus against verifier executables.

A verifier is a command line that reads one proof file under configured trust
and exits 0 to accept or 1 to refuse. Anything else, a signal included, is an
abnormal exit and always a failure: a verifier must refuse bad input, not die
on it. That is the whole contract, so a third implementation runs the corpus
unchanged.

    scripts/xprv-conformance.py run
    scripts/xprv-conformance.py run --verifier mine='my-verifier {proof} --net {network_id} --key {publisher_key}'
    scripts/xprv-conformance.py run --only 'negative-*leaf*' --json report.json

The two reference verifiers are built in as `cpp` and `python`. Name others
with --verifier NAME=TEMPLATE; the template is split like a shell command and
may use {proof}, {network_id}, {publisher_key} and {manifests}. {manifests} is
a file holding a JSON array of base64 raw manifests the verifier is to hold
from outside the proof (spec §1.3); it is an empty array for most vectors. A
verifier that ignores it fails the vectors that need it.

For the built-in verifiers a refusal must also print the vector's
`reason_contains`, so refusing for the wrong reason fails too. Another
verifier's wording is its own; --check-reasons applies the check to it.

The built-in verifiers are also run with --json, and a refusal's report (spec
§7) must name the vector's `layer`: parse, trust or graph. --check-layers asks
the same of another verifier, which must then print that report, a JSON object
with a `layer` member, as its standard output.

A vector may carry `known_failing`: an object naming verifiers that do not yet
meet its expectation, each with what it `does` instead (accept, reject,
wrong-reason, wrong-layer, abnormal or timeout) and `because` of what. Such a verifier is
expected to miss in exactly that way. A different failure fails the run, and
so does meeting the expectation while still tagged, so a tag can neither hide
a new defect nor outlive the one it records.

This runner needs no XRPL library. It only builds each vector's bytes and
compares exit statuses.
"""
from __future__ import annotations

import argparse
import fnmatch
import importlib.util
import json
import os
import shlex
import signal
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
VECTOR_DIR = REPO / "tests" / "xprv" / "vectors"
VECTOR_TOOL = REPO / "scripts" / "xprv-vectors.py"

ACCEPT, REJECT, ABNORMAL, TIMEOUT = "accept", "reject", "abnormal", "timeout"
WRONG_REASON = "wrong-reason"
WRONG_LAYER = "wrong-layer"


def load_vector_tool() -> Any:
    """The corpus tool's edit functions, without importing a verifier."""
    spec = importlib.util.spec_from_file_location("xprv_vectors", VECTOR_TOOL)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot import {VECTOR_TOOL}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@dataclass
class Case:
    """One proof as a verifier receives it, and what it must do with it."""

    vector: str
    form: str  # json or binary
    data: bytes
    expect: str  # accept or reject
    network_id: int
    publisher_key: str
    # Base64 raw manifests the verifier holds from outside the proof (§1.3).
    manifests: list[str] = field(default_factory=list)
    reason_contains: str | None = None
    # The layer a refusal's report must name (§7): parse, trust or graph.
    layer: str | None = None
    known_failing: dict[str, dict[str, str]] = field(default_factory=dict)

    @property
    def label(self) -> str:
        return f"{self.vector} [{self.form}]"


def expand(path: Path, tool: Any) -> list[Case]:
    """Every concrete proof a vector file describes."""
    vec = json.loads(path.read_text())
    name = path.stem
    expect = vec.get("expect", {})
    verdict = expect.get("verdict")
    if verdict not in (ACCEPT, REJECT):
        raise SystemExit(f"{path.name}: expect.verdict must be accept or reject")
    known = vec.get("known_failing", {})
    kinds = {ACCEPT, REJECT, WRONG_REASON, WRONG_LAYER, ABNORMAL, TIMEOUT}
    layer = vec.get("layer")
    if verdict == REJECT and layer not in ("parse", "trust", "graph"):
        raise SystemExit(f"{path.name}: a reject vector names its layer: parse, trust or graph")
    if not isinstance(known, dict) or not all(
        isinstance(tag, dict) and tag.get("does") in kinds and isinstance(tag.get("because"), str)
        for tag in known.values()
    ):
        raise SystemExit(
            f"{path.name}: known_failing maps a verifier to {{does, because}}, "
            f"with does one of {', '.join(sorted(kinds))}"
        )

    def case(form: str, data: bytes, trust: dict[str, Any]) -> Case:
        return Case(
            vector=name,
            form=form,
            data=data,
            expect=verdict,
            network_id=int(trust["network_id"]),
            publisher_key=str(trust["publisher_key"]),
            manifests=list(trust.get("manifests", [])),
            reason_contains=expect.get("reason_contains"),
            layer=layer if verdict == REJECT else None,
            known_failing=dict(known),
        )

    if "base" in vec:
        base = json.loads((path.parent / f"{vec['base']}.json").read_text())
        trust = vec.get("trust") or base["trust"]
        if "json_patch" in vec:
            text = tool.patched_text(base["form"]["json"], vec["json_patch"])
            return [case("json", text.encode(), trust)]
        raw = tool.apply_mutations(bytes.fromhex(base["form"]["binary_hex"]), vec["mutations"])
        return [case("binary", raw, trust)]

    form = vec["form"]
    trust = vec["trust"]
    cases = []
    if "json" in form:
        cases.append(case("json", json.dumps(form["json"]).encode(), trust))
    if "binary_hex" in form:
        cases.append(case("binary", bytes.fromhex(form["binary_hex"]), trust))
    return cases


@dataclass
class Outcome:
    verdict: str
    detail: str = ""
    output: str = ""
    # Standard output alone: a reporting verifier prints its report there.
    stdout: str = ""


def run_verifier(template: list[str], case: Case, proof: Path, manifests: Path, timeout: float) -> Outcome:
    cmd = [
        part.format(proof=str(proof), network_id=case.network_id, publisher_key=case.publisher_key,
                    manifests=str(manifests))
        for part in template
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return Outcome(TIMEOUT, f"no exit within {timeout:g}s")
    except OSError as exc:
        raise SystemExit(f"cannot run {cmd[0]}: {exc}") from None
    text = (r.stdout + r.stderr).decode(errors="replace")
    stdout = r.stdout.decode(errors="replace")
    if r.returncode == 0:
        return Outcome(ACCEPT, output=text, stdout=stdout)
    if r.returncode == 1:
        return Outcome(REJECT, output=text, stdout=stdout)
    if r.returncode < 0:
        try:
            what = signal.Signals(-r.returncode).name
        except ValueError:
            what = f"signal {-r.returncode}"
        return Outcome(ABNORMAL, what, text)
    return Outcome(ABNORMAL, f"exit status {r.returncode}", text)


def reported_layer(stdout: str) -> str | None:
    """The `layer` of the report a verifier printed, or None if it printed none."""
    try:
        report = json.loads(stdout)
    except ValueError:
        return None
    layer = report.get("layer") if isinstance(report, dict) else None
    return layer if isinstance(layer, str) else None


def judge(case: Case, verifier: str, got: Outcome, check_reasons: bool,
          check_layer: bool = False) -> tuple[str, str]:
    """(status, note). Status is pass, known, fixed or FAIL."""
    met = got.verdict == case.expect
    does = got.verdict
    note = got.verdict + (f" ({got.detail})" if got.detail else "")
    # A file without the magic is not a binary proof to a command line, which
    # picks the form from the magic, so its stated binary reason cannot appear.
    recognisable = case.form == "json" or case.data[:4] == b"XPRV"
    if met and check_reasons and recognisable and case.expect == REJECT and case.reason_contains:
        if case.reason_contains.lower() not in got.output.lower():
            met = False
            does = WRONG_REASON
            note = f"refused, but not for {case.reason_contains!r}"
    if met and check_layer and case.expect == REJECT and case.layer:
        layer = reported_layer(got.stdout)
        if layer != case.layer:
            met = False
            does = WRONG_LAYER
            note = (f"refused at layer {layer}, and the vector says {case.layer}" if layer
                    else "refused, but printed no report naming a layer")
    if verifier in case.known_failing:
        tag = case.known_failing[verifier]
        if met:
            return "fixed", f"{note}; meets the expectation, remove its known_failing tag"
        # A tag excuses the failure it names and no other: an abort or a
        # timeout is never hidden behind a tag written for a wrong refusal.
        if does != tag["does"]:
            return "FAIL", f"tagged as known to {tag['does']}, but it now does this: {note}"
        return "known", f"{note}; {tag['because']}"
    if met:
        return "pass", note
    return "FAIL", f"expected {case.expect}, got {note}"


def cmd_run(args: argparse.Namespace) -> int:
    tool = load_vector_tool()
    verifiers: dict[str, list[str]] = {}
    xprv_bin = Path(args.xprv_bin)
    builtin = {
        "cpp": [str(xprv_bin), "verify", "{proof}", "{publisher_key}", "--network", "{network_id}",
                "--manifests", "{manifests}", "--json"],
        "python": [
            "uv", "run", "--script", str(REPO / "scripts" / "verify-xprv.py"),
            "{proof}", "--trusted-key", "{publisher_key}", "--network", "{network_id}",
            "--manifests", "{manifests}", "--json",
        ],
    }
    for name in args.use or ["cpp", "python"]:
        if name not in builtin:
            raise SystemExit(f"--use {name}: built-in verifiers are {', '.join(builtin)}")
        verifiers[name] = builtin[name]
    for spec in args.verifier or []:
        name, sep, template = spec.partition("=")
        if not sep or not name or not template:
            raise SystemExit(f"--verifier {spec!r}: expected NAME=COMMAND TEMPLATE")
        verifiers[name] = shlex.split(template)
    # The reference verifiers print why they refuse, so a refusal for the
    # wrong reason is caught. Another verifier's wording is its own.
    reasoned = set(verifiers) if args.check_reasons else set()
    if args.check_reasons is None:
        reasoned = set(verifiers) & set(builtin)
    # Likewise the layer: the built-in ones print a report (spec §7).
    layered = set(verifiers) if args.check_layers else set()
    if args.check_layers is None:
        layered = set(verifiers) & set(builtin)
    if "cpp" in verifiers and not xprv_bin.exists():
        raise SystemExit(f"{xprv_bin} not found; build xprv or pass --xprv-bin / XPRV_BIN")

    vector_dir = Path(args.vectors)
    paths = sorted(vector_dir.glob("*.json"))
    if args.only:
        paths = [p for p in paths if any(fnmatch.fnmatch(p.stem, pat) for pat in args.only)]
    if not paths:
        raise SystemExit(f"no vectors matched in {vector_dir}")
    cases = [c for p in paths for c in expand(p, tool)]

    rows: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="xprv-conformance-") as td:
        tmp = Path(td)
        files, held = [], []
        for i, c in enumerate(cases):
            f = tmp / f"{i:04d}.{'json' if c.form == 'json' else 'bin'}"
            f.write_bytes(c.data)
            files.append(f)
            # Always a file, so a template can name it unconditionally: a
            # vector with no held manifests gets an empty array.
            m = tmp / f"{i:04d}.manifests.json"
            m.write_text(json.dumps(c.manifests))
            held.append(m)

        def one(job: tuple[int, str]) -> tuple[int, str, Outcome]:
            i, name = job
            return i, name, run_verifier(verifiers[name], cases[i], files[i], held[i], args.timeout)

        jobs = [(i, name) for i in range(len(cases)) for name in verifiers]
        with ThreadPoolExecutor(max_workers=args.jobs) as pool:
            results = list(pool.map(one, jobs))

    by_case: dict[int, dict[str, Outcome]] = {}
    for i, name, got in results:
        by_case.setdefault(i, {})[name] = got

    counts = {"pass": 0, "known": 0, "fixed": 0, "FAIL": 0}
    disagreements = 0
    width = max(len(c.label) for c in cases)
    for i, c in enumerate(cases):
        verdicts = {name: by_case[i][name].verdict for name in verifiers}
        split = len(set(verdicts.values())) > 1
        disagreements += split
        row: dict[str, Any] = {
            "vector": c.vector, "form": c.form, "expect": c.expect,
            "verifiers": {}, "disagree": split,
        }
        cells = []
        for name in verifiers:
            status, note = judge(c, name, by_case[i][name], name in reasoned, name in layered)
            counts[status] += 1
            row["verifiers"][name] = {
                "verdict": by_case[i][name].verdict, "status": status, "note": note,
            }
            cells.append(f"{name}={by_case[i][name].verdict}" + ("" if status == "pass" else f" [{status}]"))
        rows.append(row)
        interesting = split or any(v["status"] != "pass" for v in row["verifiers"].values())
        if interesting or args.verbose:
            print(f"{c.label:<{width}}  expect {c.expect:<6}  " + "  ".join(cells))
            for name, v in row["verifiers"].items():
                if v["status"] != "pass":
                    print(f"{'':<{width}}    {name}: {v['note']}")

    print(
        f"\n{len(paths)} vectors, {len(cases)} proofs, {len(verifiers)} verifiers "
        f"({', '.join(verifiers)}): {counts['pass']} pass, {counts['known']} known failing, "
        f"{counts['fixed']} fixed but still tagged, {counts['FAIL']} FAIL; "
        f"{disagreements} proofs on which the verifiers disagree"
    )
    if args.json:
        Path(args.json).write_text(json.dumps({"counts": counts, "disagreements": disagreements, "rows": rows}, indent=2) + "\n")
    return 1 if counts["FAIL"] or counts["fixed"] else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run every vector against every verifier")
    r.add_argument("--vectors", default=str(VECTOR_DIR), help="vector directory")
    r.add_argument("--only", action="append", help="glob over vector names; repeatable")
    r.add_argument("--use", action="append", choices=["cpp", "python"],
                   help="built-in verifiers to run (default: both)")
    r.add_argument("--verifier", action="append", metavar="NAME=TEMPLATE",
                   help="another verifier; template uses {proof} {network_id} {publisher_key}")
    r.add_argument("--xprv-bin", default=os.environ.get("XPRV_BIN", str(REPO / "build" / "src" / "xprv" / "xprv")),
                   help="the C++ verifier (default: XPRV_BIN or build/src/xprv/xprv)")
    r.add_argument("--check-reasons", action=argparse.BooleanOptionalAction, default=None,
                   help="require expect.reason_contains in a refusing verifier's output "
                        "(default: for the built-in verifiers only)")
    r.add_argument("--check-layers", action=argparse.BooleanOptionalAction, default=None,
                   help="require a refusal's printed report to name the vector's layer "
                        "(default: for the built-in verifiers only)")
    r.add_argument("--timeout", type=float, default=60.0, help="seconds per proof")
    r.add_argument("--jobs", type=int, default=os.cpu_count() or 4)
    r.add_argument("--json", help="write a machine-readable report here")
    r.add_argument("-v", "--verbose", action="store_true", help="print passing proofs too")
    r.set_defaults(fn=cmd_run)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
