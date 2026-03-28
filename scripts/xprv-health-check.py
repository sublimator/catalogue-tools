#!/usr/bin/env python3
"""Quick health check against xprv.it — pick a random tx from the corpus and prove it.

Randomly picks between JSON, binary, and SSE formats.

Usage:
    python3 scripts/xprv-health-check.py                    # random seed from time
    python3 scripts/xprv-health-check.py --seed 42          # reproducible
    python3 scripts/xprv-health-check.py --host localhost:8080
"""
import datetime, json, random, ssl, sys, time, urllib.request, argparse
from pathlib import Path

CORPUS = Path(__file__).parent / "xprv-tx-corpus.generated.json"

def check_json(url, ctx, timeout):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
        data = resp.read()
        body = json.loads(data)
        steps = len(body.get("steps", []))
        net = body.get("network_id", "?")
        return f"json steps={steps} network={net} bytes={len(data)}"

def check_bin(url, ctx, timeout):
    req = urllib.request.Request(url + "&format=bin", headers={"Accept": "application/octet-stream"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
        data = resp.read()
        magic = data[:4]
        assert magic == b"XPRV", f"bad magic: {magic!r}"
        return f"bin magic=XPRV version={data[4]} bytes={len(data)}"

def check_sse(url, ctx, timeout):
    req = urllib.request.Request(url, headers={"Accept": "text/event-stream"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
        events = []
        t_start = time.monotonic()
        t_prev = t_start
        for line in resp:
            line = line.decode("utf-8", errors="replace").strip()
            if line.startswith("data: "):
                now = time.monotonic()
                ev = json.loads(line[6:])
                etype = ev.get("type", "?")
                delta = now - t_prev
                elapsed = now - t_start
                step = ev.get("step", {}) if etype == "step" else {}
                step_type = step.get("type", "")
                events.append(etype)
                if etype == "error":
                    print(f"  sse ERROR: {ev.get('error', '?')}")
                # Extract useful context per step type
                detail = ""
                if step_type == "anchor":
                    detail = f" seq={step.get('ledger_index','?')}"
                elif step_type == "ledger_header":
                    hdr = step.get("header", {})
                    detail = f" seq={hdr.get('seq','?')}"
                elif step_type == "map_proof":
                    detail = f" tree={step.get('tree','?')}"
                label = f"{etype}({step_type})" if step_type else etype
                print(f"  sse: {label:30s} +{delta:.2f}s  @{elapsed:.2f}s{detail}")
                t_prev = now
        step_count = sum(1 for e in events if e == "step")
        has_done = "done" in events
        has_error = "error" in events
        status = "done" if has_done else "error" if has_error else "incomplete"
        return f"sse events={len(events)} steps={step_count} status={status}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--host", default="xprv.it")
    ap.add_argument("--timeout", type=int, default=90)
    args = ap.parse_args()

    if args.seed is not None:
        seed = args.seed
    else:
        # Auto-incrementing seed from a state file so each run picks a different tx
        state_file = Path(__file__).parent / ".xprv-health-seed"
        try:
            seed = int(state_file.read_text().strip()) + 1
        except (FileNotFoundError, ValueError):
            seed = 1
        state_file.write_text(str(seed))
    rng = random.Random(seed)

    corpus = json.loads(CORPUS.read_text())
    all_txs = corpus["hot_set"] + corpus["cold_set"]
    pick = rng.choice(all_txs)

    tx = pick["tx_hash"]
    label = pick["label"]
    ledger = pick.get("ledger_index", "?")
    scheme = "http" if "localhost" in args.host else "https"
    url = f"{scheme}://{args.host}/prove?tx={tx}"

    mode = rng.choice(["json", "bin", "sse"])
    now = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] seed={seed} mode={mode} label={label} ledger={ledger} tx={tx[:16]}...")

    ctx = ssl.create_default_context()
    t0 = time.monotonic()
    try:
        if mode == "json":
            result = check_json(url, ctx, args.timeout)
        elif mode == "bin":
            result = check_bin(url, ctx, args.timeout)
        else:
            result = check_sse(url, ctx, args.timeout)
        elapsed = time.monotonic() - t0
        print(f"OK {elapsed:.1f}s {result}")
    except Exception as e:
        elapsed = time.monotonic() - t0
        print(f"FAIL {elapsed:.1f}s {e}")
        print("don't forget to check the logs ;)")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
