#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///
import argparse
import json
import random
import resource
import subprocess
import threading
import tempfile

# Raise fd limit — macOS defaults to 256
try:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
except Exception:
    pass
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TxCase:
  tx_hash: str
  ledger_index: int | None
  label: str


class InFlightTracker:
  """Thread-safe tracker for in-flight requests."""
  def __init__(self):
      self._lock = threading.Lock()
      self._active: dict[int, tuple[float, TxCase]] = {}  # idx → (start_time, case)

  def start(self, idx: int, case: 'TxCase'):
      with self._lock:
          self._active[idx] = (time.perf_counter(), case)

  def finish(self, idx: int):
      with self._lock:
          self._active.pop(idx, None)

  def snapshot(self) -> list[tuple[int, float, 'TxCase']]:
      """Returns list of (idx, elapsed_secs, case) sorted by elapsed desc."""
      now = time.perf_counter()
      with self._lock:
          items = [(idx, now - start, case) for idx, (start, case) in self._active.items()]
      items.sort(key=lambda x: -x[1])
      return items

  def count(self) -> int:
      with self._lock:
          return len(self._active)


@dataclass(frozen=True)
class HitResult:
  idx: int
  case: TxCase
  status: int
  elapsed: float
  proof_path: Path | None = None
  verify_ok: bool | None = None
  verify_rc: int | None = None
  skip_reason: str | None = None
  detail: str | None = None


def excerpt(data: bytes | str, limit: int = 160) -> str:
  if isinstance(data, bytes):
      text = data.decode("utf-8", errors="replace")
  else:
      text = data
  text = " ".join(text.strip().split())
  if len(text) > limit:
      return text[: limit - 3] + "..."
  return text


def load_cases(path: Path, n_txn: int, random_seed: int | None) -> list[TxCase]:
  data = json.loads(path.read_text())
  raw_cases = list(data.get("hot_set", [])) + list(data.get("cold_set", []))
  cases = [
      TxCase(
          tx_hash=c["tx_hash"],
          ledger_index=c.get("ledger_index"),
          label=c.get("label", ""),
      )
      for c in raw_cases
      if c.get("tx_hash")
  ]
  if not cases:
      raise SystemExit("no tx_hash values found in corpus")
  if random_seed is not None:
      rng = random.Random(random_seed)
      rng.shuffle(cases)
  return cases[: min(n_txn, len(cases))]


def hit(
    base_url: str,
    case: TxCase,
    idx: int,
    timeout: float,
    format_name: str,
    proof_dir: Path | None,
    xproof: Path | None,
    tracker: InFlightTracker | None = None,
) -> HitResult:
  suffix = "bin" if format_name == "bin" else "json"
  url = (
      f"{base_url}/prove?tx={urllib.parse.quote(case.tx_hash)}"
      f"&format={urllib.parse.quote(format_name)}"
  )
  started = time.perf_counter()
  if tracker:
      tracker.start(idx, case)
  req = urllib.request.Request(url, method="GET")
  try:
      with urllib.request.urlopen(req, timeout=timeout) as resp:
          body = resp.read()
          elapsed = time.perf_counter() - started
          proof_path = None
          verify_ok = None
          verify_rc = None
          skip_reason = None
          detail = None
          if resp.status == 200 and proof_dir is not None:
              proof_path = proof_dir / f"{idx:04d}-{case.tx_hash[:16]}.{suffix}"
              proof_path.write_bytes(body)
              if xproof is not None:
                  completed = subprocess.run(
                      [str(xproof), "verify", str(proof_path)],
                      capture_output=True,
                      text=True,
                  )
                  verify_rc = completed.returncode
                  verify_ok = completed.returncode == 0
                  if not verify_ok:
                      detail = excerpt(completed.stderr or completed.stdout or "")
          else:
              skip_reason = f"http_{resp.status}"
          return HitResult(
              idx,
              case,
              resp.status,
              elapsed,
              proof_path,
              verify_ok,
              verify_rc,
              skip_reason,
              detail,
          )
  except urllib.error.HTTPError as e:
      body = e.read()
      return HitResult(
          idx,
          case,
          e.code,
          time.perf_counter() - started,
          skip_reason=f"http_{e.code}",
          detail=excerpt(body),
      )
  except Exception as exc:
      return HitResult(
          idx,
          case,
          0,
          time.perf_counter() - started,
          skip_reason=type(exc).__name__,
          detail=excerpt(str(exc)),
      )
  finally:
      if tracker:
          tracker.finish(idx)


def fetch_json(url: str, timeout: float) -> dict:
  req = urllib.request.Request(url, method="GET")
  with urllib.request.urlopen(req, timeout=timeout) as resp:
      return json.loads(resp.read())


def summarize_peers(snapshot: dict) -> str:
  ready = snapshot.get("ready_peers", 0)
  connected = snapshot.get("connected_peers", 0)
  in_flight = snapshot.get("in_flight_connects", 0)
  queued = snapshot.get("queued_connects", 0)
  crawls = snapshot.get("queued_crawls", 0)
  wanted = snapshot.get("wanted_ledgers", [])
  peers = snapshot.get("peers", [])

  selected = [p for p in peers if p.get("selection_count", 0) > 0]
  selected.sort(
      key=lambda p: (
          -int(p.get("selection_count", 0)),
          -int(p.get("ready", False)),
          p.get("endpoint", ""),
      )
  )
  top = ", ".join(
      f"{p.get('endpoint')} sel={p.get('selection_count', 0)} "
      f"range={p.get('first_seq', 0)}-{p.get('last_seq', 0)}"
      for p in selected[:3]
  ) or "none"

  wanted_str = ",".join(str(x) for x in wanted[:4])
  if len(wanted) > 4:
      wanted_str += ",..."

  return (
      f"ready={ready}/{connected} in_flight={in_flight} queued={queued} "
      f"queued_crawls={crawls} wanted=[{wanted_str}] top={top}"
  )


def peers_poller(
    base_url: str,
    timeout: float,
    interval: float,
    stop_event: threading.Event,
    started: float,
    tracker: InFlightTracker | None = None,
) -> None:
  if interval <= 0:
      return
  while not stop_event.wait(interval):
      try:
          snapshot = fetch_json(f"{base_url}/peers", timeout=timeout)
          elapsed = time.perf_counter() - started
          parts = [f"PEERS +{elapsed:.1f}s {summarize_peers(snapshot)}"]
          if tracker:
              inflight = tracker.snapshot()
              parts.append(f"outstanding={len(inflight)}")
              if inflight:
                  laggards = inflight[:5]
                  lag_strs = [
                      f"{c.tx_hash[:12]}({e:.1f}s)"
                      for _, e, c in laggards
                  ]
                  parts.append(f"laggards=[{', '.join(lag_strs)}]")
          # Node cache stats from /health
          try:
              health = fetch_json(f"{base_url}/health", timeout=timeout)
              nc = health.get("node_cache", {})
              if nc:
                  parts.append(
                      f"ncache={nc.get('entries',0)}/{nc.get('max_entries',0)}"
                      f" hit={nc.get('hits',0)} miss={nc.get('misses',0)}"
                      f" fetch={nc.get('fetches',0)}"
                  )
          except Exception:
              pass
          print(" ".join(parts), flush=True)
      except Exception as exc:
          elapsed = time.perf_counter() - started
          print(f"PEERS +{elapsed:.1f}s error={exc}", flush=True)


def main() -> int:
  ap = argparse.ArgumentParser()
  ap.add_argument("--corpus", required=True)
  ap.add_argument("--base-url", default="http://127.0.0.1:8080")
  ap.add_argument("--n-txn", type=int, default=10)
  ap.add_argument("--repeat", type=int, default=1, help="requests per txn")
  ap.add_argument(
      "--format",
      choices=("json", "bin"),
      default="json",
      help="proof response format to request and optionally verify",
  )
  ap.add_argument(
      "--workers",
      type=int,
      help="concurrent workers; default is all requests at once",
  )
  ap.add_argument("--timeout", type=float, default=120.0)
  ap.add_argument(
      "--peers-interval",
      type=float,
      default=5.0,
      help="seconds between /peers summaries; 0 disables polling",
  )
  ap.add_argument(
      "--random-seed",
      type=int,
      help="shuffle the corpus deterministically before selecting n txns",
  )
  ap.add_argument(
      "--verify",
      action="store_true",
      help="save each successful proof and run `xproof verify` on it",
  )
  ap.add_argument(
      "--xproof",
      default="build/src/xproof/xproof",
      help="path to xproof binary for --verify",
  )
  ap.add_argument(
      "--proof-dir",
      help="directory to write downloaded proofs into when --verify is enabled",
  )
  ap.add_argument("--seed", type=int, help=argparse.SUPPRESS)
  args = ap.parse_args()

  random_seed = args.random_seed if args.random_seed is not None else args.seed
  cases = load_cases(Path(args.corpus), args.n_txn, random_seed)
  jobs: list[TxCase] = []
  for case in cases:
      jobs.extend([case] * args.repeat)
  workers = args.workers if args.workers is not None else max(1, len(jobs))

  print(
      f"loaded {len(cases)} txns, firing {len(jobs)} requests with {workers} workers "
      f"(random_seed={random_seed if random_seed is not None else 'off'}, format={args.format})"
  )
  for case in cases:
      print(
          f"txn {case.tx_hash} ledger={case.ledger_index if case.ledger_index is not None else '?'} "
          f"label={case.label or '-'}"
      )

  proof_dir: Path | None = None
  xproof_path: Path | None = None
  if args.verify:
      xproof_path = Path(args.xproof)
      if not xproof_path.exists():
          raise SystemExit(f"xproof binary not found: {xproof_path}")
      if args.proof_dir:
          proof_dir = Path(args.proof_dir)
          proof_dir.mkdir(parents=True, exist_ok=True)
      else:
          proof_dir = Path(tempfile.mkdtemp(prefix="prove-blast-"))
      print(f"proof_dir={proof_dir}")

  started = time.perf_counter()
  tracker = InFlightTracker()
  stop_event = threading.Event()
  poller = threading.Thread(
      target=peers_poller,
      args=(args.base_url, args.timeout, args.peers_interval, stop_event, started, tracker),
      daemon=True,
  )
  poller.start()

  status_counts: dict[int, int] = {}
  verify_counts: dict[str, int] = {"ok": 0, "fail": 0, "skipped": 0}
  skip_reason_counts: dict[str, int] = {}
  failed_results: list[HitResult] = []
  try:
      with ThreadPoolExecutor(max_workers=workers) as ex:
          futs = [
              ex.submit(
                  hit,
                  args.base_url,
                  case,
                  i + 1,
                  args.timeout,
                  args.format,
                  proof_dir,
                  xproof_path,
                  tracker,
              )
              for i, case in enumerate(jobs)
          ]
          for fut in as_completed(futs):
              result = fut.result()
              status_counts[result.status] = status_counts.get(result.status, 0) + 1
              parts = [
                  f"{result.idx}: {result.status} {result.elapsed:.3f}s",
                  f"ledger={result.case.ledger_index if result.case.ledger_index is not None else '?'}",
                  result.case.tx_hash[:16],
              ]
              if args.verify:
                  if result.verify_ok is True:
                      verify_counts["ok"] += 1
                      parts.append("verify=ok")
                  elif result.verify_ok is False:
                      verify_counts["fail"] += 1
                      failed_results.append(result)
                      parts.append(f"verify=fail(rc={result.verify_rc}) tx={result.case.tx_hash}")
                  else:
                      verify_counts["skipped"] += 1
                      reason = result.skip_reason or "unknown"
                      skip_reason_counts[reason] = skip_reason_counts.get(reason, 0) + 1
                      parts.append(f"verify=skipped({reason})")
              if result.detail:
                  parts.append(f"detail={result.detail}")
              print(" ".join(parts))
  finally:
      stop_event.set()
      poller.join(timeout=1.0)

  try:
      snapshot = fetch_json(f"{args.base_url}/peers", timeout=args.timeout)
      print(f"FINAL PEERS {summarize_peers(snapshot)}")
  except Exception as exc:
      print(f"FINAL PEERS error={exc}")

  summary = " ".join(
      f"{status}={count}" for status, count in sorted(status_counts.items())
  )
  print(f"SUMMARY {summary}")
  if args.verify:
      print(
          "VERIFY "
          f"ok={verify_counts['ok']} fail={verify_counts['fail']} skipped={verify_counts['skipped']}"
      )
      if skip_reason_counts:
          reasons = " ".join(
              f"{reason}={count}"
              for reason, count in sorted(skip_reason_counts.items())
          )
          print(f"VERIFY_SKIPS {reasons}")
      if failed_results:
          print(f"\nFAILED TX HASHES ({len(failed_results)}):")
          for r in failed_results:
              print(f"  {r.case.tx_hash} ledger={r.case.ledger_index} {r.case.label}")
              if r.proof_path:
                  print(f"    proof: {r.proof_path}")
              if r.detail:
                  print(f"    detail: {r.detail}")

  return 0


if __name__ == "__main__":
  raise SystemExit(main())
