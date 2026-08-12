# writ

Owned-asynchrony primitives over Boost.Asio, extracted from the peer-client
conformance corpus. A *writ* is a written grant of authority; the library's
question is the essay's question: does the writ still run here?

This is deliberately layer 0. It is not an Asio replacement, owns no reactor,
and refuses receipts for frame-local work (the proportionality gate).

## Primitives

| Primitive | Fossil | Counterexample it encodes |
|---|---|---|
| `writ::home` | `PeerSet::strand_` + `ASSERT_ON_STRAND` | execution home is orthogonal to authority owner; birth ≠ continuation; the strand probe must be out-of-line/noinline (clang-16 LICM, LLVM #63022 / fix 84c033d9) |
| `writ::retained_signal` | `detail::HistoryWaitSignal` | lost wake (cancel is an edge, not state); per-subscriber deadlines (sibling isolation); typed outcomes; cancellation sampled at the subscriber boundary dominates a concurrently queued shared wake |
| `writ::exact` (`generation`, `mint`, `incarnation<T>`, `exact_ref<T>`) | `PeerGeneration` / `reserve_connection_generation` / pins | stale incarnation aliasing; scope-first uniform refusal (no presence leak); counter honesty (exhaustion is sticky refusal, never wrap) |
| `writ::census<Key>` | `in_flight_` / `crawl_in_flight_` / `shutdown_drained_on_strand` / co_stop drains | admit-before-launch; sealed admission; drain is a conjunction, received (never inferred) via a composed retained_signal; leaks wedge visibly; refusals never counterfeit consumption |
| `writ::receipt<Tag>` + `receipt_slot` | drain receipts / `PeerUseReceiptState` | Tag-constructibility as the mint capability; closed disposition domain; latched first fact; exchange-with-empty consumption |

Each future primitive enters only with: a real counterexample, a forced-order
reproduction, and a negative control that can go red. One negative control per
primitive; no mutation cathedral before the source is stationary.

## Roadmap (in extraction order)

1. `scope` — spawn/join/transfer/borrow over home+census+exact; race with a
   named commit predicate that cancels and joins losers.
2. `probate` — shutdown settlement (reserved name).
3. Layer 2: stable model IDs (`writ.core/*/v1` schema namespace), the
   `//@@writ` annotation vocabulary, checker plugins, and live operation-tree
   dumps that share identifiers with the annotations.

## Sources of authority

- `essays/on-the-coroutine-tree` — the constitution.
- `.ai-docs/peer-client/design/owned-authority-state-model-lessons.md` — the
  modeling lessons this code must not regress.
- `.ai-docs/peer-client/design/asio-owned-async-cheatsheet.md` and
  `asio-macos-linux-differences.md` — portable-contract ledger.
- Second consumer required before any abstraction here is considered proven.
