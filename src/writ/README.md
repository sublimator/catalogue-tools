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
| `writ::exact` (`generation`, `mint`, `incarnation<T>`, `exact_ref<T>`) | `PeerGeneration` / `reserve_connection_generation` / pins / `peer_set_instance_id_` | stale incarnation aliasing; scope-first uniform refusal (no presence leak); counter honesty (exhaustion is sticky refusal, never wrap); unforgeable owner identity (generation carries the mint's process-unique epoch, so foreign owners and restored owners refuse) |
| `writ::census<Key>` | `in_flight_` / `crawl_in_flight_` / `shutdown_drained_on_strand` / co_stop drains | admit-before-launch; sealed admission; drain is a conjunction, received (never inferred) via a composed retained_signal; leaks wedge visibly AND commit on the dropping thread, surviving executor teardown; refusals never counterfeit consumption; member ids minted, never wrapped |
| `writ::receipt<Tag>` + `receipt_slot` | drain receipts / `PeerUseReceiptState` | Tag-constructibility as the mint capability, with move-only Tags so possession never grants the mint; closed disposition domain; latched first fact (refusal preserves the loser); exchange-with-empty consumption |
| `writ::roster<Label>` | VlCache stop obligations | census-shaped sealed multiset where duplicate diagnostic labels are legal and never identity; retirement resolves only by the internally minted member id; leaks and drain share the census lifecycle engine |

Each future primitive enters only with: a real counterexample, a forced-order
reproduction, and a negative control that can go red. One negative control per
primitive; no mutation cathedral before the source is stationary.

## Schemas

`schemas/writ.core/{home,retained-signal,exact,census,receipt,roster}/v1.toml`
publish each primitive's closed event/disposition populations and bind every
rule to the named control that keeps it honest; `schemas/README.md` defines
the namespace grammar and the `//@@writ` annotation rules. `writ.core` is
closed — project namespaces import it, never extend it. Landed with the
primitives (not after them) so the model language and the implementation
cannot drift apart; shape-provisional until the first checker.

## Roadmap (in extraction order)

1. `scope` — spawn/join/transfer/borrow over home+census+exact; race with a
   named commit predicate that cancels and joins losers. Waits for the
   second consumer to teach its shape.
2. `probate` — shutdown settlement (reserved name).
3. Layer 2 remainder: the C++ source adapter and checker plugins over the
   published schemas, and live operation-tree dumps that share identifiers
   with the annotations. The first mechanically derived closure view is owed
   by the second consumer.

## Sources of authority

- `essays/on-the-coroutine-tree` — the constitution.
- `.ai-docs/peer-client/design/owned-authority-state-model-lessons.md` — the
  modeling lessons this code must not regress.
- `.ai-docs/peer-client/design/asio-owned-async-cheatsheet.md` and
  `asio-macos-linux-differences.md` — portable-contract ledger.
- Second consumer required before any abstraction here is considered proven.
