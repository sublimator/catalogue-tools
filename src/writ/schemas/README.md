# writ schemas — the namespaced contract

Stable semantic identities for the writ primitives, published ALONGSIDE the
corrected primitives and BEFORE the second consumer, so the model language
and the implementation cannot drift apart on founding day (round-4 review:
adding identities after composition turns them into commentary glued onto an
already-fixed shape).

## Namespace grammar

```
writ.core/<machine>/v<N>
```

- `writ.core` is **closed**: only this library adds machines or versions.
- A schema id is immutable. Changing any closed population (events,
  dispositions, rules) is a NEW version, never an edit.
- Project namespaces (e.g. `catl.history/*/v1`) **import** `writ.core/*/v1`;
  they never extend it. An application machine that composes a writ
  primitive names the import and adds only its own facts.
- Each C++ header names its schema in its opening comment
  (`schema: writ.core/<machine>/v1`), and each schema names the header it
  binds. The conformance corpus is the third leg: every rule lists the
  control that keeps it honest.

## `//@@writ` source annotations

Annotations carry **application facts the library cannot derive** — an
instance key, an application event name, a mapping onto a closed disposition
population:

```cpp
//@@writ machine=catl.history.candidate_wait
// instance=ledger_seq event=candidate_supply_changed
// disposition=signaled|deadline_elapsed|canceled
auto outcome = co_await signal->co_wait_until(deadline);
```

Rules:

- The C++ source adapter mechanically recognizes standard writ calls and
  derives their built-in transitions. Comments must NOT restate
  library semantics and must NOT certify that a call occurred — derivation
  is compiler/source evidence; annotation is semantic naming only.
- Annotations bind to the statement they precede, exactly like the
  peer-client's `//@@authority` ledger they will eventually replace.

## Progressive closure

- An **open fragment** renders paths, islands, uncovered effects, and
  warnings — exploration stays cheap.
- A **declared closure** names its instance key and its machine/event/output
  populations; after declaration, unknown variants, missing dispositions,
  unwoken waits, and unbound effects are HARD failures.
- Stable rule/machine ids double as future trace and operation-tree ids, so
  the runtime can explain itself in the same language the checker reads.

## Status

The five `v1` schemas are shape-**provisional**: their populations are
frozen facts about the shipped primitives, but the TOML layout itself may be
revised until the first checker/adapter lands (no checker machinery ships in
this round, by design). The first mechanically derived closure view is owed
by the second consumer.
