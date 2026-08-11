# History peer selection Cuts 2B–4 — final implementation and live review

Review target: the implementation series after `f35b7ad`, through the shared
planner timestamp correction `7363d5c` and ordinary reconnect wake correction
`36e5f7e`, plus the final Cloud Run cutover evidence.

Disposition: **ACCEPT and retain the request-scoped history-selection cutover
in production.** Shared planning, bounded peer acquisition, exact header/node
`PeerUse`, and proof-builder integration are implemented and exercised through
the production transport. The named old Xahau proof did not complete in the
observed live peer topology; it crossed the former abort, verified its first
historical state path, then returned an honest timeout when the lone archival
peer supplied no further material. That is recorded as an availability result,
not a functional pass.

## Accepted implementation

- `co_drive_history_interest` now owns the shared participant loop: snapshot,
  inert plan, fair sponsor decision, exact grant, fresh owner-side execution,
  acknowledgement, exclusion, and revision/deadline wake. Competing `PeerSet`
  owners are woken after shared revisions and work settlement instead of
  privately polling or re-selecting.
- Header fetches retain exact `PeerUseCompletion` identities. Their bounded
  managed round admits no more than three rounds of three exact uses, halts
  synchronous fan-out after dispatch, and advances only on actionable evidence.
- Node fetches use `PeerUseOperations::NodeBatchChannel`. The coordinator seals
  the immutable node-batch intent against the exact use, writes and acknowledges
  through that channel, and closes the use from classified command/receipt
  completion. Uncorrelated node arrival remains separate content evidence and
  cannot impersonate transport completion.
- The proof builder no longer calls the legacy covering-peer selector or passes
  a generation pin into header/tree work. It contributes the immutable request
  scope; NodeCache and the shared driver own selection, acquisition, retry, and
  exact-use installation.
- The live harness accepts `--settle-seconds`, separating HTTP readiness from a
  deliberately warmed peer pool during reproducible validation.

## Live-discovered lifecycle defect

The first named Xahau history run found a real assertion at generation
creation. A terminal generation row remained keyed in the owner map; history
creation treated the endpoint as creatable and then attempted to insert the
same generation identity. Unit tests had not preserved that exact tombstone
shape.

The repair is deliberately narrow. Only a quiescent terminal tombstone is
replaceable: terminal lifecycle, no session, no active uses, no history origin,
no attachments, no watchdog, and no draining generation. The owner extracts
that exact row inside the same history-creation commit before installing the
new generation. Ordinary generation reservation now refuses any non-quiescent
terminal reuse. `CreationReplacesQuiescentTerminalGeneration` preserves the
failure shape as a regression, and provisional transition `prv.T0304` records
the exact retirement.

## Live-discovered temporal-scope defects

The next live run exposed two predicates whose subject or lifecycle phase was
too broad:

- `retry_not_before` is candidate-local current eligibility, not structural
  validity for the whole search snapshot. A deferred candidate beyond the
  request deadline was invalidating the snapshot and hiding unrelated admitted
  peers that were immediately usable.
- `physical_deadline` bounds connection establishment and demand-owned
  transient generations. It is not the lifetime of a connection after the
  admission commit. The execution guard was rejecting a healthy warmed peer
  after its old connection-attempt deadline elapsed.

The planner now keeps valid-but-deferred candidates inert without poisoning
the set. The owner-side execution guard applies the physical deadline only
before admission or to exact transient ownership; admitted connections remain
governed by their actual session lifecycle. The source-graph state label was
correspondingly narrowed to
`immutable_pre_admission_or_transient_generation_deadline`.

The shared modeling lesson now records the general rule as **predicate subject
+ lifecycle phase + quantifier domain**. Structural validity, current
eligibility, and eventual progress are separate facts, and a predecessor-phase
deadline cannot silently reacquire terminal authority after admission.

## Live-discovered ordinary reconnect liveness defect

The first history candidate could acquire Xahau peers during startup, exhaust
that initial frontier, and then remain permanently at zero peers. Its retry
gates retained failure time and magnitude, but no executor-owned root remained
after the attempt queue drained. `complete_connect_servant` pumped only already
queued work, while `consume_peer_terminal` removed the endpoint and provided no
future cause that could reach ordinary connection admission. The candidate's
Xahau counters remained at two connects and two disconnects overnight; a fresh
rollback instance immediately found Xahau peers.

The correction retains one earliest-eligible retry wake plus an exact census
of every admitted timer callback, including canceled and superseded callbacks.
Shutdown cancels scheduling intent but joins that callback population before
claiming drain. Attempt completion and exact admitted-peer terminal now
replenish configured/discovered ordinary roots. Queue admission and wake
selection share one steady-clock observation so the retry boundary cannot fall
between two samples and swallow the only successor.

## Automated evidence

- Full build: 49 targets completed; only pre-existing warnings remained.
- `xprv-gtest`: 193/193 passed, including transport-bounded proof-quorum
  selection.
- Full peer-client CTest: 755/755 passed.
- Reconnect controls:
  `RetryGateRetainsWakeUntilEndpointBecomesEligible`,
  `RetryQueueAndWakeShareEligibilityObservationAtBoundary`, and
  `StopCancelsAndJoinsRetainedConnectRetryWake` passed. The exact peer-terminal
  control also preserves the endpoint as a retry root and observes its retained
  wake.
- `peer-session-interface-gtest`: 137/137 passed, including the managed exact
  header path and a real PeerClient exact node-batch walk with no legacy pin.
- `peer-use-gtest`: 31/31 passed.
- `ledger-fetch-coordinator-bind-gtest`: 56/56 passed.
- `history-search-gtest`: 39/39 passed, including candidate-local retry gates
  and admitted use after the connection-attempt deadline.
- `node-cache-signal-gtest`: 24/24 passed.
- `peer-set-node-batch-bind-gtest`: 15/15 passed, including mismatched-ledger,
  wrong-kind, and finished-use refusal without resurrection.
- `xprv-shutdown-smoke.py --trace-shutdown`: bind-conflict, idle, and
  keep-alive boundaries passed at one and four I/O threads. Every live row was
  graceful, no row required forced termination, both XRPL and Xahau engine
  fences were present, and no validation heartbeat effect followed a fence.
  Final post-commit artifacts:
  `/var/folders/cx/wck9rldx18gb36r4p0sh_vdr0000gn/T/xprv-shutdown-smoke.st7x4y8b`.
- `git diff --check` passed.

## Live evidence

### Reconnect candidate and production cutover

- Cloud Build `27e7ef57-e22d-4107-ab9d-ad815ce6726b` built source identity
  `36e5f7e`; runtime digest:
  `sha256:2ca8ad56d2f0fa32491bd9c0541498c7bfba359185674fc09059a92719901824`.
- `xprv-reconnect-36e5f7e` was created at zero traffic. Its exact process run
  began with Xahau at one connect, one disconnect, and zero peers, then crossed
  the five-second retry boundary and reached three connects, one disconnect,
  and two peers. Across ten five-second samples the Xahau quorum advanced from
  24,974,704 to 24,974,717.
- The revision was promoted to 100 percent traffic with `xprv-00007-kkr`
  retained as the rollback target. Post-cutover health reported build
  `36e5f7e`; after the live proof probes Xahau had continued to ten connects,
  eight disconnects, and two peers with quorum 24,974,817. The latest status
  log independently showed three admitted Xahau peers and a current full
  19-of-19 quorum.
- The ERROR-or-higher Cloud Run audit contained only the three deliberately
  observed Xahau HTTP 504 proof responses. There was no assertion, abort,
  process fault, or undrained request.

### XRPL deep-history end-to-end control

- Transaction:
  `0FB184FC737FE7348B2674BA0E557BEDD0E57B24C2CEC5FBF1738E1551478943`
- Target ledger: 57,900,089; distance: 48,295,993 ledgers.
- Result after more than 110 seconds of peer warming: HTTP 200 in 2.858
  seconds; `/verify` returned `{"verified":true}`. This specifically exercised
  admitted peers beyond the old 60-second establishment deadline. Both state
  paths and the transaction tree verified, and shutdown was clean.
- Proof: `/private/tmp/xprv-phase-scope-fix-xrpl.json`.

### Xahau recent end-to-end control

- Transaction:
  `1FD40E173DB9F626764B1207899558E184FEB83BA9B6F6FD941780B3E2E2FB7B`
- Result: HTTP 200 in 2.933 seconds; `/verify` returned
  `{"verified":true}`. Both state paths and the transaction tree verified, the
  coordinator reported enabled, and shutdown was clean.
- Proof: `/private/tmp/xprv-phase-scope-fix-xahau-recent.json`.

### Required old Xahau history case

- Transaction:
  `08B6617262062EB99C6ABE4A29F8328077C38A0E06B84F02FFA534B93083336D`.
- RPC confirmed the transaction in ledger 21,865,716. Proof construction
  computed the required flag ledger 21,865,728 from a transport-covered anchor
  at 24,961,562.
- The exact production path crossed the former generation-insert assertion and
  verified the first old-history state tree with 69 placeholders. It then
  returned HTTP 504 in 24.235 seconds because the lone observed archival peer
  supplied no next old-ledger material. The final driver accounting was 39
  replans, 8 grants, 8 commits, 4 local terminals, and 1 shared terminal: this
  is an honest supply exhaustion, not the former zero-grant snapshot poison.
  The process remained alive, the engine drained, and shutdown was clean.
- This proves the repaired selection/creation/first-fetch path under the named
  workload. It does **not** claim a complete proof for that transaction under
  the observed archival-peer availability.
- Response: `/private/tmp/xprv-phase-scope-fix-xahau-old.json`.

### Exact promoted-revision proof controls

- The XRPL deep-history transaction above returned HTTP 200 in 2.046 seconds
  on `36e5f7e`. The 100,312-byte proof was submitted to production `/verify`,
  which returned `{"verified":true}`. Artifact:
  `/private/tmp/xprv-prod-xrpl-deep-36e5f7e.json`.
- The recent Xahau control returned honest HTTP 504 timeouts in 60.869 and
  11.165 seconds under the observed peer topology, including after a covering
  peer was admitted. The named old Xahau case returned the same classified
  timeout in 23.873 seconds. After all three, `active_proofs` was zero, the
  process run ID was unchanged, Xahau reconnect counters continued advancing,
  and both network quorum sequences remained current. Artifacts:
  `/private/tmp/xprv-prod-xahau-recent-36e5f7e.json`,
  `/private/tmp/xprv-prod-xahau-recent-36e5f7e-retry.json`, and
  `/private/tmp/xprv-prod-xahau-old-36e5f7e.json`.

## Authority evidence

- Final source graph after the phase-scope correction: PASS, 106 files, 467
  directives, 47 declared, 420 provisional, 0 refused, `valid=true`, with 69
  parser warnings. Artifact:
  `/private/tmp/history-peer-selection-phase-scope-v3.json`; SHA-256
  `a25a95a8250d10313ebb4cc73eb2ff35c097b0f3612be45e35e147ee1def4ba2`.
- Reconciliation: PASS, 20 rows, one composition contract with nine members;
  only the already recorded C04/T151–T163 bundle collisions remain.
- Coverage still exits 1 on the recorded repository baseline: 402 unannotated
  candidates, 12 directive rules too narrow, and one undecidable test seam.
  Conservation holds at 556 = 316 transition-covered + 228 state/channel + 12
  rule gaps; there is no model row without an AST witness and no directive
  without a binding site.
- The older conformance extractor still exits 1 because its closed grammar
  rejects the repository's newer `status`, `scope`, `bind`, and
  `@@authority-target` dialect. The current source-graph parser accepts all 467
  directives. This is a checker-version incompatibility, not hidden as a pass.
- The reconnect-focused source graph is also valid: 44 files, 371 directives,
  47 declared, 324 provisional, zero refused, and 47 parser warnings. Artifact:
  `/private/tmp/catalogue-peer-reconnect-authority-final.json`.
- Beta cppd/clangd corridor observations found exact paths from peer terminal
  through replenish to queue admission, from timer schedule to its callback,
  and directly from connect-servant completion to replenish. One broader query
  hit its 200-node bound and was not treated as an absence proof. Compiler-edge
  extraction remains corroboration until the persistent index and independent
  batch graph agree over the complete translation-unit population.

## Deployment conclusion

The accepted Cuts 2B–4 authority boundary is closed and revision
`xprv-reconnect-36e5f7e` now serves 100 percent of Cloud Run traffic. Exact
request identity drives shared search; exact use identity drives transport;
passive content arrival cannot mint or settle command authority; bounded
acquisition and terminal cleanup fail closed; deferred ordinary eligibility
retains an owned future cause and a shutdown-joined callback population.
Automated evidence and the XRPL deep-history end-to-end control are green.
Current Xahau proof supply
remains topology-dependent for both the named old case and the repeated recent
case, surfaced truthfully as bounded timeouts without freezing the pool or the
server.
