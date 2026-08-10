# History peer selection Cuts 2B–4 — final implementation and live review

Review target: the implementation series after `f35b7ad`, including this
review in the final cutover commit.

Disposition: **ACCEPT the request-scoped history-selection cutover for
deployment.** Shared planning, bounded peer acquisition, exact header/node
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

## Automated evidence

- Full build: 49 targets completed; only pre-existing warnings remained.
- `xprv-gtest`: 191/191 passed.
- `peer-session-interface-gtest`: 136/136 passed, including the managed exact
  header path and a real PeerClient exact node-batch walk with no legacy pin.
- `peer-use-gtest`: 31/31 passed.
- `ledger-fetch-coordinator-bind-gtest`: 56/56 passed.
- `history-search-gtest`: 37/37 passed after the tombstone repair.
- `node-cache-signal-gtest`: 24/24 passed.
- `peer-set-node-batch-bind-gtest`: 15/15 passed, including mismatched-ledger,
  wrong-kind, and finished-use refusal without resurrection.
- `xprv-shutdown-smoke.py --trace-shutdown`: bind-conflict, idle, and
  keep-alive boundaries passed at one and four I/O threads. Every live row was
  graceful, no row required forced termination, both XRPL and Xahau engine
  fences were present, and no validation heartbeat effect followed a fence.
  Artifacts: `/var/folders/cx/wck9rldx18gb36r4p0sh_vdr0000gn/T/xprv-shutdown-smoke.uui5s01p`.
- `git diff --check` passed.

## Live evidence

### XRPL deep-history end-to-end control

- Transaction:
  `0FB184FC737FE7348B2674BA0E557BEDD0E57B24C2CEC5FBF1738E1551478943`
- Target ledger: 57,900,089; distance: 48,295,993 ledgers.
- Result: HTTP 200 in 3.58 seconds. Both state paths verified (87 and 79
  placeholders), the transaction tree hash verified, the coordinator reported
  enabled, and shutdown was clean.
- Log: `/private/tmp/xprv-live-proof-xrpl-settled-20260810/server.log`.

### Xahau recent end-to-end control

- Transaction:
  `1FD40E173DB9F626764B1207899558E184FEB83BA9B6F6FD941780B3E2E2FB7B`
- Target ledger: 24,959,200; distance: 433 ledgers.
- Result: HTTP 200 in 1.98 seconds. Both state paths verified (69 and 67
  placeholders), the transaction tree hash verified, the coordinator reported
  enabled, and shutdown was clean.
- Log: `/private/tmp/xprv-live-proof-xahau-recent-20260810/server.log`.

### Required old Xahau history case

- Transaction:
  `08B6617262062EB99C6ABE4A29F8328077C38A0E06B84F02FFA534B93083336D`.
- RPC confirmed the transaction in ledger 21,865,716. Proof construction
  computed the required flag ledger 21,865,728 from a current anchor at
  24,959,563, a distance of 3,093,847 ledgers.
- The exact production path crossed the former generation-insert assertion and
  verified the first old-history state tree with 68 placeholders. It then
  returned HTTP 504 at the internal 60-second bound because the lone observed
  archival peer supplied no next old-ledger material. The process remained
  alive, the engine drained, and shutdown was clean.
- This proves the repaired selection/creation/first-fetch path under the named
  workload. It does **not** claim a complete proof for that transaction under
  the observed archival-peer availability.
- Log: `/private/tmp/xprv-live-proof-20260810/server.log`.

## Authority evidence

- Final source graph: PASS, 106 files, 467 directives, 47 declared, 420
  provisional, 0 refused, `valid=true`, with 69 parser warnings. Artifact:
  `/private/tmp/history-peer-selection-final-v3.json`; SHA-256
  `5ac41d88f8bf12ef76c719a21b7d3277862b24be7a5edeec402d13fe3dde50f2`.
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

## Deployment conclusion

The accepted Cuts 2B–4 authority boundary is closed: exact request identity
drives shared search; exact use identity drives transport; passive content
arrival cannot mint or settle command authority; bounded acquisition and
terminal cleanup fail closed. Automated and two independent live end-to-end
controls are green. The remaining limitation observed for the named old Xahau
case is external archival supply, surfaced truthfully as a bounded timeout.
