# xprv

The reference XPRV prover, verifier and server. The format is specified in
[`xls/spec-draft.md`](xls/spec-draft.md). This file describes how the reference
server holds and applies trust material. The spec defines the rules, and its
Appendix A lists what each reference verifier implements.

## The server's trust view

`xprv serve` keeps one engine per network. Each engine fetches its network's
validator list from the configured site and collects manifests and validations
from peers. Every proof it builds or verifies uses the trust view of spec §1.3,
built from that material.

**What `/verify` passes to the verifier.** Besides the proof, the verifier
receives an immutable snapshot of held material:

- **Validator manifests.** For every master the engine has seen listed on the
  proof's network, the newest manifest it holds, from any list it has loaded or
  from peers. A revocation in a newer list therefore reaches the verifier, and
  stays after a later list drops that master.
- **No filtering by the current list.** The snapshot is not filtered by the
  engine's current list. An older proof's own primary list may name a master
  the current list dropped, and the verifier's rule 1 filters by the proof's
  list.
- **Equivocation evidence.** The conflicting pair, for any validator the
  engine has seen equivocate.
- **The publisher manifest.** The newest one the engine has seen in a list
  response, so `/verify` refuses a proof whose list was signed under a
  publisher key since superseded.

**A publisher revocation is final.** A verified revocation of the publisher key
works as a validator's does. The engine drops its list, refuses every later
list, serves no proof (cached proofs included), and passes the revocation, so
`/verify` refuses every proof under that publisher.

**Proving uses the same rules.** The engine counts its proof quorum under the
same view. It attaches one validation per listed master, signed under that
master's current key. A validation under a superseded key, from a validator
caught mid-rotation, is not attached. An equivocating validator is dropped for
the life of the process. A revoked validator in a loaded list stays in N and
never votes (rule 3).

## Known gaps

- **Evidence in a refused list response.** A response that cannot replace the
  cached list, because its sequence is lower or unchanged, does not yet pass on
  its newer manifests. That includes a validator revocation or a publisher
  rotation, so the snapshot can lag that evidence until a list that does
  replace the cached one arrives.
- **Same-sequence publisher manifests.** A second, different publisher manifest
  at one sequence is not yet kept as a conflict.
- **Same-sequence lists.** A second list at the cached sequence is refused as a
  refresh, and the conflict is not kept. Rule 7 is applied only to lists bundled
  in a proof.
