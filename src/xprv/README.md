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
  proof's network, the newest manifest it holds, from any authenticated list
  response or from peers. A list that fails freshness or promotion still
  contributes its manifests, but never its membership. A revocation in a newer list therefore reaches the verifier, and
  stays after a later list drops that master.
- **No filtering by the current list.** The snapshot is not filtered by the
  engine's current list. An older proof's own primary list may name a master
  the current list dropped, and the verifier's rule 1 filters by the proof's
  list.
- **Equivocation evidence.** The conflicting pair, for any validator the
  engine has seen equivocate.
- **The publisher manifest.** The newest the engine has authenticated, whether
  or not the list that carried it replaced the cached one, so `/verify` refuses
  a proof whose list was signed under a publisher key since superseded. Every
  sequence seen is kept until a conflict, and the conflicting pair after it.

**A publisher revocation or conflict is final.** Either one bars proof
serving on that network for the life of the process, cached proofs included.

- **A revocation:** a verified revocation of the publisher key. The engine also
  drops its list and refuses every later one.
- **A conflict:** two different publisher manifests at one sequence. Only the
  publisher's own master key can produce that.

The engine passes the evidence (the revocation, or the conflicting pair) to
`/verify`, which then refuses every proof under that publisher. Only that
network is affected. `/health` reports it per network as `publisher_state`:
`revoked` or `equivocated`, instead of `active`.

**Proving uses the same rules.** The engine counts its proof quorum under the
same view. It attaches one validation per listed master, signed under that
master's current key. A validation under a superseded key, from a validator
caught mid-rotation, is not attached. An equivocating validator is dropped for
the life of the process. A revoked validator in a loaded list stays in N and
never votes (rule 3).

## Known gaps

- **Same-sequence lists.** A second list at the cached sequence is refused as a
  refresh, and the conflict is not kept. Rule 7 cannot be exercised yet: each
  proof bundles one primary list, the verifiers take manifests but no other
  lists, and the server keeps no conflicting signed list to pass on.
