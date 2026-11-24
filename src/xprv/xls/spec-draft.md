# XPRV: XRPL Proof Verification Format

| | |
|---|---|
| **Status** | Draft |
| **Date** | 2026-11-24 |
| **Authors** | Nicholas Dudfield ([@sublimator](https://github.com/sublimator)) |
| **Discussion** | [XRPLF/XRPL-Standards#107](https://github.com/XRPLF/XRPL-Standards/discussions/107) |
| **Reference** | XLS-41d XPOP (Richard Holland), [XRPL-Standards](https://github.com/XRPLF/XRPL-Standards) |
| **Companion** | [Plain-English Guide](companion.md) — walkthrough, trust model, and design rationales |

## Acknowledgements

This work builds on the XPOP (XLS-41d) standard by Richard Holland ([@RichardAH](https://github.com/RichardAH)), which defined the original proof-of-payment format for XRPL transactions.

The key insight enabling historical proofs — using LedgerHashes (skip lists) as the cryptographic bridge between a current validated ledger and any historical ledger — emerged from a discussion with [@wojake](https://github.com/wojake) about the ephemeral nature of validator signatures. After exploring and rejecting approaches that would require validators to re-sign historical ledgers (contentious, poor incentives), [@mDuo13](https://github.com/mDuo13) suggested the use of LedgerHashes, which are already maintained by the protocol and provide a compact, permanent navigational structure through the ledger history.

See [XRPLF/XRPL-Standards#107 comment](https://github.com/XRPLF/XRPL-Standards/discussions/107#discussioncomment-7344712) for the original discussion.

## Abstract

XPRV is a compact, offline-verifiable proof that data exists (or existed) in an XRPL or Xahau ledger. It extends the XPOP concept (XLS-41d) with composable proof steps that can reach any historical ledger via skip list navigation, and adds a compact binary representation designed for progressive verification.

The name XPRV refers to **XRPL Proof Verification**. It is unrelated to the
`xprv` prefix used for BIP32 extended private keys.

Two representations of the same proof, no mixing:

- **JSON form** — human-readable, for debugging and review
- **Binary form** — compact TLV-encoded, for on-ledger storage, wire protocol, QR codes

Both forms encode the same proof data and MUST verify identically. Every valid
JSON proof has a binary form. A proof has only a binary form in one case:

- A `map_proof` leaf (`0x03` / `0x04`) carries bytes for which no conforming
  JSON leaf exists under §2.3 (for example, fields the network codec cannot
  decode and re-encode exactly). That proof has no JSON form (§2.3, §3.8) and
  verifies in binary.

An anchor `STValidation` that does not round-trip under the XRPL codec is
invalid in both forms. It is not a JSON proof (§2.1), and it is not a §3.4
binary payload.

## Normative Language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**,
and **MAY** in this document are to be interpreted as described in RFC 2119.

## Normative References

XPRV depends on existing XRPL protocol encodings rather than redefining them.
When this document says "canonical STObject serialization", "field order",
or "XRPL variable-length prefix", it means the XRPL binary codec rules and
definitions referenced below.

| Reference | Used for |
|-----------|----------|
| [XRPL Binary Format](https://xrpl.org/docs/references/protocol/binary-format) | Canonical STObject serialization, field IDs, canonical field order, and XRPL variable-length `Blob` prefix encoding |
| [XRPL LedgerHashes](https://xrpl.org/docs/references/protocol/ledger-data/ledger-entry-types/ledgerhashes) | Skip-list SLE fields, recent/previous-history semantics, and LedgerHashes keylet derivation |
| [XRPL binary codec definitions](https://github.com/XRPLF/xrpl.js/blob/main/packages/ripple-binary-codec/src/enums/definitions.json) | Type codes, field codes, ledger-entry codes, transaction result codes, and field metadata |
| [XRPL rippled protocol layer](https://github.com/XRPLF/rippled/tree/develop/src/xrpl/protocol) | Manifest and STValidation signing formats, `MAN\0` and `VAL\0` signing data, signatures, and public-key formats |
| XLS-41d XPOP ([XRPL-Standards](https://github.com/XRPLF/XRPL-Standards)) | Prior proof-of-payment trust chain and proof vocabulary |
| [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) | Normative language |
| [RFC 4648](https://www.rfc-editor.org/rfc/rfc4648) | Canonical padded base64 (§3.5) for validator manifests inside signed validator-list blobs |
| [RFC 8032](https://www.rfc-editor.org/rfc/rfc8032) | Ed25519 signature verification |
| [FIPS 180-4](https://csrc.nist.gov/pubs/fips/180-4/upd1/final) | SHA-512 |
| [RFC 1950](https://www.rfc-editor.org/rfc/rfc1950) | zlib compressed data format |

## 1. Proof Graph Model

XPRV is logically a directed acyclic graph (DAG) of authenticated objects.
JSON and binary forms serialize that DAG as a parent-first ordered list of
steps.

Three step types:

| Type | Consumes | Produces |
|------|----------|----------|
| `anchor` | nothing | trusted ledger hash |
| `ledger_header` | trusted ledger hash (from an anchor, a parent header, or a skip list leaf) | trusted header plus its `parent_hash`, `transaction_hash`, and `account_hash` |
| `map_proof` | trusted tree root | one or more trusted leaves from that tree; recognized LedgerHashes leaves also produce ledger hashes |

Validity rules:

- A proof MUST contain exactly one logical `anchor`, and every other step MUST
  be serialized after the anchor's *first* record. In the JSON form the anchor is
  a single step and is literally first. In the binary form the anchor is split
  across two TLV records (§3.3): `0x01` is first, and the optional `0x05` is
  last. "Anchor first" therefore means anchor-core-first, not
  anchor-complete-first.
- Every non-anchor step MUST be supported by an earlier authenticated ancestor.
- A `ledger_header` is supported when its computed hash is authenticated by the
  anchor, by a prior header's `parent_hash`, or by a hash contained in an
  authenticated skip list leaf.
- A `map_proof` is supported when its recomputed root matches the `transaction_hash` or
  `account_hash` of an earlier authenticated `ledger_header`.
- A `map_proof` MAY contain one or more leaves. All included leaves are
  authenticated outputs of that step.
- Multiple later steps MAY share the same supporting ancestor, so the logical
  proof is a tree/DAG even though the serialized form is a list.

Implementation status notes are collected in Appendix A.

### 1.1 Trust and Field Authentication

Trust enters the proof exactly once: the verifier's configured Validator List
(VL) publisher key. Every subsequent object is authenticated by signature or
by preimage membership — a field is trusted only because it lies inside bytes
whose hash or signature is already trusted, never because a step asserts it.
Validator manifests newer than the list's own are no exception: they count
only when signed by a master key the list already names (§1.3).

The anchor's validators sign full STValidation messages — which include a
`LedgerSequence` field alongside the ledger hash — but chain verification
consumes precisely one attested value from them: the 32-byte ledger hash. The
anchor step itself carries no `ledger_index` field.
Because the ledger hash is `SHA512-Half` of the full header preimage
(§5), a `ledger_header` step that matches a trusted hash authenticates **all**
of its fields simultaneously — `ledger_index`, `total_coins`, `parent_hash`,
`transaction_hash`, `account_hash`, and the close-time fields. Ledger indices
in a verified proof therefore come only from authenticated header preimages
(an anchor-only proof establishes the validated ledger hash alone), and cannot
be forged without a hash collision.

The full chain of custody:

```
verifier config: trusted publisher master key
 └─ MAN\0 manifest sig ──────────→ publisher signing key
     └─ sig over raw blob bytes ─→ VL blob (validator manifests)
         └─ MAN\0 manifest sigs ─→ validator signing keys (the Unique Node
                                     List, UNL; a newer manifest signed by
                                     a listed master replaces its key, §1.3)
             └─ VAL\0 sigs, quorum ≥ ⌈0.8·N⌉ ─→ anchor ledger hash (32 bytes;
                 │                               the only attested value the
                 │                               chain consumes)
                 └─ LWR\0 preimage match ─→ all 9 header fields at once
                     ├─ parent_hash ──────→ previous ledger hash
                     ├─ account_hash ─────→ state map root
                     │   └─ trie root recompute ─→ state leaves
                     │       └─ Hashes (skip-list serialized ledger entry,
                     │                  SLE) ────→ up to 256 more
                     │           │                        ledger hashes
                     │           └─ LWR\0 preimage match ─→ historical
                     │                                      header … (recurse)
                     └─ transaction_hash ─→ tx map root
                         └─ trie root recompute ─→ tx + metadata leaves
```

A skip list's `Hashes` vector is positional, but verification does not rely
on position: whichever header matches one of those hashes carries its own
`ledger_index` inside its preimage, so the index is self-authenticating.

### 1.2 Step Order and Proof Identity

All non-UNL steps MUST be serialized parent-before-child so every step is
supported by an earlier authenticated ancestor (§1). Producers SHOULD emit the
compact canonical traversal: depth-first pre-order over the proof DAG, with a
header's children ordered state-tree first, then tx-tree, then `parent_hash`
child header; at most one merged multi-leaf `map_proof` per authenticated root;
and trie branches in `0`-`F` order. Verifiers MUST accept any valid
parent-before-child order.

**Proof identity versus claim identity.** A proof's content-addressed
identity is `SHA512-Half` (§5.4) of its **uncompressed binary form** —
`SHA512-Half(header_with_flags_zero || uncompressed_TLV_body)` — where the
10-byte header has `flags` zero and the uncompressed TLV body (§3.1–§3.7,
minimal LEB128 per §4) preserves the proof's step order, with `0x05` present
if and only if the proof bundles its list. Compression (§3.1) is a container
option only: decompress before hashing. Because version 10's `0x05` carries
exact signed list bytes (§3.5), every valid JSON proof has an uncompressed
binary form and therefore a well-defined proof identity.

This specification deliberately does **not** define a byte-canonical JSON form
or a unique canonical proof per ledger fact. Two JSON documents that differ
only in member order or whitespace encode the same proof and MUST verify
identically; two valid proofs of the same datum legitimately have different
proof identities whenever they differ in anchor ledger, validation subset, step
order, or `0x05` presence. Applications that identify or deduplicate what was
proven MUST key on the authenticated inclusion claim —
`(network_id, authenticated ledger_hash, tree, leaf_key)` (plus the hashed leaf
bytes or transaction result as §5.5 requires) — never on proof identity. A
verifier caching verdicts MUST finish full parse and framing validation of the
received representation before consulting or storing a cached verdict (§1.4).

### 1.3 Trust Material

A verifier assesses the anchor against a **trust view** that it resolves before
it counts any vote and holds fixed for the whole proof. The view has two parts:

- **Membership.** The validator master keys, and their count N, come from
  exactly one signed Validator List: the **primary list** (P1–P4 below).
- **Delegations.** For each master in the primary list, the signing key that
  counts is the one named by the highest-sequence manifest that verifies under
  that master. The candidates are the master's manifest inside the primary list
  and any other validator manifests the verifier holds.

**The anchor's close time.** The only time reference is the `close_time` of the
anchor ledger: the `ledger_header` whose computed hash equals the anchor hash.
In a proof with any step after the anchor, that header is the next logical
step, because the anchor hash is the only capability the anchor produces (§1).
The verifier checks that the header's preimage hashes to the anchor hash and
reads its `close_time` before it selects the primary list and counts quorum.
The header grants no capability until the anchor has verified. An anchor-only
proof has no close time: it establishes only that the anchor hash was
validated by a quorum of the primary list, and claims nothing about when.
Verification never consults the verifier's own clock, so for the same trust
material the verdict does not depend on when the proof is checked (newer
material can still change it; see Staleness below).

**Selecting the primary list.**

- **P1.** The publisher's master key is the configured primary publisher key
  for the proof's network (§8). Its signing key is the one named by the
  highest-sequence publisher manifest the verifier holds that verifies under
  that master (rules 2 and 9 cover a conflicting or revoked one).
- **P2.** The candidates are the lists the verifier holds that verify under
  that signing key. The list bundled in the proof is one of them. A bundled
  list that does not verify is not a candidate, and it does not poison a
  primary list the verifier already holds. A list signed with a superseded
  publisher signing key is not a candidate; its validator manifests still are,
  since each verifies under its own master.
- **P3.** The primary list is the candidate with the highest list `sequence`
  among those in effect at the anchor's close time
  (`effective <= close_time < expiration`; a list without `effective` is in
  effect from 0). When the proof has the anchor ledger's header and no
  candidate is in effect, membership is unavailable and the anchor MUST be
  rejected. An anchor-only proof has no close time and skips only that
  comparison against a ledger close time. It does not waive candidate-list
  well-formedness, soundness, manifest checks, or quorum. Its primary list is
  the highest-sequence candidate, and it fails when there is no candidate. The
  interval is a consistency check, not a barrier against the list's own
  validators, who attest the close time themselves.

A decoded list `blob` is well-formed only when:

- its bytes are valid UTF-8 with no UTF-8 byte-order mark (BOM);
- its JSON text contains no non-finite numbers (`NaN`, `Infinity`, `-Infinity`)
  and no duplicate object members at any depth;
- its root value is a JSON object whose `sequence`, `expiration`, and optional
  `effective` (default `0`) are uint32 integer tokens — integers in
  `0 .. 2^32 - 1`, never a float, boolean, or string — satisfying
  `expiration > effective`;
- its `validators` member is a JSON array of objects, each carrying a string
  `validation_public_key` (66 hex characters, 33 bytes, matched case-insensitively
  against the entry manifest's `PublicKey` because the signed list is
  publisher-authored JSON rather than XPRV's JSON form) and a string
  `manifest` whose standard padded RFC 4648 base64 re-encoding of the decoded
  bytes equals the original string byte-for-byte (rejecting non-alphabet
  characters, whitespace, bad padding, and non-zero unused pad bits per
  RFC 4648 §3.5). The publisher signature still covers the original signed
  list bytes verbatim.

A blob that fails any of these checks is not a candidate, whether or not the
proof has a close time.
- **P4.** A lower-sequence list from the same publisher contributes only its
  validator manifests, like a secondary list.

**Resolution rules.** Rules 1–8 resolve the view; rules 9–11 state the
publisher's revocation and the conditions a list must meet before any
delegation from it is resolved.

1. A validator manifest is a candidate only if it verifies (§5.1) and its
   `PublicKey` is a master in the primary list. A manifest for any other master grants
   nothing.
2. The highest `Sequence` wins. Byte-identical candidates are one candidate.
   Two candidates with the same master and the same `Sequence` but different
   raw bytes are equivocation, at any sequence the verifier holds, not only
   the highest: only that master's key could have signed both,
   so it is misused or compromised. That master is treated as revoked (rule 3):
   it stays in N, never votes, and the verifier SHOULD report the conflict. The
   rest of the anchor is unaffected. A publisher manifest conflict is different:
   the list's own trust root is in doubt, and the anchor MUST be rejected.
3. If the winning manifest is a revocation (`Sequence` `0xFFFFFFFF`), that
   master has no signing key. N does not change.
4. If a resolved signing key is also another master's resolved key, or equals
   any master key in the primary list, the view is unresolvable and the anchor
   MUST be rejected.
5. Each master contributes at most one vote, and only through its resolved
   signing key. A validation signed by a superseded key does not count.
6. Quorum is ⌈0.8 × N⌉ masters, N being the number of masters in the primary
   list. N MUST be at least 1; a primary list with no masters makes
   membership unavailable. XPRV counts against this N and does not apply the
   Negative UNL.
7. Two different **eligible** lists under the primary publisher key with the
   same highest eligible `sequence` (P3) make the view unresolvable, and the
   anchor MUST be rejected: the publisher itself has equivocated at the
   selected sequence, so there is no membership to fall back on.
   Byte-identical signed `blob`s are one list. A higher unique eligible
   sequence is ordinary selection: ineligible blobs at the same sequence and
   conflicts at lower sequences do not trigger this rule.
8. Only full validations count. A validation without `vfFullValidation`
   (`Flags & 0x00000001`) is a partial validation and never counts toward
   quorum, as in rippled.
9. If the winning publisher manifest is a revocation, the publisher has no
   signing key: no list verifies, and the anchor MUST be rejected. A revoked
   publisher is final, as a revoked validator is.
10. The primary list itself MUST be sound before supplementary validator
    manifests are applied to its delegations.
    The anchor MUST be rejected if the list's own manifests give two masters
    the same signing key, or give a master a signing key equal to any master
    key in the list. Two masters can share a delegation only through a misused
    or compromised key, so held manifests cannot repair such a list. The resolved
    view is then checked again under rule 4.
11. Keys are bound, and the primary list names each master once:
    - `unl.public_key` equals the configured primary publisher key, and any
      candidate publisher manifest's `PublicKey` equals that key. This holds
      for a list-less anchor as well;
    - each primary-list entry's `validation_public_key` equals the `PublicKey`
      of that entry's own manifest;
    - a primary list that names a master twice, or carries a manifest that is
      not valid (§5.1), is invalid, and the anchor MUST be rejected. An invalid
      manifest held from elsewhere is simply not a candidate (rule 1).
      Secondary lists and lower-sequence lists supply only individual verified
      validator manifests under rule 1. They are not the list this rule's
      second and third bullets constrain.

The rules sort what the material implicates. A validator's own revocation or
equivocation removes that validator's vote and does not by itself invalidate
the rest of the view (rules 2 and 3); quorum must still be met without it.
Evidence against the trust root (a conflicting or revoked publisher, rules 2
and 9), against the list (rules 7, 10 and 11), or a key two masters resolve to
(rule 4), makes the anchor unavailable. A manifest whose signing key equals its
own master key is not a valid manifest (§5.1): held from elsewhere it is never a
candidate, and inside the primary list it makes the list invalid (rule 11).

**Bundled list alongside externally held lists.** Parse-layer envelope faults
(§2.4, §3.8) reject the proof before trust resolution. On a well-framed proof,
the bundled `unl.manifest` enters P1 independently whenever it verifies under
the configured primary key (and is ignored otherwise, without poisoning held
material), while an unverified or P3-malformed bundled `blob` is simply not a
list candidate: it does not poison externally held lists, and validator
manifests are never harvested from it (only from signature-verified,
P3-well-formed lists and discrete manifests). Once a primary list is selected
by P3, a rule 10 or rule 11 defect rejects the anchor at the trust layer with
no fallback to a lower-sequence list.

**Sources and packaging.** Lists and validator manifests authenticate
themselves by signature, so where they come from affects only availability,
never trust. Material may be:

- **ambient**: already held by the verifier or fetched by it on its own, e.g.
  from a cache, its own peer connections, or any node's `manifest` RPC;
- **direct**: handed over by whoever supplies the proof, e.g. a file or an
  endpoint;

and either **discrete** (separate objects, verified and cached on their own) or
**bundled** with the proof (the anchor's embedded list and `0x05` are the
bundled form of one candidate list).

XPRV defines no wrapper for discrete material; it uses the formats XRPL nodes
already exchange:

- **A validator list** is the publisher's signed list document, the one rippled
  fetches from its configured list sites. It is not itself a `unl` object, but
  each candidate list it carries maps onto one (§2.1):
  - version 1 (`public_key`, `manifest`, `blob`, `signature`, `version`): one
    candidate, whose `unl` members are those four fields, with `version`
    dropped;
  - version 2 (`public_key`, `manifest`, `blobs_v2`, `version`): one candidate
    per `blobs_v2` entry, with that entry's `blob` and `signature`, the
    document's `public_key`, and the entry's own `manifest` if it has one, else
    the document's.

  The site document carries `manifest` and `blob` as base64, and `blob`'s
  content is the list JSON; a `unl` object carries the same bytes as hex
  (§2.1). Decode, then re-encode; the bytes are what signatures cover.
- **A validator manifest** is its raw serialized STObject, the bytes carried
  in a list entry and in peer manifest messages, and returned base64-encoded by
  rippled's `manifest` RPC.

Each verifies by its own signatures (§5.1), so the transport that delivers it
is irrelevant. Every combination MUST resolve the same view
from the same material under the same verifier policy. Supplied material never
lowers what the verifier already holds: an older manifest does not displace a
newer one it knows.

**One primary publisher per network.** A verifier is configured with exactly
one primary publisher key per network. Any other publisher keys it holds for
that network are secondary publishers. Merging the memberships of several
publishers is out of scope.

**Secondary lists.** A verifier MAY be configured with secondary list
publishers. A secondary list contributes only its validator manifests, as
candidates under rule 1. It adds no masters, does not affect N, and never
stands in for the primary list: if the primary list is unavailable or expired,
membership is unavailable. A secondary list's own expiry does not matter for
this use, since manifests do not expire.

**Embedded-only verification.** A verifier holding no material beyond the
proof resolves the view from the embedded list alone, delegations included.
That is conforming.

**Staleness.** Manifests carry no dates, so a view describes the delegations
the verifier knows now, not those in force when the anchor ledger closed. A
proof whose quorum depended on keys since rotated can fail under a newer view.
That is expected; the remedy is a fresher anchor (§1.4). A verifier MUST NOT
fall back to an older view after failing under its current one.

### 1.4 Replacing the Anchor

The anchor is the only step whose authority comes from signatures. Everything
below it is authenticated by hashes and never goes stale. A proof therefore
divides into three parts:

- **Anchor:** proves that ledger hash `H_a` was validated, under a trust view.
- **Bridge:** headers and LedgerHashes leaves (§5.3) that authenticate an older
  ledger hash `H_t` starting from `H_a`.
- **Tail:** the steps that consume `H_t`, typically a target header and its
  map proofs.

A stale proof is renewed by a new anchor, a bridge from the new anchor ledger
to `H_t`, and the retained tail. The result still contains exactly one anchor
(§1): the old anchor is removed, although its ledger header may remain as an
ordinary step if the bridge reaches it. Renewed proofs follow the
canonical-order rule of §1.2, and producers SHOULD build the canonical bridge
(§6) from the new anchor rather than extend the old bridge.

For a historical target reached through a flag ledger (§6.2), every step from
the flag ledger's header downward is independent of the anchor: the flag ledger
depends only on the target, and every later ledger's state holds that era's
long LedgerHashes list, which contains the flag ledger's hash. Renewal then
replaces only the anchor, the anchor ledger's header and its long-list
`map_proof`.

A renewed proof is a different proof artifact with a different identity (§1.2).
Separately, a verdict depends on more than the proof bytes: two verifiers
holding different trust material or definitions snapshots can reach different
verdicts on identical bytes. Any cached verdict (accepting or rejecting) MUST
be keyed by: (1) the proof's §1.2 identity after full parse and framing
validation of the received representation, (2) the verifier's policy and
protocol-definitions identity, (3) the full resolved trust view (including any
lower-sequence manifest equivocation or eligible-list conflict, not winning
manifests alone), and (4) when the proof has descendants, the matching anchor
ledger header's `close_time` checked against the primary list window. If any of
these inputs is unavailable, the verdict MUST NOT be cached. See
[companion.md](companion.md) for practical cache-key guidance.

Renewal produces an ordinary proof of the current format version. A tail
without an anchor is not a proof; carrying tails or anchors as standalone
objects would need a new format version (§9.1).

## 2. JSON Form

The proof chain is a JSON object with top-level `format_version`,
`network_id`, and an array of step objects. The `steps` array is the canonical
parent-first linearization of the logical proof DAG.

```json
{
    "format_version": 10,
    "network_id": 0,
    "steps": [
        { "type": "anchor", ... },
        { "type": "ledger_header", ... },
        { "type": "map_proof", ... }
    ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `format_version` | uint8 | XPRV format version (shared 1-byte version space with the binary form) |
| `network_id` | uint32 | `0` = XRPL mainnet, `21337` = Xahau mainnet |
| `steps` | array | Ordered sequence of step objects |

XPRV has a single format version: the JSON `format_version` and the binary
file header `version` byte (§3.1) carry the same number. Verifiers MUST
reject proofs with an unrecognized version. A proof is this object: a bare
array of steps carries no version, and verifiers MUST reject it.

The JSON form is closed. The proof object, every step, every `header` and every
`unl` carry exactly the members this section lists, and verifiers MUST reject
any other member. The one exception is the diagnostic `__depth__` on a trie
inner node (§2.3). A member nothing hashes is an assertion nothing checks, and
a closed form leaves no place for one. Integer fields are integer number
tokens: `7`, not `7.0`, `"7"`, or a boolean.

All proof-level hashes, binary blobs, and structural hex keys in the JSON form
are **uppercase hexadecimal only** (ASCII `0`-`9` and `A`-`F`, even length for
byte strings). There is no case normalization on the proof envelope or trie —
verifiers MUST reject lowercase hex digits in `anchor.ledger_hash`, `unl.*`,
`validations` keys and values, `header.*_hash`, trie branch keys, trie
pruned-subtree placeholders, and trie leaf keys. Decoded leaf content is
governed by §2.3 (matching `rippled` / `xahaud` reference JSON, where ordinary
`UInt64` fields are unpadded lowercase hex). Ledger header fields are named and
typed.

### 2.1 Anchor Step

```json
{
    "type": "anchor",
    "ledger_hash": "<64 hex>",
    "unl": {
        "public_key": "<66 hex, 33-byte XRPL master public key of VL publisher>",
        "manifest": "<hex, raw publisher manifest bytes>",
        "blob": "<hex, raw UNL blob bytes>",
        "signature": "<hex, publisher signature over blob>"
    },
    "validations": {
        "<hex, signing public key>": "<hex, serialized STValidation>",
        ...
    }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger_hash` | hex string (32 bytes) | Hash of the anchor ledger |
| `unl.public_key` | hex string (33 bytes) | VL publisher's master public key, XRPL 33-byte format (`0xED` Ed25519 or `0x02`/`0x03` compressed secp256k1; XRPL and Xahau mainnet publishers use `0xED`). Always present. |
| `unl.manifest` | hex string (non-empty) | Publisher manifest (binary STObject) binding master → signing key |
| `unl.blob` | hex string (non-empty) | Exact raw signed list bytes (parsed and checked at §1.3 P3 candidate admission) |
| `unl.signature` | hex string (non-empty) | Signature over raw blob bytes by manifest's signing key |
| `validations` | object | Map of signing_key_hex → raw STValidation bytes. The map key MUST be exactly the embedded STValidation `SigningPubKey` encoded as uppercase hex. |

The anchor carries no `ledger_index` field: ledger sequence numbers in a
verified proof come only from authenticated `ledger_header` preimages (§1.1).

`unl.manifest`, `unl.blob` and `unl.signature` are present together or not at
all, and when present each MUST be non-empty. An anchor whose `unl` carries
only `public_key` does not bundle its list: it is the JSON counterpart of a
binary proof without `0x05` (§3.3), and the same rules apply. The verifier
resolves the list from trust material it holds (§1.3) and MUST reject the proof
if it holds none. A `unl` with some but not all of the three members, with any
empty bundled member, or with any other member, MUST be rejected.

A list's `effective` and `expiration` (in the signed blob) are seconds since
the Ripple Epoch (2000-01-01 00:00:00 UTC), the same unit as a ledger's
`close_time` (§2.2). A list without `effective` is in effect from 0. The blob's
`sequence`, `expiration`, and `effective` are uint32 integer tokens with
`expiration > effective`, as §1.3 requires of a candidate. A float such as
`100.0` is not that token.

Every `validations` entry, in JSON and in binary `0x01`, MUST be a complete
canonical `STValidation` under the XRPL codec named in the normative
references: the bytes round-trip, and the object contains `LedgerHash` (32
bytes), `Signature`, and a 33-byte `SigningPubKey` whose prefix is `0xED`,
`0x02`, or `0x03`. The round-trip is against that verifier's
protocol-definitions snapshot (Appendix A): a validation carrying a field the
snapshot cannot decode fails the round trip and is refused. The
check is not a closed list of field names. In JSON, the outer map key MUST
equal that `SigningPubKey` in uppercase hex. `Signature` is present when the
field exists, including when its blob is empty. A verifier MUST reject the
anchor when any entry fails this. An empty blob is not that failure: emptiness,
and any other failure of signature canonicality or verification, leaves the
vote uncounted. Omitting `Signature` fails this structural check. An entry that
passes it counts toward quorum only when its `LedgerHash` equals the anchor
hash, its signature is canonical and verifies the `STValidation` signing data
defined in §5.1, `Flags` has `vfFullValidation`, and the key is a primary-list
master's resolved signing key (§1.3 rules 5 and 8), at most one vote per
master. Failing one of those four leaves the vote uncounted. It does not reject
the anchor, and the remaining votes can still make quorum.

An anchor carries the VL snapshot it was built against, so an offline verifier can
establish quorum entirely from the proof — or, per §3.3, from equivalent trust it
already holds. Producers SHOULD include every distinct validation available for
the anchor ledger, not merely the minimum that reaches quorum, and including
validations signed with keys newer than the embedded list's manifests: a
verifier that resolves a different view (§1.3) needs the surplus to establish
quorum under its signing keys.

Quorum is assessed against the one trust view §1.3 resolves. Membership never
comes from more than one list, since mixing memberships allows quorum to be
assembled from validators that were never simultaneously trusted. A proof whose
embedded snapshot is stale still verifies for an embedded-only verifier as long
as that snapshot yields quorum; a verifier holding newer manifests may reject
it (§1.3, Staleness).

### 2.2 Ledger Header Step

```json
{
    "type": "ledger_header",
    "header": {
        "ledger_index": 103188348,
        "total_coins": "99999999999000000",
        "parent_hash": "<64 hex>",
        "transaction_hash": "<64 hex>",
        "account_hash": "<64 hex>",
        "parent_close_time": 751234560,
        "close_time": 751234567,
        "close_time_resolution": 10,
        "close_flags": 0
    }
}
```

> **Alignment note:** The canonical XPRV ledger-header JSON subset intentionally
> aligns with rippled's ledger JSON naming for the hashing-relevant fields:
> `ledger_index`, `total_coins`, `parent_hash`, `transaction_hash`,
> `account_hash`, `parent_close_time`, `close_time`,
> `close_time_resolution`, and `close_flags`. Extra convenience fields such as
> `ledger_hash` are intentionally excluded from this canonical subset.

All fields REQUIRED:

| Field | Type | Binary size |
|-------|------|-------------|
| `ledger_index` | uint32 | 4 bytes |
| `total_coins` | decimal string (unsigned 64-bit integer with no leading zeros, as rippled writes it) | uint64, 8 bytes |
| `parent_hash` | hex string | 32 bytes |
| `transaction_hash` | hex string | 32 bytes |
| `account_hash` | hex string | 32 bytes |
| `parent_close_time` | uint32 | 4 bytes |
| `close_time` | uint32 | 4 bytes (seconds since the Ripple Epoch: 2000-01-01 00:00:00 UTC) |
| `close_time_resolution` | uint8 | 1 byte |
| `close_flags` | uint8 | 1 byte |

Total canonical binary size: **118 bytes** (fixed).

### 2.3 Map Proof Step

```json
{
    "type": "map_proof",
    "tree": "tx" | "state",
    "trie": { ... }
}
```

The trie is a nested JSON structure. Three JSON types discriminate three node types:

| JSON type | Node type |
|-----------|-----------|
| **String** (64 hex chars) | Pruned subtree hash (placeholder) |
| **Object** (keys `"0"`-`"F"`) | Inner node |
| **Array** `[key, data]` | Leaf node |

The root `trie` value MUST be an inner-node object. A bare `[key, data]` leaf
or a bare hash string is not a root: the binary trie always begins with a
4-byte branch header (§3.7), and a lone-leaf tree root is out of scope (§3.8).
Verifiers MUST reject that shape at parse time.

The leaf `key` is a 64-character uppercase hex string (32 bytes, §2). Leaf
`data` is an object carrying the leaf's decoded XRPL content, and nothing else:

- State tree: the decoded ledger entry, also called a serialized ledger entry
  (SLE) after canonical STObject serialization.
- Transaction tree: an object with exactly the members `tx` and `meta`: the
  decoded transaction and its metadata.

The verifier re-serializes that content to canonical binary and hashes the result
(§5.2): state leaves as canonical STObject bytes, tx leaves as
`VL(tx) || VL(meta)`.

**Leaf round-trip validity rule.** Codecs silently drop unknown or
non-serialized fields, accept alternate spellings, and can coerce types, so
"nothing else" is checked by round-trip rather than assumed. Before hashing, a
verifier MUST serialize the leaf content (`sle` for state; `tx` and `meta`
separately for tx) with its network codec, decode those exact serialized bytes
into the network's **reference JSON** — the exact JSON emitted by `rippled` on
XRPL (`network_id == 0`) or `xahaud` on Xahau (`network_id == 21337`), with no
RPC-only convenience members (`index`, `hash`, `blob`, `delivered_amount`,
etc.) — and reject the leaf unless the input JSON equals that decoded reference
object under strict comparison:

- **Objects:** identical member-name sets at every depth; no extra, unknown,
  or non-serialized members, and no `null` values. The authenticated leaf key
  is the trie's `key` element, never an `index` or `hash` member inside the
  leaf object.
- **Arrays:** identical length and element order.
- **Types:** identical JSON types at every position — booleans never equal
  numbers, numbers never equal strings, integers never equal floats.
- **Numbers:** equal numeric values (closing out-of-range integer truncation).
- **Strings:** exact byte-for-byte string equality against the reference
  rendering, with no alternate-spelling tolerance. In particular, following
  `rippled` / `xahaud`:
  - issued-currency `Amount.value` uses `STAmount::getText()` and `Number` uses
    `to_string(Number)` (one exact text per value; no alternate decimal or
    scientific notation);
  - ordinary `UInt64` fields use unpadded lowercase hex (e.g. `"1"`, `"ff"`),
    while base-ten `UInt64` fields (`MaximumAmount`, `OutstandingAmount`,
    `MPTAmount`, `LockedAmount`, and XRPL's `ConfidentialOutstandingAmount`)
    use base-ten decimal strings;
  - `PathSet` hops carry `rippled`'s `type` bitmask integer alongside their
    present hop fields;
  - binary blobs, hashes, enum names, classic base58 addresses (never
    X-addresses), and currency codes match the reference rendering exactly.

**Network reference JSON for native assets.** Only the network's own system
code is native (`"XRP"` on XRPL `network_id == 0`, `"XAH"` on Xahau
`network_id == 21337`); on XRPL, `"XAH"` with an `issuer` is an ordinary
issued currency, not native XRP. Following `rippled` / `xahaud`, the zero
currency's JSON shape depends on the field type:

- **`Issue`-valued fields** (including `XChainBridge` endpoints): the object
  `{"currency": "XRP"}` on XRPL or `{"currency": "XAH"}` on Xahau, with no
  `issuer` member (`Issue::setJson`).
- **`Currency`-valued fields and `PathSet` hop `currency` members:** the bare
  network-native string (`"XRP"` or `"XAH"`).
- **Native `Amount` fields:** a decimal drops string.

JSON leaves are decoded only. A leaf for which no conforming JSON leaf exists
under the rules above has no JSON form: the producer MUST NOT emit that proof
as JSON, and SHOULD direct the consumer to the binary form, whose leaves are
the exact bytes and need no codec to verify. A hex string is not leaf data,
and verifiers MUST reject it.

> **Why decoded only.** What a reader sees must be what the verifier hashed.
> Re-serializing the decoded content is what binds the legible form to the
> authenticated one. A leaf that also carried its hashed bytes would make only
> those bytes authenticated, leaving the displayed members as unauthenticated
> assertions free to disagree with what was proven. The binary form already
> carries exact bytes for any leaf, including one whose fields a codec doesn't
> know, so JSON doesn't need a second, opaque leaf form.

At each inner node, ALL non-empty branches MUST be present so the verifier can
recompute the parent hash. Branches on the proof path are objects (recurse);
branches off the path are hash strings (pruned).

#### Minimal trie shape and inner-node members

Every trie, in both JSON and binary form (§3.7), has one minimal shape:

- Every inner node MUST have at least one non-empty branch (an empty inner node
  `{}` is invalid).
- Every inner node MUST have at least one leaf somewhere in its subtree (an
  expanded subtree containing only pruned hash placeholders is invalid).
- No pruned hash placeholder may be all-zero (`0000...0000`, 32 zero bytes): an
  empty branch is represented by absence in JSON and by `00` in binary.
- Every leaf at depth `d` (`1 <= d <= 64`, child of an inner node at 0-indexed
  depth `d - 1`) MUST have a 32-byte `key` whose first `d` nibbles match the
  `d` branch nibbles on the path from the root to that leaf.
- Inner nodes sit at 0-indexed depths `0..63` (leaves and placeholders at
  depths `1..64`). An inner node at depth `>= 64` MUST be rejected.

Branch keys are uppercase only: `0`-`9` and `A`-`F`. Lowercase is not a branch
key, and there is no case normalization for branch keys — the canonical set of
ordinal translations is uppercase.

An inner node's members are exactly those branch keys, plus the optional
diagnostic `__depth__`. Verifiers MUST reject an inner node carrying any other
member, or more than one member for the same branch.

`__depth__` is a diagnostic. Where present it MUST be an integer number token
equal to the node's 0-indexed nesting depth — `0` at the root, one nibble per
level — and verifiers MUST reject a mismatch. It is derivable from structure
and grants no authority on its own: nothing hashes it.

Accepting both `a` and `A` would put two members on one branch. One is hashed
into the root and the other is not, so a leaf can be enumerated without being
authenticated — smuggling, against blind or automated readers. Verifiers MUST
derive the branch set once and use that parse for every purpose: root recompute,
leaf enumeration, re-encoding, and display.

A `map_proof` MAY contain one or more leaves from the same authenticated root.
Canonical form uses a single `map_proof` step per authenticated tree root and
includes all required leaves in that trie rather than emitting repeated sibling
proofs against the same root.

Leaves are canonical only when they participate in the proof graph: either they
provide data consumed by a descendant step, or they are retained as terminal
authenticated outputs of the proof. Extra authenticated leaves beyond that are
non-canonical.

### 2.4 JSON Parsing Policy

A JSON verifier MUST reject at the parse layer:

- an unrecognized version, a `network_id` outside `0 .. 2^32 - 1` (uint32), a
  bare array of steps, an unknown step type (§2), or a proof that does not
  contain exactly one logical anchor as its first step (§1);
- a member the closed form does not list, an integer field that is not an
  integer token, or any lowercase hex digit in a proof-level hex member or trie
  branch key (§2; leaf content is governed by §2.3);
- a `total_coins` string that is not the decimal text of an unsigned 64-bit
  integer with no leading zeros (§2.2);
- a `validations` entry whose map key is not 66 uppercase hex characters
  matching the embedded `SigningPubKey`, or whose value is not a complete
  canonical `STValidation` (§2.1);
- a `unl` that bundles its list only in part, or carries an empty `manifest`,
  `blob`, or `signature`, or a `public_key` that is not 33 bytes with prefix
  `0xED`, `0x02`, or `0x03` (§2.1);
- a trie root that is not an inner-node object, an empty inner node, an inner
  node with no leaf beneath it, an all-zero (`00...00`) hash placeholder, a
  leaf whose `key` does not begin with its trie path nibbles, or an inner node
  at 0-indexed depth `>= 64` (§2.3);
- a trie inner node with a lowercase or non-branch member, a repeated branch,
  or a mismatched `__depth__` (§2.3). This branch-spelling class is JSON-only:
  a binary trie identifies a branch by position in a 2-bit field (§3.7), so a
  branch there cannot be misspelled or repeated;
- duplicate object members anywhere in the proof;
- text that is not UTF-8, or that carries a UTF-8 byte-order mark;
- a non-finite number (`NaN`, `Infinity`, `-Infinity`);
- hex-string leaf data, or any leaf that fails the strict codec round-trip
  check of §2.3 (`null` or boolean values, non-serialized/unknown members,
  numeric codes for known enums, X-addresses, wrong native-currency spelling,
  or an amount, `Number`, `UInt64`, or path step in any text or shape other
  than the reference rendering);
- text longer than its configured maximum, checked before parsing (§8), or JSON
  nested deeper than the verifier's nesting limit. Valid proofs stay well below
  128 levels (a 64-level trie plus envelope and XRPL leaf structure), so
  verifiers MUST support JSON nesting to at least 128 levels and MAY refuse
  deeper JSON at the parse layer.

A signed validator-list `blob` is subject to the same UTF-8, no-BOM,
finite-number, and unique-member rules plus P3's field and canonical padded
RFC 4648 base64 checks; a blob that fails them is disqualified as a candidate
under §1.3 (and rejects the anchor if no valid primary list remains).

The binary counterpart is §3.8.

## 3. Binary Form

### 3.1 File Header

Every binary proof begins with a **10-byte file header**:

```
Offset  Size  Field
  0       4   magic       "XPRV" (0x58 0x50 0x52 0x56)
  4       1   version     0x0A (10; same version space as JSON format_version)
  5       1   flags       bit 0: zlib compressed body
  6       4   network_id  big-endian uint32 (0 = XRPL, 21337 = Xahau)
```

A binary input shorter than 10 bytes or whose first 4 bytes are not `"XPRV"`
is malformed and MUST be rejected. When flag bit 0 is set, the entire body (all
TLV records concatenated) is compressed as a single RFC 1950 zlib stream. The
consumer decompresses before parsing TLVs. The body is that one complete
stream: a truncated stream, a second stream, or any byte after the stream's end
makes the proof malformed. Compression is a container option only; it does not
change the proof or its uncompressed binary projection (§1.2).
Flag bits 1-7 are reserved and MUST be zero. Verifiers MUST reject a binary
proof with any reserved flag bit set.

**Endianness rule:** every multi-byte integer in the binary form is
big-endian (network byte order, matching XRPL canonical serialization). The
trie branch header (§3.7) is defined as a byte array and has no endianness.

### 3.2 TLV Framing

After the file header, the body is a sequence of TLV (Type-Length-Value) records:

```
[type: 1 byte][length: LEB128 varint][payload: <length> bytes]
```

Type bytes:

| Byte | Step type | Description |
|------|-----------|-------------|
| `0x01` | Anchor core | Ledger hash, publisher key, raw STValidations |
| `0x02` | Ledger header | Fixed 118-byte canonical layout |
| `0x03` | Map proof (tx) | Binary trie for transaction tree |
| `0x04` | Map proof (state) | Binary trie for account state tree |
| `0x05` | Anchor list | Publisher manifest, list signature, exact signed validator list bytes |

All other type bytes are reserved in the current format version. Verifiers MUST
reject unknown TLV types.

Length is an unsigned LEB128 varint, minimal and at most 5 bytes (§4):

| Payload size | Length bytes |
|-------------|-------------|
| < 128 | 1 |
| < 16,384 | 2 |
| < 2,097,152 | 3 |

### 3.3 TLV Ordering

TLV ordering puts the smaller proof chain ahead of the optional validator list:

```
[0x01 anchor core]       ← anchor hash, publisher key, raw validations
[0x02 header]            ← anchor close_time and tree roots
[0x03/0x04 map proofs]   ← proves data in the ledger
[0x02 header] ...        ← parent_hash / skip-list hops (if historical)
[0x05 anchor list]       ← LAST (optional): exact signed validator list
```

The anchor is split into two TLV records (`0x01` core + `0x05` list) so the
validation signatures and proof chain arrive before the ~15KB validator list. A
body MUST contain exactly one `0x01`, and `0x01` MUST be the first record.
**`0x05` is OPTIONAL**, MUST appear last when present, and a body MUST NOT
contain more than one.

Omitting `0x05` produces a **complete, separately framed list-less proof** for
cases where the verifier already holds the validator list externally — for
example an anchor previously published on-chain, or a UNL cached from a prior
proof — avoiding ~15KB of duplicated list bytes on bandwidth-constrained
transports (QR, BLE, NFC).

A verifier that receives a proof without `0x05` MUST resolve the primary list
from its own configured or cached trust for the proof's publisher key (§1.3),
and MUST reject the proof if it cannot. It MUST NOT treat an absent `0x05` as
an absent trust requirement: the anchor's validations still have to reach
quorum against a verified UNL, and the publisher key still has to be one the
verifier trusts (§8). An absent `0x05` changes where the UNL comes from, never
whether one is required.

Verifiers that do not implement externally held lists MUST require `0x05`,
assemble the logical anchor by merging its payload into the `0x01` anchor core,
and verify that single logical anchor before any descendant step is trusted.
Such a verifier rejects a `0x05`-less proof at the **trust** layer (for want of
trust material), not at the **parse** layer — the distinction matters for error
reporting, because the proof is well-formed and verifiable by a
better-provisioned verifier.

**Finality.** A verifier reading records incrementally MAY issue a definitive
refusal immediately on any parse or framing fault, but an **acceptance** (and
any quorum shortfall a trailing `0x05` could reverse) is final only once the
entire body has been framed and verified — including single-stream RFC 1950
zlib termination with no trailing bytes (§3.1, §3.8), resolution of all
supplied trust material including any trailing `0x05` (§1.3), and (whenever
non-anchor steps exist) checking the matching anchor ledger's `0x02` header
`close_time` against the primary list window. An unread trailing `0x05` can
reverse an early result (by being malformed or carrying a newer, revoked, or
conflicting manifest or list).

See Appendix A for reference implementation status.

Canonical binary order is the form-independent canonical order (§1.2), with
`0x05` anchor list kept last.

### 3.4 Anchor Core Payload (0x01)

```
[ledger_hash: 32 bytes]
[publisher_key: 33 bytes]
[validation_count: LEB128]
  for each validation, in strictly ascending order of its embedded 33-byte SigningPubKey:
    [validation_len: LEB128][validation: raw STValidation bytes]
```

`publisher_key` is the VL publisher's 33-byte XRPL master public key (`0xED`,
`0x02`, or `0x03`; §5.1). Verifiers MUST reject a `publisher_key` with an
unknown prefix at the parse layer.

When `validation_count` is 0 the payload ends immediately after that count:
`32 + 33 + 1 = 66` bytes in all. That payload parses; it then fails quorum at
the trust layer, because N is at least 1 and no validation votes.

Each `validation` carries the exact raw signed `STValidation` bytes — no field
decomposition is performed. Every entry MUST be a complete canonical
`STValidation` under the XRPL codec (§2.1), carrying `LedgerHash` (32 bytes),
`Signature`, and a 33-byte `SigningPubKey` (`0xED`, `0x02`, or `0x03`). The
entries MUST appear in **strictly ascending** unsigned byte order of their
embedded 33-byte `SigningPubKey` (no duplicates, no out-of-order entries). Any
entry that fails canonical `STValidation` parsing, any ordering or duplicate
violation, or any trailing byte after the last validation is a **parse
failure**.

An `STValidation` the XRPL codec cannot round-trip is invalid in both forms
(§2.1); it is never a one-form proof.

### 3.5 Anchor List Payload (0x05)

```
[manifest_len: LEB128][manifest: raw publisher manifest bytes]
[signature_len: LEB128][signature: list signature bytes]
[list_len: LEB128][list: the exact signed list bytes]
```

Each of the three fields (`manifest`, `signature`, `list`) MUST be non-empty
(`len > 0`), and the payload MUST end immediately after `list` (no trailing
bytes).

`list` carries the exact signed bytes covered by `signature` — identical to
JSON `unl.blob`. At the framing layer (§3.8), `list` is simply a non-empty
byte sequence; UTF-8, JSON, and strict base64 checks apply at §1.3 P3
candidate admission in both forms so a malformed bundled list has the same
disposition in binary and JSON. Because the signed bytes are carried verbatim,
any valid validator list — including lists carrying `effective`, whitespace, or
extra fields — encodes directly in binary without loss or shape restrictions.
When compressed (§3.1, `flags` bit 0), the zlib container compresses the list
automatically.

### 3.6 Ledger Header Payload (0x02)

Fixed 118-byte canonical serialization:

```
[ledger_index: 4 bytes, big-endian uint32]
[total_coins: 8 bytes, big-endian uint64]
[parent_hash: 32 bytes]
[transaction_hash: 32 bytes]
[account_hash: 32 bytes]
[parent_close_time: 4 bytes, big-endian uint32]
[close_time: 4 bytes, big-endian uint32]
[close_time_resolution: 1 byte]
[close_flags: 1 byte]
```

### 3.7 Map Proof Payload (0x03, 0x04)

The tree type is encoded in the TLV type byte (`0x03` = tx, `0x04` = state).

The binary trie uses a **2-bit-per-branch header** — a 4-byte array — for
each inner node. Header byte `k` holds branches `4k` through `4k+3`, two bits
each, low bits first: branch `4k` occupies bits 0-1 of byte `k`, branch
`4k+1` bits 2-3, and so on. (Equivalently: read the 4 bytes as a
little-endian uint32 and branch `i` occupies bits `2i..2i+1`.) So the byte
stream reads branch types in the same 0-F order as the depth-first children
that follow. The 2-bit codes:

| Bits | Node type |
|------|-----------|
| `00` | Empty branch |
| `01` | Leaf |
| `10` | Inner (recurse) |
| `11` | Hash placeholder (pruned subtree) |

Serialization is depth-first, pre-order:

```
[branch_header: 4 bytes, byte k = branches 4k..4k+3]
  for each non-empty branch (in order 0-F):
    if 01 (leaf):  [key: 32 bytes][data_len: LEB128][data: raw canonical binary]
    if 10 (inner): [branch_header: 4 bytes] ... (recurse)
    if 11 (hash):  [hash: 32 bytes]
```

Leaf `data` is the raw leaf item bytes as hashed in §5.2 — state tree:
canonical STObject bytes; tx tree: `VL(tx) || VL(meta)`. Note the 32-byte key
precedes the data on the wire but is appended **after** the data in the leaf
hash preimage.

Every inner node advances exactly one nibble, so a node's 0-indexed depth is
its nesting depth (`0` at the root) and is never encoded. Binary tries obey the
exact same minimal trie shape as JSON tries (§2.3):

- no inner node may be empty (`branch_header == 0x00000000`);
- every inner node MUST have at least one leaf somewhere in its subtree;
- no hash placeholder (`11`) may be all-zero (`0x00 * 32`);
- every leaf at depth `d` (`1 <= d <= 64`) MUST have a 32-byte `key` whose
  first `d` nibbles match the `d` branch nibbles from the root;
- no inner node may sit at 0-indexed depth `>= 64`;
- the TLV payload MUST end immediately after the root trie finishes (no
  trailing bytes).

### 3.8 Binary Parsing Policy

The binary form is closed. A verifier MUST reject at the parse layer:

- an input shorter than 10 bytes, or bad file magic (not `"XPRV"`);
- a file header version other than the current one (§9.1);
- nonzero reserved flag bits;
- unknown TLV types;
- a TLV length that exceeds the remaining body bytes;
- trailing bytes inside **any** TLV payload (`0x01`–`0x05`), or an `0x02`
  payload whose length is not 118 bytes;
- an `0x01` record with an unknown `publisher_key` prefix, a non-canonical
  `STValidation`, or validations whose embedded 33-byte `SigningPubKey`s are
  not strictly ascending (§3.4);
- an `0x05` record with an empty `manifest`, `signature`, or `list` (§3.5);
- duplicate `0x01` or `0x05` records;
- a missing `0x01` record, or a first record other than `0x01`;
- any TLV record after `0x05`;
- an `0x03` / `0x04` trie with an empty inner node, an inner node with no leaf
  beneath it, an all-zero hash placeholder, a leaf whose `key` does not begin
  with its trie path nibbles, or an inner node at 0-indexed depth `>= 64`
  (§3.7);
- a body, plain or decompressed, that exceeds the verifier's configured maximum
  size (§8);
- a compressed body that is not exactly one complete zlib stream (§3.1).

The empty tree, lone-leaf tree root, and proofs of absence are out of scope for
the current format. A `map_proof` MUST contain at least one authenticated leaf
and enough inner nodes or hash placeholders to recompute a non-empty SHAMap
root.

The JSON counterpart of this policy is §2.4.

Unknown XRPL fields inside decoded SLE, transaction, metadata, manifest, or
validation objects are not XPRV extension points. If the configured XRPL codec
cannot round-trip a leaf, that proof has no JSON form (§2.3); its binary form,
whose leaves are exact bytes, is unaffected.

## 4. LEB128 Varint Encoding

```
encode(value):
    while value >= 0x80:
        emit(value & 0x7F | 0x80)
        value >>= 7
    emit(value)

decode(bytes):
    value = 0; shift = 0
    for each byte:
        value |= (byte & 0x7F) << shift
        if byte < 0x80: return value
        shift += 7
```

Encoders MUST emit the minimal-length encoding (no redundant continuation
bytes — `0x85 0x00` is an invalid encoding of `5`). Decoders MUST reject
non-minimal encodings, varints longer than 5 bytes, and decoded values
≥ 2^32 (all lengths and counts fit in uint32). Every length and count therefore
has a single valid binary encoding.

## 5. Verification Algorithm

The step-local cryptographic checks are identical for both forms. Verification
maintains capability sets of authenticated ledger hashes, tx roots, and state
roots. Parent-before-child serialized order ensures each step can only consume
capabilities produced by earlier authenticated steps.

```
function verify(chain, network_id, primary_publisher_key):
    // The caller configures one primary publisher key per network (§8),
    // and network_id selects it.
    assert chain.network_id == network_id

    // §1: exactly one anchor, and it comes first.
    assert chain.steps is non-empty and chain.steps[0] is anchor
    assert no later step is an anchor
    anchor = chain.steps[0]

    trusted_ledger_hashes = set()
    trusted_tx_roots = set()
    trusted_ac_roots = map()   // account_hash -> set of authenticated ledger_index

    // Structurally recognized LedgerHashes leaves, tagged with the ledger
    // index of the header that authenticated their state root. Recognition
    // grants NO authority on its own; see §5.3.
    skip_candidates = []       // (leaf_key, containing_index, Hashes[], LastLedgerSequence?)

    for each step in chain.steps:

        if step is anchor:
            // The anchor's close time (§1.3): the next logical step, if any,
            // whose preimage hashes to anchor.ledger_hash. It is read here
            // to select the primary list and grants nothing until the anchor
            // has verified. An anchor-only proof has no close time.
            // If a next step exists it must be the anchor ledger's header;
            // anything else is rejected here, not treated as anchor-only.
            close_time = that header's close_time, or none when the anchor
                         is the only step
            view = resolve_trust_view(anchor, primary_publisher_key, close_time)
            // resolve_trust_view applies §1.3 P1-P4 and rules 1-11: the
            // publisher's signing key, the primary list (the anchor's own
            // list is one candidate; a list-less anchor's come from held
            // trust alone, §3.3), its soundness, each master's delegation,
            // and collisions. Manifests verify as §5.1 states. It rejects
            // the anchor where those rules say so.
            // For each validation, reject the anchor unless it is a complete
            // canonical STValidation (§2.1) (and in JSON its outer map key
            // equals uppercase SigningPubKey):
            //   deserialize under the XRPL codec and require a round-trip
            //   require LedgerHash, Signature, and a 33-byte SigningPubKey
            //     with prefix 0xED, 0x02, or 0x03
            // Then, without rejecting the anchor:
            //   count it only if LedgerHash == anchor.ledger_hash
            //     and the VAL\0 signature is canonical and verifies (§5.1)
            //     and Flags has vfFullValidation (§1.3 rule 8)
            //     and SigningPubKey is a master's resolved key (rule 5)
            // Quorum: >= ceil(0.8 x N) distinct masters (§1.3 rule 6)
            assert quorum_met(view)
            list_effective, list_expiration = the primary list's interval
            trusted_ledger_hashes.add(step.ledger_hash)

        elif step is ledger_header:
            computed_hash = SHA512-Half(
                0x4C575200 ||                    // "LWR\0"
                uint32_be(ledger_index) ||
                uint64_be(total_coins) ||
                parent_hash ||
                transaction_hash ||
                account_hash ||
                uint32_be(parent_close_time) ||
                uint32_be(close_time) ||
                uint8(close_time_resolution) ||
                uint8(close_flags)
            )

            // Supported either by an already-trusted hash, or by resolving a
            // recognized skip-list leaf against THIS header's index. The
            // second case cannot be decided when the leaf was proven, because
            // the predicate is relational: it needs the child index, which
            // only exists once the child header is presented.
            supported = computed_hash in trusted_ledger_hashes
            if not supported:
                for (leaf_key, containing_index, hashes, last_seq) in skip_candidates:
                    if computed_hash not in hashes:
                        continue
                    list_type = classify_ledger_hashes_link(
                        leaf_key,
                        containing_index,
                        step.header.ledger_index)
                    if list_type is not None and ledger_hashes_last_sequence_consistent(
                            list_type,
                            containing_index,
                            step.header.ledger_index,
                            last_seq):
                        supported = true
                        break
            assert supported

            // The header the anchor hash authenticates must have closed
            // inside the primary list's validity interval:
            // effective <= close_time < expiration (effective defaults to 0).
            if computed_hash == anchor.ledger_hash:
                assert list_effective <= step.header.close_time < list_expiration

            trusted_ledger_hashes.add(step.header.parent_hash)
            trusted_tx_roots.add(step.header.transaction_hash)
            trusted_ac_roots[step.header.account_hash].add(step.header.ledger_index)

        elif step is map_proof:
            if step.tree == tx:
                expected_roots = trusted_tx_roots
            else:
                expected_roots = keys(trusted_ac_roots)

            root_hash = compute_trie_root(step.trie)
            assert root_hash in expected_roots

            if step.tree == state:
                // Record structurally well-formed LedgerHashes leaves. This
                // grants no ledger-hash capability — it only makes the leaf
                // available for a later header to resolve against (§5.3).
                // Every authenticated header with this account_hash is a
                // containing ledger; the leaf is a candidate under each.
                for containing_index in trusted_ac_roots[root_hash]:
                    for each leaf in step.trie:
                        if leaf is a well-formed LedgerHashes SLE:
                            skip_candidates.append(
                                (leaf.key, containing_index, leaf.SLE.Hashes,
                                 leaf.SLE.LastLedgerSequence))

    return true
```

The `skip_candidates` indirection is not an optimization; it is the only order
that can work. A LedgerHashes leaf is proven before the header it authorizes,
but the recognition predicate (§5.3) is relational over
`(leaf_key, containing_index, child_index)` — so at leaf time the third argument
does not exist yet. A verifier that tried to grant ledger-hash capability at
`map_proof` time would have to either accept every `Hashes` entry unconditionally
(forgeable: any authenticated state leaf with a `Hashes` field would mint
trust) or guess the child index. Both reference verifiers therefore split
recognition from resolution, and expose exactly this signature:
`classify_ledger_hashes_link(leaf_key, containing_ledger_seq, child_ledger_seq)`.

### 5.1 Hash Prefixes

| Prefix | Hex | ASCII | Used for |
|--------|-----|-------|----------|
| `ledgerMaster` | `0x4C575200` | `LWR\0` | Ledger header hash |
| `innerNode` | `0x4D494E00` | `MIN\0` | SHAMap inner node hash |
| `txNode` | `0x534E4400` | `SND\0` | SHAMap transaction leaf hash |
| `leafNode` | `0x4D4C4E00` | `MLN\0` | SHAMap account state leaf hash |
| `manifest` | `0x4D414E00` | `MAN\0` | Manifest signing data |
| `validation` | `0x56414C00` | `VAL\0` | STValidation signing data |

Manifest signing data is:

```
MAN\0 || canonical_STObject(manifest without Signature and MasterSignature)
```

A manifest is an STObject whose fields are exactly this set:
`Version`, `Sequence`, `PublicKey`, `SigningPubKey`, `Signature`, `Domain`,
`MasterSignature`. Any other field makes it invalid.

| Manifest field | Presence |
|---|---|
| `Sequence` | Required. `0xFFFFFFFF` is a revocation |
| `PublicKey` | Required. The validator's or publisher's master public key |
| `MasterSignature` | Required. Signature by `PublicKey` over the signing data |
| `Version` | Optional. If present it MUST be `0` |
| `Domain` | Optional. If present it MUST match the grammar below |
| `SigningPubKey` | Required on a non-revocation, forbidden on a revocation. The signing key the master delegates to |
| `Signature` | Required on a non-revocation, forbidden on a revocation. Signature by `SigningPubKey` over the signing data |

Verifiers MUST verify the manifest `MasterSignature` with the manifest
`PublicKey` (master public key). A non-revocation manifest MUST include both
`SigningPubKey` and `Signature`, and verifiers MUST verify `Signature` with
`SigningPubKey`. A revocation MUST include neither. As in rippled, a manifest is
also invalid if:

- its bytes are not the canonical serialization of its fields;
- it carries a `Version` other than 0;
- it has a `Domain` that is not well-formed under the grammar below;
- its `SigningPubKey` equals its own `PublicKey`.

A well-formed `Domain` is 4 to 128 bytes. Split on `.` it has at least two
labels. Each label is 1 to 63 bytes and neither starts nor ends with `-`.
Every non-final label is ASCII `A-Z`, `a-z`, `0-9`, or `-`. The final label
is at least two bytes and is ASCII letters only (`A-Z` or `a-z`). The check
uses those code-point ranges, not a locale's letter class. A trailing dot, an
empty label, or a non-ASCII byte fails it.

An invalid manifest is never a candidate (§1.3 rule 1).

STValidation signing data is:

```
VAL\0 || canonical_STObject(STValidation without Signature)
```

That object keeps every field other than `Signature`. A field a transaction
signing encoding would drop, such as `TxnSignature`, stays in the body.

Validator-list blob signing data has **no** 4-byte hash prefix:

```
raw_blob_bytes   // exact UTF-8 JSON bytes of the signed list (unl.blob / 0x05 list)
```

A validation that has passed the structural checks in §2.1 has its signature
verified with the embedded `SigningPubKey`. A failed or non-canonical signature
leaves that vote uncounted; it does not reject the anchor (§2.1). An unknown
public-key prefix never reaches this step: §2.1 / §3.4 already rejected the
anchor.

**Supported public-key encodings.** Publisher master keys, publisher signing
keys, validator master keys, and validator signing keys all use the same
33-byte XRPL public-key encoding:

| Key prefix | Length | Algorithm | Verification input |
|------------|--------|-----------|--------------------|
| `0xED` | 33 bytes | Ed25519 | raw signing data, with the 32-byte Ed25519 public key `A` following the `0xED` prefix |
| `0x02` or `0x03` | 33 bytes | secp256k1 ECDSA | DER-encoded ECDSA signature over `SHA512-Half(signing_data)` using the 33-byte compressed point |

Verifiers MUST reject public keys that are not 33 bytes or carry any other
prefix. (XRPL and Xahau mainnet publisher master keys use `0xED`; signing keys
and private-network publishers may use either algorithm.)

Every signature a verifier checks (validations, manifests, the list blob) MUST be
in canonical form:

- **secp256k1:** strict DER with `0 < r < n` and `0 < s <= n/2` (low S), as
  libsecp256k1 enforces.
- **Ed25519:** the libsodium verification profile: 64-byte signature `(R, S)`
  with `S < L` (RFC 8032 §5.1.7), canonical point encodings for both `A` and
  `R` (`y < p`, and no negative zero `x == 0` with sign bit `1`), rejection of
  small-order `A` and small-order `R`, and the cofactorless verification
  equation `[S]B = R + [k]A`.

A non-canonical twin is a second valid encoding of one signature, and two
verifiers that disagree on it disagree on the same bytes.

### 5.2 SHAMap Node Hashing

Inner node — each `child_i` is the 32-byte hash of branch `i`: a real
child's hash, a pruned placeholder hash, or 32 zero bytes for an empty
branch:

```
hash = SHA512-Half(MIN\0 || child_0 || child_1 || … || child_15)
```

Leaf node — the 32-byte leaf key is appended **after** the data:

```
state tree:  hash = SHA512-Half(MLN\0 || sle_bytes          || key)
tx tree:     hash = SHA512-Half(SND\0 || VL(tx) || VL(meta) || key)
```

`sle_bytes` is the canonical STObject serialization of the ledger entry.
`VL(x)` is the XRPL variable-length prefix encoding of `x` followed by its
bytes.

### 5.3 Skip-list Mechanics

XRPL stores historical ledger hashes in `LedgerHashes` SLEs. XPRV uses those
authenticated state leaves as positional skip lists.

Recognition is **structural** and happens when the leaf is proven; resolution is
**relational** and happens when a child `ledger_header` is presented. Conditions
1, 3 and 4 below are structural. Conditions 2 and 5 are relational: they involve
the child's authenticated `ledger_index` and therefore cannot be evaluated until
that header appears (§5). A recognized leaf grants no capability until it is
resolved.

LedgerHashes recognition predicate:

1. The leaf MUST come from an authenticated state-tree `map_proof`.
2. The leaf key MUST be the expected LedgerHashes key for the hop being
   resolved — the short key for a recent hop, or `long_skip_key(child_index)`
   for a flag hop. Evaluated at child-header time.
3. The decoded SLE MUST have `LedgerEntryType == LedgerHashes` (ledger-entry
   type code `0x0068`).
4. The SLE MUST contain a well-formed `Hashes` field (`STVector256`, XRPL's
   serialized list of 256-bit hashes) with **at most 256 entries**, each
   exactly 32 bytes. If `LastLedgerSequence` is present, it MUST be a uint32.
5. The child ledger header being trusted from the SLE MUST have a computed
   header hash equal to one entry in `Hashes`, its authenticated
   `ledger_index` MUST be in the range covered by that SLE, and if
   `LastLedgerSequence` is present it MUST satisfy the consistency rule below.

Only leaves that satisfy this predicate produce ledger-hash capabilities.
Verifiers MUST NOT harvest ledger hashes from arbitrary state leaves that happen
to contain a `Hashes` field. Failing recognition or the link/range check below
removes that leaf only as a source of header support: the leaf itself remains
an authenticated state leaf, and another valid source (such as `parent_hash` or
another skip-list leaf) can still support the header.

Short/recent LedgerHashes key:

```
short_skip_key = SHA512-Half(uint16_be(0x0073))
```

This is the fixed recent-history LedgerHashes SLE. In every ledger `L > 1`, it
contains up to the previous 256 ledger hashes. A candidate child header with
index `T` is covered when `L - 256 <= T < L` (bounded below by genesis). The
short list never contains ledger `L`'s own hash.

Long/previous-history LedgerHashes key:

```
long_skip_key(T) = SHA512-Half(uint16_be(0x0073) || uint32_be(T >> 16))
```

This selects the 65536-ledger era containing flag ledger `T`. Long lists contain
hashes of flag ledgers, where a flag ledger has `ledger_index % 256 == 0`.
Every full era has up to 256 flag hashes; the earliest era is shorter because
ledger indices start at 1. A candidate child header from a long list MUST be a
flag ledger with `0 < T < L`, and MUST have the same `T >> 16` era as the
long-list key.

Both reference verifiers implement the link and `LastLedgerSequence` predicates
as:

```
classify_ledger_hashes_link(leaf_key, L, T):
    // L: index of the ledger whose state holds the leaf; T: candidate child
    if not (0 < T < L):
        return none
    if leaf_key == short_skip_key:
        return recent if L - T <= 256 else none
    if T % 256 == 0 and leaf_key == long_skip_key(T):
        return flag
    return none

ledger_hashes_last_sequence_consistent(list_type, L, T, last_seq):
    // If the SLE omits LastLedgerSequence, the check passes.
    if last_seq is none:
        return true
    if list_type == recent:
        return last_seq == L - 1
    // flag list:
    return (last_seq < L
            and last_seq % 256 == 0
            and (last_seq >> 16) == (T >> 16)
            and T <= last_seq)
```

When the SLE includes `LastLedgerSequence`, verifiers MUST enforce
`ledger_hashes_last_sequence_consistent` before accepting the leaf as support
for child header `T`. The authoritative child ledger index still comes from
the child header preimage, not from the SLE position or from
`LastLedgerSequence`.

### 5.4 SHA512-Half

`SHA512-Half(x)` = first 256 bits (32 bytes) of `SHA-512(x)`.

### 5.5 What Successful Verification Means

`verify()` returning true means every step is cryptographically supported by
the anchor — nothing more. Consumers MUST still bind the authenticated
outputs to their expectations:

- A transaction proof authenticates the leaf whose key equals the
  transaction hash. The consumer MUST check that key equals the transaction
  it expects, and inspect `TransactionResult` in the metadata — proof of
  inclusion is not proof of success.
- A state proof is point-in-time: it authenticates the entry's value **at
  the proven ledger**, not currently. Consumers making "current state"
  claims MUST check the proven ledger's authenticated `close_time` (not merely
  the anchor ledger's) for acceptable staleness. `close_time` is a
  ledger-header field, so it is authenticated by the header hash when the
  header is trusted.
- Reported ledger indices come only from authenticated `ledger_header`
  preimages (an anchor-only proof establishes the validated anchor hash alone;
  §1.1, §2.1).

## 6. Composition Patterns

Producers select the shortest available hop:

- **Distance 0 (target is the anchor ledger):** no skip list and no second
  header — three steps total:

```
anchor → ledger_header(anchor) →
map_proof(tx, transaction_hash)
```

- **Distance 1 (target is the anchor's immediate parent, `anchor - 1`):**
  supported directly by the anchor header's authenticated `parent_hash`, with
  no skip list — four steps total. This takes precedence over the flag-ledger
  skip-list rule even when `target % 256 == 0`:

```
anchor → ledger_header(anchor) →
ledger_header(parent) →
map_proof(tx, transaction_hash)
```

- **Flag-ledger target at distance ≥ 2 (`ledger_index % 256 == 0`):** reached
  in one hop via the anchor's long (era) skip list regardless of distance — its
  era-list leaf is never larger than the always-full recent list, so this is
  shorter than the recent list even for a recent flag ledger.
- **Near non-flag target (`2 ≤ distance ≤ 256`):** reached in one hop via the
  recent short skip list.
- **Distant non-flag target (`distance > 256`):** 2-hop path through an
  intermediate flag ledger.

### 6.1 Recent Non-Flag Transaction (short skip list, 2 ≤ distance ≤ 256)

```
anchor → ledger_header →
map_proof(state, short_skip_list_key) →
ledger_header →
map_proof(tx, transaction_hash)
```

### 6.2 Historical Transaction (long skip list / 2-hop path)

```
anchor → ledger_header →
map_proof(state, long_skip_list_key) →
ledger_header(flag) →
map_proof(state, short_skip_list_key) →
ledger_header(target) →
map_proof(tx, transaction_hash)
```

For a target at distance ≥ 2 that is itself a flag ledger (`target % 256 == 0`),
the anchor ledger's long skip list directly contains the target hash, so the
canonical proof omits the intermediate short-list hop:

```
anchor → ledger_header →
map_proof(state, long_skip_list_key(target)) →
ledger_header(target) →
map_proof(tx, transaction_hash)
```

For a distant non-flag target, the **flag ledger** is the smallest multiple of
256 strictly above the target ledger. The anchor's state tree long skip list
contains hashes of flag ledgers. The flag ledger's short skip list contains
hashes of the 256 ledgers before it, including the target.

The canonical historical path uses at most one long-list hop and, for non-flag
targets, one short-list hop. Longer skip-list chains may be independently
verifiable if every step is authenticated, but they are non-canonical and
SHOULD NOT be emitted.

### 6.3 Account State Proof

```
anchor → ledger_header → map_proof(state, account_key)
```

### 6.4 Bundled Headers From One Skip List

A single skip list state proof may justify multiple child ledger headers.
Canonical form emits the supporting `map_proof(state)` once, then the child
`ledger_header` subtrees in descending `ledger_index`.

```
anchor → ledger_header →
map_proof(state, long_skip_list_key) →
ledger_header(flag_3) → ...
ledger_header(flag_2) → ...
ledger_header(flag_1) → ...
```

### 6.5 Multi-Leaf Proofs From Same Root

A single `map_proof` may include multiple leaves from the same authenticated
tree root. Canonical form merges them into one trie rather than emitting
repeated sibling proofs against the same root.

```
anchor → ledger_header → map_proof(state)   // multiple state leaves
anchor → ledger_header → map_proof(tx)      // multiple tx leaves
```

See Appendix A for reference implementation status.

## 7. Conformance and Verifier Output

A conforming proof is one that satisfies the JSON or binary syntax rules,
contains exactly one logical anchor, and verifies every step against previously
authenticated outputs.

A conforming producer MUST:

- emit a recognized format version;
- emit no JSON form for a proof whose leaves cannot satisfy §2.3, and point
  the consumer to the binary form;
- emit no member the closed JSON form does not list (§2), and no unknown,
  reserved, or extension values in the binary form (§3.8).

A conforming producer SHOULD emit canonical proof order (§1.2).

A conforming verifier MUST:

- reject malformed syntax, unknown reserved values, and unsupported versions;
- never abort or crash on malformed input;
- verify every signature, hash preimage, and SHAMap root needed by the proof;
- reject any step whose parent capability has not already been authenticated;
- expose data as authenticated only after whole-proof acceptance, derived
  solely from steps and leaves that passed validation and root matching
  (failure diagnostics MAY describe the failing check and its location);
- report whether the proof verified; on refusal, expose the refusal layer
  (`parse`, `trust`, or `graph`) and reason; on acceptance, expose every
  authenticated output that consumers need to bind (§5.5).

### 7.1 Structured Verification Report (`--json`)

A verifier supporting machine-readable reporting emits a single JSON object on
standard output whose `verified` boolean matches its exit status (`0` for
`true`, `1` for `false`):

- **Refused:** `{"verified": false, "layer": "parse" | "trust" | "graph", "reason": "<text>"}`.
  The three layers correspond to:
  - `parse`: syntax, framing, compression, header/envelope/trie/leaf structure,
    `network_id` uint32 bounds, single anchor first, and canonical
    `STValidation` / leaf round-trip rules (§2.4, §3.8).
  - `trust`: configured network/publisher binding, primary list availability
    and soundness, manifest resolution, quorum, and signatures (§1.3, §5.1).
  - `graph`: well-formed post-anchor steps lacking authenticated support
    (unsupported `ledger_header` or `map_proof` root, or skip-list consistency
    failure; §5.2, §5.3).
- **Accepted:** an object with:
  - `verified`: `true`, `format_version` (uint8), `network_id` (uint32),
    `verifier` (implementation identifier string);
  - `definitions`: `{"leaf": "<fingerprint>", "validation": "<fingerprint>"}`
    identifying the loaded protocol-definitions snapshots (Appendix A);
  - `anchor`: `{"ledger_hash": "<64 hex>", "votes": <int>, "list_size": <int>}`;
  - `headers`: array of authenticated headers, each carrying `ledger_hash`,
    `ledger_index`, `close_time`, `parent_hash`, `transaction_hash`,
    `account_hash`, and `supported_by` (`"anchor"`, `"parent_hash"`, or
    `"ledger_hashes"`);
  - `leaves`: array of authenticated leaves, each carrying `tree` (`"tx"` or
    `"state"`), full 32-byte `key`, uppercase hex raw item `bytes` (§3.7:
    `sle_bytes` for state, `VL(tx) || VL(meta)` for tx, excluding the §5.2
    hash prefix and appended key), and `ledgers` (array of
    `{"ledger_hash", "ledger_index"}` for every authenticated ledger in the
    proof whose root matches that leaf's `map_proof`).

When a proof carries more than one fault, a verifier reports the first fault it
encounters; this specification does not prescribe a cross-fault ordering beyond
the parse-before-trust boundary (§1.3, §2.4).

## 8. Security Considerations

- **Trust configuration.** A verifier MUST be configured with exactly one
  primary publisher key for each network it accepts. It MAY also hold secondary
  publisher keys, whose lists only refresh members' manifests (§1.3).
  `network_id` selects the primary key: a proof whose `network_id` does not
  match the network of the configured key MUST be rejected. `network_id` is
  otherwise unauthenticated metadata.
- **Ephemeral validations.** A producer can only capture validations while the
  anchor ledger is being validated, and a node keeps them for minutes.
- **Quorum.** Quorum is counted against the primary list's unreduced N (§1.3
  rule 6). rippled can lower its own threshold by excluding validators on the
  Negative UNL; XPRV does not, so a ledger rippled considers validated can
  still lack an XPRV quorum while validators are offline.
- **Hashes and signatures.** All hashes use SHA512-Half as specified by the
  XRPL protocol. Every signature MUST be in canonical form under the profiles
  in §5.1 (libsecp256k1 strict DER low-S; libsodium Ed25519 profile).
- **Failure.** The verifier MUST reject chains where any step fails.
- **Resource limits.** Verifiers MUST enforce configured maximum sizes before
  parsing: on the binary body, plain or decompressed (a compressed body is
  bounded while it inflates, against zlib bombs), and on JSON text. They MUST
  reject trie inner nodes at 0-indexed depth `>= 64` (§2.3, §3.7, §3.8).
- **Two forms.** Binary and JSON forms of the same proof MUST verify
  identically. A proof's identity is taken over its uncompressed binary form,
  never the JSON text (§1.2). A cached verdict also requires full validation of
  the received representation, the verifier's policy and protocol-definitions
  identity, and the resolved trust view (§1.4).
- **Proofs without `0x05`.** A verifier accepting them sources the list as §3.3
  requires. Cached trust material SHOULD be bound to the publisher key it was
  verified under, so a cached list cannot be applied to a proof from a
  different publisher.
- **Sources of trust material** (§1.3) are channels for evidence, not trust
  roots. A source can withhold a newer manifest, but it cannot make an
  unverified one count. A verifier that keeps material between runs SHOULD keep
  each master's highest manifest sequence, so that a restart or an older bundle
  cannot return signing to a key the validator has rotated away from.
- **Reporting the view.** Holding the newest manifests is not provable offline.
  A verifier SHOULD report the view it used, so a consumer can judge whether it
  was current enough.

## 9. Registries, MIME Types, and File Extensions

### 9.1 Format Versions

| Version | Status |
|---------|--------|
| 10 (`0x0A`) | current draft |
| 9 | superseded; served publicly by xprv.it until superseded by version 10 |
| 8 | superseded; served publicly by xprv.it until superseded by version 9 |
| 7 | superseded; served publicly by xprv.it until 2026-11-05 |
| 6 | superseded; served publicly by xprv.it until 2026-10-25 |
| 5 and earlier | superseded, never released |

The number changes whenever a change alters which inputs are valid, and a
number is never reused: two verifiers claiming one version, given the same
trust material, verifier policy/resource limits, and protocol-definitions
snapshot, MUST NOT disagree about the same bytes. Verifiers load field
definitions at run time and report the identity of the snapshot they loaded
(§7, Appendix A). v6, v7, v8, and v9 proofs were served publicly, so current
verifiers reject them by version rather than by content, and producers reissue
them.

Compared to version 9, version 10:

- replaces `0x05`'s shaped list decomposition with the exact signed list bytes
  (§3.5), eliminating JSON-only proofs;
- replaces `0x01`'s common/unique validation decomposition with raw
  `STValidation` records sorted by embedded `SigningPubKey` (§3.4);
- removes the unauthenticated advisory `ledger_index` from the anchor in both
  forms (§2.1, §3.4);
- enforces one minimal trie shape (no empty inner nodes, no inner nodes without
  a descendant leaf, no all-zero hash placeholders, and leaf keys matching
  their trie nibble path; §2.3, §3.7);
- enforces uppercase hexadecimal on all proof-level hex members and trie branch
  keys with no case normalization (§2);
- replaces the hand-maintained JSON leaf type table with strict round-trip
  equality against exact `rippled` / `xahaud` reference JSON (§2.3), with no
  alternate-spelling or decimal-notation tolerance;
- requires unique JSON members and canonical padded RFC 4648 base64 manifests
  in signed validator-list blobs (§1.3), strict `total_coins` decimal text
  (§2.2), explicit `LastLedgerSequence` and `len(Hashes) <= 256` skip-list
  rules (§5.3), the full libsodium Ed25519 verification profile (§5.1), and
  complete framing and trust resolution before final acceptance (§3.3).

Versions other than 10 are unrecognized by this draft and MUST be rejected.

### 9.2 Network IDs

| `network_id` | Network |
|--------------|---------|
| 0 | XRPL mainnet |
| 21337 | Xahau mainnet |

Other values are reserved for future registry entries or private deployments.
Verifiers MUST bind any accepted value to its configured primary publisher key
(§8).

### 9.3 Binary Type Registries

The TLV type registry is defined in §3.2. All unassigned values are reserved in
the current format version and MUST be rejected.

### 9.4 MIME Types and File Extensions

| Form | Extension | MIME type |
|------|-----------|-----------|
| Binary | `.xprv.bin` | `application/octet-stream` |
| JSON | `.xprv.json` | `application/json` |

The binary MIME type is a placeholder until a dedicated media type is
registered.

## 10. Reference Implementations

- **C++ prover + verifier**: [catalogue-tools/src/xprv](https://github.com/sublimator/catalogue-tools/tree/main/src/xprv)
- **Python verifier**: [verify-xprv.py](https://github.com/sublimator/catalogue-tools/blob/main/scripts/verify-xprv.py)
- **Live demo**: [xprv.it](https://xprv.it)

## Appendix A. Reference Implementation Status

What the reference verifiers implement of this draft (at code pin `c348a9c4`).
How the reference server holds and passes trust material is described in
`src/xprv/README.md` in the reference repository.

A canonical `STValidation` (§2.1) is a round-trip under the verifier's
protocol-definitions snapshot, not a fixed list of field names. Both verifiers
report the fingerprints of their loaded definitions in `definitions.{leaf,validation}`
(§7.1): `xprv verify` uses the `xrpl_definitions.json` / `xahau_definitions.json`
embedded in `catl::xdata`, while `verify-xprv.py` uses `xrpl-py` 5.2.0 (with
adapters for `rippled`'s base-ten `UInt64`, issued-amount, and `Number` text).
Those two snapshots differ on some non-validation fields and on Xahau-only
fields. §9.1 requires two verifiers of one version to agree on the bytes that
their shared snapshot defines; it does not claim agreement on fields defined by
only one snapshot.

| Capability | C++ `xprv verify` | Python `verify-xprv.py` |
|---|---|---|
| Both forms; capability-based DAG verification (§1, §5) | yes | yes |
| Structured `--json` report (§7.1) | yes | yes |
| Trust view from bundled list plus held manifests (§1.3) | yes (`--manifests FILE`) | yes (`--manifests FILE`) |
| Primary lists from outside the proof; list-less anchor (§3.3) | no: rejected at `trust` | no: same |
| Online list refresh before verifying | no; the reference server refreshes lists | no |
| Binary encoder (`convert`, JSON ↔ binary) | yes | no (decodes binary only) |
| Decoded Xahau JSON leaves | yes | no: verifies Xahau binary proofs; `xrpl-py` ships XRPL definitions only |
| Exact `rippled` leaf JSON (§2.3: native `Issue`, amounts, `Number`, base-ten `UInt64`) | yes; ordinary `UInt64` lowercase unpadded hex & path-step `type` in progress | same |
| Full libsodium Ed25519 profile (§5.1) | yes | in progress (`PyNaCl`; currently `cryptography` + `S < L`) |
| Sibling fan-out and multi-leaf proofs | accepted | accepted |

`FILE` is a JSON array of base64 raw manifests. A list-less anchor is
well-formed and unverifiable by both reference verifiers until external
primary-list storage is added.

## Appendix B. Conformance Corpus and Runner

The shared test corpus lives in `tests/xprv/vectors/` and is executed by
`scripts/xprv-conformance.py` (with shared leaf-value unit fixtures in
`tests/xprv/fixture/leaf-value-cases.json`).

**Vector shapes.** Each JSON file in `tests/xprv/vectors/` falls into one of
three forms:

- **Positive vectors** carry a captured proof in both JSON and binary form,
  configured trust (`network_id`, `publisher_key`), and verifier-independent
  intermediates (`LWR\0` header preimages and hashes, per-leaf hashed bytes and
  hashes, recomputed trie roots, and derived skip-list keys) produced by
  `scripts/xprv-vectors.py` and checked by C++ (`tests/xprv/vectors-gtest.cpp`)
  so neither implementation grades its own homework.
- **Derived vectors** (`accept-*` and `negative-*`) reference a positive `base`
  plus explicit binary byte edits (`set`, `insert`, `delete_at`/`delete_len`,
  `truncate`, `drop_last`, `xor_last`, `recompress`, `append`,
  `append_zlib_zeros`, `append_zeros`) or JSON-pointer patches (`set`, `select`,
  `remove`, `duplicate`, etc.). Every derived negative is verified against the
  reference verifier before being written; unsubstantiated claims fail
  generation.
- **Inline vectors** carry a full proof directly — notably the synthetic
  trust-world suite (`scripts/xprv_trust_world.py`), which supplies per-vector
  `trust.manifests` (base64 held manifests) covering manifest rotation,
  revocation, equivocation, list windows, and publisher conflicts, plus the
  historical duplicate-nibble forgery regression.

Each negative vector declares its expected refusal `layer` (`parse`, `trust`,
or `graph`, matching §7.1) and a `reason_contains` substring. Any expected
discrepancy is recorded as a typed `known_failing` entry (`{does, because}`)
rather than an unconditional skip, so a crash, timeout, or different failure
category still fails the run.

**Black-box runner contract.** `scripts/xprv-conformance.py run` invokes any
verifier via a command template (`{proof}`, `{publisher_key}`, `{network_id}`,
`{manifests}`): exit `0` means accept, exit `1` means refuse, and any other
exit status, signal, or timeout is recorded as `abnormal` (fail). For the
built-in `cpp` and `python` verifiers (and any external verifier marked with
`--reports NAME`), the runner parses standard output as a §7.1 JSON report and
requires `verified` to match the exit status, `reason_contains` to appear in
`report.reason` (log lines on stderr do not count), and `report.layer` to equal
the vector's `layer`.

**Coverage note:** a positive list-less (`0x05`-omitted) proof verified solely
from externally held lists is not yet covered because neither reference
verifier holds external primary lists; the negative list-less refusal at
`trust` is covered.
