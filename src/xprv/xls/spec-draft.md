# XPRV: XRPL Proof Verification Format

| | |
|---|---|
| **Status** | Draft |
| **Date** | 2026-11-05 |
| **Authors** | Nicholas Dudfield ([@sublimator](https://github.com/sublimator)) |
| **Discussion** | [XRPLF/XRPL-Standards#107](https://github.com/XRPLF/XRPL-Standards/discussions/107) |
| **Reference** | XLS-41d XPOP (Richard Holland), [XRPL-Standards](https://github.com/XRPLF/XRPL-Standards) |

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

Both forms encode the same proof data and MUST verify identically. Two kinds of
proof have one form only. A proof whose validator list carries `effective`, or
any other top-level key beyond the three the binary layout reproduces, has no
binary form (§3.5). A proof with a leaf that its codec cannot decode and
re-encode exactly has no JSON form (§2.3).

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
| [RFC 4648](https://www.rfc-editor.org/rfc/rfc4648) | Base64 encoding for decomposed validator manifests |
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
consumes precisely one attested value from them: the 32-byte ledger hash.
Because the ledger hash is `SHA512-Half` of the full header preimage
(§5), a `ledger_header` step that matches a trusted hash authenticates **all**
of its fields simultaneously — `ledger_index`, `total_coins`, `parent_hash`,
`transaction_hash`, `account_hash`, and the close-time fields. Ledger indices
in a verified proof therefore come from header preimages, not from step
metadata, and cannot be forged without a hash collision.

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

### 1.2 Canonical Proof Order

Both JSON and binary forms use the same canonical parent-before-child order:

1. All non-UNL steps MUST be serialized parent-before-child.
2. The canonical traversal is depth-first pre-order over the logical proof DAG.
3. Children of a `ledger_header` are canonically ordered as: state-tree subtree
   first, then tx-tree subtree.
4. A given authenticated tree root MUST appear at most once in canonical form.
   If multiple leaves are required from that root, they MUST be merged into one
   multi-leaf `map_proof`.
5. If one authenticated skip-list leaf supports multiple child
   `ledger_header` steps, those child headers MUST be ordered by descending
   `ledger_index`.
6. Within a `map_proof`, trie branch order (`0`-`F` at every inner node)
   defines the canonical leaf ordering.

Other parent-before-child orders may still be verifiable, but they are
non-canonical and MUST NOT be emitted when the proof will be hashed, signed,
cached, compared, or deduplicated; elsewhere producers SHOULD still emit
canonical order. §1.4, §7 and §8 apply this same rule.

**Proof identity is defined over the binary form only.** When a proof must be
hashed, signed, cached, or deduplicated, the input is the canonical binary
encoding of §3 — canonical step order per this section, minimal LEB128 (§4), and
the byte layouts of §3.4 through §3.7. That encoding is byte-deterministic, so a
proof hash is well defined. The body holds the `0x05` record when the proof
bundles its list and not otherwise, so a proof with its list and the same proof
without it have different identities.

Compression is not part of a proof's identity. The canonical form is the
uncompressed one: the 10-byte header with `flags` zero, followed by the
uncompressed TLV body. A compressed container (§3.1) is the same proof; to
compute its identity, decompress it first. zlib output is not byte-deterministic
across implementations, so the compressed bytes MUST NOT be used as an
identity.

This specification deliberately does **not** define a byte-canonical JSON form.
JSON here is the human-readable and debugging representation: object member
order carries no meaning, whitespace is unconstrained, and the `validations` map
(§2.1) is an unordered object. Two JSON documents that differ only in member
order or formatting encode the same proof and MUST verify identically, but they
are not expected to be byte-identical, and nothing in this specification requires
them to be. A producer needing a stable identifier for a JSON-delivered proof
hashes the equivalent canonical binary encoding rather than the JSON text.

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
  that signing key. The list bundled in the proof is one of them. A list signed
  with a superseded publisher signing key is not a candidate; its validator
  manifests still are, since each verifies under its own master.
- **P3.** The primary list is the candidate with the highest list `sequence`
  among those in effect at the anchor's close time
  (`effective <= close_time < expiration`; a list without `effective` is in
  effect from 0). An anchor-only proof has no close time, and its primary list
  is the highest-sequence candidate. If no candidate is in effect, membership
  is unavailable and the anchor MUST be rejected. The interval is a
  consistency check, not a barrier against the list's own validators, who
  attest the close time themselves.
- **P4.** A lower-sequence list from the same publisher contributes only its
  validator manifests, like a secondary list.

**Resolution rules.** Rules 1–8 resolve the view; rules 9–11 state the
publisher's revocation and the conditions a list must meet before any
delegation from it is resolved.

1. A manifest is a candidate only if it verifies (§5.1) and its `PublicKey` is
   a master in the primary list. A manifest for any other master grants
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
7. Two different lists under the primary publisher key with the same highest
   `sequence` make the view unresolvable, and the anchor MUST be rejected: the
   publisher itself has equivocated, so there is no membership to fall back
   on. Byte-identical lists are one list.
8. Only full validations count. A validation without `vfFullValidation`
   (`Flags & 0x00000001`) is a partial validation and never counts toward
   quorum, as in rippled.
9. If the winning publisher manifest is a revocation, the publisher has no
   signing key: no list verifies, and the anchor MUST be rejected. A revoked
   publisher is final, as a revoked validator is.
10. The primary list itself MUST be sound before any held manifest is applied.
    The anchor MUST be rejected if the list's own manifests give two masters
    the same signing key, or give a master a signing key equal to any master
    key in the list. Two masters can share a delegation only through a misused
    or compromised key, so held manifests cannot repair such a list. The resolved
    view is then checked again under rule 4.
11. Keys are bound, and a list names each master once:
    - the proof's `unl.public_key`, the publisher manifest's `PublicKey` and the
      configured primary publisher key are one key;
    - each list entry's `validation_public_key` equals the `PublicKey` of that
      entry's own manifest;
    - a list that names a master twice is invalid, and the anchor MUST be
      rejected.

The rules sort what the material implicates. A validator's own revocation or
equivocation costs that validator its vote and nothing else (rules 2 and 3).
Evidence against the trust root (a conflicting or revoked publisher, rules 2
and 9), against the list (rules 7, 10 and 11), or a key two masters resolve to
(rule 4), makes the anchor unavailable. A manifest whose signing key equals its
own master key is not a valid manifest (§5.1), so it is never a candidate.

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

A renewed proof is a different proof, with a different identity (§1.2).
Separately, a verdict depends on the trust view as well as on the proof bytes:
two verifiers holding different manifests can reach different verdicts on
identical bytes. A cached verdict MUST therefore be keyed by both the proof's
§1.2 identity and the whole resolved view, under one fixed verifier policy:

- the configured publisher key, and the publisher's state: its winning
  manifest's raw bytes, or its revocation, or both manifests of a conflict;
- the primary list's signed blob bytes;
- each master's state: its winning manifest's raw bytes, its revocation, or
  both manifests of an equivocation at any sequence.

A key built from winning manifests alone is not enough. An equivocation below
the highest sequence changes the verdict without changing any master's
highest-sequence manifest, and an equivocation has no single winning manifest.

Renewal produces an ordinary proof of the current format version. A tail
without an anchor is not a proof; carrying tails or anchors as standalone
objects would need a new format version (§9.1).

## 2. JSON Form

The proof chain is a JSON object with top-level `format_version`,
`network_id`, and an array of step objects. The `steps` array is the canonical
parent-first linearization of the logical proof DAG.

```json
{
    "format_version": 7,
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
| `format_version` | uint32 | XPRV format version (shared with the binary form) |
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
tokens: `7`, not `7.0` or `"7"`.

All hashes and binary data are hex-encoded strings. Producers MUST emit uppercase
hexadecimal (`A`-`F`); verifiers MAY accept lowercase and MUST normalize before
comparing. Ledger header fields are named and typed.

That leniency applies to hex **values** only. Structural **keys** are not
normalized: trie branch keys (§2.3) and `validations` map keys (§2.1) are
uppercase-only. Two spellings of one key put two members where the format admits
one, which is a smuggling vector rather than a formatting question.

### 2.1 Anchor Step

```json
{
    "type": "anchor",
    "ledger_hash": "<64 hex>",
    "ledger_index": 103188348,
    "unl": {
        "public_key": "<66 hex, Ed25519 master public key of VL publisher>",
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
| `ledger_index` | uint32 | Sequence number of the anchor ledger. **Advisory** — this field itself lies outside every signed preimage. Verifiers MUST NOT report it as authenticated unless cross-checked: the embedded STValidations carry a signed `LedgerSequence` field, and verifiers SHOULD verify it equals `ledger_index` for the validations that form quorum; the `ledger_index` of the `ledger_header` step whose computed hash equals `ledger_hash` is equally authoritative (§1.1). |
| `unl.public_key` | hex string (33 bytes) | VL publisher's master public key, XRPL format (`0xED` prefix + 32-byte Ed25519 key). Always present. |
| `unl.manifest` | hex string | Publisher manifest (binary STObject) binding master → signing key |
| `unl.blob` | hex string | Raw UNL blob (JSON validator list) |
| `unl.signature` | hex string | Signature over blob by manifest's signing key |
| `validations` | object | Map of signing_key_hex → raw STValidation bytes. The map key MUST be exactly the embedded STValidation `SigningPubKey` encoded as uppercase hex. |

`unl.manifest`, `unl.blob` and `unl.signature` are present together or not at
all. An anchor whose `unl` carries only `public_key` does not bundle its list:
it is the JSON counterpart of a binary proof without `0x05` (§3.3), and the
same rules apply. The verifier resolves the list from trust material it holds
(§1.3) and MUST reject the proof if it holds none. A `unl` with some but not
all of the three members, or with any other member, MUST be rejected.

A list's `effective` and `expiration` (in the blob, and in `0x05`) are seconds
since the Ripple Epoch (2000-01-01 00:00:00 UTC), the same unit as a ledger's
`close_time` (§2.2). A list without `effective` is in effect from 0.

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
| `total_coins` | decimal string | uint64, 8 bytes |
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

The leaf `key` is a 64-character uppercase hex string (32 bytes). Leaf `data` is
an object carrying the leaf's decoded XRPL content, and nothing else:

- State tree: the decoded ledger entry, also called a serialized ledger entry
  (SLE) after canonical STObject serialization.
- Transaction tree: an object with exactly the members `tx` and `meta`: the
  decoded transaction and its metadata.

The verifier re-serializes that content to canonical binary and hashes the result
(§5.2): state leaves as canonical STObject bytes, tx leaves as
`VL(tx) || VL(meta)`.

Codecs skip what they do not serialize, so "nothing else" is checked, not
assumed. Before hashing, a verifier MUST reject a leaf with any member, at any
depth, that is not a serialized field of its codec definitions. That includes
the definitions' non-serialized names such as `hash` and `index`. Within an
`STArray`, each element wraps exactly one serialized `STObject` field. Fields
whose JSON value is itself structured carry exactly one variant's members:

| Field type | JSON value |
|---|---|
| Amount | a drops string; `{value, currency, issuer}` with a non-native currency; or `{value, mpt_issuance_id}` |
| Issue | `"XRP"` or `"XAH"`; `{currency}` with a native currency; `{currency, issuer}` with a non-native currency; or `{mpt_issuance_id}` |
| PathSet | an array of paths, each an array of steps; a step is a non-empty object whose members are among `account`, `currency` and `issuer` |
| XChainBridge | exactly `LockingChainDoor`, `LockingChainIssue`, `IssuingChainDoor` and `IssuingChainIssue`, each issue as above |
| Vector256 | an array of hash strings |

Any other field's value is a string or a number. An issue with a currency but no
issuer is native in the binary form whatever currency it names, so only `XRP`
or `XAH` may stand there.

JSON leaves are decoded only. A leaf whose decoded object does not serialize
back to its exact hashed bytes under the producer's codec definitions has no
JSON form: the producer MUST NOT emit that proof as JSON, and SHOULD direct the
consumer to the binary form, whose leaves are the exact bytes and need no codec
to verify. A hex string is not leaf data, and verifiers MUST reject it.

Non-XRPL convenience members are not part of a leaf. Some decoders add `index`
or `hash` (duplicating the trie leaf key) or `blob` (duplicating the hashed
bytes). Producers MUST remove them before emitting a proof, and verifiers MUST
reject a leaf that carries one, as they reject any other member nothing hashes:
per §1.1 a field is trusted only because it lies inside bytes whose hash is
already trusted, never because a step asserts it. The authenticated leaf key is
the trie's `key` element, never a member inside the decoded object.

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

#### Inner-node members

Branch keys are uppercase only: `0`-`9` and `A`-`F`. Lowercase is not a branch
key, and there is no case normalization for branch keys — the canonical set of
ordinal translations is uppercase.

An inner node's members are exactly those branch keys, plus the optional
diagnostic `__depth__`. Verifiers MUST reject an inner node carrying any other
member, or more than one member for the same branch.

`__depth__` is a diagnostic. Where present it MUST equal the node's nesting depth —
`0` at the root, one nibble per level — and verifiers MUST reject a mismatch. It is
derivable from structure and grants no authority on its own: nothing hashes it.

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

A JSON verifier MUST reject:

- an unrecognized version, a bare array of steps, or an unknown step type (§2);
- a member the closed form does not list, or an integer field that is not an
  integer token (§2);
- a trie inner node with a lowercase or non-branch member, or a repeated branch
  (§2.3). This class is JSON-only: a binary trie identifies a branch by position
  in a 2-bit field (§3.7), so a branch there cannot be misspelled or repeated;
- duplicate object members anywhere in the proof;
- hex-string leaf data, and a leaf member that is not a serialized field or a
  structured value outside its variant's members (§2.3);
- a `unl` that bundles its list only in part (§2.1);
- trie nesting deeper than 64 levels;
- text longer than its configured maximum, checked before parsing (§8).

The binary counterpart is §3.8.

## 3. Binary Form

### 3.1 File Header

Every binary proof begins with a **10-byte file header**:

```
Offset  Size  Field
  0       4   magic       "XPRV" (0x58 0x50 0x52 0x56)
  4       1   version     0x07 (same version space as JSON format_version)
  5       1   flags       bit 0: zlib compressed body
  6       4   network_id  big-endian uint32 (0 = XRPL, 21337 = Xahau)
```

When flag bit 0 is set, the entire body (all TLV records concatenated) is compressed as a single RFC 1950 zlib stream. The consumer decompresses before parsing TLVs. The body is that one complete stream: a truncated stream, a second stream, or any byte after the stream's end makes the proof malformed. Compression is a container option only; it does not change the proof or its identity (§1.2).
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
| `0x01` | Anchor core | Ledger hash, publisher key, decomposed validations |
| `0x02` | Ledger header | Fixed 118-byte canonical layout |
| `0x03` | Map proof (tx) | Binary trie for transaction tree |
| `0x04` | Map proof (state) | Binary trie for account state tree |
| `0x05` | Anchor UNL | Publisher manifest, blob signature, decomposed VL |

All other type bytes are reserved in the current format version. Verifiers MUST
reject unknown TLV types.

Length is an unsigned LEB128 varint, minimal and at most 5 bytes (§4):

| Payload size | Length bytes |
|-------------|-------------|
| < 128 | 1 |
| < 16,384 | 2 |
| < 2,097,152 | 3 |

### 3.3 TLV Ordering

TLV ordering is significant for **progressive verification**:

```
[0x01 anchor core]       ← verifier checks signatures against cached UNL
[0x02 header]            ← authenticates tree roots
[0x03/0x04 map proofs]   ← proves data in the ledger
[0x02 header] ...        ← skip list hops (if historical)
[0x05 anchor UNL]        ← LAST: only needed if verifier has no cached UNL
```

The anchor is split into two TLV records (`0x01` core + `0x05` UNL) so the ~4KB
of validation signatures arrive before the ~15KB VL blob. A body MUST contain
exactly one `0x01`. **`0x05` is OPTIONAL**, and a body MUST NOT contain more
than one.

Omitting `0x05` is the point of the split, and it serves two distinct cases:

- **Externally supplied trust.** The proof is pinned to an anchor whose VL
  snapshot the verifier already holds — for example an anchor previously
  published on-chain, or a UNL cached from a prior proof. The ~15KB blob is pure
  duplication in that case.
- **Progressive verification under latency or bandwidth limits.** Streaming
  transports — animated QR, BLE, NFC — want the smallest byte count that can
  reach a verdict. A verifier with cached trust can begin verifying at `0x01`
  and finish without ever receiving the blob.

A verifier that receives a proof without `0x05` MUST resolve the UNL from its own
configured or cached trust for the proof's publisher key (§1.3), and MUST reject
the proof if it cannot. It MUST NOT treat an absent `0x05` as an absent trust
requirement: the anchor's validations still have to reach quorum against a
verified UNL, and the publisher key still has to be one the verifier trusts
(§8). An absent `0x05` changes where the UNL comes from, never whether one is
required.

Verifiers that do not implement cached-UNL trust MUST require `0x05`, assemble
the logical anchor by merging its payload into the `0x01` anchor core, and verify
that single logical anchor before any descendant step is trusted. Such a verifier
rejects a `0x05`-less proof for want of trust material, not for malformedness —
the distinction matters for error reporting, because the proof may be perfectly
valid for a better-provisioned verifier.

See Appendix A for reference implementation status.

Canonical binary order is the form-independent canonical order (§1.2), with
`0x05` anchor UNL kept last.

### 3.4 Anchor Core Payload (0x01)

```
[ledger_hash: 32 bytes]
[ledger_index: 4 bytes, big-endian uint32, advisory as in §2.1]
[publisher_key: 33 bytes, XRPL format (0xED prefix + 32-byte Ed25519 key)]
[validation_count: LEB128]
[common_fields_len: LEB128][common_fields: serialized STObject]
[unique_field_count: LEB128]
  for each unique field name:
    [name_len: LEB128][name: UTF-8 bytes]
  for each validator:
    [signing_key: 33 bytes]
    [unique_fields_len: LEB128][unique_fields: serialized STObject]
```

**Validation decomposition**: STValidation objects across validators typically
share many fields (LedgerSequence, SigningTime, Cookie, ServerVersion, etc.).
The encoder:

1. Parses all STValidation objects into field maps
2. Collects the union of all field names across all validations
3. Marks a field as **common** only if it appears in every validation with an
   identical value
4. Encodes those common fields once as a single serialized STObject
5. Encodes all remaining fields as the **unique field names** list, then emits
   per-validator STObjects containing whatever subset of those unique fields
   that validator actually carries

The unique field names list MUST be sorted lexicographically by XRPL field
name. Per-validator records MUST be sorted by ascending 33-byte signing key.
For each validator, the binary validator key MUST equal the embedded
STValidation `SigningPubKey`; verifiers MUST reject mismatches. To reconstruct
a full STValidation, a verifier merges the decoded common field map with the
decoded per-validator unique field map, then serializes the merged map using
canonical XRPL STObject field order. Producers MUST verify that this process
round-trips to the original raw STValidation bytes before emitting decomposed
validations.

With a single validation every field is common, `SigningPubKey` and
`Signature` included: the unique field names list is empty, and that
validator's record carries an empty STObject.

This preserves fields that appear only in some validations while still reducing
per-validator overhead from ~200 bytes to ~80 bytes.

### 3.5 Anchor UNL Payload (0x05)

```
[manifest_len: LEB128][manifest: raw publisher manifest bytes]
[signature_len: LEB128][signature: blob signature bytes]
[shape: 1 byte]
[sequence: 4 bytes, big-endian uint32]
[expiration: 4 bytes, big-endian uint32]
[validator_count: LEB128]
  for each validator:
    [validation_public_key: 33 bytes]
    [manifest_len: LEB128][manifest: raw validator manifest bytes]
```

The **shape byte** preserves JSON key ordering of the original VL blob for
exact byte reconstitution (required for signature verification). Bits 4-7 are
reserved and MUST be zero.

| Bits | Meaning |
|------|---------|
| 0-2 | Top-level key permutation index (3! = 6 permutations of `{sequence, expiration, validators}`) |
| 3 | Validator key permutation (2! = 2 permutations of `{validation_public_key, manifest}`) |

Top-level permutation indexes:

| Index | Key order |
|-------|-----------|
| 0 | `sequence`, `expiration`, `validators` |
| 1 | `sequence`, `validators`, `expiration` |
| 2 | `expiration`, `sequence`, `validators` |
| 3 | `expiration`, `validators`, `sequence` |
| 4 | `validators`, `sequence`, `expiration` |
| 5 | `validators`, `expiration`, `sequence` |

Validator permutation bit:

| Bit 3 | Per-validator key order |
|-------|-------------------------|
| 0 | `validation_public_key`, `manifest` |
| 1 | `manifest`, `validation_public_key` |

Blob reconstitution is exact and intentionally narrow:

1. Emit `{`.
2. Emit the top-level keys in the selected order with no whitespace.
3. For `sequence` and `expiration`, emit a JSON number in base-10 with no sign,
   leading zero, decimal point, or exponent.
4. For `validators`, emit an array in payload order. Each element is an object
   whose keys are in the selected validator order, with no whitespace.
5. `validation_public_key` is uppercase hex.
6. `manifest` is standard RFC 4648 base64 with padding, produced from the raw
   validator manifest bytes.
7. Separate object members and array elements with exactly one comma and no
   spaces.
8. Emit `}`.

The shaped encoding is valid only for a VL blob whose top-level keys are
exactly `{sequence, expiration, validators}` and whose validator objects have
exactly `{validation_public_key, manifest}`. Producers MUST verify the shaped
encoding round-trips to the exact signed blob bytes before emitting binary
form. If it does not round-trip, a binary producer MUST fail rather than
emit a non-verifiable decomposed blob; the JSON form can still carry the raw
blob bytes in `unl.blob`. A list that carries `effective`, or any other
top-level key, therefore has no binary form, and every list bundled in a
binary proof is in effect from 0.

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

Every inner node advances exactly one nibble, so a node's depth is its
nesting depth and is never encoded.

### 3.8 Binary Parsing Policy

The binary form is closed. A verifier MUST reject:

- a file header version other than the current one (§9.1);
- nonzero reserved flag bits;
- unknown TLV types;
- a TLV length that exceeds the remaining body bytes;
- trailing bytes inside a fixed-size payload;
- duplicate `0x01` or `0x05` records;
- a missing `0x01` record, or a first record other than `0x01`;
- any TLV record after `0x05`;
- a `0x05` record that precedes the `0x01` record;
- records after a logical verification failure;
- an invalid shape byte (`shape & 0xF0 != 0` or top-level permutation index
  greater than 5);
- a body, plain or decompressed, that exceeds the verifier's configured maximum
  size (§8);
- a compressed body that is not exactly one complete zlib stream (§3.1);
- trie recursion deeper than 64 nibbles.

The empty tree, lone-leaf tree root, and proofs of absence are out of scope for
the current format. A `map_proof` MUST contain at least one authenticated leaf and enough inner
nodes or hash placeholders to recompute a non-empty SHAMap root.

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
≥ 2^32 (all lengths and counts fit in uint32). Without this rule the same
proof would admit multiple binary encodings, breaking stable proof hashes
and cache keys (§1.2).

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
    skip_candidates = []       // (leaf_key, containing_index, Hashes[])

    for each step in chain.steps:

        if step is anchor:
            // The anchor's close time (§1.3): the next logical step, if any,
            // whose preimage hashes to anchor.ledger_hash. It is read here
            // to select the primary list and grants nothing until the anchor
            // has verified. An anchor-only proof has no close time.
            close_time = close time of the next step, if it is the anchor
                         ledger's header, else none
            view = resolve_trust_view(anchor, primary_publisher_key, close_time)
            // resolve_trust_view applies §1.3 P1-P4 and rules 1-11: the
            // publisher's signing key, the primary list (the anchor's own
            // list is one candidate; a list-less anchor's come from held
            // trust alone, §3.3), its soundness, each master's delegation,
            // and collisions. Manifests verify as §5.1 states. It rejects
            // the anchor where those rules say so.
            // For each validation:
            //   deserialize STValidation
            //   verify LedgerHash == anchor.ledger_hash
            //   verify the VAL\0 signature using SigningPubKey (§5.1)
            //   count it only if Flags has vfFullValidation (§1.3 rule 8)
            //     and SigningPubKey is a master's resolved key (rule 5)
            //   SHOULD: report a counted validation whose signed
            //     LedgerSequence differs from anchor.ledger_index (§2.1)
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
                for (leaf_key, containing_index, hashes) in skip_candidates:
                    if computed_hash not in hashes:
                        continue
                    if classify_ledger_hashes_link(
                            leaf_key,
                            containing_index,
                            step.header.ledger_index) is not None:
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
                                (leaf.key, containing_index, leaf.SLE.Hashes))

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

| Manifest field | Meaning |
|---|---|
| `PublicKey` | The validator's (or publisher's) master public key |
| `SigningPubKey` | The signing key the master delegates to |
| `Sequence` | Manifest sequence; `0xFFFFFFFF` is a revocation |
| `Signature` | Signature by `SigningPubKey` over the signing data |
| `MasterSignature` | Signature by `PublicKey` over the signing data |
| `Domain` | Optional; a properly formed domain if present |

Verifiers MUST verify the manifest `MasterSignature` with the manifest
`PublicKey` (master public key). A non-revocation manifest MUST include both
`SigningPubKey` and `Signature`, and verifiers MUST verify `Signature` with
`SigningPubKey`. A revocation MUST include neither. As in rippled, a manifest is
also invalid if:

- its bytes are not the canonical serialization of its fields;
- it carries a `Version` other than 0;
- it has a `Domain` that is not a well-formed domain;
- its `SigningPubKey` equals its own `PublicKey`.

An invalid manifest is never a candidate (§1.3 rule 1).

STValidation signing data is:

```
VAL\0 || canonical_STObject(STValidation without Signature)
```

Verifiers MUST verify each validation signature with the embedded
`SigningPubKey`.

Supported public-key encodings:

| Key prefix | Algorithm | Verification input |
|------------|-----------|--------------------|
| `0xED` | Ed25519 | raw signing data, with the 32-byte Ed25519 key following the `0xED` prefix |
| `0x02` or `0x03` | secp256k1 ECDSA | DER-encoded ECDSA signature over `SHA512-Half(signing_data)` |

Verifiers MUST reject unknown public-key prefixes.

Every signature a verifier checks (validations, manifests, the list blob) MUST be
in canonical form:

- **secp256k1:** strict DER with `0 < r < n` and `0 < s <= n/2` (low S), as
  libsecp256k1 enforces.
- **Ed25519:** 64 bytes with `S < L` (RFC 8032 §5.1.7), as libsodium enforces.

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
4. The SLE MUST contain a well-formed `Hashes` field (`STVector256`, XRPL's serialized list of 256-bit hashes), with each
   entry exactly 32 bytes.
5. The child ledger header being trusted from the SLE MUST have a computed
   header hash equal to one entry in `Hashes`, and its authenticated
   `ledger_index` MUST be in the range covered by that SLE.

Only leaves that satisfy this predicate produce ledger-hash capabilities.
Verifiers MUST NOT harvest ledger hashes from arbitrary state leaves that happen
to contain a `Hashes` field.

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

Both reference verifiers implement the predicate as:

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
```

If the SLE includes `LastLedgerSequence`, verifiers SHOULD cross-check it
against the range above. The authoritative child ledger index still comes from
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
  claims MUST check the authenticated `close_time` for acceptable staleness.
  `close_time` is a ledger-header field, so it is authenticated by the header
  hash when the header is trusted.
- Reported ledger indices MUST come from an authenticated source — a header
  preimage, or the signed `LedgerSequence` inside the anchor validations —
  never from the bare advisory `anchor.ledger_index` (§1.1, §2.1).

## 6. Composition Patterns

Producers select the shortest available hop. A **flag-ledger target**
(`ledger_index % 256 == 0`) is reached in one hop via the anchor's long (era)
skip list regardless of distance — its era-list leaf is never larger than the
always-full recent list, so this is the shortest path even for a recent flag
ledger. A near **non-flag** target uses the recent short skip list; only a
distant non-flag target needs the 2-hop path through an intermediate flag
ledger.

If the transaction is in the anchor ledger itself, no skip-list hop is needed:

```
anchor → ledger_header(anchor) →
map_proof(tx, transaction_hash)
```

### 6.1 Recent Transaction (short skip list, ≤ 256 ledgers behind anchor)

```
anchor → ledger_header →
map_proof(state, skip_list_key) →
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

For a distant target that is itself a flag ledger (`target % 256 == 0`), the
anchor ledger's long skip list directly contains the target hash, so the
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
- emit canonical proof order (§1.2) when the proof will be hashed, signed,
  cached, compared, or deduplicated;
- emit no JSON form for a proof whose leaves cannot be round-tripped exactly
  through its codec, and point the consumer to the binary form;
- emit binary VL decomposition only when the shaped blob round-trips exactly;
- emit no member the closed JSON form does not list (§2), and no unknown,
  reserved, or extension values in the binary form (§3.8).

A conforming verifier MUST:

- reject malformed syntax, unknown reserved values, and unsupported versions;
- verify every signature, hash preimage, and SHAMap root needed by the proof;
- reject any step whose parent capability has not already been authenticated;
- report whether the proof verified, and expose the authenticated outputs
  that consumers need to inspect.

At minimum, verifier output SHOULD expose:

- the configured `network_id` and whether it matched the trusted publisher-key
  configuration;
- the trusted anchor ledger hash and the validation quorum summary;
- each authenticated ledger header and its source (anchor, parent hash, or
  recognized LedgerHashes leaf);
- each authenticated state or transaction leaf key and raw hashed bytes;
- each recognized LedgerHashes-derived ledger hash;
- warnings for non-canonical but verifiable ordering or duplication.

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
  XRPL protocol. Every signature MUST be in canonical form (§5.1).
- **Failure.** The verifier MUST reject chains where any step fails.
- **Resource limits.** Verifiers MUST enforce configured maximum sizes before
  parsing: on the binary body, plain or decompressed (a compressed body is
  bounded while it inflates, against zlib bombs), and on JSON text. They MUST
  reject trie recursion deeper than 64 nibbles (§3.8).
- **Two forms.** Binary and JSON forms of the same proof MUST verify
  identically. Hashes, signatures and cache keys are taken over the canonical
  uncompressed binary encoding, never the JSON text (§1.2).
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
| 7 (`0x07`) | current draft |
| 6 | superseded; served publicly by xprv.it until 2026-10-25 |
| 5 and earlier | superseded, never released |

The number changes whenever a change alters which inputs are valid, and a
number is never reused: two verifiers claiming one version must not disagree
about the same bytes. v6 proofs were served publicly, so current verifiers
reject them by version rather than by content, and producers reissue them.

Versions other than 7 are unrecognized by this draft and MUST be rejected.

### 9.2 Network IDs

| `network_id` | Network |
|--------------|---------|
| 0 | XRPL mainnet |
| 21337 | Xahau mainnet |

Other values are reserved for future registry entries or private deployments.
Verifiers MUST bind any accepted value to its configured primary publisher key
(§8).

### 9.3 Binary Type Registries

The TLV type registry is defined in §3.2. The shape-byte registry is defined in
§3.5. All unassigned values are reserved in the current format version and MUST be rejected.

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

What the reference verifiers implement of this draft. How the reference server
holds and passes trust material is described in `src/xprv/README.md` in the
reference repository.

| Capability | C++ `xprv verify` | Python `verify-xprv.py` |
|---|---|---|
| Both forms; capability-based verification of the DAG (§1, §5) | yes | yes |
| Trust view from the bundled list plus held manifests (§1.3) | yes, `--manifests FILE` | yes, `--manifests FILE` |
| Primary lists from outside the proof; a list-less anchor (§3.3) | no: rejected for want of a list | no: same |
| Online list refresh before verifying | no; the reference server refreshes lists | no |
| Canonical binary encoder (`convert`, JSON to binary and back) | yes | no; it decodes binary only |
| Decoded Xahau JSON leaves | yes | no: xrpl-py ships XRPL definitions only, so it verifies Xahau binary proofs and points Xahau JSON proofs to the binary form |
| Sibling fan-out and multi-leaf proofs | accepted; non-canonical repetition warned | same |

FILE is a JSON array of base64 raw manifests. A list-less anchor is well-formed
and unverifiable by both verifiers until one can hold lists from outside the
proof.

## Appendix B. Test Vectors

Known-answer vectors live in `tests/xprv/vectors/` in the reference repository.

**Positive vectors** carry one proof in both JSON and binary form. Each states
its configured trust: the network and its publisher key, taken from the
verifier's configuration, never from the key the proof names. Each also
carries the intermediate values an independent implementation needs in order
to localise a mismatch rather than merely observe one:

- for each `ledger_header`: the full `LWR\0` preimage (122 bytes: 4-byte prefix
  plus the 118-byte canonical header), its recorded length, and the resulting
  ledger hash;
- for each `map_proof`: the recomputed trie root, and per leaf the exact bytes
  hashed (§5.2) with the resulting leaf hash;
- the derived short and long skip-list keys (§5.3).

**Negative vectors** are stored as a reference to a positive vector plus
explicit edits, rather than as whole copies. There are two kinds:
- **byte mutations of the binary form:** set, insert, truncate, recompress or
  append;
- **JSON-pointer patches of the JSON form,** for attacks that live in JSON
  structure or in the anchor's contents.

Each names the rule it violates and the rejection reason expected, so a
verifier that rejects for the *wrong* reason is also a finding. Each also names
the layer it tests:
- **parse:** malformed syntax;
- **graph:** a post-anchor capability or leaf rule;
- **trust:** the list, quorum or signatures.

The family covers:
- the container: version, flags, magic, TLV type, order and length; non-minimal
  LEB128; a zlib trailer;
- the closed JSON form: a bare list, an unknown member, a float version, a
  malformed validation value, a wrong `__depth__`;
- leaf rules: an unhashed leaf member, a map proof without leaves;
- trust: a failed quorum, forged validation signatures, a publisher-key
  mismatch, a missing list.

### Cross-validation, and why the split matters

The corpus is deliberately not authored by a single implementation. The
intermediates are derived by the Python reference verifier
(`scripts/xprv-vectors.py annotate`), and the C++ implementation is held to them
by `tests/xprv/vectors-gtest.cpp`. Neither side grades its own homework: a
divergence between the two, including a disagreement between their XRPL codecs
about the bytes a decoded leaf serialises to, fails a build rather than shipping.

**Two passes in C++.** Every vector runs through the production verification
path under its configured trust, and its refusal reason is checked. Parse and
graph negatives also run with the anchor pretrusted, which isolates the checks
after the anchor. Trust negatives are left out of that pass, since skipping the
anchor checks is its purpose.

**No unsubstantiated claims.** The generator refuses to record a claim it
cannot substantiate. If a mutation or patch is *accepted* by the reference
verifier, `mutate` errors out instead of writing a vector that asserts a
rejection which does not occur.

**One encoder.** Producing the binary form is the C++ implementation's job
(`xprv convert`), since it owns the canonical encoder. Reimplementing that
encoder in the vector tooling would create a second encoder to keep in sync,
and a divergence between the two would be indistinguishable from a divergence
between implementations.

**The historical forgery.** One JSON-form negative carries its whole proof
inline: a real anchor and real UNL with a forged tail, where a LedgerHashes
leaf hangs off a lowercase branch key beside its uppercase sibling (§2.3). Both
reference verifiers accepted it before that rule existed.

**Not yet covered:** a *positive* proof without the optional `0x05` anchor UNL
(§3.3), verified from trust the verifier already holds. That needs a verifier
that holds lists from outside the proof. The negative case, a missing list
refused for want of trust, is covered.
