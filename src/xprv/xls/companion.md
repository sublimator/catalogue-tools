# XPRV in Plain English: A Reader's Guide and Design Companion

> **Status:** Non-normative companion to [`spec-draft.md`](spec-draft.md)
> (Format Version 10). If anything here ever seems to disagree with
> `spec-draft.md`, the specification wins.

[`spec-draft.md`](spec-draft.md) is written so two independent verifiers never
disagree about the same bytes. That requires precision — exact byte offsets,
refusal layers, canonical encodings, and edge-case rules — which can make it
dense on a first read.

This companion is the conversation you would have before opening the spec. It
explains how an XPRV proof actually works, walks through a real chain from a
single public key down to a ten-year-old transaction, and records *why* the
format has the rules it has — including the subtle bugs and real forgeries that
shaped version 10.

---

## 1. The Problem XPRV Solves

Suppose someone tells you that a particular XRP Ledger transaction happened in
2016 — or that an account held a certain balance at ledger 50,000,000 — and you
want to verify that claim *without* running a multi-terabyte full-history node
and *without* blindly trusting an RPC server.

Every XRPL ledger header already commits cryptographically to:
- all account state in that ledger (`account_hash`, the root of the state SHAMap),
- all transactions and their execution metadata (`transaction_hash`, the root of
  the tx SHAMap), and
- the previous ledger (`parent_hash`).

And whenever a ledger closes, the network's validators sign `STValidation`
messages attesting to its hash. If 80% of the validators on a publisher's
Unique Node List (UNL) signed that ledger hash, the network reached consensus
on it.

The catch is scale. Walking `parent_hash` links back 100 million ledgers would
take gigabytes of headers. Luckily, XRPL already solved fast historical lookup
inside its own state tree:

- The **short skip list** (a `LedgerHashes` entry at a fixed key) holds the
  hashes of the previous 256 ledgers.
- The **long skip list** (a `LedgerHashes` entry whose key is derived from a
  ledger number) holds the hashes of every 256th "flag" ledger (`seq % 256 == 0`)
  going all the way back to genesis.

Because those skip lists live inside the ledger's state SHAMap, they are
authenticated by the ledger's `account_hash`. Put a fresh validator quorum on
top and a couple of SHAMap inclusion proofs underneath, and you can prove *any*
historical transaction or ledger entry in a single self-contained file —
typically a few dozen kilobytes — verifiable offline in milliseconds with
nothing configured except the network's publisher public key.

That file is an **XPRV proof**.

---

## 2. Walking a Proof: Authority Flows Forward

Here is the classic 7-step proof for an arbitrary historical transaction (say,
from millions of ledgers ago, proved under today's anchor ledger):

```text
Configured trust root: publisher master public key (33 bytes)
  │
  ▼
Step 1 · anchor
  Publisher manifest + signed UNL + validator manifests + ≥ 80% validations
  ──▶ unlocks: trusted anchor ledger hash
  │
  ▼
Step 2 · ledger_header (anchor ledger)
  118-byte header hashes to the trusted anchor ledger hash (LWR\0)
  ──▶ unlocks: anchor's account_hash, transaction_hash, parent_hash, close_time
  │
  ▼
Step 3 · map_proof (state tree of anchor ledger)
  Abbreviated SHAMap hashing to anchor's account_hash
  Reveals the long-skip-list LedgerHashes leaf covering target's flag ledger
  ──▶ unlocks: trusted hash of flag ledger F (where F % 256 == 0)
  │
  ▼
Step 4 · ledger_header (flag ledger F)
  118-byte header hashes to F's trusted hash
  ──▶ unlocks: F's account_hash
  │
  ▼
Step 5 · map_proof (state tree of flag ledger F)
  Abbreviated SHAMap hashing to F's account_hash
  Reveals the short-skip-list LedgerHashes leaf covering target ledger T
  ──▶ unlocks: trusted hash of target ledger T
  │
  ▼
Step 6 · ledger_header (target ledger T)
  118-byte header hashes to T's trusted hash
  ──▶ unlocks: T's transaction_hash (and account_hash)
  │
  ▼
Step 7 · map_proof (tx tree of target ledger T)
  Abbreviated SHAMap hashing to T's transaction_hash
  Reveals the target transaction + metadata leaf
  ──▶ PROVEN: transaction T + metadata occurred in ledger T
```

Closer targets need fewer hops (spec §6):
- **Same ledger as the anchor** (distance 0): 3 steps (`anchor` → anchor
  `ledger_header` → `map_proof`). No skip list at all.
- **Anchor's immediate parent** (distance 1): 4 steps (`anchor` → anchor
  `ledger_header` → parent `ledger_header` via `parent_hash` → `map_proof`).
  Still no skip list — even if the parent happens to be a flag ledger.
- **Recent ledger** (distance 2 to 256): 5 steps (anchor's short skip list
  reaches the target in one hop).
- **Older ledger** (distance > 256): 7 steps, as shown above.
- **Account state instead of a transaction**: the final `map_proof` simply
  reads the target ledger's `state` tree (`account_hash`) instead of its `tx`
  tree (`transaction_hash`).

### Why the verifier doesn't hardcode those templates

Notice that none of those shapes requires a special-case parser. XPRV models
verification as a tiny **capability machine** (spec §1, §5):

- Start with empty sets of trusted ledger hashes, trusted state roots, and
  trusted transaction roots.
- Step 1 (`anchor`) checks the quorum and puts the anchor ledger hash into
  `trusted_ledger_hashes`.
- Every subsequent `ledger_header` step hashes its 118 bytes (`LWR\0 || header`)
  and checks that the resulting hash is *already* in `trusted_ledger_hashes`.
  If so, it adds `parent_hash` to `trusted_ledger_hashes`, `account_hash` to
  `trusted_state_roots`, and `transaction_hash` to `trusted_tx_roots`.
- Every `map_proof` step recomputes its SHAMap root and checks that the root is
  *already* in `trusted_state_roots` (for `state`) or `trusted_tx_roots` (for
  `tx`). If a `state` proof reveals a recognized `LedgerHashes` skip-list entry,
  every hash inside its `Hashes` array is added to `trusted_ledger_hashes`.
- Authority only flows **forward** (earlier steps unlock later steps), so a
  verifier can check a proof in a single streaming pass without buffering the
  whole DAG to look ahead.

One small rule handles 3-step proofs, 7-step proofs, anchor-only proofs,
multi-leaf proofs from one ledger, and fan-out proofs that branch from one
skip list to several historical ledgers.

### The golden rule: never trust a wrapper

One principle explains half the spec on its own:

> **A field is trusted only because it sits inside bytes whose hash or
> signature you already verified — never because an enclosing wrapper claims
> it.**

Watch how many design rules fall straight out of that one sentence:

- **No `ledger_index` on the anchor** (spec §2.1, §3.4). Earlier drafts carried
  an advisory `ledger_index` in the anchor, even though nothing signed or
  hashed it — you could change it to `42` and the proof still verified. Version
  10 removed it completely. Ledger numbers come exclusively from authenticated
  `ledger_header` preimages.
- **Closed JSON objects** (spec §2). Every proof object, step, `header`, and
  `unl` allows only the exact members the spec lists. A member nothing hashes
  is an assertion nothing checks; a closed schema leaves nowhere to smuggle
  fake metadata (like a fabricated `"ledger_hash"` inside a header or an extra
  `"summary"` field).
- **No `hash`, `index`, `blob`, or `delivered_amount` inside JSON leaves**
  (spec §2.3). RPC decoders often attach convenience fields like `hash` or
  `index` next to decoded ledger objects. Those fields are not part of the
  hashed XRPL serialization, so verifiers reject them. The authenticated leaf
  key is the trie's `key` element — never a field inside the leaf body.
- **`__depth__` is the one explicit exception** (spec §2.3). JSON trie inner
  nodes allow an optional integer `__depth__` diagnostic for human readability,
  and verifiers *still* reject it if it disagrees with the node's actual
  nesting depth.

---

## 3. How Trust Works (And Why Offline Proofs Are Tricky)

Everything below the anchor is pure SHA512-Half math. All the interesting
security design lives in **Step 1**: turning one 33-byte publisher public key
into confidence that a ledger hash actually achieved consensus.

### The chain of signatures

```text
Configured publisher master key (trusted by verifier config)
  │
  ├─ signs publisher Manifest (MAN\0, MasterSignature + Signature)
  │    ──▶ delegates to publisher ephemeral signing key
  │         │
  │         └─ signs Validator List blob (JSON with sequence, expiration, validators[])
  │              ──▶ names N validator master keys (each with a validator Manifest)
  │                   │
  │                   └─ each validator Manifest (MAN\0) delegates master ──▶ signing key
  │                        │
  │                        └─ validator signs STValidation (VAL\0) for anchor ledger_hash
  │                             ──▶ count distinct masters with valid full validations
  │                                 require count ≥ ceil(0.8 × N)
```

Notice how each validator is counted **at most once per master key**, even if
it rotated keys or someone included duplicate validations, and only **full**
validations (`Flags & 0x00000001 != 0`) count — partial validations do not
sign off on ledger finality.

### Trust roots vs. evidence channels

A common mistake when thinking about offline proofs is imagining that the
proof's bundled UNL is "less trusted" than a list fetched over HTTPS, or that
a `--manifests` file is somehow "trusted input."

In XPRV, there is only **one trust root per network**: the configured primary
publisher public key (spec §1.3, §8). Everything else — the list bundled inside
the proof, manifests passed via `--manifests`, secondary publisher lists, peer
gossip — is just **cryptographic evidence** whose authenticity is checked
against that root.

An untrusted channel (including the proof itself!) can *withhold* newer
evidence, such as a key rotation or a revocation published later, which is why
a live verifier like `xprv serve` feeds fresh held manifests and lists into the
same trust-view algorithm. What an untrusted channel can *never* do is make an
unverified manifest or forged list count.

### Why the trust-view rules are as strict as they are

Each rule in spec §1.3 exists to close a concrete failure mode:

- **Revocations never shrink `N` (the denominator).** A manifest with
  `Sequence == 0xFFFFFFFF` permanently revokes a master key. A revoked
  validator stays in the list's `N` and contributes zero votes. That is
  deliberately conservative: revoking keys can never make quorum *easier* to
  reach. And the same law applies to the publisher itself — a verified
  publisher revocation invalidates that publisher permanently.
- **Two masters sharing a signing key kills the whole anchor.** Suppose a
  validator list maps two different master keys onto the same ephemeral signing
  key. Which one is honest? Rather than guessing or silently discounting one,
  XPRV rejects the anchor outright — a list with colliding signing keys signals
  a compromised or broken publisher, and rejecting loudly is the safest
  response.
- **Equivocation is checked differently for manifests vs. lists (new in v10).**
  If a validator master signs two different manifests at *any* same sequence
  number, that master key itself equivocated — it stays in `N` and can never
  vote. If a publisher master signs two different publisher manifests at *any*
  same sequence, the publisher is dead. For validator *lists* (which are signed
  by the publisher's ephemeral signing key), however, equivocation only
  triggers among **eligible** lists at the highest eligible sequence — an
  expired or otherwise ineligible historical list does not retroactively poison
  a newer valid one.
- **No wall clock — freshness is measured against the anchor's `close_time`.**
  A proof verified today and verified ten years from now against the same trust
  material must produce the same verdict. Using the verifier's system clock
  would make old valid proofs suddenly start failing as lists expire. Instead,
  a list's `[effective, expiration)` window is checked against the anchor
  ledger header's `close_time` (when a matching anchor header step is present;
  anchor-only proofs remain valid modular building blocks without one).
- **Negative UNL is intentionally ignored.** On live XRPL, `rippled` can
  temporarily lower its quorum denominator when validators go offline via the
  Negative UNL. Reconstructing Negative UNL state offline adds complexity and
  weakens the standalone guarantee, so XPRV always requires `ceil(0.8 × N)`
  against the primary list's full `N`. During a heavy outage, a ledger that
  `rippled` validated may simply lack an XPRV quorum until enough validators
  are back online.
- **One primary publisher key; secondary keys refresh manifests only.** Merging
  validator membership across multiple publishers introduces policy questions
  that belong outside a wire format. A verifier configures exactly one primary
  publisher key per network (defining membership `N`). Any secondary publisher
  keys may contribute fresher manifests (rotations or revocations) for masters
  *already* on the primary list, and nothing more.
- **Why `0x05` / bundled `unl` list is optional on the wire.** If a verifier
  already holds the publisher's verified list (for instance, in a batch of
  hundreds of proofs anchored to the same recent ledger), repeating a ~22 KB
  validator list in every proof wastes space. Omitting `0x05` (or the
  `manifest`/`blob`/`signature` trio in JSON) lets a stateful verifier supply
  the list from its held trust view, while a stateless CLI verifier cleanly
  refuses the proof at the **trust** layer for want of a list (spec §3.3).

---

## 4. Two Forms, One Meaning (JSON vs. Binary)

XPRV defines two encodings of the same proof graph that share a single format
version number (`10`) and must always verify identically:

| | Binary (`.xprv.bin`) | JSON (`.xprv.json`) |
|---|---|---|
| **Built for** | Wire transport, storage, streaming, forward compatibility | Inspection, debugging, human-readable tooling |
| **Container** | 10-byte header (`XPRV`, version, flags, `network_id`) + TLV records; optional RFC 1950 zlib compression | Closed UTF-8 JSON object with `format_version`, `network_id`, `steps` |
| **SHAMap leaves** | Exact raw XRPL serialized bytes | Decoded XRPL JSON (`sle`, or `{"tx": ..., "meta": ...}`) |
| **Needs XRPL field definitions for leaves?** | **No.** Binary leaves hash their exact bytes directly. | **Yes.** Must re-serialize and round-trip against the network codec. |

### Why binary is the fallback for unfamiliar leaves (and JSON is decoded-only)

Whenever XRPL or Xahau activates a new amendment with new transaction fields, a
verifier whose field definitions haven't been updated yet cannot decode that
transaction to JSON.

In **binary form**, that doesn't matter: the verifier hashes the leaf's raw
bytes directly (`MLN\0 || sle_bytes || key` or `SND\0 || VL(tx) || VL(meta) ||
key`) and authenticates the inclusion proof without needing to understand the
new fields at all. (Field definitions are only needed for the anchor's
`STValidation` entries and, on `LedgerHashes` state leaves, extracting the skip
list.)

In **JSON form**, XPRV deliberately refuses to allow an opaque hex fallback or
a side-by-side `"blob"` field. Why? Because if a JSON leaf carried both
human-readable JSON and a raw hex blob, only the blob would be authenticated by
the hash — leaving the displayed JSON free to disagree with what was actually
proven. So JSON leaves are **decoded-only**: if a leaf cannot be decoded and
round-tripped cleanly under the verifier's definitions snapshot, that proof
simply has no JSON form, and the producer points the consumer to binary.

### The big version 10 simplification: carry exact bytes

If you compare version 10 to earlier drafts (v6–v9), the single biggest change
is what got *deleted*:

1. **No more shaped UNL encoding in `0x05`.** Earlier versions parsed the
   publisher's signed JSON list, stripped it down to a compact binary structure,
   and recorded a "shape byte" plus a field permutation so the decoder could
   reconstruct the exact signed JSON text byte-for-byte to verify the
   publisher's signature. Whenever a publisher added a new JSON member,
   changed whitespace, or included an `effective` field in a new position,
   reconstitution broke — creating "JSON-only" proofs that could not even be
   converted to binary! Version 10 drops the whole scheme: `0x05` carries
   `[manifest][signature][exact signed list bytes]`. Every valid proof now has
   a binary form, and once zlib runs on the body, the size difference is tiny.
2. **No more common/unique validation splitting in `0x01`.** Earlier versions
   factored out fields shared across all 35 `STValidation` messages into a
   common header and stored per-validator deltas separately, plus a field-name
   dictionary. That added hundreds of lines of tricky codec logic to both
   verifiers and invited edge-case bugs whenever validations carried different
   optional fields. Version 10 replaces all of it with raw length-prefixed
   `STValidation` bytes sorted in strictly ascending order of their embedded
   33-byte `SigningPubKey`.

Both deletions removed major divergence surfaces while making the spec and
both reference verifiers noticeably shorter and easier to audit.

### Proof identity vs. "what was proven"

People often ask: *what is the hash/ID of an XPRV proof?*

Version 10 keeps the two concepts cleanly separated (spec §1.2, §1.4):

- **Artifact (CAS) identity:** `SHA512-Half` of the complete uncompressed
  binary form (with `flags = 0`, even if received compressed or as JSON). This
  gives storage systems and caches a deterministic byte handle for a specific
  proof artifact.
- **Claim identity:** What the proof actually proves is the tuple
  `(network_id, authenticated ledger_hash, tree, leaf_key)` (plus the leaf
  bytes). Two completely valid proofs of the *same* historical transaction
  taken ten minutes apart will have different anchor ledgers, different
  validation sets, and therefore different CAS hashes — and both prove the
  exact same claim.

One subtle caching pitfall to avoid (spec §1.4): **never cache a "valid"
verdict by CAS hash before validating the exact representation you received.**
A malformed JSON proof (say, carrying an extra unhashed member or a bad
`__depth__`) could otherwise look identical after conversion to a previously
cached valid binary proof and slip through. A cached verdict is only reusable
after validating the received bytes under the same verifier policy, definitions
snapshot, and resolved trust view.

---

## 5. Why the Spec Is So Strict (Stories from Real Bugs)

On a cold read, some rules in `spec-draft.md` can look fussy — refusing
lowercase hex, refusing empty trie nodes, insisting on strict RFC 4648 base64,
requiring a full codec round-trip on JSON leaves. Almost every one of those
rules exists because a real forgery or a real verifier disagreement happened
without it.

### The duplicate-nibble forgery (why uppercase-only & single-pass parse)

In an early version, JSON trie branch keys were normalized case-insensitively.
An attacker could take a real anchor and a real UNL, find a trie inner node
with a legitimate branch `"A"`, and add a *second* sibling member `"a"` hanging
a forged `LedgerHashes` leaf off the same inner node. One code path hashed
`"A"` into the matching trie root while another path enumerated `"a"` as an
authenticated leaf!

The historical forgery is preserved in `tests/xprv/vectors/` as
`negative-json-duplicate-nibble-forge.json`. The fix is simple and complete:
**uppercase hex only everywhere in JSON, zero case normalization anywhere, and
derive the parsed trie once for root hashing, leaf enumeration, and display.**

### Why a strict codec round-trip replaced the leaf type table

Version 9 tried to guard JSON leaves with a hand-maintained table listing
permitted JSON types per XRPL field type. Independent reviews immediately broke
it in half a dozen ways:
- Python's `xrpl-py` silently **drops `null` members** during serialization, so
  adding `"InvoiceID": null` to a verified transaction hashed to the exact same
  bytes — an unauthenticated member smuggling straight through!
- `xrpl-py` silently coerces JSON booleans (`false`/`true`) into `0`/`1`, so
  `"Flags": false` verified cleanly.
- Some codecs accept X-addresses (silently unpacking destination tags), accept
  unknown currency strings as native XRP, or stop parsing numbers at trailing
  garbage characters.

Rather than playing whack-a-mole with a type table, version 10 states **one
rule** (spec §2.3): serialize the leaf content with the network codec, decode
those exact bytes back into canonical reference JSON, and require exact
structural, type, and value equality with the input leaf. If the codec dropped
a field, coerced a type, expanded an alias, or normalized a spelling, the
round-trip won't match and the leaf is refused.

And when `catl::xdata` disagreed with `rippled` on native `Issue` objects
(emitting bare `"XRP"` instead of `{"currency": "XRP"}`) and mistakenly treated
issued `"XAH"` on XRPL mainnet as native XRP — causing live HTTP 500s on real
mainnet transactions — the fix was equally clear: `rippled` (for XRPL) and
`xahaud` (for Xahau) are the reference JSON encoders, and only a network's own
system code is native.

### Why minimal trie shape is enforced at parse

An empty inner node `{}`, an expanded inner node containing only pruned hash
placeholders (zero leaves beneath it), an all-zero `00...00` placeholder
standing in for an empty branch, or a leaf sitting under a nibble path that
doesn't match its own 32-byte `key` all change the proof bytes without changing
(or while misrepresenting) the authenticated tree. Refusing all four at the
parse layer in both JSON and binary keeps tries minimal and eliminates
disagreement between verifiers.

### Why crypto, base64, and signed-list JSON are locked down

Two verifiers claiming the same format version must never disagree on the same
bytes (spec §9.1). That is impossible if one verifier's parser is permissive:
- **secp256k1:** Non-canonical DER encodings or high-S signatures are rejected
  everywhere (matching `libsecp256k1` strict DER + low-S normalization).
- **Ed25519:** Pure-Python libraries often accept small-order public keys or
  non-canonical encodings that `libsodium` rejects. Spec §5.1 pins the exact
  `libsodium` Ed25519 profile (canonical `A`/`R`, `S < L`, non-small-order
  `A`/`R`, cofactorless equation) so all verifiers agree on every edge case.
- **Base64 & signed-list JSON:** A lenient base64 decoder that silently skips
  non-alphabet bytes or ignores non-zero pad bits will accept a corrupted
  manifest that a strict decoder rejects. Likewise, duplicate keys in a signed
  list's JSON (`{"sequence": 1, "sequence": 2}`) can be resolved differently by
  different JSON parsers. Version 10 requires canonical padded RFC 4648 base64
  and unique JSON object members in signed lists.
- **Version discipline:** Whenever a rule changes which inputs are valid, the
  format version number bumps (v6 → v7 → v8 → v9 → v10) and old numbers are
  never reused. Old proofs fail cleanly on version instead of producing silent
  disagreements.

---

## 6. Building Your Own Verifier: A Practical Roadmap

If you want to write a third independent XPRV verifier (in Rust, Go,
TypeScript, Swift, etc.), you do not need to boil the ocean all at once. Here
is the shortest path that gets you verified against the shared conformance
corpus:

1. **Start with binary form (`.xprv.bin`) only.**
   Binary leaves are authenticated as raw bytes, so you do *not* need a JSON
   leaf codec for arbitrary transaction or ledger-entry types just to verify
   binary proofs. You only need:
   - the 10-byte container header, optional bounded RFC 1950 zlib inflation,
     minimal LEB128 varints, and TLV framing (spec §3, §4);
   - `Manifest` parsing, canonical `STValidation` round-trip validation against
     your protocol-definitions snapshot (spec §2.1, §3.4), and `LedgerHashes`
     extraction for skip-list leaves (spec §5.3).
2. **Implement the hashing primitives first** (spec §5.1–§5.4):
   - `SHA512-Half` (first 32 bytes of SHA-512),
   - Ledger header hash (`LWR\0 || 118 bytes`),
   - SHAMap inner node (`MIN\0 || 16 × 32-byte child hashes`),
   - SHAMap state leaf (`MLN\0 || sle_bytes || key`) and tx leaf
     (`SND\0 || VL(tx) || VL(meta) || key`),
   - Short and long skip-list keys (§5.3).
   Check each intermediate against `tests/xprv/vectors/positive-tx-skiplist.json`
   — it records every header preimage, every leaf's hashed bytes and leaf hash,
   every recomputed trie root, and the skip-list keys so you can isolate any
   byte-level mismatch immediately.
3. **Wire up canonical signature verification** (spec §5.1):
   - secp256k1 strict DER + low-S (`libsecp256k1` or equivalent),
   - Ed25519 matching the `libsodium` profile.
4. **Implement the §1.3 trust view and §5 capability machine:**
   - Primary publisher key check, publisher manifest + list signature,
     strict RFC 4648 base64 + unique JSON keys on the signed list,
     validator manifest resolution, revocations, equivocation, and `ceil(0.8 × N)`
     full-validation quorum.
   - Forward-only capability loop over `0x02` headers and `0x03`/`0x04` map
     proofs, enforcing minimal trie shape and the `LedgerHashes` skip-list
     extraction rules.
5. **Run the black-box conformance suite against your executable.**
   All `scripts/xprv-conformance.py` expects is a CLI that accepts a proof file,
   network ID, and trusted publisher key, exiting `0` to accept and `1` to
   refuse (never crashing or timing out):
   ```sh
   scripts/xprv-conformance.py run \
     --verifier mine='my-verifier {proof} --network {network_id} --key {publisher_key}'
   ```
   The corpus includes positive proofs, accept vectors (valid variations you
   must accept), and negative vectors covering parse, graph, and trust refusals
   — generated independently of any single implementation so neither reference
   verifier grades its own homework (spec Appendix B).
6. **Add JSON form (`.xprv.json`) when you need human-readable proofs:**
   - Enforce the closed JSON schema, uppercase-only hex, strict integer tokens,
     and single-pass trie parsing (§2).
   - Connect your network's XRPL binary codec and implement the §2.3 strict
     round-trip check on every decoded leaf.

---

## 7. Where to Go Next

- **Normative specification:** [`spec-draft.md`](spec-draft.md)
- **Reference server trust-view lifecycle:** [`../README.md`](../README.md)
- **Shared test vectors & conformance runner:** `tests/xprv/vectors/`,
  `scripts/xprv-vectors.py`, and `scripts/xprv-conformance.py`
- **Reference implementations:**
  - C++ prover, verifier, converter, and server: `src/xprv/`
  - Standalone Python verifier: `scripts/verify-xprv.py`
