# How an XPRV Proof Works

> **Status:** Non-normative companion to [`spec-draft.md`](spec-draft.md)
> (Format Version 10). Where anything here is simplified or differs from
> `spec-draft.md`, the specification governs.

[`spec-draft.md`](spec-draft.md) is written so two independent verifiers —
given the same trust material, verifier policy/limits, and protocol-definitions
snapshot — never disagree about the same bytes. That requires exact wire
layouts, refusal layers, and edge-case rules, which makes it dense on a first
read.

This companion is the conversation you would have before opening the spec. It
walks through a real XRPL proof from one configured public key down to a single
Payment and its execution result, explains how trust and skip lists fit
together, and records *why* version 10 has the rules it has.

---

## 1. The Problem XPRV Solves

Suppose someone tells you that an XRP Ledger transaction occurred in an older
ledger — or that an account held a certain balance at ledger 103,053,705 — and
you want to verify that claim *without* syncing terabytes of full history and
*without* blindly trusting an RPC server.

Every XRPL ledger header already commits cryptographically to:
- all account state in that ledger (`account_hash`, the root of the state
  **SHAMap** — XRPL's 16-way Merkle radix hash tree),
- all transactions and their execution metadata (`transaction_hash`, the root of
  the transaction SHAMap), and
- the immediately preceding ledger (`parent_hash`).

When a ledger closes, validators sign `STValidation` messages attesting to its
hash. If at least 80% of the distinct validator master keys on a verifier's
selected Unique Node List (UNL) issued full validations for that hash, the
verifier has cryptographic proof that those validators attested to that ledger.

Walking `parent_hash` links back millions of ledgers would take gigabytes of
headers. XRPL already solves historical lookup inside its own state SHAMap
through `LedgerHashes` entries (skip lists):

- The **short skip list** (at a fixed state key) holds the hashes of up to 256
  immediately preceding ledgers.
- The **long skip list** is a collection of era entries — one `LedgerHashes`
  entry per 65,536-ledger era — each holding the hashes of every 256th "flag"
  ledger (`seq % 256 == 0`) in that era, together covering history back to
  genesis.

Because those skip lists live inside the ledger's state SHAMap, they are
authenticated by `account_hash`. Put a validator quorum on a recent **anchor**
ledger and attach a couple of abbreviated SHAMap inclusion proofs underneath,
and you can prove any historical transaction or account-state entry in a single
self-contained file (a few dozen kilobytes), verifiable offline with nothing
configured except the network's primary publisher public key.

Better still, because a newer ledger commits to all preceding history through
skip lists, fresh validator signatures can anchor arbitrarily old history — and
if you want to preserve a historical proof long after the original validations
disappear from peers, you can simply **renew the anchor** against a newer
ledger (spec §1.4).

---

## 2. Walking a Real Proof

The shared conformance corpus includes a real XRPL mainnet proof
(`tests/xprv/vectors/positive-tx-skiplist.json`). It proves a `Payment`
transaction (key `DF2B3DE0...`) inside ledger **103,053,705**, anchored to a
later ledger **103,062,326** using 32 validations against a 35-validator list
(sequence 84; quorum requires `ceil(0.8 × 35) = 28`):

```text
Configured trust root: XRPL primary publisher master public key (33 bytes)
  │
  ▼
Step 1 · anchor
  Publisher manifest + signed UNL (seq 84, 35 masters) + 32 validations (≥ 28 valid)
  ──▶ unlocks trusted anchor ledger hash: CC0A27B7…
  │
  ▼
Step 2 · ledger_header (ledger 103,062,326 — the anchor)
  LWR\0 preimage hashes to CC0A27B7… (trusted)
  ──▶ unlocks anchor account_hash, transaction_hash, parent_hash, and close_time
      (close_time checks that list seq 84 is in effect for this anchor)
  │
  ▼
Step 3 · map_proof (state tree of 103,062,326)
  SHAMap hashes to anchor's account_hash
  Proves long-skip-list LedgerHashes leaf A37663E4…
  ──▶ reveals candidate flag-ledger hashes for this era
  │
  ▼
Step 4 · ledger_header (flag ledger 103,053,824)
  LWR\0 preimage matches a candidate in A37663E4…, AND passes the hop checks
  (valid long-list key for 103,053,824, seq % 256 == 0, within containing ledger's range)
  ──▶ unlocks 103,053,824's account_hash
  │
  ▼
Step 5 · map_proof (state tree of 103,053,824)
  SHAMap hashes to 103,053,824's account_hash
  Proves short-skip-list LedgerHashes leaf B4979A36…
  ──▶ reveals candidate hashes for the 256 ledgers preceding 103,053,824
  │
  ▼
Step 6 · ledger_header (target ledger 103,053,705)
  LWR\0 preimage matches a candidate in B4979A36…, AND passes the hop checks
  (short-list key, within 256 ledgers of 103,053,824, respecting LastLedgerSequence)
  ──▶ unlocks 103,053,705's transaction_hash (and account_hash)
  │
  ▼
Step 7 · map_proof (tx tree of 103,053,705)
  SHAMap hashes to 103,053,705's transaction_hash
  Proves tx leaf DF2B3DE0…: Payment (2,000,000 drops) + meta (tecUNFUNDED_PAYMENT)
  ──▶ PROVEN: DF2B3DE0… was included in ledger 103,053,705, and it FAILED
```

Look closely at Step 7 — it illustrates the most important consumer lesson in
spec §5.5: **a verified proof proves ledger inclusion, not application
success.** Here the proof verifies cleanly, the consumer checks that the
authenticated leaf key matches the full expected transaction ID (`DF2B3DE0…`),
and inspecting the authenticated metadata shows
`TransactionResult: "tecUNFUNDED_PAYMENT"`. The payment was included in ledger
103,053,705, charged a 12-drop fee, and **failed**. Likewise, an authenticated
account-state leaf proves an account's balance *at that specific ledger* — a
fresh anchor can authenticate a historical observation without refreshing it.
To decide whether a balance is recent enough, check the authenticated
`close_time` of the supporting ledger containing that balance (not the anchor's
`close_time`) against an application freshness policy.

### Shorter routes (spec §6)

Seven steps is the route for a distant **non-flag** target (`distance > 256`
and `target % 256 != 0`). Closer or flag-aligned targets take fewer steps:
- **Same ledger as the anchor** (distance 0): 3 steps (`anchor` → anchor
  `ledger_header` → `map_proof`). No skip list needed.
- **Anchor's immediate parent** (distance 1): 4 steps (`anchor` → anchor
  `ledger_header` → parent `ledger_header` via `parent_hash` → `map_proof`).
  Still no skip list — even when the parent happens to be a flag ledger.
- **Any flag ledger at distance ≥ 2** (`target % 256 == 0`, regardless of age),
  or **any recent ledger** (distance 2 to 256): 5 steps (one long-list or
  short-list hop straight to the target header).
- **Account state instead of a transaction**: the final `map_proof` proves a
  leaf in the target ledger's `state` tree (`account_hash`) rather than its
  `tx` tree (`transaction_hash`).

### The capability model (and how streaming works)

Verifiers do not hardcode those 3/4/5/7-step templates. XPRV models
verification as a small **forward-only capability machine** (spec §1, §5):

- Step 1 (`anchor`) unlocks the anchor ledger hash.
- A `ledger_header` step computes `SHA512-Half(LWR\0 || 118 bytes)`. To be
  accepted, that hash must either already be directly trusted (the anchor hash
  or a previously authenticated `parent_hash`) **or** match a candidate hash in
  an earlier authenticated `LedgerHashes` skip-list leaf *and* pass all hop
  constraints (expected skip-list key, containing ledger's index range,
  `seq % 256 == 0` for long hops, and `LastLedgerSequence` bounds; spec §5.3).
  Only after that link check succeeds do the header's `parent_hash`,
  `account_hash`, and `transaction_hash` become trusted capabilities for later
  steps.
- A `map_proof` step recomputes its SHAMap root and checks that the root is
  already trusted for that tree (`state` or `tx`). Recognized `LedgerHashes`
  leaves in a `state` proof supply *candidate* ancestor hashes for subsequent
  headers to match and validate.

Because capabilities flow forward, a binary reader can process records
progressively in one pass — with one important caveat on **finality** (spec
§3.3): when a proof has descendants, selecting the primary validator list needs
the matching anchor header's `close_time`, and the optional bundled list record
(`0x05`) may arrive last on the wire. So framing errors cause immediate refusal,
while *acceptance* stays provisional until framing finishes and the trust view
is resolved.

### The golden rule: never trust a wrapper

One principle explains half of the spec on its own:

> **A field is trusted only because it sits inside bytes whose hash or
> signature you already verified — never because an enclosing wrapper claims
> it.**

Look at what follows immediately from that single rule:
- **No `ledger_index` on the anchor** (spec §2.1, §3.4). Earlier drafts carried
  an advisory `ledger_index` in the anchor even though nothing signed or hashed
  it — you could change it to `42` and the proof still verified. Version 10
  removed it from both forms. Ledger numbers come only from authenticated
  `ledger_header` preimages.
- **Closed JSON objects** (spec §2). Every proof object, step, `header`, and
  `unl` carries only the exact members the spec lists. A member nothing hashes
  is an assertion nothing checks; a closed form leaves nowhere to smuggle
  unauthenticated metadata.
- **No `hash`, `index`, `blob`, or `delivered_amount` inside JSON leaves**
  (spec §2.3). RPC decoders often attach convenience fields next to decoded
  objects. They are not part of the hashed XRPL serialization, so verifiers
  reject them. The authenticated leaf key is the trie's `key` element, never a
  field inside the leaf body.
- **`__depth__` is the one diagnostic exception** (spec §2.3). JSON trie inner
  nodes allow an optional integer `__depth__` for human readability, and
  verifiers still reject any mismatch against actual nesting depth.

---

## 3. How Trust Works (And Why Offline Proofs Are Tricky)

Everything below the anchor is SHA512-Half math. The subtle security design
lives in **Step 1**: turning one 33-byte publisher master key into confidence
that the selected validators attested to the anchor ledger hash.

### The signature chain

```text
Configured primary publisher master key (33 bytes)
  │
  ├─ signs publisher Manifest (MAN\0, MasterSignature + Signature)
  │    ──▶ delegates to publisher ephemeral signing key
  │         │
  │         └─ signs Validator List blob (JSON: sequence, expiration, validators[])
  │              ──▶ names N validator master keys (each with a validator Manifest)
  │                   │
  │                   └─ each validator Manifest (MAN\0) delegates master ──▶ signing key
  │                        │
  │                        └─ validator signs STValidation (VAL\0) for anchor ledger_hash
  │                             ──▶ count distinct masters with valid full validations
  │                                 require count ≥ ceil(0.8 × N)
```

Only **full** validations (`Flags & 0x00000001 != 0`) count; partial
validations never count toward finality. Each validator master counts at most
once: repeated embedded `SigningPubKey` entries in the anchor are a structural
parse error, while validations under an old and a new signing key for the same
master may coexist structurally, with only the master's resolved current
signing key eligible to vote.

### Trust roots vs. evidence channels

There is only **one trust root per network**: the configured primary publisher
public key (spec §1.3, §8). Everything else — the list bundled inside the
proof, extra manifests passed via `--manifests`, secondary publisher lists, or
peer gossip — is just **cryptographic evidence** checked against keys rooted
back to that publisher.

An untrusted channel (including the proof itself) can *withhold* newer evidence
such as a rotation or revocation, which is why the reference server (`xprv
serve`) passes its held validator manifests (including revocations and
equivocation evidence) into verification. What an untrusted channel can never
do is make an unverified manifest or forged list count.

### Why the trust-view rules are strict

Each rule in spec §1.3 closes a concrete failure mode:

- **Revocations never shrink `N`.** A manifest with `Sequence == 0xFFFFFFFF`
  permanently revokes a master key. A revoked validator stays in the
  denominator `N` and contributes zero votes, so revocations can never lower
  the quorum bar. The same law applies to the publisher itself: a verified
  publisher revocation permanently invalidates that publisher.
- **Two masters sharing a signing key rejects the anchor.** If a list maps two
  distinct master keys onto the same ephemeral signing key, guessing which one
  is honest is unsafe — the list signals a broken or compromised publisher, so
  XPRV fails closed and rejects the anchor outright.
- **Manifest vs. list equivocation.** Two different manifests signed by the
  same master key at *any* same sequence prove the master key itself
  equivocated: that validator stays in `N` and can never vote (or, for the
  publisher's master key, the publisher is invalidated). For validator *lists*
  (signed by the publisher's ephemeral signing key), equivocation applies only
  among **eligible** list candidates at the highest eligible sequence —
  version 10 clarifies that an expired or otherwise ineligible list does not
  poison a newer eligible one.
- **No wall clock — freshness uses the anchor's `close_time`.** A proof
  verified today and verified years later against the same trust material must
  give the same verdict. A list's `[effective, expiration)` interval is checked
  against the anchor header's `close_time` (when that header is present;
  anchor-only proofs remain valid standalone building blocks without one).
- **Negative UNL is intentionally ignored.** Live `rippled` nodes can lower
  their quorum bar via the Negative UNL when validators go offline. Offline
  verifiers do not reconstruct Negative UNL state and always require
  `ceil(0.8 × N)` against unreduced `N`. That is simpler and stricter: during
  an outage, a ledger `rippled` validated may wait for enough online validators
  to reach an XPRV quorum.
- **One primary publisher key; secondary keys refresh manifests only.**
  Merging UNLs across publishers is an application policy problem outside a
  wire spec. One primary publisher key defines membership `N`; secondary keys
  can only contribute fresher manifests for masters already on the primary list.
- **Different kinds of failures have different scopes** (spec §1.3):
  - A malformed proof envelope or non-canonical `STValidation` is an immediate
    **parse refusal**.
  - A structurally complete `STValidation` whose signature fails to verify
    simply does not count as a vote (a **trust refusal** only if remaining
    votes drop below quorum).
  - A well-framed bundled `unl.blob`/`0x05` whose signature or list-blob checks
    fail is **disqualified as a list candidate** rather than poisoning an
    externally held primary list; if no other valid candidate is available, the
    proof fails at the **trust** layer for unavailable membership.
- **Why bundling the list (`0x05` / `unl.manifest`+`blob`+`signature`) is
  optional on the wire.** A stateful verifier checking many proofs anchored to
  the same ledger can supply a verified primary list it already holds instead
  of repeating a ~14.6 KB list in every proof. Neither reference verifier holds
  external primary lists yet (spec Appendix A), so today both reject list-less
  proofs cleanly at the trust layer.

---

## 4. Two Forms, One Meaning (JSON vs. Binary)

XPRV defines two encodings sharing one format version (`10`) that must verify
identically:

| | Binary (`.xprv.bin`) | JSON (`.xprv.json`) |
|---|---|---|
| **Built for** | Wire transport, storage, streaming, forward compatibility | Inspection, debugging, human-readable tooling |
| **Container** | 10-byte header (`XPRV`, version, flags, `network_id`) + TLV records; optional RFC 1950 zlib compression | Closed UTF-8 JSON object (`format_version`, `network_id`, `steps`) |
| **SHAMap leaves** | Exact raw XRPL serialized bytes | Decoded XRPL JSON (`sle`, or `{"tx": ..., "meta": ...}`) |
| **Needs field definitions for leaves?** | **No.** Binary leaves hash their exact bytes directly. | **Yes.** Must re-serialize and round-trip against the network codec. |

### Why binary is forward-compatible and JSON is decoded-only

When XRPL or Xahau activates a new amendment with new fields, a verifier with
older field definitions cannot decode those new leaves to JSON.

In **binary form**, that does not break verification: the verifier hashes the
leaf's raw bytes directly (`MLN\0 || sle_bytes || key` or `SND\0 || VL(tx) ||
VL(meta) || key`). Field definitions are only needed for the anchor's
`STValidation` entries and extracting `LedgerHashes` skip lists.

In **JSON form**, XPRV deliberately forbids an opaque hex fallback or an
attached `"blob"` field. If a JSON leaf carried both decoded JSON and raw
bytes, only the raw bytes would be hashed, leaving the displayed JSON free to
lie. So JSON leaves are **decoded-only**: if a leaf cannot round-trip cleanly
under the verifier's codec and definitions snapshot, that proof has no JSON
form, and the producer directs the consumer to binary.

### The version 10 wire simplification: exact bytes over clever packing

Compared to versions 6–9, the biggest change in version 10 is what was removed:

1. **No shaped UNL encoding in `0x05`.** Earlier versions decomposed the
   publisher's signed JSON list into a packed binary layout plus a "shape byte"
   and field permutation so decoders could reconstruct the exact signed JSON
   bytes. Whenever a publisher added a member, changed whitespace, or moved
   `effective`, reconstitution broke — creating "JSON-only" proofs that had no
   binary form at all. Version 10 carries
   `[manifest][signature][exact signed list bytes]` directly, eliminating
   JSON-only proofs completely.
2. **No common/unique validation splitting in `0x01`.** Earlier versions
   factored shared `STValidation` fields into a common table plus per-validator
   deltas and a field-name list. Version 10 replaces all of that with raw
   length-prefixed `STValidation` bytes sorted in strictly ascending order of
   their embedded 33-byte `SigningPubKey`.

What does that simplification cost on the wire? On the shared positive vector
(35 list members, 14,628-byte signed list, 32 validations), uncompressed size
moves from 34,888 bytes (v9) to 42,785 bytes (v10); with zlib level 9 on the
TLV body, the compressed body moves from **31,135 bytes to 35,010 bytes**
(+3,875 bytes, about 12.4% on this sample — including both exact-byte records
and removal of the advisory anchor `ledger_index`). Trading ~12% compressed
size to delete two fragile decompositions and guarantee every valid proof has a
binary form is the core v10 bargain.

### Artifact identity (CAS) vs. what was proven

Version 10 keeps two distinct identities separate (spec §1.2, §1.4):
- **Artifact (CAS) identity:** `SHA512-Half` of the complete uncompressed
  binary form (with `flags = 0`). Useful for deduplicating and storing a
  specific proof artifact.
- **Claim identity:** What was actually proven is
  `(network_id, authenticated ledger_hash, tree, leaf_key)` (plus the leaf
  bytes). Two valid proofs of the same transaction produced ten minutes apart
  can use different anchor ledgers or validation sets and therefore carry
  different CAS hashes, yet prove the exact same claim.

**Caching pitfall:** never treat a CAS hash as a shortcut around validating the
exact bytes you received. A malformed JSON proof (for example, carrying an
extra unhashed member or a wrong `__depth__`) could convert to the same binary
bytes as a cached valid proof. A sound verdict cache key therefore includes:
1. the CAS identity **after** full parse and framing validation of the received
   representation;
2. the verifier's policy and protocol-definitions fingerprints
   (`definitions.{leaf,validation}`);
3. the **whole** resolved trust view — configured publisher key, publisher
   manifest state, selected primary list blob (or conflict/unavailable state),
   and every primary-list master's state (active winning manifest, revocation,
   or lower-sequence equivocation — winning manifests alone miss a lower-sequence
   equivocation!); and
4. when the proof has descendants, the matching anchor ledger header's
   `close_time` checked against the primary list window.

If any of those inputs is unavailable (for example, an incomplete failure state),
skip the cache rather than caching under a partial key.

---

## 5. Why the Spec Is Strict (Lessons from Testing and Review)

Many rules in `spec-draft.md` look strict on first read — uppercase-only hex,
minimal trie shape, strict RFC 4648 base64, codec round-trip equality. Almost
all of them trace back to concrete bugs and reproduced failure cases.

- **Inconsistent trie branch keys → uppercase-only hex and single-pass parse.**
  In an earlier verifier, case-insensitive handling of JSON trie branch keys
  allowed a reproduced test case (`negative-json-duplicate-nibble-forge.json`)
  where a legitimate branch `"A"` hashed into the real trie root while a
  lowercase sibling `"a"` hung a forged `LedgerHashes` leaf off the same inner
  node and was enumerated as authenticated. Single-pass branch parsing closes
  the smuggling vector; version 10 goes further and requires **uppercase hex
  only** across all proof-level hex members and trie branch keys in JSON with no
  case normalization (while decoded leaf content follows `rippled` / `xahaud`
  reference JSON, where ordinary `UInt64` fields are unpadded lowercase hex).
- **Codec quirks → the leaf round-trip rule.** Earlier drafts guarded JSON
  leaves with a hand-maintained table of permitted JSON types per XRPL field
  type. Reviews quickly turned up holes: `xrpl-py` silently drops `null`
  members (so `"InvoiceID": null` hashed as if absent) and coerces booleans to
  `0`/`1` (so `"Flags": false` verified); other codecs expand X-addresses or
  collapse unknown currency codes to native XRP. Differential fuzzing also
  showed that even a narrow "equivalent decimal notation" tolerance causes real
  verifier disagreements in both directions (`catl::xdata` caps decimal input at
  20 digits like `rippled`, while `xrpl-py` rejects written exponents above 80).
  Version 10 replaces the type table with one rule (spec §2.3): serialize the
  leaf content with the network codec, decode those exact bytes into exact
  `rippled` (XRPL) or `xahaud` (Xahau) reference JSON, and require strict
  structural, type, and exact string equality — one canonical text per amount,
  `Number`, `UInt64`, and `PathSet` step, with no alternate-notation tolerance.
  Similarly, live-ledger sampling caught `catl::xdata` treating issued `XAH` on
  XRPL mainnet as native XRP and writing native `Issue` as a bare string instead
  of `{"currency": "XRP"}` — establishing `rippled`/`xahaud` as the reference
  and scoping native currency strictly to each network's own system code.
- **Minimal trie shape.** Empty inner nodes `{}`, expanded subtrees with no
  descendant leaf, all-zero `00...00` placeholders standing in for empty
  branches, and leaves whose 32-byte `key` disagrees with their nibble path
  either bloat proofs or create verifier divergence. All four are refused at
  parse in both JSON and binary (spec §2.3, §3.7).
- **Strict signatures, base64, and signed-list JSON.** Two verifiers claiming
  the same version cannot agree on the same bytes (under the same trust view,
  policy, and definitions snapshot) if one parser silently forgives malformed
  inputs. Version 10 therefore pins strict DER low-S secp256k1 and the
  `libsodium` Ed25519 verification profile (§5.1), rejects lenient base64
  decoders that skip garbage characters or ignore non-zero pad bits, rejects
  duplicate JSON object keys in signed lists (§1.3), and bumps the format
  version number whenever validity changes (§9.1).

---

## 6. Building Your Own Verifier: A Practical Roadmap

If you want to build an independent verifier (in Rust, Go, TypeScript, etc.),
the integrated v10 repository gives you a shared test corpus
(`tests/xprv/vectors/`) and a black-box conformance runner
(`scripts/xprv-conformance.py`):

1. **Start with binary form (`.xprv.bin`).**
   You do not need a JSON leaf codec for arbitrary transactions or ledger
   entries to verify binary proofs. Implement:
   - the 10-byte header, bounded RFC 1950 zlib inflation, minimal LEB128
     varints, and TLV framing (spec §3, §4);
   - `Manifest` parsing, canonical `STValidation` round-trip validation against
     your protocol-definitions snapshot (spec §2.1, §3.4), and `LedgerHashes`
     extraction for skip-list leaves (spec §5.3).
2. **Verify your hash primitives against the positive vector's intermediates.**
   Implement `SHA512-Half`, ledger header hashing (`LWR\0`), SHAMap inner nodes
   (`MIN\0`), state/tx leaves (`MLN\0` / `SND\0`), and skip-list keys (spec
   §5.1–§5.4). `tests/xprv/vectors/positive-tx-skiplist.json` records every
   intermediate (`LWR\0` preimages, leaf hashed bytes, leaf hashes, trie roots,
   and skip-list keys) so you can localize any mismatch down to one step.
3. **Wire up canonical signature checks** (spec §5.1):
   strict DER low-S secp256k1 and the `libsodium` Ed25519 profile.
4. **Implement the §1.3 trust view and §5 capability machine**, then test your
   binary path against the binary form of `positive-tx-skiplist.json` and the
   binary-mutation negative vectors.
5. **Add JSON form (`.xprv.json`)** if you need human-readable proofs:
   enforce the closed schema, proof-level uppercase-only hex, strict integer
   tokens, single-pass trie parsing, and the §2.3 exact codec round-trip check.
6. **Run the full black-box conformance suite** once both forms are working.
   `scripts/xprv-conformance.py` feeds each vector's JSON and binary proofs (and
   any held manifests file) to a command template and checks exit `0` (accept)
   vs. `1` (refuse), treating any crash, signal, or timeout as a failure. If
   your verifier also emits the spec §7.1 `--json` report, pass `--reports mine`
   to check `verified`, refusal `layer`, and `reason` strictly from the report.
   By default it also runs the built-in `cpp` and `python` reference verifiers
   alongside yours (which requires a built `xprv` binary, or `--use python` to
   skip the C++ build):
   ```sh
   scripts/xprv-conformance.py run \
     --verifier mine='my-verifier {proof} --network {network_id} --key {publisher_key} --manifests {manifests}'
   ```

---

## 7. Where to Go Next

- **Normative specification:** [`spec-draft.md`](spec-draft.md)
- **Reference server trust-view lifecycle:** [`../README.md`](../README.md)
- **Shared test vectors & conformance runner (on the integrated v10 branch):**
  `tests/xprv/vectors/`, `scripts/xprv-vectors.py`, and
  `scripts/xprv-conformance.py`
- **Reference implementations:**
  - C++ prover, verifier, converter, and server: `src/xprv/`
  - Standalone Python verifier: `scripts/verify-xprv.py`
