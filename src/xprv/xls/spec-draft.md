# XPRV: XRPL Proof Verification Format

| | |
|---|---|
| **Status** | Draft |
| **Date** | 2026-03-29 |
| **Authors** | Nicholas Dudfield ([@sublimator](https://github.com/sublimator)) |
| **Discussion** | [XRPLF/XRPL-Standards#107](https://github.com/XRPLF/XRPL-Standards/discussions/107) |
| **Reference** | [XLS-41d XPOP](https://github.com/XRPLF/XRPL-Standards/discussions/107) (Richard Holland) |

## Acknowledgements

This work builds on the XPOP (XLS-41d) standard by Richard Holland ([@RichardAH](https://github.com/RichardAH)), which defined the original proof-of-payment format for XRPL transactions.

The key insight enabling historical proofs — using LedgerHashes (skip lists) as the cryptographic bridge between a current validated ledger and any historical ledger — emerged from a discussion with [@wojake](https://github.com/wojake) about the ephemeral nature of validator signatures. After exploring and rejecting approaches that would require validators to re-sign historical ledgers (contentious, poor incentives), [@mDuo13](https://github.com/mDuo13) suggested the use of LedgerHashes, which are already maintained by the protocol and provide a compact, permanent navigational structure through the ledger history.

See [XRPLF/XRPL-Standards#107 comment](https://github.com/XRPLF/XRPL-Standards/discussions/107#discussioncomment-7344712) for the original discussion.

## Abstract

XPRV is a compact, offline-verifiable proof that data exists (or existed) in an XRPL or Xahau ledger. It extends the XPOP concept (XLS-41d) with composable proof steps that can reach any historical ledger via skip list navigation, and adds a compact binary representation designed for progressive verification.

Two representations of the same proof, no mixing:

- **JSON form** — human-readable, for debugging and review
- **Binary form** — compact TLV-encoded, for on-ledger storage, wire protocol, QR codes

Both forms verify identically and encode the same proof data. JSON is the
human-readable form; binary is the compact transport and storage form.

## 1. Proof Graph Model

XPRV is logically a directed acyclic graph (DAG) of authenticated objects.
JSON and binary forms serialize that DAG as a parent-first ordered list of
steps.

Three step types:

| Type | Consumes | Produces |
|------|----------|----------|
| `anchor` | nothing | trusted ledger hash |
| `ledger_header` | trusted ledger hash (from an anchor, a parent header, or a skip list leaf) | trusted header plus its `transaction_hash` and `account_hash` roots |
| `map_proof` | trusted tree root | one or more trusted leaves from that tree |

Validity rules:

- A proof MUST contain exactly one `anchor`, and it MUST be the first
  serialized step.
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

> **Reference status:** The current C++ and Python reference verifiers implement
> capability-based verification of this parent-before-child DAG model. They
> keep authenticated ledger hashes and tree roots alive across sibling fan-out,
> and may emit warnings for non-canonical duplication or ordering while still
> accepting a valid proof.

## 2. JSON Form

The proof chain is a JSON object with top-level `format_version`,
`network_id`, and an array of step objects. The `steps` array is the canonical
parent-first linearization of the logical proof DAG.

```json
{
    "format_version": 3,
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
| `format_version` | uint32 | JSON proof format version |
| `network_id` | uint32 | `0` = XRPL mainnet, `21337` = Xahau mainnet |
| `steps` | array | Ordered sequence of step objects |

All hashes and binary data are hex-encoded strings. Leaf data is JSON objects (decoded XRPL objects). Ledger header fields are named and typed.

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
| `ledger_index` | uint32 | Sequence number of the anchor ledger |
| `unl.public_key` | hex string (33 bytes) | VL publisher's Ed25519 master public key |
| `unl.manifest` | hex string | Publisher manifest (binary STObject) binding master → signing key |
| `unl.blob` | hex string | Raw UNL blob (JSON validator list) |
| `unl.signature` | hex string | Signature over blob by manifest's signing key |
| `validations` | object | Map of signing_key_hex → raw STValidation bytes |

> **TBD: verifier modes and signature set policy.**
>
> The current format embeds the VL snapshot used when the proof was built, so
> an offline verifier can validate the anchor strictly against that embedded
> trust snapshot. A future online verifier mode may additionally refresh the
> latest VL and re-check the included anchor validations against the updated
> manifest/signing-key view.
>
> To support that future mode, producers SHOULD include as many distinct anchor
> validations for the selected anchor ledger as are available, not merely the
> minimum threshold needed to establish quorum at proof-build time.
>
> A proof built against a now-stale embedded VL snapshot is still valid if that
> embedded snapshot yields proof quorum for the included anchor validations. A
> refresh-and-retry path is only required when live peer manifests show quorum
> for a ledger but the embedded VL snapshot does not.

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
| `close_time` | uint32 | 4 bytes |
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

Leaf `data` is a JSON object — the decoded XRPL ledger entry, transaction, or
transaction+metadata. The verifier re-serializes to canonical binary for
hashing.

At each inner node, ALL non-empty branches MUST be present so the verifier can
recompute the parent hash. Branches on the proof path are objects (recurse);
branches off the path are hash strings (pruned).

A `map_proof` MAY contain one or more leaves from the same authenticated root.
Canonical form uses a single `map_proof` step per authenticated tree root and
includes all required leaves in that trie rather than emitting repeated sibling
proofs against the same root.

Leaves are canonical only when they participate in the proof graph: either they
provide data consumed by a descendant step, or they are retained as terminal
authenticated outputs of the proof. Extra authenticated leaves beyond that are
non-canonical.

## 3. Binary Form

### 3.1 File Header

Every binary proof begins with a **10-byte file header**:

```
Offset  Size  Field
  0       4   magic       "XPRV" (0x58 0x50 0x52 0x56)
  4       1   version     0x04
  5       1   flags       bit 0: zlib compressed body
  6       4   network_id  little-endian uint32 (0 = XRPL, 21337 = Xahau)
```

When flag bit 0 is set, the entire body (all TLV records concatenated) is zlib-compressed as a single stream. The consumer decompresses before parsing TLVs.

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

Length is unsigned LEB128 varint:

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

The anchor is split into two TLV records (`0x01` core + `0x05` UNL) so the ~4KB of validation signatures arrive before the ~15KB VL blob.

> **Note:** Progressive verification with a cached UNL is not yet implemented in the reference verifiers — both currently require the full anchor (core + UNL) before verification can begin. The TLV split is designed to enable future progressive verification for animated QR codes, BLE/NFC scanning, and streaming use cases.

Canonical serialized order:

1. All non-UNL steps MUST be serialized parent-before-child.
2. The canonical traversal is depth-first pre-order over the logical proof DAG.
3. Children of a `ledger_header` are canonically ordered as: state-tree subtree
   first, then tx-tree subtree.
4. A given authenticated tree root MUST appear at most once in canonical form.
   If multiple leaves are required from that root, they MUST be merged into one
   multi-leaf `map_proof`.
5. If one authenticated skip list leaf supports multiple child
   `ledger_header` steps, those child headers MUST be ordered by descending
   `ledger_index`.
6. Within a `map_proof`, trie branch order (`0`-`F` at every inner node)
   defines the canonical leaf ordering.
7. `0x05` anchor UNL remains last.

Other parent-before-child orders may still be verifiable, but they are
non-canonical and SHOULD NOT be emitted when a stable proof hash, signature, or
cache key is required.

### 3.4 Anchor Core Payload (0x01)

```
[ledger_hash: 32 bytes]
[ledger_index: 4 bytes, big-endian uint32]
[publisher_key: 33 bytes, Ed25519 compressed]
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

The **shape byte** preserves JSON key ordering of the original VL blob for exact byte reconstitution (required for signature verification):

| Bits | Meaning |
|------|---------|
| 0-2 | Top-level key permutation index (3! = 6 permutations of `{sequence, expiration, validators}`) |
| 3 | Validator key permutation (2! = 2 permutations of `{validation_public_key, manifest}`) |

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

The binary trie uses a **2-bit-per-branch header** (uint32, little-endian) for each inner node. The 16 branches (0-F) each occupy 2 bits:

| Bits | Node type |
|------|-----------|
| `00` | Empty branch |
| `01` | Leaf |
| `10` | Inner (recurse) |
| `11` | Hash placeholder (pruned subtree) |

Serialization is depth-first, pre-order:

```
[branch_header: 4 bytes, uint32 little-endian]
  for each non-empty branch (in order 0-F):
    if 01 (leaf):  [key: 32 bytes][data_len: LEB128][data: raw canonical binary]
    if 10 (inner): [branch_header: 4 bytes] ... (recurse)
    if 11 (hash):  [hash: 32 bytes]
```

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

## 5. Verification Algorithm

The step-local cryptographic checks are identical for both forms. Verification
maintains capability sets of authenticated ledger hashes, tx roots, and state
roots. Parent-before-child serialized order ensures each step can only consume
capabilities produced by earlier authenticated steps.

```
function verify(chain, trusted_publisher_keys):
    trusted_ledger_hashes = set()
    trusted_tx_roots = set()
    trusted_ac_roots = set()

    for each step in chain.steps:

        if step is anchor:
            // 1. Assert unl.public_key is in trusted_publisher_keys
            // 2. Verify publisher manifest signatures:
            //      MAN\0 + manifest fields without Signature/MasterSignature
            // 3. Use the manifest's SigningPubKey to verify unl.signature
            //    over unl.blob
            // 4. Decode unl.blob → validator entries
            // 5. Verify each validator manifest the same way and build the
            //    signing_key → master_key map
            // 6. For each validation:
            //      deserialize STValidation
            //      verify LedgerHash == anchor.ledger_hash
            //      verify VAL\0 signing data signature using SigningPubKey
            //      verify SigningPubKey maps to a verified UNL validator
            // 7. Quorum: >= 80% of UNL validators must validate
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

            assert computed_hash in trusted_ledger_hashes

            trusted_ledger_hashes.add(step.header.parent_hash)
            trusted_tx_roots.add(step.header.transaction_hash)
            trusted_ac_roots.add(step.header.account_hash)

        elif step is map_proof:
            if step.tree == tx:
                expected_roots = trusted_tx_roots
            else:
                expected_roots = trusted_ac_roots

            root_hash = compute_trie_root(step.trie)
            assert root_hash in expected_roots

            if step.tree == state:
                for each ledger_hash in all Hashes leaves authenticated by this
                proof:
                    trusted_ledger_hashes.add(ledger_hash)

    return true
```

### 5.1 Hash Prefixes

| Prefix | Hex | ASCII | Used for |
|--------|-----|-------|----------|
| `ledgerMaster` | `0x4C575200` | `LWR\0` | Ledger header hash |
| `innerNode` | `0x4D494E00` | `MIN\0` | SHAMap inner node hash |
| `txNode` | `0x534E4400` | `SND\0` | SHAMap transaction leaf hash |
| `leafNode` | `0x4D4C4E00` | `MLN\0` | SHAMap account state leaf hash |

### 5.2 SHA512-Half

`SHA512-Half(x)` = first 256 bits (32 bytes) of `SHA-512(x)`.

## 6. Composition Patterns

### 6.1 Recent Transaction (short skip list, ≤ 256 ledgers)

```
anchor → ledger_header →
map_proof(state, skip_list_key) →
ledger_header →
map_proof(tx, transaction_hash)
```

### 6.2 Historical Transaction (2-hop skip list, > 256 ledgers)

```
anchor → ledger_header →
map_proof(state, long_skip_list_key) →
ledger_header(flag) →
map_proof(state, short_skip_list_key) →
ledger_header(target) →
map_proof(tx, transaction_hash)
```

The **flag ledger** is the nearest multiple of 256 at or above the target ledger. The anchor's state tree long skip list contains hashes of flag ledgers. The flag ledger's short skip list contains hashes of the 256 ledgers before it.

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

> **Reference status:** Implemented in the current C++ and Python reference
> verifiers for parent-before-child proofs. They accept sibling fan-out and
> multi-leaf proofs, and warn when repeated sibling roots or header orderings
> are non-canonical.

## 7. Security Considerations

- The verifier MUST be configured with one or more trusted VL publisher keys.
- Validation messages are ephemeral — they must be captured in real-time.
- The 80% quorum threshold matches the XRPL consensus requirement.
- All hashes use SHA512-Half as specified by the XRPL protocol.
- The verifier MUST reject chains where any step fails.
- Binary and JSON forms of the same proof MUST verify identically.
- Producers SHOULD emit canonical serialized order (section 3.3) whenever
  proofs are hashed, signed, cached, or deduplicated.
- **TBD:** reference verifiers should support both an offline mode that trusts
  only the embedded VL snapshot and an online mode that can refresh the latest
  VL before assessing the included anchor validations.

## 8. MIME Types and File Extensions

| Form | Extension | MIME type |
|------|-----------|-----------|
| Binary | `.xprv.bin` | `application/octet-stream` |
| JSON | `.xprv.json` | `application/json` |

## 9. Reference Implementations

- **C++ prover + verifier**: [catalogue-tools/src/xprv](https://github.com/sublimator/catalogue-tools/tree/main/src/xprv)
- **Python verifier**: [verify-xprv.py](https://github.com/sublimator/catalogue-tools/blob/main/scripts/verify-xprv.py)
- **Live demo**: [xprv.it](https://xprv.it)
