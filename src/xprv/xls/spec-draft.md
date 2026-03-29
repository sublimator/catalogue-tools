# XPRV: XRPL Proof Verification Format

**Status:** Draft
**Date:** 2026-03-29
**Authors:** Nicholas Dudfield (@sublimator)
**Discussion:** [XRPLF/XRPL-Standards#107](https://github.com/XRPLF/XRPL-Standards/discussions/107)
**Reference:** [XLS-41d XPOP](https://github.com/XRPLF/XRPL-Standards/discussions/107) (Richard Holland)

## Abstract

XPRV is a compact, offline-verifiable proof that data exists (or existed) in an XRPL or Xahau ledger. It extends the XPOP concept (XLS-41d) with composable proof steps that can reach any historical ledger via skip list navigation, and adds a compact binary representation designed for progressive verification.

Two representations of the same proof, no mixing:

- **JSON form** — human-readable, for debugging and review
- **Binary form** — compact TLV-encoded, for on-ledger storage, wire protocol, QR codes

Both forms are losslessly interconvertible and verify identically.

## 1. Proof Chain Model

A proof chain is an ordered sequence of steps. Three step types:

| Type | Consumes | Produces |
|------|----------|----------|
| `anchor` | nothing | trusted ledger hash |
| `ledger_header` | trusted hash (or skip list entry) | trusted `tx_hash` + `account_hash` |
| `map_proof` | trusted tree root | trusted leaf data (may contain hashes for further steps) |

A chain MUST start with an `anchor`. Subsequent steps MUST be `ledger_header` or `map_proof` in any valid order per the verification rules in section 5.

## 2. JSON Form

The proof chain is a JSON object with a top-level `network_id` and an array of step objects.

```json
{
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

### 2.2 Ledger Header Step

```json
{
    "type": "ledger_header",
    "header": {
        "seq": 103188348,
        "drops": "99999999999000000",
        "parent_hash": "<64 hex>",
        "tx_hash": "<64 hex>",
        "account_hash": "<64 hex>",
        "parent_close_time": 751234560,
        "close_time": 751234567,
        "close_time_resolution": 10,
        "close_flags": 0
    }
}
```

All fields REQUIRED:

| Field | Type | Binary size |
|-------|------|-------------|
| `seq` | uint32 | 4 bytes |
| `drops` | decimal string | uint64, 8 bytes |
| `parent_hash` | hex string | 32 bytes |
| `tx_hash` | hex string | 32 bytes |
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

Leaf `data` is a JSON object — the decoded XRPL ledger entry, transaction, or transaction+metadata. The verifier re-serializes to canonical binary for hashing.

At each inner node, ALL non-empty branches MUST be present so the verifier can recompute the parent hash. Branches on the proof path are objects (recurse); branches off the path are hash strings (pruned).

## 3. Binary Form

### 3.1 File Header

Every binary proof begins with a **10-byte file header**:

```
Offset  Size  Field
  0       4   magic       "XPRV" (0x58 0x50 0x52 0x56)
  4       1   version     0x02
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

The anchor is split into two TLV records (`0x01` core + `0x05` UNL) so the ~4KB of validation signatures arrive before the ~15KB VL blob. A verifier with a cached UNL can confirm a payment before the UNL blob is fully received. This enables animated QR codes, BLE/NFC progressive scanning, and SSE streaming.

### 3.4 Anchor Core Payload (0x01)

```
[ledger_hash: 32 bytes]
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

**Validation decomposition**: STValidation objects across validators typically share many fields (LedgerSequence, SigningTime, Cookie, ServerVersion, etc.). The encoder:

1. Parses all STValidation objects into field maps
2. Identifies fields with identical values across all validators (**common fields**)
3. Encodes common fields once as a single serialized STObject
4. Identifies the remaining **unique field names** (typically just the signature)
5. Encodes unique fields per validator as individual STObjects

This reduces per-validator overhead from ~200 bytes to ~80 bytes.

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
[seq: 4 bytes, big-endian uint32]
[drops: 8 bytes, big-endian uint64]
[parent_hash: 32 bytes]
[tx_hash: 32 bytes]
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

The verification algorithm is identical for both forms.

```
function verify(chain, trusted_publisher_keys):
    trusted_ledger_hash = null
    trusted_tx_hash = null
    trusted_ac_hash = null
    prev_header = null

    for each step in chain.steps:

        if step is anchor:
            // 1. Verify UNL manifest chain
            //    manifest is signed by unl.public_key (master)
            //    extract signing key from manifest
            //    verify unl.signature over unl.blob using signing key
            //    assert unl.public_key is in trusted_publisher_keys
            // 2. Decode unl.blob → list of validator entries
            //    each entry has a manifest mapping master → signing key
            // 3. For each validation:
            //    deserialize STValidation
            //    verify LedgerHash == anchor.ledger_hash
            //    verify signature using SigningPubKey
            //    verify SigningPubKey maps to a UNL validator
            // 4. Quorum: >= 80% of UNL validators must validate
            trusted_ledger_hash = step.ledger_hash

        elif step is ledger_header:
            computed_hash = SHA512-Half(
                0x4C575200 ||                    // "LWR\0"
                uint32_be(seq) ||
                uint64_be(drops) ||
                parent_hash ||
                tx_hash ||
                account_hash ||
                uint32_be(parent_close_time) ||
                uint32_be(close_time) ||
                uint8(close_time_resolution) ||
                uint8(close_flags)
            )

            if trusted_ledger_hash != null:
                assert computed_hash == trusted_ledger_hash
            elif prev_header != null:
                assert computed_hash == prev_header.parent_hash
            else:
                FAIL

            trusted_tx_hash = step.header.tx_hash
            trusted_ac_hash = step.header.account_hash
            trusted_ledger_hash = null
            prev_header = step.header

        elif step is map_proof:
            if step.tree == tx:
                expected_root = trusted_tx_hash
            else:
                expected_root = trusted_ac_hash
            assert expected_root != null

            root_hash = compute_trie_root(step.trie)
            assert root_hash == expected_root

            prev_header = null

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
map_proof(tx, tx_hash)
```

### 6.2 Historical Transaction (2-hop skip list, > 256 ledgers)

```
anchor → ledger_header →
map_proof(state, long_skip_list_key) →
ledger_header(flag) →
map_proof(state, short_skip_list_key) →
ledger_header(target) →
map_proof(tx, tx_hash)
```

The **flag ledger** is the nearest multiple of 256 at or above the target ledger. The anchor's state tree long skip list contains hashes of flag ledgers. The flag ledger's short skip list contains hashes of the 256 ledgers before it.

### 6.3 Account State Proof

```
anchor → ledger_header → map_proof(state, account_key)
```

### 6.4 Multiple Proofs from Same Ledger

```
anchor → ledger_header → map_proof(tx, tx1) → map_proof(tx, tx2)
```

## 7. Security Considerations

- The verifier MUST be configured with one or more trusted VL publisher keys.
- Validation messages are ephemeral — they must be captured in real-time.
- The 80% quorum threshold matches the XRPL consensus requirement.
- All hashes use SHA512-Half as specified by the XRPL protocol.
- The verifier MUST reject chains where any step fails.
- Binary and JSON forms of the same proof MUST verify identically.

## 8. MIME Types and File Extensions

| Form | Extension | MIME type |
|------|-----------|-----------|
| Binary | `.xprv.bin` | `application/octet-stream` |
| JSON | `.xprv.json` | `application/json` |

## 9. Reference Implementations

- **C++ prover + verifier**: [catalogue-tools/src/xprv](https://github.com/sublimator/catalogue-tools/tree/main/src/xprv)
- **Python verifier**: [verify-xprv.py](https://github.com/sublimator/catalogue-tools/blob/main/scripts/verify-xprv.py)
- **Live demo**: [xprv.it](https://xprv.it)
