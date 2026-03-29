# XPOP v2 Proof Chain Specification

**Status:** Draft
**Date:** 2026-03-19

## Abstract

An XPOP v2 proof chain is a self-contained, offline-verifiable proof that some data exists (or existed) in an XRPL/Xahau ledger. It decomposes the original XPOP (XLS-41d) into composable steps that can prove data in any ledger — current or historical.

Two representations of the same proof, no mixing:

- **JSON form** — human-readable, for debugging and review
- **Binary form** — compact TLV-encoded, for on-ledger storage, wire protocol, QR codes

Both forms are losslessly interconvertible and verify identically.

## 1. Proof Chain Model

A proof chain is an ordered sequence of steps. Three step types:

| Type | Consumes | Produces |
|------|----------|----------|
| `anchor` | nothing | trusted ledger hash |
| `ledger_header` | trusted hash (or previous header) | trusted `tx_hash` + `account_hash` |
| `map_proof` | trusted tree root | trusted leaf data (may contain hashes for further steps) |

A chain MUST start with an `anchor`. Subsequent steps MUST be `ledger_header` or `map_proof` in any valid order per the verification rules in section 4.

## 2. JSON Form

The proof chain is a JSON array of step objects.

```json
[
    { "type": "anchor", ... },
    { "type": "ledger_header", ... },
    { "type": "map_proof", ... }
]
```

All hashes and binary data are hex-encoded strings. Leaf data is JSON objects (decoded XRPL objects). Ledger header fields are named and typed.

### 2.1 Anchor Step

```json
{
    "type": "anchor",
    "ledger_hash": "<64 hex>",
    "unl": {
        "public_key": "<66 hex, Ed25519 master public key of VL publisher>",
        "manifest": "<base64, signed manifest binding master key to signing key>",
        "blob": "<base64, signed list of validator manifests>",
        "signature": "<hex, signature over blob by manifest's signing key>",
        "version": 1
    },
    "validations": {
        "<base58 validator public key>": "<hex, serialized STValidation>",
        ...
    }
}
```

### 2.2 Ledger Header Step

```json
{
    "type": "ledger_header",
    "header": {
        "seq": 83258110,
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

| Field | Type | Size |
|-------|------|------|
| `seq` | uint32 | 4 bytes |
| `drops` | decimal string | uint64 |
| `parent_hash` | hex string | 32 bytes |
| `tx_hash` | hex string | 32 bytes |
| `account_hash` | hex string | 32 bytes |
| `parent_close_time` | uint32 | 4 bytes |
| `close_time` | uint32 | 4 bytes |
| `close_time_resolution` | uint8 | 1 byte |
| `close_flags` | uint8 | 1 byte |

### 2.3 Map Proof Step

```json
{
    "type": "map_proof",
    "tree": "tx" | "state",
    "key": "<64 hex>",
    "trie": { ... }
}
```

The trie is a nested JSON structure. Three JSON types discriminate three node types:

| JSON type | Node type |
|-----------|-----------|
| **String** (64 hex chars) | Pruned subtree hash |
| **Object** (keys `"0"`-`"F"`) | Inner node |
| **Array** `[key, data]` | Leaf node |

Leaf `data` is a JSON object — the decoded XRPL ledger entry, transaction, etc. The verifier re-serializes to canonical binary for hashing.

Example trie proving key `A3F1...`:

```json
{
    "0": "1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF",
    "A": {
        "3": {
            "F": {
                "1": [
                    "A3F1E2D3C4B5A69788796A5B4C3D2E1FA3F1E2D3C4B5A69788796A5B4C3D2E1F",
                    {"Account": "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh", "Balance": "1000000", "LedgerEntryType": "AccountRoot"}
                ]
            },
            "0": "AABBCCDD11223344AABBCCDD11223344AABBCCDD11223344AABBCCDD11223344"
        },
        "B": "1122334455667788112233445566778811223344556677881122334455667788"
    }
}
```

At each inner node, ALL non-empty branches MUST be present so the verifier can recompute the parent hash. Branches on the proof path are objects (recurse); branches off the path are hash strings (pruned).

## 3. Binary Form

The proof chain is a sequence of concatenated TLV (Type-Length-Value) records. No JSON, no hex encoding — raw bytes throughout.

### 3.1 TLV Framing

Each step is encoded as:

```
[type: 1 byte][length: varint][payload: <length> bytes]
```

Type bytes:

| Byte | Step type |
|------|-----------|
| `0x01` | anchor |
| `0x02` | ledger_header |
| `0x03` | map_proof (tx tree) |
| `0x04` | map_proof (state tree) |

Length is unsigned LEB128:

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

| Payload size | Length bytes |
|-------------|-------------|
| < 128 | 1 |
| < 16,384 | 2 |
| < 2,097,152 | 3 |

The proof chain is the concatenation of all TLV records. No outer framing, no terminator — the consumer reads until EOF.

### 3.2 Anchor Payload

```
[ledger_hash: 32 bytes]
[unl_blob_len: varint][unl_blob: <len> bytes, raw from VL site]
[unl_manifest_len: varint][unl_manifest: <len> bytes, raw]
[unl_signature_len: varint][unl_signature: <len> bytes, raw]
[unl_public_key: 33 bytes, Ed25519 compressed]
[validation_count: varint]
  for each validation:
    [validator_pubkey: 33 bytes]
    [validation_len: varint][validation: <len> bytes, serialized STValidation]
```

### 3.3 Ledger Header Payload

The canonical binary serialization of the ledger header, fixed layout:

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

Total: 118 bytes (fixed). The TLV length field will always be 118.

### 3.4 Map Proof Payload

```
[key: 32 bytes]
[binary_trie: remaining bytes]
```

The tree type is encoded in the TLV type byte (`0x03` = tx, `0x04` = state), so it's not repeated in the payload.

The binary trie uses a 2-bit-per-branch header (uint32) for each inner node:

| Bits | Node type |
|------|-----------|
| `00` | Empty branch |
| `01` | Leaf (key + data follows) |
| `10` | Inner (recurse, another branch header follows) |
| `11` | Hash (32 bytes, pruned subtree) |

Serialization is depth-first, pre-order:

```
[branch_header: 4 bytes, uint32 little-endian]
  for each non-empty branch (in order 0-F):
    if 01 (leaf):  [key: 32 bytes][data_len: varint][data: <len> bytes]
    if 10 (inner): [branch_header: 4 bytes] ... (recurse)
    if 11 (hash):  [hash: 32 bytes]
```

## 4. Verification

The verification algorithm is identical for both forms. The verifier converts to internal representation and runs:

```
function verify(chain):
    trusted_ledger_hash = null
    trusted_tx_hash = null
    trusted_ac_hash = null
    prev_header = null
    prev_leaf_data = null

    for each step in chain:

        if step is anchor:
            // 1. Verify UNL manifest chain
            //    manifest is signed by unl.public_key (master)
            //    extract signing key from manifest
            //    verify unl.signature over unl.blob using signing key
            // 2. Decode unl.blob → list of validator entries
            //    each entry has a manifest mapping master→signing key
            // 3. For each validation:
            //    deserialize STValidation (canonical binary)
            //    verify LedgerHash == anchor.ledger_hash
            //    verify signature using SigningPubKey
            //    verify SigningPubKey maps to a UNL validator via manifests
            // 4. Quorum: >= 80% of UNL validators must validate
            trusted_ledger_hash = step.ledger_hash

        elif step is ledger_header:
            computed_hash = SHA512-Half(
                0x4C575200 ||                    // HashPrefix::ledgerMaster "LWR\0"
                uint32_be(seq) ||
                uint64_be(drops) ||
                parent_hash ||
                tx_hash ||
                account_hash ||
                uint32_be(close_time) ||
                uint32_be(parent_close_time) ||
                uint8(close_time_resolution) ||
                uint8(close_flags)
            )

            if trusted_ledger_hash != null:
                // Mode A: after anchor or map_proof
                assert computed_hash == trusted_ledger_hash
            elif prev_header != null:
                // Mode B: parent hash chain (consecutive headers)
                assert computed_hash == prev_header.parent_hash
            else:
                FAIL

            trusted_tx_hash = step.header.tx_hash
            trusted_ac_hash = step.header.account_hash
            trusted_ledger_hash = null
            prev_header = step.header
            prev_leaf_data = null

        elif step is map_proof:
            // Select expected root from most recent header
            if step.tree == tx:
                expected_root = trusted_tx_hash
            else:
                expected_root = trusted_ac_hash
            assert expected_root != null

            // Recompute trie root hash bottom-up
            //   Inner: SHA512-Half(0x4D494E00 || h0 || h1 || ... || h15)
            //   Leaf (tx, NM):  SHA512-Half(0x534E4400 || data || key)
            //   Leaf (tx, MD):  SHA512-Half(0x534E4400 || vl(tx) || vl(meta) || key)
            //   Leaf (state):   SHA512-Half(0x4D4C4E00 || data || key)
            root_hash = compute_trie_root(step.trie)
            assert root_hash == expected_root

            // Extract leaf data
            leaf_data = extract_leaf(step.trie, step.key)
            assert leaf_data != null
            prev_leaf_data = leaf_data
            prev_header = null

            // If next step is ledger_header, its computed hash
            // must appear in leaf_data (e.g. in sfHashes of
            // an ltLEDGER_HASHES entry). The verifier computes
            // the next header's hash and searches for it.
            // This is implicit — no index declared.

    return true
```

### 4.1 Hash Prefixes

| Prefix | Hex | ASCII | Used for |
|--------|-----|-------|----------|
| `ledgerMaster` | `0x4C575200` | `LWR\0` | Ledger header hash |
| `innerNode` | `0x4D494E00` | `MIN\0` | SHAMap inner node hash |
| `txNode` | `0x534E4400` | `SND\0` | SHAMap transaction leaf hash |
| `leafNode` | `0x4D4C4E00` | `MLN\0` | SHAMap account state leaf hash |

### 4.2 SHA512-Half

`SHA512-Half(x)` = first 256 bits (32 bytes) of `SHA-512(x)`.

## 5. Composition Patterns

### 5.1 Current Ledger Transaction

```
anchor → ledger_header → map_proof(tx)
```

### 5.2 Historical Transaction (2-hop skip list)

```
anchor → ledger_header →
map_proof(state, $LONG_SKIP_LIST_KEY) →
ledger_header →
map_proof(state, $SHORT_SKIP_LIST_KEY) →
ledger_header →
map_proof(tx, $TXID)
```

### 5.3 Recent Historical Transaction (parent hash chain)

```
anchor → ledger_header → ledger_header → ... → ledger_header → map_proof(tx)
```

### 5.4 Account State Proof

```
anchor → ledger_header → map_proof(state)
```

### 5.5 Multiple Proofs from Same Ledger

Multiple `map_proof` steps after the same `ledger_header` — each verified against the same header's roots, provided no intervening `ledger_header` resets them.

```
anchor → ledger_header → map_proof(tx, $TXID_1) → map_proof(tx, $TXID_2) → map_proof(state, $KEY)
```

## 6. Security Considerations

- The verifier MUST be configured with one or more trusted VL publisher keys. The anchor's UNL MUST chain to a trusted publisher.
- Validation messages are ephemeral — they must be captured in real-time or obtained from an archival source.
- The 80% quorum threshold matches the XRPL consensus requirement.
- All hashes use SHA512-Half as specified by the XRPL protocol.
- The verifier MUST reject chains where any step fails. Partial verification is not meaningful.
- Binary and JSON forms of the same proof MUST verify identically.
