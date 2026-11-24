"""A synthetic XPRV trust world: test keys, manifests, signed lists, validations.

Trust-layer rules cannot be tested by editing a captured mainnet proof, because
every edit breaks a real signature. Here the tests hold the keys. Everything is
deterministic and offline: keys come from fixed seeds, so a case always has the
same bytes.

Two users. `scripts/xprv-vectors.py trust` writes `cases()` as shared vectors,
each carrying its whole proof and its own trust, so the C++ and the Python
verifier are asked the same questions. `scripts/test-verify-xprv.py` builds
one-off anchors with the same helpers for what a verdict cannot show.

Call `use(verifier)` first: the helpers need the Python verifier's primitives.
"""
from __future__ import annotations

import base64
import hashlib
import copy
import json
import struct
from binascii import hexlify, unhexlify
from typing import Any

from xrpl import CryptoAlgorithm
from xrpl.core.binarycodec import decode, encode, encode_for_signing
from xrpl.core.keypairs import derive_keypair, generate_seed, is_valid_message, sign

v: Any = None  # scripts/verify-xprv.py, set by use()

HEADER = {
    "ledger_index": 1000,
    "total_coins": "99999999999000000",
    "parent_hash": "11" * 32,
    "transaction_hash": "22" * 32,
    "account_hash": "33" * 32,
    "parent_close_time": 799999990,
    "close_time": 800000000,
    "close_time_resolution": 10,
    "close_flags": 0,
}
ANCHOR_HASH = ""  # hash of HEADER, set by use()

# What both verifiers say of a header no earlier step supports. They share no
# sentence: the Python one returns "not supported by any earlier authenticated
# ancestor or recognized LedgerHashes leaf"; the C++ one returns "not supported
# by an earlier authenticated step" to a caller and prints "unsupported by
# prior anchor/header/recognized LedgerHashes leaf".
NO_SUPPORT = "supported by"
# Likewise for a header outside the list's window ("...the list's validity
# interval", "...the signed list interval").
OUTSIDE_WINDOW = "anchor close time is outside the"


def use(verifier: Any) -> None:
    global v, ANCHOR_HASH
    v = verifier
    ANCHOR_HASH = v.hash_header(HEADER).hex().upper()


# ─── keys, manifests, validations ────────────────────────────────────


def keypair(n: int) -> tuple[str, str]:
    seed = generate_seed(entropy=f"{n:032x}", algorithm=CryptoAlgorithm.ED25519)
    return derive_keypair(seed)


def secp_keypair(n: int) -> tuple[str, str]:
    seed = generate_seed(entropy=f"{n:032x}", algorithm=CryptoAlgorithm.SECP256K1)
    return derive_keypair(seed)


def sign_manifest(fields: dict, master: tuple[str, str], signing: tuple[str, str] | None) -> bytes:
    """Sign arbitrary manifest fields; validly signed even when the shape is not."""
    data = v.manifest_signing_data(fields)
    if signing is not None:
        fields["Signature"] = sign(data, signing[1])
    fields["MasterSignature"] = sign(data, master[1])
    return unhexlify(encode(fields))


def make_manifest(master: tuple[str, str], signing: tuple[str, str], seq: int = 1) -> bytes:
    fields = {"Sequence": seq, "PublicKey": master[0], "SigningPubKey": signing[0]}
    return sign_manifest(fields, master, signing)


def revocation(master: tuple[str, str]) -> bytes:
    return sign_manifest({"Sequence": v.MANIFEST_REVOKED, "PublicKey": master[0]}, master, None)


def make_validation(signing: tuple[str, str], ledger_hash: str, *, full: bool = True) -> str:
    fields = {
        "Flags": 0x80000001 if full else 0x80000000,
        "LedgerSequence": 1000,
        "SigningTime": 800000000,
        "LedgerHash": ledger_hash,
        "SigningPubKey": signing[0],
    }
    data = v.PREFIX_VALIDATION + unhexlify(encode_for_signing(fields))[4:]
    fields["Signature"] = sign(data, signing[1])
    return encode(fields)


class Fixture:
    """A signed publisher, list and validations; a case changes one part."""

    def __init__(self, validators: int = 5, *, signer_keys: Any = None) -> None:
        make_signer = signer_keys or keypair
        self.publisher = keypair(1)
        self.publisher_signing = keypair(2)
        self.masters = [keypair(100 + i) for i in range(validators)]
        self.signers = [make_signer(200 + i) for i in range(validators)]
        self.blob_fields: dict = {"sequence": 1, "expiration": 900000000}
        self.entries = [
            {
                "validation_public_key": m[0],
                "manifest": base64.b64encode(make_manifest(m, s)).decode(),
            }
            for m, s in zip(self.masters, self.signers)
        ]
        self.voters = list(self.signers)
        self.partial_voters: list[tuple[str, str]] = []
        # The exact signed list text, when a case needs bytes json.dumps
        # would not write (a repeated member, say).
        self.blob_text: str | None = None

    @property
    def trusted_key(self) -> str:
        return self.publisher[0]

    def anchor(self, *, publisher=None, publisher_signing=None, claimed_key=None, ledger_hash=None) -> dict:
        publisher = publisher or self.publisher
        publisher_signing = publisher_signing or self.publisher_signing
        ledger_hash = ledger_hash or ANCHOR_HASH
        text = self.blob_text or json.dumps({**self.blob_fields, "validators": self.entries})
        blob = text.encode()
        return {
            "type": "anchor",
            "ledger_hash": ledger_hash,
            "unl": {
                "public_key": claimed_key or publisher[0],
                "manifest": hexlify(make_manifest(publisher, publisher_signing)).decode().upper(),
                "blob": hexlify(blob).decode().upper(),
                "signature": sign(blob, publisher_signing[1]),
            },
            "validations": {
                **{s[0]: make_validation(s, ledger_hash) for s in self.voters},
                **{s[0]: make_validation(s, ledger_hash, full=False) for s in self.partial_voters},
            },
        }

    def proof(self, network_id: int = 0, header: dict | None = None, **anchor_kw) -> dict:
        return {
            "format_version": v.FORMAT_VERSION,
            "network_id": network_id,
            "steps": [
                self.anchor(**anchor_kw),
                {"type": "ledger_header", "header": header or HEADER},
            ],
        }


# ─── signature twins ─────────────────────────────────────────────────


def der(r: int, s: int) -> bytes:
    def integer(x: int) -> bytes:
        body = x.to_bytes((x.bit_length() + 7) // 8 or 1, "big")
        if body[0] & 0x80:
            body = b"\x00" + body
        return b"\x02" + bytes([len(body)]) + body

    seq = integer(r) + integer(s)
    return b"\x30" + bytes([len(seq)]) + seq


def der_parts(sig: bytes) -> tuple[int, int]:
    rlen = sig[3]
    r = int.from_bytes(sig[4 : 4 + rlen], "big")
    slen = sig[5 + rlen]
    s = int.from_bytes(sig[6 + rlen : 6 + rlen + slen], "big")
    return r, s


def with_signature(validation_hex: str, change: Any) -> str:
    """The validation with its Signature bytes replaced by change(old)."""
    fields = decode(validation_hex)
    fields["Signature"] = change(unhexlify(fields["Signature"])).hex().upper()
    return encode(fields)


def high_s(sig: bytes) -> bytes:
    r, s = der_parts(sig)
    return der(r, v.SECP256K1_ORDER - s)


def padded_r(sig: bytes) -> bytes:
    canonical = der(*der_parts(sig))
    r_len = canonical[3]
    body = b"\x02" + bytes([r_len + 1]) + b"\x00" + canonical[4 : 4 + r_len] + canonical[4 + r_len :]
    return b"\x30" + bytes([len(body)]) + body


def s_plus_l(sig: bytes) -> bytes:
    s = int.from_bytes(sig[32:], "little") + v.ED25519_ORDER
    return sig[:32] + s.to_bytes(32, "little")


# The neutral point of the curve, in its canonical encoding. Its order is 1,
# so it is the simplest point of small order.
NEUTRAL_POINT = b"\x01" + bytes(31)


def ed25519_scalar(private_hex: str) -> int:
    """The secret scalar behind a test key (RFC 8032 §5.1.5)."""
    h = bytearray(hashlib.sha512(unhexlify(private_hex)[-32:]).digest()[:32])
    h[0] &= 248
    h[31] = (h[31] & 127) | 64
    return int.from_bytes(h, "little")


def neutral_r_signature(data: bytes, key: tuple[str, str]) -> bytes:
    """A signature by `key` over `data` whose R is the neutral point.

    Verification checks [S]B = R + [k]A. With R neutral and S = k * a that
    holds for the real key, so a verifier that checks only the equation
    accepts it. libsodium refuses an R of small order.
    """
    public = unhexlify(key[0])[1:]
    k = int.from_bytes(hashlib.sha512(NEUTRAL_POINT + public + data).digest(), "little")
    s = k * ed25519_scalar(key[1]) % v.ED25519_ORDER
    return NEUTRAL_POINT + s.to_bytes(32, "little")


def validation_signing_data(fields: dict) -> bytes:
    unsigned = {k: val for k, val in fields.items() if k != "Signature"}
    return v.PREFIX_VALIDATION + unhexlify(encode_for_signing(unsigned))[4:]


# ─── the binary form of a synthetic proof ────────────────────────────


def leb128(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def trie_with_leaves(leaves: list[tuple[bytes, bytes]]) -> bytes:
    """A binary trie of one inner node. Each leaf sits on its key's first nibble."""
    by_branch: dict[int, tuple[bytes, bytes]] = {}
    for key, data in leaves:
        branch = key[0] >> 4
        if branch in by_branch:
            raise AssertionError("two test leaves share a first nibble")
        by_branch[branch] = (key, data)
    header = 0
    body = bytearray()
    for branch in sorted(by_branch):
        header |= v.BR_LEAF << (branch * 2)
        key, data = by_branch[branch]
        body += key + leb128(len(data)) + data
    return header.to_bytes(4, "little") + bytes(body)


def binary_proof(proof: dict) -> bytes:
    """The uncompressed binary form of a synthetic proof (spec §3).

    The C++ encoder owns the canonical form of a JSON proof. This writer exists
    for the proofs it cannot be asked for: ones whose map proof is an opaque
    binary trie (`__binary_payload__`), which has no JSON form to convert. The
    generator checks it against `xprv convert` on a proof both can write.
    """

    def tlv(kind: int, payload: bytes) -> bytes:
        return bytes([kind]) + leb128(len(payload)) + payload

    def vl(data: bytes) -> bytes:
        return leb128(len(data)) + data

    body, unl = b"", b""
    for step in proof["steps"]:
        if step["type"] == "anchor":
            raws = [unhexlify(step["validations"][key]) for key in sorted(step["validations"])]
            body += tlv(
                v.TLV_ANCHOR,
                unhexlify(step["ledger_hash"]) + unhexlify(step["unl"]["public_key"])
                + leb128(len(raws)) + b"".join(vl(raw) for raw in raws),
            )
            if "blob" in step["unl"]:
                unl = tlv(v.TLV_UNL, b"".join(vl(unhexlify(step["unl"][k])) for k in ("manifest", "signature", "blob")))
        elif step["type"] == "ledger_header":
            h = step["header"]
            body += tlv(
                v.TLV_HEADER,
                struct.pack(">IQ", h["ledger_index"], int(h["total_coins"]))
                + unhexlify(h["parent_hash"]) + unhexlify(h["transaction_hash"]) + unhexlify(h["account_hash"])
                + struct.pack(">IIBB", h["parent_close_time"], h["close_time"], h["close_time_resolution"], h["close_flags"]),
            )
        else:
            body += tlv(v.TLV_MAP_TX if step["tree"] == "tx" else v.TLV_MAP_STATE, step["__binary_payload__"])
    return b"XPRV" + bytes([v.FORMAT_VERSION, 0]) + struct.pack(">I", proof["network_id"]) + body + unl


# ─── the cases ───────────────────────────────────────────────────────


def cases() -> list[dict[str, Any]]:
    """Every shared trust-world vector, in a fixed order."""
    out: list[dict[str, Any]] = []

    def proof_of(*steps: dict) -> dict:
        return {"format_version": v.FORMAT_VERSION, "network_id": 0, "steps": list(steps)}

    def add(name: str, rule: str, proof: dict, f: Fixture, *, reason: str | None = None,
            layer: str = "trust", manifests: list[bytes] | None = None, binary: bool = False,
            known_failing: dict[str, dict[str, str]] | None = None) -> None:
        accept = reason is None
        vec: dict[str, Any] = {"name": f"trust-{name}"}
        vec["states" if accept else "violates"] = rule
        if not accept:
            vec["layer"] = layer
        vec["trust"] = {"network_id": 0, "publisher_key": f.trusted_key}
        if manifests is not None:
            vec["trust"]["manifests"] = [base64.b64encode(m).decode() for m in manifests]
        vec["form"] = {"binary_hex": binary_proof(proof).hex().upper()} if binary else {"json": proof}
        vec["expect"] = {"verdict": "accept"} if accept else {"verdict": "reject", "reason_contains": reason}
        if known_failing:
            vec["known_failing"] = known_failing
        out.append(vec)

    def anchored(name: str, rule: str, f: Fixture, **kw: Any) -> None:
        anchor_kw = kw.pop("anchor_kw", {})
        add(name, rule, proof_of(f.anchor(**anchor_kw)), f, **kw)

    def b64(raw: bytes) -> str:
        return base64.b64encode(raw).decode()

    # ── the anchor: publisher, list, validations ─────────────────────
    f = Fixture()
    anchored("bound-anchor", "spec §1.3: a list signed under the configured publisher, with a full quorum of its members", f)

    f = Fixture()
    anchored("publisher-manifest-of-another-master",
             "spec §1.3 P1: the publisher manifest's master key is the configured publisher key", f,
             anchor_kw=dict(publisher=keypair(9), publisher_signing=keypair(10), claimed_key=f.trusted_key),
             reason="publisher manifest master key")

    f = Fixture()
    anchored("publisher-signing-key-is-its-master",
             "spec §1.3 rule 1: a manifest's signing key is not its master key", f,
             anchor_kw=dict(publisher_signing=f.publisher), reason="publisher manifest")

    f = Fixture()
    f.entries, f.voters = [], []
    anchored("empty-validator-list", "spec §1.3 rule 6: N is at least 1; a list with no validators is no list", f,
             reason="no verified validators")

    f = Fixture()
    forged = (f.masters[0][0], keypair(999)[1])
    f.entries[0]["manifest"] = b64(make_manifest(forged, f.signers[0]))
    anchored("forged-validator-manifest", "spec §1.3 rule 1: every list entry's manifest verifies under its master", f,
             reason="invalid validator manifest")

    f = Fixture()
    f.entries[0]["validation_public_key"] = f.masters[1][0]
    anchored("advertised-key-not-the-manifest-master",
             "spec §1.3: an entry's validation_public_key is its manifest's master key", f,
             reason="master key mismatch")

    f = Fixture()
    f.entries.append(dict(f.entries[0]))
    anchored("duplicate-master", "spec §1.3 rule 10: a master is listed once", f, reason="duplicate validator master")

    f = Fixture()
    f.entries[0]["manifest"] = b64(make_manifest(f.masters[0], f.masters[1]))
    f.voters = f.signers[1:]
    anchored("signing-key-reused-as-a-master", "spec §1.3 rule 11: no key serves two roles in one list", f,
             reason="reused across roles")

    f = Fixture()
    anchor = f.anchor()
    first, second = list(anchor["validations"])[:2]
    anchor["validations"][first], anchor["validations"][second] = anchor["validations"][second], anchor["validations"][first]
    add("validations-under-each-others-keys", "spec §2.1: a validation sits under its own SigningPubKey",
        proof_of(anchor), f, reason="does not match SigningPubKey", layer="parse")

    # ── quorum ───────────────────────────────────────────────────────
    quorum = "spec §1.3 rule 6: quorum is ceil(0.8 x N) distinct masters"
    f = Fixture()
    f.voters = f.signers[:3]
    anchored("quorum-three-of-five", quorum, f, reason="quorum not met")
    f = Fixture()
    f.voters = f.signers[:4]
    anchored("quorum-four-of-five", quorum, f)
    f = Fixture(validators=6)  # 80% of 6 is 4.8: a floor would accept four
    f.voters = f.signers[:4]
    anchored("quorum-four-of-six", quorum, f, reason="quorum not met")
    f = Fixture(validators=6)
    f.voters = f.signers[:5]
    anchored("quorum-five-of-six", quorum, f)

    partial = "spec §2.1: only a full validation (vfFullValidation) is a vote"
    f = Fixture()
    f.voters, f.partial_voters = [], list(f.signers)
    anchored("partial-validations-only", partial, f, reason="quorum not met")
    f = Fixture()
    f.voters, f.partial_voters = f.signers[:3], f.signers[3:]
    anchored("three-full-two-partial", partial, f, reason="quorum not met")
    f = Fixture()
    f.voters, f.partial_voters = f.signers[:4], f.signers[4:]
    anchored("four-full-one-partial", partial, f)

    revoked = "spec §1.3 rule 8: a revoked master stays in N and never votes"
    f = Fixture()
    f.entries[0]["manifest"] = b64(revocation(f.masters[0]))
    f.voters = f.signers[1:]  # 4 of N=5: quorum 4 is still met
    anchored("revoked-master-and-four-votes", revoked, f)
    f = Fixture()
    f.entries[0]["manifest"] = b64(revocation(f.masters[0]))
    f.voters = f.signers[:4]  # the revoked validator's vote must not count
    anchored("revoked-master-votes-with-three", revoked, f, reason="quorum not met")
    # At N=5 four votes meet the quorum whether the revoked master stays in N
    # or leaves it. At N=6 they differ: ceil(0.8 x 6) is 5, ceil(0.8 x 5) is 4.
    f = Fixture(validators=6)
    f.entries[0]["manifest"] = b64(revocation(f.masters[0]))
    f.voters = f.signers[1:5]
    anchored("revoked-master-stays-in-n", revoked, f, reason="quorum not met")
    f = Fixture(validators=6)
    f.entries[0]["manifest"] = b64(revocation(f.masters[0]))
    f.voters = f.signers[1:]
    anchored("revoked-master-of-six-and-five-votes", revoked, f)

    # ── votes that must not matter, and votes that must not count ─────
    f = Fixture()
    anchor = f.anchor()
    spare = keypair(250)
    anchor["validations"][spare[0]] = make_validation(spare, anchor["ledger_hash"])
    add("vote-from-an-unlisted-key", "spec §2.1: a validation from a key outside the list is uncounted, not fatal",
        proof_of(anchor), f)

    preimage = "spec §5.1: validation signing data is the object with only Signature removed"
    f = Fixture()
    f.voters = f.signers[:4]  # exactly the quorum, so one lost vote fails it
    signer = f.signers[0]
    fields = {
        "Flags": 0x80000001, "LedgerSequence": 1000, "SigningTime": 800000000,
        "LedgerHash": ANCHOR_HASH, "SigningPubKey": signer[0], "TxnSignature": "AA",
    }
    covered = dict(fields)
    covered["Signature"] = sign(v.PREFIX_VALIDATION + unhexlify(encode(fields)), signer[1])
    anchor = f.anchor()
    anchor["validations"][signer[0]] = encode(covered)
    add("signature-covers-every-other-field", preimage, proof_of(anchor), f)
    dropped = dict(fields)
    dropped["Signature"] = sign(v.PREFIX_VALIDATION + unhexlify(encode_for_signing(fields))[4:], signer[1])
    anchor = f.anchor()
    anchor["validations"][signer[0]] = encode(dropped)
    add("signature-over-the-transaction-preimage", preimage, proof_of(anchor), f, reason="quorum not met")

    empty = "spec §2.1: Signature is present when the field exists; an empty or short one is an uncounted vote"
    for label, key in (("ed25519", keypair(250)[0]), ("secp256k1", secp_keypair(251)[0])):
        base = decode(make_validation(keypair(250), ANCHOR_HASH))
        base["SigningPubKey"] = key
        for what, signature in (("empty", ""), ("one-byte", "00")):
            f = Fixture()
            anchor = f.anchor()
            anchor["validations"][key] = encode({**base, "Signature": signature})
            add(f"{what}-signature-{label}", empty, proof_of(anchor), f)
        f = Fixture()
        anchor = f.anchor()
        anchor["validations"][key] = encode({k: val for k, val in base.items() if k != "Signature"})
        add(f"absent-signature-{label}", "spec §2.1: a validation without a Signature field is not a complete STValidation",
            proof_of(anchor), f, reason="canonical STValidation", layer="parse")

    canonical = "spec §5.1: a signature is in canonical form or its vote is uncounted"
    f = Fixture(signer_keys=secp_keypair)
    anchored("secp256k1-validators", "spec §5.1: secp256k1 signing keys vote like Ed25519 ones", f)
    for name, change in (("high-s", high_s), ("non-minimal-der", padded_r), ("der-trailing-byte", lambda sig: sig + b"\x00")):
        f = Fixture(signer_keys=secp_keypair)
        anchor = f.anchor()
        for key in list(anchor["validations"])[:2]:
            anchor["validations"][key] = with_signature(anchor["validations"][key], change)
        add(f"secp256k1-{name}-signatures", canonical, proof_of(anchor), f, reason="quorum not met")
    f = Fixture()
    anchor = f.anchor()
    for key in list(anchor["validations"])[:2]:
        anchor["validations"][key] = with_signature(anchor["validations"][key], s_plus_l)
    add("ed25519-s-plus-l-signatures", canonical, proof_of(anchor), f, reason="quorum not met")

    # What libsodium refuses beyond S >= L (§5.1). Both of these satisfy the
    # verification equation [S]B = R + [k]A. For the first that is asserted
    # here with xrpl-py's own verifier, which checks the equation and accepts
    # it: without libsodium the Python verifier counted these votes.
    def equation_holds(data: bytes, signature: bytes, key_hex: str) -> bool:
        return is_valid_message(data, signature, key_hex)

    f = Fixture()
    anchor = f.anchor()
    for signer in f.signers[:2]:
        fields = decode(anchor["validations"][signer[0]])
        data = validation_signing_data(fields)
        signature = neutral_r_signature(data, signer)
        assert equation_holds(data, signature, signer[0]), "the neutral-R signature should satisfy the equation"
        anchor["validations"][signer[0]] = encode({**fields, "Signature": hexlify(signature).decode().upper()})
    add("ed25519-small-order-r-signatures",
        "spec §5.1: an Ed25519 signature whose R is a point of small order is not canonical; its vote is uncounted",
        proof_of(anchor), f, reason="quorum not met")

    # A listed master delegates to the neutral point as its signing key. Under
    # the equation alone, (neutral, 0) is then a signature of anything by that
    # key, since [0]B and neutral + [k]neutral are both the neutral point: of
    # the manifest, and of a validation of any ledger. Three honest votes and
    # this one make four of five. (xrpl-py's verifier happens to refuse this
    # key, so the equation is not asserted here; one that follows ZIP 215 or
    # checks the equation directly accepts it.)
    f = Fixture()
    neutral_key = "ED" + hexlify(NEUTRAL_POINT).decode().upper()
    forged = NEUTRAL_POINT + bytes(32)
    master = f.masters[0]
    fields = {"Sequence": 1, "PublicKey": master[0], "SigningPubKey": neutral_key}
    data = v.manifest_signing_data(fields)
    fields["Signature"] = hexlify(forged).decode().upper()
    fields["MasterSignature"] = sign(data, master[1])
    f.entries[0]["manifest"] = b64(unhexlify(encode(fields)))
    f.voters = f.signers[1:4]
    anchor = f.anchor()
    vote = {k: val for k, val in decode(make_validation(f.signers[0], ANCHOR_HASH)).items() if k != "Signature"}
    vote["SigningPubKey"] = neutral_key
    anchor["validations"][neutral_key] = encode({**vote, "Signature": hexlify(forged).decode().upper()})
    add("manifest-delegating-to-a-small-order-key",
        "spec §5.1: an Ed25519 key of small order signs nothing; a manifest that delegates to one is invalid",
        proof_of(anchor), f, reason="manifest")

    # ── the closed unl object ────────────────────────────────────────
    f = Fixture()
    anchor = f.anchor()
    anchor["unl"] = {"public_key": anchor["unl"]["public_key"]}
    add("list-not-bundled", "spec §2.1, §3.3: an anchor without its list needs one the verifier already holds",
        proof_of(anchor), f, reason="no external list")
    f = Fixture()
    anchor = f.anchor()
    anchor["unl"]["extra"] = "00"
    add("unknown-unl-member", "spec §2.1: unl carries its listed members and no other", proof_of(anchor), f,
        reason="unknown member in unl", layer="parse")
    f = Fixture()
    anchor = f.anchor()
    del anchor["unl"]["blob"], anchor["unl"]["signature"]
    add("list-bundled-in-part", "spec §2.1: manifest, blob and signature are present together or not at all",
        proof_of(anchor), f, reason="together, or none of them", layer="parse")

    # ── the signed list's own fields ─────────────────────────────────
    interval = "spec §1.3 P3: a list's expiration is after its effective time"
    f = Fixture()
    f.blob_fields = {"sequence": 1, "effective": 10, "expiration": 5}
    anchored("list-expires-before-it-is-effective", interval, f, reason="validity interval is invalid")
    f = Fixture()
    f.blob_fields = {"sequence": 1, "expiration": 0}
    anchored("list-expiration-zero", interval, f, reason="validity interval is invalid")
    token = "spec §2.1: sequence, expiration and effective are uint32 integer tokens"
    for name, blob_fields in (
        ("sequence", {"sequence": 100.0, "expiration": 900000000}),
        ("expiration", {"sequence": 1, "expiration": 900000000.0}),
        ("effective", {"sequence": 1, "effective": 0.0, "expiration": 900000000}),
    ):
        f = Fixture()
        f.blob_fields = blob_fields
        anchored(f"list-{name}-is-a-float", token, f, reason="UNL blob validity fields are malformed")

    # A signed list is text, and the publisher signs exactly its bytes. Text
    # with two readings is refused, never read one way (Nicholas, 2026-11-24).
    base64_rule = "spec §1.3: a validator manifest in a signed list is canonical padded base64"
    for name, spoil in (
        ("trailing-newline", lambda text: text + "\n"),
        ("inner-space", lambda text: text[:8] + " " + text[8:]),
        ("no-padding", lambda text: text.rstrip("=")),
        ("nonzero-pad-bits", _nonzero_pad_bits),
    ):
        f = Fixture()
        clean = _padded_manifest(f)
        spoiled = spoil(clean)
        assert spoiled != clean, name
        f.entries[0]["manifest"] = spoiled
        anchored(f"list-manifest-base64-{name}", base64_rule, f, reason="not canonical base64")
    f = Fixture()
    f.entries[0]["manifest"] = _padded_manifest(f)
    anchored("list-manifest-base64-padded", "spec §1.3: canonical padded base64 is accepted, padding included", f)
    f = Fixture()
    text = json.dumps({**f.blob_fields, "validators": f.entries})
    f.blob_text = '{"sequence": 2, ' + text[1:]
    anchored("list-repeats-a-member", "spec §2.4: a signed list's JSON has no repeated member", f,
             reason="duplicate member")

    # ── manifest shape ───────────────────────────────────────────────
    template = "spec §1.3 rule 1: a manifest has exactly the fields of its template"

    def manifest_case(name: str, rule: str, build: Any, *, accept: bool = False) -> None:
        f = Fixture()
        f.entries[0]["manifest"] = b64(build(f))
        # The two verifiers word this differently ("invalid validator manifest:
        # ..." and "manifest does not match the version-zero template"), so the
        # shared reason is no narrower than the word itself.
        anchored(name, rule, f, reason=None if accept else "manifest")

    def fields_of(f: Fixture, **extra: Any) -> dict:
        return {"Sequence": 1, "PublicKey": f.masters[0][0], "SigningPubKey": f.signers[0][0], **extra}

    manifest_case("manifest-without-its-signing-signature", template,
                  lambda f: sign_manifest(fields_of(f), f.masters[0], None))

    def without_sequence(f: Fixture) -> bytes:
        fields = fields_of(f)
        del fields["Sequence"]
        return sign_manifest(fields, f.masters[0], f.signers[0])

    manifest_case("manifest-without-a-sequence", template, without_sequence)
    manifest_case("manifest-version-one", template,
                  lambda f: sign_manifest(fields_of(f, Version=1), f.masters[0], f.signers[0]))
    manifest_case("revocation-carrying-signing-fields", template,
                  lambda f: sign_manifest(fields_of(f, Sequence=0xFFFFFFFF), f.masters[0], f.signers[0]))

    def revocation_with_empty_signing_fields(f: Fixture) -> bytes:
        fields = {"Sequence": 0xFFFFFFFF, "PublicKey": f.masters[0][0], "SigningPubKey": ""}
        data = v.manifest_signing_data(fields)
        fields["Signature"] = ""
        fields["MasterSignature"] = sign(data, f.masters[0][1])
        return unhexlify(encode(fields))

    manifest_case("revocation-with-empty-signing-fields", template, revocation_with_empty_signing_fields)
    manifest_case("manifest-field-outside-the-template", template,
                  lambda f: sign_manifest(fields_of(f, LedgerSequence=1000), f.masters[0], f.signers[0]))
    manifest_case("manifest-with-flags", template,
                  lambda f: sign_manifest(fields_of(f, Flags=0), f.masters[0], f.signers[0]))
    domain = "spec §1.3 rule 1: a manifest Domain is a well-formed ASCII host name"
    for name, text in (("without-a-dot", b"nodot"), ("with-a-trailing-dot", b"example.com."),
                       ("punycode-stub", b"xn--.com"), ("not-ascii", "bücher.de".encode())):
        manifest_case(f"manifest-domain-{name}", domain,
                      lambda f, text=text: sign_manifest(fields_of(f, Domain=text.hex().upper()), f.masters[0], f.signers[0]))
    manifest_case("manifest-domain-well-formed", domain,
                  lambda f: sign_manifest(fields_of(f, Domain=b"validator.example.com".hex().upper()), f.masters[0], f.signers[0]),
                  accept=True)

    # ── manifests held from outside the proof (§1.3 trust view) ──────
    overlay = "spec §1.3 rules 2 to 5: a held manifest re-points a listed master to its newest signing key"
    rotated_key = keypair(300)

    def rotation(f: Fixture) -> bytes:
        return make_manifest(f.masters[0], rotated_key, seq=2)

    f = Fixture()
    f.voters = [rotated_key] + f.signers[1:4]  # 4 of 5 only if the rotation is known
    anchored("rotated-key-without-its-manifest", overlay, f, reason="quorum not met", manifests=[])
    anchored("rotated-key-with-its-manifest", overlay, f, manifests=[rotation(f)])
    f = Fixture()
    f.voters = f.signers[:4]  # validator 0 signs with its listed key
    anchored("listed-key-before-rotation", overlay, f, manifests=[])
    anchored("listed-key-after-rotation", overlay, f, reason="quorum not met", manifests=[rotation(f)])

    conflict = "spec §1.3 rule 2: a master with two manifests at one sequence loses its vote and stays in N"
    f = Fixture()
    twin = make_manifest(f.masters[0], rotated_key, seq=1)  # the list has sequence 1
    f.voters = f.signers[:4]
    anchored("equivocating-validator-among-four", conflict, f, reason="quorum not met", manifests=[twin])
    f = Fixture()
    f.voters = f.signers[1:]
    anchored("equivocating-validator-and-four-others", conflict, f, manifests=[twin])
    f = Fixture(validators=6)  # four of N=6 fail; four of N=5 would pass
    f.voters = f.signers[1:5]
    anchored("equivocating-validator-stays-in-n", conflict, f, reason="quorum not met", manifests=[twin])
    f = Fixture()
    a = make_manifest(f.masters[0], rotated_key, seq=2)
    b = make_manifest(f.masters[0], keypair(301), seq=2)
    f.voters = [rotated_key] + f.signers[1:4]
    anchored("held-manifests-conflict", conflict, f, reason="quorum not met", manifests=[a, b])
    f = Fixture()
    a = make_manifest(f.masters[0], keypair(301), seq=2)
    b = make_manifest(f.masters[0], keypair(302), seq=2)
    winner = make_manifest(f.masters[0], rotated_key, seq=3)
    f.voters = [rotated_key] + f.signers[1:4]
    for name, order in (("winner-first", [winner, a, b]), ("winner-last", [a, b, winner]), ("winner-between", [b, winner, a])):
        anchored(f"conflict-below-the-winner-{name}",
                 "spec §1.3 rule 2: a conflict at any held sequence counts, in whatever order the manifests arrive",
                 f, reason="quorum not met", manifests=order)

    f = Fixture()
    bad = bytearray(rotation(f))
    bad[-1] ^= 0x01  # MasterSignature is the last field
    f.voters = [rotated_key] + f.signers[1:4]
    anchored("held-manifest-with-a-bad-signature", "spec §1.3 rule 1: a held manifest that does not verify grants nothing",
             f, reason="quorum not met", manifests=[bytes(bad)])
    anchored("held-bytes-that-are-no-manifest", "spec §1.3 rule 1: held bytes that are no manifest are ignored, not fatal",
             f, manifests=[b"\x01\x02\x03nope", rotation(f)])

    f = Fixture()
    key_a, key_b = keypair(310), keypair(311)
    high = make_manifest(f.masters[0], key_a, seq=3)
    low = make_manifest(f.masters[0], key_b, seq=2)
    f.voters = [key_a] + f.signers[1:4]
    anchored("highest-sequence-wins", "spec §1.3 rule 3: the highest manifest sequence wins, not the last seen",
             f, manifests=[high, low])

    f = Fixture()
    anchored("held-manifest-takes-another-validators-key", "spec §1.3 rule 11: no key serves two masters",
             f, reason="resolved signing key collides with another validator key",
             manifests=[make_manifest(f.masters[0], f.signers[1], seq=2)])
    f = Fixture()
    f.voters = f.signers[:4]
    anchored("held-revocation", revoked, f, reason="quorum not met", manifests=[revocation(f.masters[0])])
    f = Fixture(validators=6)  # four of N=6 fail; four of N=5 would pass
    f.voters = f.signers[1:5]
    anchored("held-revocation-stays-in-n", revoked, f, reason="quorum not met", manifests=[revocation(f.masters[0])])
    f = Fixture()
    f.voters = [keypair(401)] + f.signers[1:4]
    anchored("held-manifest-for-an-unlisted-master", "spec §1.3 rule 5: a held manifest never adds a member",
             f, reason="quorum not met", manifests=[make_manifest(keypair(400), keypair(401), seq=9)])

    publisher = "spec §1.3 P1: the publisher's signing key comes from its highest held manifest"
    f = Fixture()
    anchored("publisher-manifest-reissued-with-the-same-key", publisher, f,
             manifests=[make_manifest(f.publisher, f.publisher_signing, seq=2)])
    anchored("publisher-key-superseded", publisher, f, reason="superseded publisher key",
             manifests=[make_manifest(f.publisher, keypair(500), seq=2)])
    anchored("publisher-revoked", "spec §1.3 P1: a revoked publisher signs nothing", f, reason="publisher is revoked",
             manifests=[revocation(f.publisher)])

    # ── the anchor and its header (§1.3 P3) ──────────────────────────
    window = "spec §1.3 P3: the list is in effect at the anchor ledger's close time"
    f = Fixture()
    add("anchor-header-inside-the-list-window", window, f.proof(), f)
    f = Fixture()
    f.blob_fields["expiration"] = HEADER["close_time"]
    add("anchor-header-at-list-expiration", window, f.proof(), f, reason=OUTSIDE_WINDOW)
    f = Fixture()
    f.blob_fields["effective"] = HEADER["close_time"] + 1
    add("anchor-header-before-list-is-effective", window, f.proof(), f, reason=OUTSIDE_WINDOW)
    f = Fixture()
    f.blob_fields["effective"] = HEADER["close_time"]
    add("anchor-header-as-list-becomes-effective", window, f.proof(), f)

    # ── LedgerHashes links (§5.3), on headers the test keys anchor ───
    out.extend(_link_cases(add, proof_of))
    return out


def _padded_manifest(f: Fixture) -> str:
    """A first-entry manifest whose base64 text ends in padding."""
    for size in range(0, 3):
        domain = (b"validator" + b"x" * size + b".example.com").hex().upper()
        raw = sign_manifest(
            {"Sequence": 1, "PublicKey": f.masters[0][0], "SigningPubKey": f.signers[0][0], "Domain": domain},
            f.masters[0], f.signers[0],
        )
        text = base64.b64encode(raw).decode()
        if text.endswith("="):
            return text
    raise AssertionError("no padded manifest found")


def _nonzero_pad_bits(text: str) -> str:
    """The same bytes with a set bit among those padding leaves unused."""
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    pad = len(text) - len(text.rstrip("="))
    last = len(text) - pad - 1
    return text[:last] + alphabet[alphabet.index(text[last]) | 1] + text[last + 1 :]


def _link_cases(add: Any, proof_of: Any) -> list[dict[str, Any]]:
    """Skip-list links between headers, in both forms where both exist."""

    def header(index: int, **fields: Any) -> dict:
        return {**HEADER, "ledger_index": index, **fields}

    def ledger_hashes(hashes: list[str], last: int | None, entry_type: str = "LedgerHashes") -> dict:
        leaf: dict[str, Any] = {"LedgerEntryType": entry_type, "Flags": 0}
        if last is not None:
            leaf["LastLedgerSequence"] = last
        leaf["Hashes"] = hashes
        return leaf

    def linked(name: str, rule: str, *, containing: int, child: int, key_hex: str, last: int | None,
               reason: str | None = None, hashes: Any = None, entry_type: str = "LedgerHashes",
               forms: tuple[str, ...] = ("json", "binary")) -> None:
        child_header = header(child, parent_hash="44" * 32)
        child_hash = v.hash_header(child_header).hex().upper()
        leaf = ledger_hashes(hashes(child_hash) if hashes else [child_hash], last, entry_type)
        for form in forms:
            f = Fixture()
            if form == "json":
                trie: dict[str, Any] = {key_hex[0]: [key_hex, leaf]}
                root = v.hash_trie(trie, is_tx=False).hex().upper()
                step: dict[str, Any] = {"type": "map_proof", "tree": "state", "trie": trie}
            else:
                payload = trie_with_leaves([(unhexlify(key_hex), unhexlify(encode(leaf)))])
                root = v.hash_binary_trie(payload, 0, is_tx=False)[0].hex().upper()
                step = {"type": "map_proof", "tree": "state", "__binary_payload__": payload}
            top = header(containing, account_hash=root)
            anchor = f.anchor(ledger_hash=v.hash_header(top).hex().upper())
            proof = proof_of(anchor, {"type": "ledger_header", "header": top}, step,
                             {"type": "ledger_header", "header": child_header})
            add(f"{name}-{form}" if len(forms) > 1 else name, rule, proof, f, reason=reason,
                layer="graph", binary=form == "binary")

    recent = "spec §5.3: the recent list supports a ledger at most 256 below the one holding it"
    short = v.short_skip_key_hex()
    linked("recent-link", recent, containing=1000, child=900, key_hex=short, last=999)
    linked("recent-link-at-the-window-edge", recent, containing=1000, child=744, key_hex=short, last=999)
    linked("recent-link-past-the-window", recent, containing=1000, child=743, key_hex=short, last=999,
           reason=NO_SUPPORT)
    below = "spec §5.3: a skip list supports only ledgers below the one holding it"
    linked("recent-link-to-its-own-ledger", below, containing=1000, child=1000, key_hex=short, last=999,
           reason=NO_SUPPORT)
    linked("recent-link-to-a-later-ledger", below, containing=1000, child=1001, key_hex=short, last=999,
           reason=NO_SUPPORT)
    linked("recent-link-with-wrong-last-sequence",
           "spec §5.3: a recent list's LastLedgerSequence is the ledger before the one holding it",
           containing=1000, child=900, key_hex=short, last=998, reason=NO_SUPPORT)
    linked("ledger-hashes-at-no-skip-key", "spec §5.3: a LedgerHashes entry is a link only at a skip-list key",
           containing=1000, child=900, key_hex="00" * 32, last=999, reason=NO_SUPPORT)
    linked("account-root-carrying-hashes", "spec §5.3: only a LedgerHashes entry is a link",
           containing=1000, child=900, key_hex=short, last=999, entry_type="AccountRoot", reason=NO_SUPPORT)
    linked("ledger-hashes-with-257-hashes", "spec §5.3: a skip list holds at most 256 hashes",
           containing=1000, child=900, key_hex=short, last=999, reason=NO_SUPPORT,
           hashes=lambda child_hash: [child_hash] + ["AB" * 32] * 256)

    flag = "spec §5.3: a flag list supports a flag ledger of its own 65536-ledger group"
    linked("flag-link", flag, containing=200000, child=131072, key_hex=v.long_skip_key_hex(131072), last=196352)
    linked("flag-link-to-a-non-flag-ledger", flag, containing=200000, child=131073,
           key_hex=v.long_skip_key_hex(131072), last=196352, reason=NO_SUPPORT)
    linked("flag-link-from-another-group", flag, containing=200000, child=131072,
           key_hex=v.long_skip_key_hex(65536), last=196352, reason=NO_SUPPORT)
    linked("flag-link-to-its-own-ledger", below, containing=196608, child=196608,
           key_hex=v.long_skip_key_hex(196608), last=196352, reason=NO_SUPPORT)
    last_rule = "spec §5.3: a flag list's LastLedgerSequence is a flag ledger of the child's group, at or above the child"
    for name, last in (("wrong-last-sequence", 196353),  # not a flag ledger
                       ("last-sequence-from-another-group", 65536),
                       ("last-sequence-below-the-child", 131071)):
        linked(f"flag-link-with-{name}", last_rule, containing=200000, child=131072,
               key_hex=v.long_skip_key_hex(131072), last=last, reason=NO_SUPPORT)

    # A binary leaf is authenticated as bytes; a verifier need not decode it.
    opaque = "spec §2.3, §3.7: a binary leaf the codec cannot decode is still authenticated"
    for tree, key in (("state", b"\x11" * 32), ("tx", b"\x21" * 32)):
        f = Fixture()
        payload = trie_with_leaves([(key, b"\xff\x00")])
        root = v.hash_binary_trie(payload, 0, is_tx=tree == "tx")[0].hex().upper()
        top = header(1000, **{"transaction_hash" if tree == "tx" else "account_hash": root})
        proof = proof_of(f.anchor(ledger_hash=v.hash_header(top).hex().upper()),
                         {"type": "ledger_header", "header": top},
                         {"type": "map_proof", "tree": tree, "__binary_payload__": payload})
        add(f"opaque-{tree}-leaf", opaque, proof, f, binary=True)
    f = Fixture()
    payload = trie_with_leaves([(unhexlify(short), b"\xff\x00")])
    root = v.hash_binary_trie(payload, 0, is_tx=False)[0].hex().upper()
    top = header(1000, account_hash=root)
    proof = proof_of(f.anchor(ledger_hash=v.hash_header(top).hex().upper()),
                     {"type": "ledger_header", "header": top},
                     {"type": "map_proof", "tree": "state", "__binary_payload__": payload},
                     {"type": "ledger_header", "header": header(900, parent_hash="44" * 32)})
    add("opaque-bytes-at-the-skip-key", "spec §5.3: bytes that do not decode to a LedgerHashes entry are no link",
        proof, f, reason=NO_SUPPORT, layer="graph", binary=True)
    return []
