#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["xrpl-py>=4.0.0"]
# ///

from __future__ import annotations

import base64
import contextlib
import importlib.util
import io
import json
import sys
import unittest
from binascii import hexlify, unhexlify
from pathlib import Path

from xrpl import CryptoAlgorithm
from xrpl.core.binarycodec import encode, encode_for_signing
from xrpl.core.keypairs import derive_keypair, generate_seed, sign


def load_verifier():
    path = Path(__file__).with_name("verify-xprv.py")
    spec = importlib.util.spec_from_file_location("verify_xprv", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


v = load_verifier()
CHILD_HASH = "AB" * 32


def keypair(n: int) -> tuple[str, str]:
    seed = generate_seed(entropy=f"{n:032x}", algorithm=CryptoAlgorithm.ED25519)
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
ANCHOR_HASH = v.hash_header(HEADER).hex().upper()


class Fixture:
    """A signed publisher, list and validations; tests mutate one part."""

    def __init__(self, validators: int = 5) -> None:
        self.publisher = keypair(1)
        self.publisher_signing = keypair(2)
        self.masters = [keypair(100 + i) for i in range(validators)]
        self.signers = [keypair(200 + i) for i in range(validators)]
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

    @property
    def trusted_key(self) -> str:
        return self.publisher[0]

    def anchor(self, *, publisher=None, publisher_signing=None, claimed_key=None) -> dict:
        publisher = publisher or self.publisher
        publisher_signing = publisher_signing or self.publisher_signing
        blob = json.dumps({**self.blob_fields, "validators": self.entries}).encode()
        return {
            "type": "anchor",
            "ledger_hash": ANCHOR_HASH,
            "ledger_index": HEADER["ledger_index"],
            "unl": {
                "public_key": claimed_key or publisher[0],
                "manifest": hexlify(make_manifest(publisher, publisher_signing)).decode(),
                "blob": hexlify(blob).decode(),
                "signature": sign(blob, publisher_signing[1]),
            },
            "validations": {
                **{s[0]: make_validation(s, ANCHOR_HASH) for s in self.voters},
                **{s[0]: make_validation(s, ANCHOR_HASH, full=False) for s in self.partial_voters},
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


def quietly(fn, *args, **kwargs):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*args, **kwargs)


class LedgerHashesRecognitionTests(unittest.TestCase):
    def test_rejects_generic_state_leaf_with_hashes(self) -> None:
        leaves = [
            (
                v.short_skip_key_hex(),
                {
                    "LedgerEntryType": "AccountRoot",
                    "Hashes": [CHILD_HASH],
                },
            )
        ]
        self.assertEqual(v.extract_ledger_hashes_leaves(leaves), [])

    def test_rejects_malformed_hash_vector(self) -> None:
        leaves = [
            (
                v.short_skip_key_hex(),
                {
                    "LedgerEntryType": "LedgerHashes",
                    "Hashes": ["not-a-hash"],
                },
            )
        ]
        self.assertEqual(v.extract_ledger_hashes_leaves(leaves), [])

    def test_rejects_oversized_hash_vector(self) -> None:
        leaves = [
            (
                v.short_skip_key_hex(),
                {
                    "LedgerEntryType": "LedgerHashes",
                    "Hashes": [CHILD_HASH] * 257,
                },
            )
        ]
        self.assertEqual(v.extract_ledger_hashes_leaves(leaves), [])

    def test_accepts_well_formed_ledger_hashes_leaf(self) -> None:
        leaves = [
            (
                v.short_skip_key_hex(),
                {
                    "LedgerEntryType": "LedgerHashes",
                    "Hashes": [CHILD_HASH.lower()],
                    "LastLedgerSequence": 999,
                },
            )
        ]
        recognized = v.extract_ledger_hashes_leaves(leaves)
        self.assertEqual(len(recognized), 1)
        self.assertEqual(recognized[0].hashes, (CHILD_HASH,))


class LedgerHashesLinkTests(unittest.TestCase):
    def support(
        self,
        *,
        key: str,
        containing: int,
        last: int | None,
    ):
        return {
            CHILD_HASH: [
                v.LedgerHashesSupport(
                    leaf_key_hex=key,
                    containing_ledger_sequence=containing,
                    last_ledger_sequence=last,
                    hashes_count=1,
                    source="test leaf",
                )
            ]
        }

    def test_accepts_recent_boundary(self) -> None:
        candidates = self.support(key=v.short_skip_key_hex(), containing=1000, last=999)
        resolved = v.find_ledger_hashes_support(candidates, CHILD_HASH, 744)
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.list_type, "recent")

    def test_rejects_recent_outside_range(self) -> None:
        candidates = self.support(key=v.short_skip_key_hex(), containing=1000, last=999)
        self.assertIsNone(v.find_ledger_hashes_support(candidates, CHILD_HASH, 743))

    def test_rejects_wrong_key(self) -> None:
        candidates = self.support(key="00" * 32, containing=1000, last=999)
        self.assertIsNone(v.find_ledger_hashes_support(candidates, CHILD_HASH, 900))

    def test_accepts_flag_in_matching_era(self) -> None:
        child = 131072
        candidates = self.support(
            key=v.long_skip_key_hex(child), containing=200000, last=196352
        )
        resolved = v.find_ledger_hashes_support(candidates, CHILD_HASH, child)
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.list_type, "flag")

    def test_rejects_non_flag_and_wrong_era(self) -> None:
        candidates = self.support(
            key=v.long_skip_key_hex(131072), containing=200000, last=196352
        )
        self.assertIsNone(v.find_ledger_hashes_support(candidates, CHILD_HASH, 131073))

        wrong_era = self.support(
            key=v.long_skip_key_hex(65536), containing=200000, last=196352
        )
        self.assertIsNone(v.find_ledger_hashes_support(wrong_era, CHILD_HASH, 131072))


class AnchorVerificationTests(unittest.TestCase):
    def verify(self, anchor: dict, trusted_key: str):
        return quietly(v.verify_anchor, anchor, trusted_key)

    def assertRejected(self, result, reason: str) -> None:
        self.assertFalse(result.ok)
        self.assertIn(reason, result.message)

    def test_accepts_bound_anchor(self) -> None:
        f = Fixture()
        result = self.verify(f.anchor(), f.trusted_key)
        self.assertTrue(result.ok, result.message)
        self.assertEqual((result.matched_unl, result.unl_size), (5, 5))

    def test_rejects_publisher_manifest_not_bound_to_trusted_key(self) -> None:
        f = Fixture()
        anchor = f.anchor(
            publisher=keypair(9), publisher_signing=keypair(10), claimed_key=f.trusted_key
        )
        self.assertRejected(self.verify(anchor, f.trusted_key), "does not match trusted key")

    def test_rejects_empty_validator_list(self) -> None:
        f = Fixture()
        f.entries, f.voters = [], []
        self.assertRejected(self.verify(f.anchor(), f.trusted_key), "no verified validators")

    def test_rejects_invalid_validator_manifest(self) -> None:
        f = Fixture()
        forged = (f.masters[0][0], keypair(999)[1])
        f.entries[0]["manifest"] = base64.b64encode(make_manifest(forged, f.signers[0])).decode()
        self.assertRejected(self.verify(f.anchor(), f.trusted_key), "invalid validator manifest")

    def test_rejects_advertised_key_not_bound_to_manifest(self) -> None:
        f = Fixture()
        f.entries[0]["validation_public_key"] = f.masters[1][0]
        self.assertRejected(self.verify(f.anchor(), f.trusted_key), "master key mismatch")

    def test_rejects_duplicate_master(self) -> None:
        f = Fixture()
        f.entries.append(dict(f.entries[0]))
        self.assertRejected(self.verify(f.anchor(), f.trusted_key), "duplicate validator master")

    def test_rejects_signing_key_reused_as_master(self) -> None:
        f = Fixture()
        f.entries[0]["manifest"] = base64.b64encode(
            make_manifest(f.masters[0], f.masters[1])
        ).decode()
        f.voters = f.signers[1:]
        self.assertRejected(self.verify(f.anchor(), f.trusted_key), "reused across roles")

    def test_rejects_validation_map_key_mismatch(self) -> None:
        f = Fixture()
        anchor = f.anchor()
        validations = anchor["validations"]
        first, second = list(validations)[:2]
        validations[first], validations[second] = validations[second], validations[first]
        self.assertRejected(self.verify(anchor, f.trusted_key), "does not match SigningPubKey")

    def test_quorum_is_ceil_eighty_percent_of_masters(self) -> None:
        f = Fixture()
        f.voters = f.signers[:3]
        self.assertRejected(self.verify(f.anchor(), f.trusted_key), "quorum not met: 3/5 (need 4)")
        f.voters = f.signers[:4]
        self.assertTrue(self.verify(f.anchor(), f.trusted_key).ok)

    def test_partial_validations_do_not_count_toward_quorum(self) -> None:
        f = Fixture()
        f.voters, f.partial_voters = [], list(f.signers)
        self.assertRejected(self.verify(f.anchor(), f.trusted_key), "quorum not met: 0/5 (need 4)")
        f.voters, f.partial_voters = f.signers[:3], f.signers[3:]
        self.assertRejected(self.verify(f.anchor(), f.trusted_key), "quorum not met: 3/5 (need 4)")
        f.voters, f.partial_voters = f.signers[:4], f.signers[4:]
        self.assertTrue(self.verify(f.anchor(), f.trusted_key).ok)

    def test_quorum_rounds_up_when_eighty_percent_is_fractional(self) -> None:
        f = Fixture(validators=6)  # 80% of 6 is 4.8: floor would accept 4
        f.voters = f.signers[:4]
        self.assertRejected(self.verify(f.anchor(), f.trusted_key), "quorum not met: 4/6 (need 5)")
        f.voters = f.signers[:5]
        self.assertTrue(self.verify(f.anchor(), f.trusted_key).ok)

    def test_rejects_invalid_validity_interval(self) -> None:
        f = Fixture()
        f.blob_fields = {"sequence": 1, "effective": 10, "expiration": 5}
        self.assertRejected(self.verify(f.anchor(), f.trusted_key), "validity interval is invalid")


class ManifestTemplateTests(unittest.TestCase):
    """Validly signed manifests whose shape the C++ verifier rejects."""

    def assertValidatorManifestRejected(self, manifest: bytes) -> None:
        f = Fixture()
        f.entries[0]["manifest"] = base64.b64encode(manifest).decode()
        result = quietly(v.verify_anchor, f.anchor(), f.trusted_key)
        self.assertFalse(result.ok)
        self.assertIn("invalid validator manifest", result.message)

    def fields(self, f: Fixture, **extra) -> dict:
        return {"Sequence": 1, "PublicKey": f.masters[0][0], "SigningPubKey": f.signers[0][0], **extra}

    def test_rejects_missing_signing_signature(self) -> None:
        f = Fixture()
        self.assertValidatorManifestRejected(sign_manifest(self.fields(f), f.masters[0], None))

    def test_rejects_missing_sequence(self) -> None:
        f = Fixture()
        fields = self.fields(f)
        del fields["Sequence"]
        self.assertValidatorManifestRejected(sign_manifest(fields, f.masters[0], f.signers[0]))

    def test_rejects_nonzero_version(self) -> None:
        f = Fixture()
        self.assertValidatorManifestRejected(
            sign_manifest(self.fields(f, Version=1), f.masters[0], f.signers[0])
        )

    def test_rejects_revocation_carrying_signing_fields(self) -> None:
        f = Fixture()
        self.assertValidatorManifestRejected(
            sign_manifest(self.fields(f, Sequence=0xFFFFFFFF), f.masters[0], f.signers[0])
        )

    def test_revocation_refuses_even_empty_signing_fields(self) -> None:
        f = Fixture()
        fields = {"Sequence": 0xFFFFFFFF, "PublicKey": f.masters[0][0], "SigningPubKey": ""}
        data = v.manifest_signing_data(fields)
        fields["Signature"] = ""
        fields["MasterSignature"] = sign(data, f.masters[0][1])
        ok, err, _ = v.verify_manifest(unhexlify(encode(fields)))
        self.assertFalse(ok)
        self.assertIn("revocation carries", err)

        clean = {"Sequence": 0xFFFFFFFF, "PublicKey": f.masters[0][0]}
        ok, err, _ = v.verify_manifest(sign_manifest(clean, f.masters[0], None))
        self.assertTrue(ok, err)

    def test_rejects_field_outside_template(self) -> None:
        f = Fixture()
        self.assertValidatorManifestRejected(
            sign_manifest(self.fields(f, LedgerSequence=1000), f.masters[0], f.signers[0])
        )

    def test_rejects_malformed_domain(self) -> None:
        f = Fixture()
        domain = hexlify(b"nodot").decode().upper()
        self.assertValidatorManifestRejected(
            sign_manifest(self.fields(f, Domain=domain), f.masters[0], f.signers[0])
        )

    def test_accepts_well_formed_domain(self) -> None:
        f = Fixture()
        domain = hexlify(b"validator.example.com").decode().upper()
        manifest = sign_manifest(self.fields(f, Domain=domain), f.masters[0], f.signers[0])
        f.entries[0]["manifest"] = base64.b64encode(manifest).decode()
        self.assertTrue(quietly(v.verify_anchor, f.anchor(), f.trusted_key).ok)

    def test_rejects_publisher_signing_key_equal_to_master(self) -> None:
        f = Fixture()
        result = quietly(v.verify_anchor, f.anchor(publisher_signing=f.publisher), f.trusted_key)
        self.assertFalse(result.ok)
        self.assertIn("signing key equals its master key", result.message)


class ProofBindingTests(unittest.TestCase):
    def verify(self, proof: dict, trusted_key: str | None, network_id: int | None):
        return quietly(v.verify, proof, trusted_key, network_id)

    def test_accepts_anchor_header_inside_list_window(self) -> None:
        f = Fixture()
        result = self.verify(f.proof(), f.trusted_key, 0)
        self.assertTrue(result.ok, result.errors)

    def test_rejects_anchor_header_outside_list_window(self) -> None:
        f = Fixture()
        f.blob_fields["expiration"] = HEADER["close_time"]
        result = self.verify(f.proof(), f.trusted_key, 0)
        self.assertFalse(result.ok)
        self.assertTrue(any("outside the signed list interval" in e for e in result.errors))

    def test_rejects_proof_from_other_network(self) -> None:
        f = Fixture()
        result = self.verify(f.proof(network_id=21337), f.trusted_key, 0)
        self.assertFalse(result.ok)
        self.assertTrue(any("does not match configured network" in e for e in result.errors))

    def test_rejects_known_key_configured_for_wrong_network(self) -> None:
        result = self.verify(Fixture().proof(), v.XAHAU_VL_KEY, 0)
        self.assertFalse(result.ok)
        self.assertTrue(any("belongs to network 21337" in e for e in result.errors))

    def assertFailsWith(self, result, reason: str) -> None:
        self.assertFalse(result.ok)
        self.assertTrue(any(reason in e for e in result.errors), result.errors)

    def test_custom_key_without_network_defaults_to_xrpl(self) -> None:
        f = Fixture()
        self.assertTrue(self.verify(f.proof(), f.trusted_key, None).ok)
        self.assertFailsWith(
            self.verify(f.proof(network_id=21337), f.trusted_key, None),
            "does not match configured network 0",
        )

    def test_rejects_missing_or_boolean_network_id(self) -> None:
        f = Fixture()
        proof = f.proof()
        del proof["network_id"]
        self.assertFailsWith(self.verify(proof, f.trusted_key, 0), "network_id must be an integer")
        proof["network_id"] = False
        self.assertFailsWith(self.verify(proof, f.trusted_key, 0), "network_id must be an integer")

    def test_rejects_missing_or_unknown_format_version(self) -> None:
        f = Fixture()
        proof = f.proof()
        del proof["format_version"]
        self.assertFailsWith(self.verify(proof, f.trusted_key, 0), "unsupported format_version")
        proof["format_version"] = 999
        self.assertFailsWith(self.verify(proof, f.trusted_key, 0), "unsupported format_version")

    def test_bare_step_list_belongs_to_network_zero(self) -> None:
        f = Fixture()
        steps = f.proof()["steps"]
        self.assertTrue(self.verify(steps, f.trusted_key, 0).ok)
        self.assertFailsWith(
            self.verify(steps, f.trusted_key, 21337), "does not match configured network 21337"
        )


def _der(r: int, s: int) -> bytes:
    def integer(x: int) -> bytes:
        body = x.to_bytes((x.bit_length() + 7) // 8 or 1, "big")
        if body[0] & 0x80:
            body = b"\x00" + body
        return b"\x02" + bytes([len(body)]) + body

    seq = integer(r) + integer(s)
    return b"\x30" + bytes([len(seq)]) + seq


def _der_parts(sig: bytes) -> tuple[int, int]:
    rlen = sig[3]
    r = int.from_bytes(sig[4 : 4 + rlen], "big")
    slen = sig[5 + rlen]
    s = int.from_bytes(sig[6 + rlen : 6 + rlen + slen], "big")
    return r, s


class SignatureCanonicalityTests(unittest.TestCase):
    """Python must refuse what libsecp256k1 refuses in the C++ verifier."""

    def setUp(self) -> None:
        seed = generate_seed(entropy="ab" * 16, algorithm=CryptoAlgorithm.SECP256K1)
        self.public, self.private = derive_keypair(seed)
        self.data = b"VAL\x00" + b"xprv canonicality"
        self.low = unhexlify(sign(self.data, self.private))

    def test_low_s_signature_verifies(self) -> None:
        r, s = _der_parts(self.low)
        self.assertLessEqual(s, v.SECP256K1_ORDER // 2)
        self.assertTrue(v.verify_signature(self.data, self.low, self.public))

    def test_high_s_twin_is_refused(self) -> None:
        r, s = _der_parts(self.low)
        high = _der(r, v.SECP256K1_ORDER - s)
        self.assertFalse(v.secp256k1_signature_is_canonical(high))
        self.assertFalse(v.verify_signature(self.data, high, self.public))

    def test_non_minimal_der_is_refused(self) -> None:
        r, s = _der_parts(self.low)
        canonical = _der(r, s)
        padded_r = b"\x02" + bytes([canonical[3] + 1]) + b"\x00" + canonical[4 : 4 + canonical[3]]
        rest = canonical[4 + canonical[3] :]
        body = padded_r + rest
        self.assertFalse(v.secp256k1_signature_is_canonical(b"\x30" + bytes([len(body)]) + body))
        self.assertFalse(v.secp256k1_signature_is_canonical(canonical + b"\x00"))


if __name__ == "__main__":
    unittest.main()
