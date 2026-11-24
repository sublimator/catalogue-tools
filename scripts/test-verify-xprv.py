#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["xrpl-py==5.2.0"]
# ///
"""Tests of the Python verifier that no shared vector carries.

A rule that a verdict can show lives in tests/xprv/vectors, where the C++
verifier is asked the same question: `scripts/xprv-vectors.py check` runs this
verifier over every vector, and `scripts/xprv-conformance.py run` runs both.

What stays here is what a verdict cannot show: a helper's output, the type of
an exception, a reason only this verifier words, a check no proof reaches,
JSON text no vector edit writes, and trust configuration that a vector always
states.
"""

from __future__ import annotations

import contextlib
import copy
import importlib.util
import io
import sys
import unittest
from binascii import hexlify, unhexlify
from pathlib import Path

from xrpl.core.binarycodec import decode, encode, encode_for_signing
from xrpl.core.keypairs import sign


def load_verifier():
    path = Path(__file__).with_name("verify-xprv.py")
    spec = importlib.util.spec_from_file_location("verify_xprv", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


v = load_verifier()

# Keys, manifests, lists and validations come from the module that also
# builds the shared trust-world vectors.
sys.path.insert(0, str(Path(__file__).parent))
import xprv_trust_world as world  # noqa: E402

world.use(v)
Fixture = world.Fixture
keypair = world.keypair
sign_manifest = world.sign_manifest

CHILD_HASH = "AB" * 32


def quietly(fn, *args, **kwargs):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*args, **kwargs)


class LedgerHashesRecognitionTests(unittest.TestCase):
    """What the leaf recogniser returns; the verdicts are trust-*-link vectors."""

    def test_rejects_malformed_hash_vector(self) -> None:
        # No proof reaches this: such a leaf would not encode, so it has no hash.
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


class AnchorVerificationTests(unittest.TestCase):
    """The anchor verifier called directly: its counts and its own checks.

    verify() refuses the anchors below earlier, while it reads the proof, and
    that is what the shared vectors show (trust-validations-under-each-others-
    keys, trust-unknown-unl-member, trust-list-bundled-in-part,
    incomplete-validation, trust-absent-signature-*). verify_anchor refuses
    them again on its own; no proof reaches that second check, so no vector
    does.
    """

    def verify(self, anchor: dict, trusted_key: str):
        return quietly(v.verify_anchor, anchor, trusted_key)

    def assertRejected(self, result, reason: str) -> None:
        self.assertFalse(result.ok)
        self.assertIn(reason, result.message)

    def test_accepts_bound_anchor(self) -> None:
        # The counts; that it is accepted is the vector trust-bound-anchor.
        f = Fixture()
        result = self.verify(f.anchor(), f.trusted_key)
        self.assertTrue(result.ok, result.message)
        self.assertEqual((result.matched_unl, result.unl_size), (5, 5))

    def test_rejects_validation_map_key_mismatch(self) -> None:
        f = Fixture()
        anchor = f.anchor()
        validations = anchor["validations"]
        first, second = list(validations)[:2]
        validations[first], validations[second] = validations[second], validations[first]
        self.assertRejected(self.verify(anchor, f.trusted_key), "does not match SigningPubKey")

    def test_unl_members_are_closed_and_bundled_whole(self) -> None:
        f = Fixture()
        anchor = f.anchor()
        anchor["unl"]["extra"] = "00"
        self.assertRejected(self.verify(anchor, f.trusted_key), "unknown member in unl")
        anchor = f.anchor()
        del anchor["unl"]["blob"], anchor["unl"]["signature"]
        self.assertRejected(self.verify(anchor, f.trusted_key), "together, or none of them")

    def test_rejects_incomplete_validation_even_when_quorum_is_present(self) -> None:
        f = Fixture()
        anchor = f.anchor()
        anchor["validations"]["00" * 33] = "00"
        self.assertRejected(
            self.verify(anchor, f.trusted_key), "not a canonical STValidation"
        )

    def test_present_empty_signature_is_an_uncounted_vote(self) -> None:
        f = Fixture()
        anchor = f.anchor()
        self.assertTrue(self.verify(anchor, f.trusted_key).ok)
        base = decode(world.make_validation(keypair(250), anchor["ledger_hash"]))
        keys = (keypair(250)[0], world.secp_keypair(251)[0])
        self.assertTrue(keys[0].startswith("ED"))
        self.assertIn(keys[1][:2], {"02", "03"})

        def with_signature(key: str, signature: str | None) -> dict:
            fields = dict(base)
            fields["SigningPubKey"] = key
            if signature is None:
                fields.pop("Signature")
            else:
                fields["Signature"] = signature
            raw = encode(fields)
            self.assertEqual(decode(raw), fields)
            trial = copy.deepcopy(anchor)
            trial["validations"][key] = raw
            return trial

        for key in keys:
            self.assertTrue(self.verify(with_signature(key, ""), f.trusted_key).ok)
            self.assertTrue(self.verify(with_signature(key, "00"), f.trusted_key).ok)
            self.assertRejected(
                self.verify(with_signature(key, None), f.trusted_key),
                "not a canonical STValidation",
            )

    def test_signing_preimage_drops_only_signature(self) -> None:
        # The signing bytes themselves; the verdicts are the vectors
        # trust-signature-covers-every-other-field and
        # trust-signature-over-the-transaction-preimage.
        first = next(iter(Fixture().anchor()["validations"].values()))
        cases = (
            ({}, False),
            ({"TxnSignature": "AA"}, True),
            ({"Amendments": ["12" * 32]}, False),
        )
        for extra, transaction_encoding_differs in cases:
            fields = decode(first)
            fields.update(extra)
            raw = encode(fields)
            self.assertEqual(decode(raw), fields)
            parsed = v.parse_validation(raw)
            self.assertIsNotNone(parsed)
            unsigned = {k: val for k, val in fields.items() if k != "Signature"}
            self.assertEqual(parsed.signing_data[4:], unhexlify(encode(unsigned)))
            transaction_body = unhexlify(encode_for_signing(fields))[4:]
            if transaction_encoding_differs:
                self.assertNotEqual(parsed.signing_data[4:], transaction_body)
            else:
                self.assertEqual(parsed.signing_data[4:], transaction_body)


class ManifestTemplateTests(unittest.TestCase):
    """Reasons only this verifier words.

    That each of these manifests is refused is a shared vector
    (trust-manifest-*, trust-revocation-*, trust-publisher-signing-key-is-its-
    master). The two verifiers share no wording for why, so those vectors
    assert no more than the word "manifest"; the reason is asserted here.
    """

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

    def test_rejects_publisher_signing_key_equal_to_master(self) -> None:
        f = Fixture()
        result = quietly(v.verify_anchor, f.anchor(publisher_signing=f.publisher), f.trusted_key)
        self.assertFalse(result.ok)
        self.assertIn("signing key equals its master key", result.message)


class ProofBindingTests(unittest.TestCase):
    """Trust configuration. A vector always names its key and its network."""

    def verify(self, proof: dict, trusted_key: str | None, network_id: int | None):
        return quietly(v.verify, proof, trusted_key, network_id)

    def assertFailsWith(self, result, reason: str) -> None:
        self.assertFalse(result.ok)
        self.assertTrue(any(reason in e for e in result.errors), result.errors)

    def test_rejects_known_key_configured_for_wrong_network(self) -> None:
        result = self.verify(Fixture().proof(), v.XAHAU_VL_KEY, 0)
        self.assertFailsWith(result, "belongs to network 21337")

    def test_custom_key_without_network_defaults_to_xrpl(self) -> None:
        f = Fixture()
        self.assertTrue(self.verify(f.proof(), f.trusted_key, None).ok)
        self.assertFailsWith(
            self.verify(f.proof(network_id=21337), f.trusted_key, None),
            "does not match configured network 0",
        )

    def test_json_map_proof_cannot_carry_binary_members(self) -> None:
        # The names are this verifier's own channel from its binary decoder.
        # That the proof is refused is the vector reserved-binary-member; the
        # C++ verifier refuses it as any unknown member.
        f = Fixture()
        proof = f.proof()
        proof["steps"].append({"type": "map_proof", "tree": "tx", "__binary_root__": "00" * 32})
        self.assertFailsWith(self.verify(proof, f.trusted_key, 0), "reserved for binary decoding")


class StructuredValueGrammarTests(unittest.TestCase):
    """§2.3: each structured value carries exactly its variant's members."""

    ISSUER = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"

    def leaf(self, name, value):
        tx = {"TransactionType": "Payment", "Account": self.ISSUER, name: value}
        return {"tx": tx, "meta": {"TransactionResult": "tesSUCCESS"}}

    def accepted(self, name, value) -> bool:
        try:
            v.require_codec_members(self.leaf(name, value), is_tx=True)
            return True
        except ValueError:
            return False

    def test_variants(self) -> None:
        iou = {"value": "1", "currency": "USD", "issuer": self.ISSUER}
        bridge = {
            "LockingChainDoor": self.ISSUER, "LockingChainIssue": "XRP",
            "IssuingChainDoor": self.ISSUER, "IssuingChainIssue": "XRP",
        }
        accept = [
            ("Amount", "1000000"), ("Amount", iou),
            ("Asset", "XRP"), ("Asset", {"currency": "XRP"}),
            ("Asset", {"currency": "USD", "issuer": self.ISSUER}),
            ("Paths", [[{"currency": "USD", "issuer": self.ISSUER}]]),
            ("XChainBridge", bridge),
        ]
        refuse = [
            ("Amount", {**iou, "note": "x"}),
            ("Amount", {**iou, "currency": "XRP"}),
            ("Asset", {"currency": "USD"}),
            ("Asset", {"currency": "USD", "issuer": self.ISSUER, "note": "x"}),
            ("Paths", [[{"currency": "USD", "issuer": self.ISSUER, "note": "x"}]]),
            ("Paths", [[{}]]),
            ("XChainBridge", {**bridge, "note": "x"}),
            ("XChainBridge", {**bridge, "IssuingChainIssue": {"currency": "USD"}}),
            ("Amendments", [{}]),
            ("Fee", {"value": "10"}),
        ]
        for name, value in accept:
            with self.subTest(accept=name, value=value):
                self.assertTrue(self.accepted(name, value))
        for name, value in refuse:
            with self.subTest(refuse=name, value=value):
                self.assertFalse(self.accepted(name, value))


class JsonTextCapTests(unittest.TestCase):
    """The text itself: its size and its encoding, before any JSON is read."""

    def test_cap_is_inclusive_and_counts_utf8_bytes(self) -> None:
        cap = v.MAX_JSON_PROOF_SIZE
        with self.assertRaises(ValueError) as at_cap:
            v.load_json_proof(b" " * cap)
        self.assertNotIn("exceeds", str(at_cap.exception))
        with self.assertRaisesRegex(ValueError, "exceeds"):
            v.load_json_proof(b" " * (cap + 1))
        # Fewer characters than the cap, more bytes: the bytes are what count.
        with self.assertRaisesRegex(ValueError, "exceeds"):
            v.load_json_proof("é" * (cap // 2 + 1))

    def test_json_text_is_utf8_without_bom_or_nonfinite(self) -> None:
        self.assertEqual(v.loads_strict_json(b'{"a": 1}')["a"], 1)
        for raw in (
            b"\xef\xbb\xbf{}",
            '{"a": 1}'.encode("utf-16"),
            '{"a": NaN}',
            '{"a": Infinity}',
            '{"a": -Infinity}',
            "﻿{}",
        ):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    v.loads_strict_json(raw)
                with self.assertRaises(ValueError):
                    v.load_json_proof(raw)


class BinaryLeafAuthenticationTests(unittest.TestCase):
    def test_collect_keeps_an_undecoded_leaf_beside_a_decoded_one(self) -> None:
        # What the collector returns; that such a proof verifies is the
        # vectors trust-opaque-state-leaf and trust-opaque-tx-leaf.
        decoded = unhexlify(
            encode(
                {
                    "LedgerEntryType": "AccountRoot",
                    "Flags": 0,
                    "Sequence": 1,
                    "Balance": "1000000",
                    "Account": "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh",
                }
            )
        )
        payload = world.trie_with_leaves(
            [
                (b"\x11" * 32, b"\xff\x00"),
                (b"\x22" * 32, decoded),
            ]
        )
        leaves = v.collect_binary_leaves(payload, is_tx=False)
        self.assertEqual(len(leaves), 2)
        by_key = {key: data for key, data in leaves}
        self.assertIsNone(by_key["11" * 32])
        self.assertIsInstance(by_key["22" * 32], dict)
        self.assertEqual(by_key["22" * 32]["LedgerEntryType"], "AccountRoot")


class StrictParsingTests(unittest.TestCase):
    def test_rejects_truncated_vl_data(self):
        with self.assertRaises(ValueError):
            v.read_vl(b"\x05x", 0)

    def test_unknown_field_in_a_validation_is_a_value_error(self):
        # Field id 0xFF is not in the xrpl-py 5.2.0 definitions, so decode()
        # raises KeyError. The anchor decoder reports a validation it cannot
        # decode as not canonical, never as a traceback.
        payload = bytes(32) + b"\xed" + bytes(32) + bytes([1]) + bytes([2]) + b"\xff\x00"
        with self.assertRaisesRegex(ValueError, "canonical STValidation"):
            v.decode_anchor_core(payload)


class StructuralParityTests(unittest.TestCase):
    def test_zero_validation_core_parses(self):
        # The decoder's output. A proof with no validations is refused for its
        # quorum: the vector anchor-core-no-validations.
        # 32-byte hash, 33-byte publisher key and a zero count: 66 bytes.
        payload = bytes(32) + b"\xed" + bytes(32) + bytes([0])
        self.assertEqual(len(payload), 66)
        self.assertEqual(v.decode_anchor_core(payload)["validations"], {})

    def test_manifest_outside_the_template_or_with_a_bad_domain_is_rejected(self):
        # The reasons, which only this verifier words: see ManifestTemplateTests.
        master, signing = keypair(100), keypair(200)
        extra = sign_manifest(
            {
                "Sequence": 1,
                "PublicKey": master[0],
                "SigningPubKey": signing[0],
                "Flags": 0,
            },
            master,
            signing,
        )
        ok, message, _ = v.verify_manifest(extra)
        self.assertFalse(ok)
        self.assertIn("outside the template", message)

        for domain in (b"example.com.", b"xn--.com", "bücher.de".encode()):
            raw = sign_manifest(
                {
                    "Sequence": 1,
                    "PublicKey": master[0],
                    "SigningPubKey": signing[0],
                    "Domain": hexlify(domain).decode().upper(),
                },
                master,
                signing,
            )
            ok, message, _ = v.verify_manifest(raw)
            self.assertFalse(ok, domain)
            self.assertIn("Domain", message)


if __name__ == "__main__":
    unittest.main()
