#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["xrpl-py==5.2.0", "pynacl>=1.5,<2"]
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
import json
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
            "LockingChainDoor": self.ISSUER, "LockingChainIssue": {"currency": "XRP"},
            "IssuingChainDoor": self.ISSUER, "IssuingChainIssue": {"currency": "XRP"},
        }
        accept = [
            ("Amount", "1000000"), ("Amount", iou),
            ("Asset", {"currency": "XRP"}),
            # XAH is native on Xahau only; on XRPL it is an issued currency.
            ("Amount", {**iou, "currency": "XAH"}),
            ("Asset", {"currency": "XAH", "issuer": self.ISSUER}),
            ("Asset", {"currency": "USD", "issuer": self.ISSUER}),
            # A step carries its type byte, as rippled writes it.
            ("Paths", [[{"currency": "USD", "issuer": self.ISSUER, "type": 48}]]),
            ("XChainBridge", bridge),
        ]
        refuse = [
            # An issue is an object, as rippled writes it.
            ("Asset", "XRP"),
            ("Asset", {"currency": "XAH"}),
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

    def test_cases_shared_with_cpp(self) -> None:
        """The table the C++ tests read too: amounts, and their one tolerance."""
        path = Path(__file__).parent.parent / "tests" / "xprv" / "fixture" / "leaf-value-cases.json"
        cases = json.loads(path.read_text())["cases"]
        self.assertGreater(len(cases), 40)
        for case in cases:
            with self.subTest(group=case["group"], value=case["value"], why=case["why"]):
                self.assertEqual(self.accepted(case["field"], case["value"]), case["accept"])


class NetworkLeafCodecTests(unittest.TestCase):
    """§2.3: a decoded leaf equals the JSON its own network's codec writes.
    xrpl-py is built for XRPL; the verifier makes it each network's codec."""

    REPO = Path(__file__).resolve().parent.parent
    ISSUER = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"
    ISSUER_HEX = "B5F762798A53D543A014CAF8B297CFF8F2F937E8"
    ZERO_ACCOUNT = "rrrrrrrrrrrrrrrrrrrrrhoLvTp"
    XAHAU = v.XAHAU_NETWORK_ID

    def leaf(self, **tx):
        return {
            "tx": {"TransactionType": "Payment", "Account": self.ISSUER, **tx},
            "meta": {"TransactionResult": "tesSUCCESS"},
        }

    def accepted(self, network, **tx) -> bool:
        try:
            with v.leaf_codec(network):
                v.require_codec_members(self.leaf(**tx), is_tx=True)
            return True
        except ValueError:
            return False

    def verify_vector(self, name, form):
        vector = json.loads((self.REPO / "tests" / "xprv" / "vectors" / f"{name}.json").read_text())
        proof = (
            v.binary_to_proof(bytes.fromhex(vector["form"]["binary_hex"]))
            if form == "binary" else vector["form"]["json"]
        )
        with contextlib.redirect_stdout(io.StringIO()):
            return v.verify(proof, vector["trust"]["publisher_key"], vector["trust"]["network_id"])

    def test_embedded_xahau_definitions_are_the_repositorys(self) -> None:
        spec = importlib.util.spec_from_file_location(
            "xprv_embed_definitions", self.REPO / "scripts" / "xprv-embed-definitions.py"
        )
        tool = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tool)
        source = json.loads(tool.SOURCE.read_text())
        self.assertEqual(
            v.xahau_definitions_text(), tool.canonical_text(tool.codec_table(source)),
            "run scripts/xprv-embed-definitions.py",
        )

    def test_a_captured_xahau_proof_verifies_in_both_forms(self) -> None:
        for name, form in (("accept-xahau-tx-json", "json"), ("accept-xahau-tx-binary", "binary")):
            with self.subTest(form=form):
                result = self.verify_vector(name, form)
                self.assertTrue(result.ok, result.reason)
                report = result.report()
                # Leaves were read with Xahau's table, validations with XRPL's.
                self.assertEqual(report["definitions"]["leaf"], v.definitions_fingerprint(self.XAHAU))
                self.assertEqual(report["definitions"]["validation"], v.definitions_fingerprint(0))
                self.assertNotEqual(report["definitions"]["leaf"], report["definitions"]["validation"])

    def test_xrpl_still_verifies_after_xahau_in_one_process(self) -> None:
        self.assertTrue(self.verify_vector("accept-xahau-tx-json", "json").ok)
        self.assertIs(v._codec.get(), v.XRPL_CODEC)
        self.assertIsNone(v._serialized_field("HookParameters"), "a Xahau-only field outside the Xahau codec")
        result = self.verify_vector("positive-tx-issued-xah", "json")
        self.assertTrue(result.ok, result.reason)
        self.assertEqual(result.report()["definitions"]["leaf"], v.definitions_fingerprint(0))

    def test_the_choice_of_codec_ends_with_its_block_even_on_an_error(self) -> None:
        with self.assertRaises(RuntimeError):
            with v.leaf_codec(self.XAHAU):
                self.assertIs(v._codec.get(), v.network_codec(self.XAHAU))
                raise RuntimeError("inside")
        self.assertIs(v._codec.get(), v.XRPL_CODEC)

    def test_two_threads_keep_their_own_codec(self) -> None:
        # Data is field 27 on XRPL and 28 on Xahau. One thread sits inside a
        # Xahau block while this one, and a third, encode as XRPL.
        import threading

        inside, release = threading.Event(), threading.Event()
        seen: dict[str, str] = {}

        def xahau_worker() -> None:
            with v.leaf_codec(self.XAHAU):
                inside.set()
                release.wait(10)
                seen["xahau"] = v.encode({"Data": "AB"})

        def xrpl_worker() -> None:
            seen["other thread"] = v.encode({"Data": "AB"})

        worker = threading.Thread(target=xahau_worker)
        worker.start()
        self.assertTrue(inside.wait(10))
        seen["this thread"] = v.encode({"Data": "AB"})
        other = threading.Thread(target=xrpl_worker)
        other.start()
        other.join(10)
        release.set()
        worker.join(10)
        self.assertEqual(seen, {"xahau": "701C01AB", "this thread": "701B01AB", "other thread": "701B01AB"})

    def test_the_public_xrpl_py_is_left_as_installed(self) -> None:
        # In a fresh interpreter: what xrpl-py itself does, before this file
        # is loaded and after it has verified a Xahau proof.
        import subprocess

        program = """
import importlib.util, io, json, sys, contextlib
import xrpl.core, xrpl.core.binarycodec as codec
def snapshot():
    zero = "rrrrrrrrrrrrrrrrrrrrrhoLvTp"
    return [
        codec.encode({"Destination": zero}), codec.encode({"Data": "AB"}), codec.decode("8100"),
        codec.encode({"TransactionType": "UNLModify", "Account": zero}),
        codec.encode({"Asset": {"currency": "XRP"}}),
        id(sys.modules["xrpl.core.binarycodec"]), id(sys.modules["xrpl.core.binarycodec.types.currency"]),
        xrpl.core.binarycodec is codec,
    ]
before = snapshot()
spec = importlib.util.spec_from_file_location("verify_xprv", sys.argv[1])
v = importlib.util.module_from_spec(spec); sys.modules["verify_xprv"] = v; spec.loader.exec_module(v)
vector = json.load(open(sys.argv[2]))
with contextlib.redirect_stdout(io.StringIO()):
    result = v.verify(vector["form"]["json"], vector["trust"]["publisher_key"], vector["trust"]["network_id"])
print(json.dumps({"same": before == snapshot(), "verified": result.ok, "before": before[:5]}))
"""
        run = subprocess.run(
            [sys.executable, "-c", program, str(self.REPO / "scripts" / "verify-xprv.py"),
             str(self.REPO / "tests" / "xprv" / "vectors" / "accept-xahau-tx-json.json")],
            capture_output=True, text=True,
        )
        self.assertEqual(run.returncode, 0, run.stderr[-800:])
        outcome = json.loads(run.stdout)
        self.assertTrue(outcome["verified"])
        self.assertTrue(outcome["same"], "importing or using the verifier changed what xrpl-py does")
        # And xrpl-py's own behaviour is the one this verifier does not share:
        # the zero address as twenty bytes, an unset account as the address.
        self.assertEqual(outcome["before"][0], "8314" + "00" * 20)
        self.assertEqual(outcome["before"][2], {"Account": "rrrrrrrrrrrrrrrrrrrrrhoLvTp"})

    def test_a_private_copy_shares_no_codec_module_with_xrpl_py(self) -> None:
        shared = "xrpl.core.binarycodec.exceptions"
        for codec in (v.XRPL_CODEC, v.XAHAU_CODEC):
            self.assertGreater(len(codec.modules), 30)
            for name, module in codec.modules.items():
                with self.subTest(module=name):
                    self.assertIsNot(sys.modules.get(name), module)
            self.assertNotIn(shared, codec.modules)
        self.assertFalse(set(map(id, v.XRPL_CODEC.modules.values())) & set(map(id, v.XAHAU_CODEC.modules.values())))
        # An import written inside a function of the copy, which runs long
        # after the copy was made, still gets the copy's module. xrpl-py has
        # two: `from ... import BinarySerializer` and `import ... as types`.
        modules = v.XAHAU_CODEC.modules
        st_object = modules["xrpl.core.binarycodec.types.st_object"]
        private_import = st_object.__dict__["__builtins__"]["__import__"]
        serializer = "xrpl.core.binarycodec.binary_wrappers.binary_serializer"
        self.assertIs(private_import(serializer, st_object.__dict__, None, ("BinarySerializer",), 0), modules[serializer])
        walked = private_import("xrpl.core.binarycodec.types", st_object.__dict__, None, (), 0)
        self.assertIs(walked.core.binarycodec.types, modules["xrpl.core.binarycodec.types"])
        # And anything else is Python's own.
        self.assertIs(private_import("json", st_object.__dict__, None, (), 0), json)

    def test_threads_that_start_cold_together_do_not_collide(self) -> None:
        # A fresh interpreter. Eight threads meet at a barrier and each takes
        # the Xahau codec for the first time at once, while eight more use
        # xrpl-py itself. Nothing is set up when a proof arrives, so there is
        # nothing to collide on.
        import subprocess

        program = """
import importlib.util, json, sys, threading
import xrpl.core.binarycodec as public
spec = importlib.util.spec_from_file_location("verify_xprv", sys.argv[1])
v = importlib.util.module_from_spec(spec); sys.modules["verify_xprv"] = v; spec.loader.exec_module(v)
barrier, results, errors = threading.Barrier(16), [], []
def xahau():
    try:
        barrier.wait(10)
        with v.leaf_codec(v.XAHAU_NETWORK_ID):
            results.append(("xahau", v.encode({"Data": "AB"})))
    except Exception as exc:
        errors.append(repr(exc))
def xrpl_py():
    try:
        barrier.wait(10)
        results.append(("public", public.encode({"Data": "AB"})))
    except Exception as exc:
        errors.append(repr(exc))
threads = [threading.Thread(target=f) for f in [xahau] * 8 + [xrpl_py] * 8]
[t.start() for t in threads]; [t.join(20) for t in threads]
print(json.dumps({"errors": errors, "results": sorted(set(results))}))
"""
        run = subprocess.run(
            [sys.executable, "-c", program, str(self.REPO / "scripts" / "verify-xprv.py")],
            capture_output=True, text=True,
        )
        self.assertEqual(run.returncode, 0, run.stderr[-800:])
        outcome = json.loads(run.stdout)
        self.assertEqual(outcome["errors"], [])
        self.assertEqual(outcome["results"], [["public", "701B01AB"], ["xahau", "701C01AB"]])

    def test_only_the_networks_own_code_is_native(self) -> None:
        issued = lambda code: {"value": "1", "currency": code, "issuer": self.ISSUER}  # noqa: E731
        cases = [
            # (network, member, value, accepted)
            (0, "Amount", issued("XAH"), True),              # XAH is an issued code on XRPL
            (0, "Amount", issued("XRP"), False),             # the native code has no issuer
            (self.XAHAU, "Amount", issued("XRP"), True),     # and XRP is an issued code on Xahau
            (self.XAHAU, "Amount", issued("XAH"), False),
            (self.XAHAU, "Paths", [[{"currency": "XAH", "type": 16}]], True),
            (self.XAHAU, "Paths", [[{"currency": "XRP", "issuer": self.ISSUER, "type": 48}]], True),
            (0, "Paths", [[{"currency": "XRP", "type": 16}]], True),
            (0, "Asset", {"currency": "XRP"}, True),
            (0, "Asset", {"currency": "XAH"}, False),        # a native issue in Xahau's spelling
            (0, "Asset", {"currency": "XAH", "issuer": self.ISSUER}, True),
        ]
        for network, member, value, want in cases:
            with self.subTest(network=network, member=member, value=value):
                self.assertEqual(self.accepted(network, **{member: value}), want)

    def test_xahau_fields_are_xahaus(self) -> None:
        hook_parameters = [{"HookParameter": {"HookParameterName": "AB", "HookParameterValue": "CD"}}]
        self.assertTrue(self.accepted(self.XAHAU, HookParameters=hook_parameters))
        self.assertFalse(self.accepted(0, HookParameters=hook_parameters))
        # And an XRPL-only field is not Xahau's.
        self.assertTrue(self.accepted(0, AssetsTotal="1"))
        self.assertFalse(self.accepted(self.XAHAU, AssetsTotal="1"))
        # A name both define can have a different code on each, so reading
        # one network's leaf with the other's table would misname its bytes.
        with v.leaf_codec(0):
            on_xrpl = v.encode({"Data": "AB"})
        with v.leaf_codec(self.XAHAU):
            on_xahau = v.encode({"Data": "AB"})
        self.assertNotEqual(on_xrpl, on_xahau)

    def test_the_reserved_code_is_hex_in_an_amount_and_refused_elsewhere(self) -> None:
        # The native code's letters in an issued code's place (rippled's
        # badCurrency). An amount may hold it; an issue and a currency field
        # may not.
        reserved = "0000000000000000000000005852500000000000"
        amount = {"value": "1", "currency": reserved, "issuer": self.ISSUER}
        self.assertEqual(v.decode(v.encode({"Amount": amount}))["Amount"]["currency"], reserved)
        self.assertTrue(self.accepted(0, Amount=amount))
        self.assertFalse(self.accepted(0, Asset={"currency": reserved, "issuer": self.ISSUER}))
        self.assertFalse(self.accepted(0, BaseAsset=reserved))
        self.assertTrue(self.accepted(0, BaseAsset="USD"))

    def test_an_unset_account_and_the_zero_account_are_two_things(self) -> None:
        # rippled prints an account field that was never set as "" and writes
        # it as an empty field; the zero account, once set, is its address and
        # twenty zero bytes. Xahau's Entropy and XRPL's UNLModify have the
        # first in the ledger; an XRPL SetFee has the second.
        for network in (0, self.XAHAU):
            with self.subTest(network=network), v.leaf_codec(network):
                self.assertEqual(v.encode({"Destination": ""}), "8300")
                self.assertEqual(v.decode("8300"), {"Destination": ""})
                self.assertEqual(v.encode({"Destination": self.ZERO_ACCOUNT}), "8314" + "00" * 20)
                self.assertEqual(v.decode("8314" + "00" * 20), {"Destination": self.ZERO_ACCOUNT})
        self.assertTrue(self.accepted(0, Destination=""))
        self.assertTrue(self.accepted(0, Destination=self.ZERO_ACCOUNT))
        # xrpl-py's special case for UNLModify is not applied: the value says
        # which form it is, there as anywhere.
        self.assertTrue(v.encode({"TransactionType": "UNLModify", "Account": ""}).endswith("8100"))
        self.assertTrue(
            v.encode({"TransactionType": "UNLModify", "Account": self.ZERO_ACCOUNT}).endswith("8114" + "00" * 20)
        )
        # In a path step an account is always twenty bytes.
        self.assertEqual(v.encode({"Paths": [[{"account": self.ZERO_ACCOUNT}]]}), "0112" + "01" + "00" * 20 + "00")

    def test_a_bridge_door_is_an_account_field(self) -> None:
        # A door never set is a length of 0 and ""; one set to the zero
        # account is twenty zero bytes. xrpl-py writes 20 for both lengths.
        def bridge(locking: str, issuing: str) -> dict:
            return {"XChainBridge": {
                "LockingChainDoor": locking, "LockingChainIssue": {"currency": "XRP"},
                "IssuingChainDoor": issuing, "IssuingChainIssue": {"currency": "XRP"},
            }}

        zero, native = "00" * 20, "00" * 20
        header = bytes(v.XRPL_CODEC.get_field_instance("XChainBridge").header).hex().upper()
        for locking, issuing, body in (
            ("", self.ISSUER, "00" + native + "14" + self.ISSUER_HEX + native),
            (self.ISSUER, "", "14" + self.ISSUER_HEX + native + "00" + native),
            ("", "", "00" + native + "00" + native),
            (self.ZERO_ACCOUNT, self.ISSUER, "14" + zero + native + "14" + self.ISSUER_HEX + native),
        ):
            with self.subTest(locking=locking, issuing=issuing):
                value = bridge(locking, issuing)
                self.assertEqual(v.encode(value), header + body)
                self.assertEqual(v.decode(header + body), value)
                self.assertTrue(self.accepted(0, **value))
        self.assertFalse(self.accepted(0, **bridge(" ", self.ISSUER)))
        # A door of any other length is no account.
        with self.assertRaises(Exception):
            v.decode(header + "13" + "00" * 19 + native + "00" + native)
        # The same on Xahau, whose bridge issues name XAH.
        with v.leaf_codec(self.XAHAU):
            value = {"XChainBridge": {
                "LockingChainDoor": "", "LockingChainIssue": {"currency": "XAH"},
                "IssuingChainDoor": self.ISSUER, "IssuingChainIssue": {"currency": "XAH"},
            }}
            self.assertEqual(v.decode(v.encode(value)), value)

    def test_xahau_reserves_every_case_of_its_own_code(self) -> None:
        # xahaud's isBadCurrency catches xah, Xah and the rest where rippled
        # compares its own code exactly. An issue and a currency field refuse
        # them; an amount, as with the reserved code itself, does not.
        for code in ("xah", "Xah", "xAH", "XAh"):
            with self.subTest(code=code):
                self.assertFalse(self.accepted(self.XAHAU, Asset={"currency": code, "issuer": self.ISSUER}))
                self.assertFalse(self.accepted(self.XAHAU, BaseAsset=code))
                self.assertTrue(self.accepted(
                    self.XAHAU, Amount={"value": "1", "currency": code, "issuer": self.ISSUER}))
                # On XRPL these are ordinary codes, and so is xrp in any case but XRP.
                self.assertTrue(self.accepted(0, Asset={"currency": code, "issuer": self.ISSUER}))
                self.assertTrue(self.accepted(0, BaseAsset=code))
        for code in ("xrp", "Xrp"):
            with self.subTest(code=code):
                self.assertTrue(self.accepted(0, Asset={"currency": code, "issuer": self.ISSUER}))
                self.assertTrue(self.accepted(self.XAHAU, Asset={"currency": code, "issuer": self.ISSUER}))
        self.assertTrue(self.accepted(self.XAHAU, Asset={"currency": "XRP", "issuer": self.ISSUER}))
        self.assertTrue(self.accepted(self.XAHAU, BaseAsset="USD"))

    def test_a_xahau_json_proof_without_a_map_step_verifies(self) -> None:
        f = world.Fixture()
        with contextlib.redirect_stdout(io.StringIO()):
            result = v.verify(f.proof(network_id=self.XAHAU), f.trusted_key, self.XAHAU)
        self.assertTrue(result.ok, result.reason)


class StrictJsonReaderTests(unittest.TestCase):
    """What json.loads reads and the C++ parser refuses (§2.4)."""

    def test_nesting_is_read_to_128_levels_and_no_deeper(self) -> None:
        for opener, closer in (("[", "]"), ('{"a":', "}")):
            with self.subTest(opener=opener):
                v.loads_strict_json(opener * 128 + ("" if opener == "[" else "1") + closer * 128)
                with self.assertRaisesRegex(ValueError, "too deep"):
                    v.loads_strict_json(opener * 129 + ("" if opener == "[" else "1") + closer * 129)
        # Mixed, and beside a shallow sibling that must not hide it.
        with self.assertRaisesRegex(ValueError, "too deep"):
            v.loads_strict_json('{"shallow": [1], "deep": ' + "[" * 128 + "]" * 128 + "}")

    def test_nesting_too_deep_for_the_interpreter_is_a_refusal_not_a_crash(self) -> None:
        with self.assertRaisesRegex(ValueError, "too deep"):
            v.loads_strict_json("[" * 200000 + "]" * 200000)
        with self.assertRaisesRegex(ValueError, "too deep"):
            v.load_json_proof('{"a":' * 200000 + "1" + "}" * 200000)

    def test_half_of_a_surrogate_pair_is_refused_anywhere(self) -> None:
        for text in (
            '"\\ud800"', '"\\udc00"', '"\\udc00\\ud800"', '"a\\ud800b"',
            '["\\ud800"]', '{"k": "\\udfff"}', '{"\\ud800": 1}', '{"k": [[{"j": "\\ud800"}]]}',
        ):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, "surrogate"):
                v.loads_strict_json(text)
        # A pair is one character, and so is anything written directly.
        self.assertEqual(v.loads_strict_json('"\\ud83d\\ude00"'), "\U0001F600")
        self.assertEqual(v.loads_strict_json('{"é": "日本", "k": "\\u00e9"}'), {"é": "日本", "k": "é"})

    def test_the_proof_reader_refuses_both(self) -> None:
        for text in ('{"x": "\\ud800"}', '{"x": ' + "[" * 128 + "]" * 128 + "}"):
            with self.subTest(text=text[:20]), self.assertRaises(ValueError):
                v.load_json_proof(text)


class RippledReferenceTests(unittest.TestCase):
    """The XRPL leaf codec held to rippled's own, over every section of
    tests/x-data/fixture/rippled-reference-fixtures.json, which records what
    rippled's codec printed and which the C++ suite holds x-data to. The steps
    are the leaf rule's: _for_codec, encode, decode, _as_reference."""

    REPO = Path(__file__).resolve().parent.parent
    FIELD = {"currency": "BaseAsset", "issue": "Asset", "amount": "Amount", "number": "AssetsTotal",
             "pathset": "Paths", "bridge": "XChainBridge"}

    @staticmethod
    def to_json(hex_text):
        return v._as_reference(v.decode(hex_text))

    @staticmethod
    def to_hex(value):
        return v.encode(v._for_codec(value)).upper()

    def test_every_section(self) -> None:
        fixture = json.loads(
            (self.REPO / "tests" / "x-data" / "fixture" / "rippled-reference-fixtures.json").read_text()
        )
        rows_checked = 0
        for section, rows in fixture.items():
            if not isinstance(rows, list) or section == "uint64_fields":
                continue
            kind, _, direction = section.rpartition("_")
            for row in rows:
                rows_checked += 1
                value = row.get("json", row.get("text"))
                if kind in self.FIELD:
                    field = self.FIELD[kind]
                    obj = {field: value}
                    prefix = bytes(v.XRPL_CODEC.get_field_instance(field).header).hex().upper()
                else:
                    # A whole object of one field, bytes and all.
                    obj, prefix = value, ""
                with self.subTest(section=section, value=value):
                    if direction == "decode":
                        # The bytes decode to rippled's JSON.
                        self.assertEqual(self.to_json(prefix + row["hex"]), obj)
                        continue
                    own = row.get("xdata")
                    refuse = own == "refuses" or (own is None and "refused" in row)
                    try:
                        written = self.to_hex(obj)
                        accepted = self.to_json(written) == obj
                    except Exception:
                        written, accepted = None, False
                    if refuse:
                        # rippled refuses it, or writes bytes it cannot read
                        # back: the leaf rule must not accept it.
                        self.assertFalse(accepted, f"accepted, as {written}")
                        continue
                    wanted = prefix + (own if own is not None else row["hex"])
                    if accepted:
                        self.assertEqual(written, wanted)
                    else:
                        # Refusing is right only for a spelling other than
                        # the one these bytes decode to.
                        self.assertNotEqual(self.to_json(wanted), obj, "refused the reference rendering")
        self.assertGreater(rows_checked, 2500)


class StandaloneScriptTests(unittest.TestCase):
    """The verifier is handed out as one file (xprv.it serves it as a
    download). It has to run with nothing of this repository beside it."""

    REPO = Path(__file__).resolve().parent.parent

    def test_format_version_is_the_repositorys(self) -> None:
        self.assertEqual(v.FORMAT_VERSION, int((self.REPO / "src" / "xprv" / "VERSION").read_text().strip()))

    def test_runs_from_a_directory_of_its_own(self) -> None:
        import shutil
        import subprocess
        import tempfile

        vector = json.loads((self.REPO / "tests" / "xprv" / "vectors" / "positive-tx-skiplist.json").read_text())
        with tempfile.TemporaryDirectory() as td:
            script = Path(td) / "verify-xprv.py"
            shutil.copyfile(self.REPO / "scripts" / "verify-xprv.py", script)
            for name, data in (
                ("proof.json", json.dumps(vector["form"]["json"]).encode()),
                ("proof.bin", bytes.fromhex(vector["form"]["binary_hex"])),
            ):
                proof = Path(td) / name
                proof.write_bytes(data)
                # The same interpreter and packages as this test, and no
                # repository on the path: only the copied file.
                run = subprocess.run(
                    [sys.executable, str(script), str(proof), "--json"],
                    capture_output=True, text=True, cwd=td,
                )
                with self.subTest(form=name):
                    self.assertEqual(run.returncode, 0, run.stderr[-600:])
                    self.assertIs(json.loads(run.stdout)["verified"], True)


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


class ExternalValidatorListTests(unittest.TestCase):
    def test_parse_external_validator_lists_v1_v2_and_malformed(self) -> None:
        import base64

        m1 = base64.b64encode(b"\x01\x02").decode()
        m2 = base64.b64encode(b"\x03\x04").decode()
        b1 = base64.b64encode(b"{}").decode()
        b2 = base64.b64encode(b'{"a":1}').decode()

        v1 = {"version": 1, "public_key": "ED" + "AA" * 32, "manifest": m1, "blob": b1, "signature": "aabbcc"}
        parsed_v1 = v.parse_external_validator_lists(v1)
        self.assertEqual(len(parsed_v1), 1)
        self.assertEqual(parsed_v1[0].manifest, b"\x01\x02")
        self.assertEqual(parsed_v1[0].blob, b"{}")
        self.assertEqual(parsed_v1[0].signature, b"\xaa\xbb\xcc")

        v2 = {
            "version": 2,
            "manifest": m1,
            "blobs_v2": [
                {"blob": b1, "signature": "1122"},
                {"manifest": m2, "blob": b2, "signature": "3344"},
                {"blob": "not base64!", "signature": "5566"},
                {"blob": b1, "signature": "11 22"},  # whitespace in hex rejected
            ],
        }
        parsed_v2 = v.parse_external_validator_lists(v2)
        self.assertEqual(len(parsed_v2), 2)
        self.assertEqual(parsed_v2[0].manifest, b"\x01\x02")
        self.assertEqual(parsed_v2[1].manifest, b"\x03\x04")

        # v2 with top-level manifest AND all entries carrying overrides retains BOTH manifests
        v2_all_overridden = {
            "version": 2,
            "manifest": m2,
            "blobs_v2": [{"manifest": m1, "blob": b1, "signature": "1122"}],
        }
        parsed_both = v.parse_external_validator_lists(v2_all_overridden)
        self.assertEqual(len(parsed_both), 2)
        self.assertEqual(parsed_both[0].manifest, b"\x01\x02")
        self.assertEqual(parsed_both[1].manifest, b"\x03\x04")
        self.assertEqual(parsed_both[1].blob, b"")

        # v1 with valid manifest and bad blob/sig still retains manifest for P1
        v1_bad_blob = {"version": 1, "manifest": m2, "blob": "not base64!", "signature": "ZZ"}
        parsed_v1_man = v.parse_external_validator_lists(v1_bad_blob)
        self.assertEqual(len(parsed_v1_man), 1)
        self.assertEqual(parsed_v1_man[0].manifest, b"\x03\x04")
        self.assertEqual(parsed_v1_man[0].blob, b"")

        v2_empty = {"version": 2, "manifest": m2, "blobs_v2": []}
        parsed_empty = v.parse_external_validator_lists(v2_empty)
        self.assertEqual(len(parsed_empty), 1)
        self.assertEqual(parsed_empty[0].manifest, b"\x03\x04")
        self.assertEqual(parsed_empty[0].blob, b"")

        self.assertEqual(len(v.parse_external_validator_lists([v1, v2])), 3)
        with self.assertRaises(ValueError):
            v.parse_external_validator_lists("not-an-object-or-array")
        with self.assertRaises(ValueError):
            v.parse_external_validator_lists([123])

    def test_cli_lists_accepts_single_v1_v2_and_array_and_rejects_bad_file(self) -> None:
        import base64
        import shutil
        import subprocess
        import tempfile

        f = Fixture()
        proof_json = f.proof(listless=True)
        proof_bin = world.binary_proof(proof_json)
        v1_doc = f.vl_doc_v1()
        v2_blob = json.dumps({**f.blob_fields, "validators": f.entries}).encode()
        v2_doc = f.vl_doc_v2(
            [{"blob": base64.b64encode(v2_blob).decode(), "signature": sign(v2_blob, f.publisher_signing[1])}]
        )

        with tempfile.TemporaryDirectory(prefix="xprv-lists-cli-") as td:
            tmp = Path(td)
            proof_path = tmp / "listless.bin"
            proof_path.write_bytes(proof_bin)
            # Standalone copy of verify-xprv.py in an isolated empty directory.
            standalone = tmp / "verify-xprv.py"
            shutil.copy2(Path(__file__).resolve().parent / "verify-xprv.py", standalone)

            for label, payload in (("v1_obj", v1_doc), ("v2_obj", v2_doc), ("array", [v1_doc])):
                lists_file = tmp / f"{label}.json"
                lists_file.write_text(json.dumps(payload))
                run = subprocess.run(
                    [
                        sys.executable,
                        str(standalone),
                        str(proof_path),
                        "--trusted-key",
                        f.trusted_key,
                        "--network",
                        "0",
                        "--lists",
                        str(lists_file),
                        "--json",
                    ],
                    capture_output=True,
                    text=True,
                    cwd=tmp,
                )
                self.assertEqual(run.returncode, 0, f"{label}: {run.stderr}")
                report = json.loads(run.stdout)
                self.assertTrue(report["verified"], label)
                self.assertEqual(report["anchor"]["votes"], 5)

            for label, bad_text in (
                ("wrong-type", '"not-a-vl-doc"'),
                ("nan", '{"version": 1, "x": NaN}'),
                ("deep-nesting", "[" * 129 + "]" * 129),
                ("lone-surrogate", '{"version": 1, "x": "\\ud800"}'),
                ("duplicate-key", '{"version": 1, "version": 1}'),
            ):
                bad_lists = tmp / f"bad-{label}.json"
                bad_lists.write_bytes(bad_text.encode("utf-8"))
                bad_run = subprocess.run(
                    [
                        sys.executable,
                        str(standalone),
                        str(proof_path),
                        "--trusted-key",
                        f.trusted_key,
                        "--lists",
                        str(bad_lists),
                    ],
                    capture_output=True,
                    text=True,
                    cwd=tmp,
                )
                self.assertEqual(bad_run.returncode, 1, label)
                self.assertIn("--lists", bad_run.stderr, label)


if __name__ == "__main__":
    unittest.main()
