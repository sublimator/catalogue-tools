#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["xrpl-py>=4.0.0"]
# ///

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
