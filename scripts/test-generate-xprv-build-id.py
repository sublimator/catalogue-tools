#!/usr/bin/env python3
"""Focused controls for generate-xprv-build-id.py."""

from __future__ import annotations

import importlib.util
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("generate-xprv-build-id.py")
SPEC = importlib.util.spec_from_file_location("generate_xprv_build_id", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load {SCRIPT}")
GENERATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GENERATOR)


class BuildIdTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.repo = Path(self.tempdir.name)
        self.run_git("init", "-q")
        self.run_git("config", "user.email", "build-id-test@example.invalid")
        self.run_git("config", "user.name", "Build ID Test")
        (self.repo / "tracked.bin").write_bytes(b"base\n")
        self.run_git("add", "tracked.bin")
        self.run_git("commit", "-qm", "base")
        self.head = self.run_git("rev-parse", "--short", "HEAD").strip()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def run_git(self, *args: str) -> str:
        return subprocess.run(
            ["git", *args],
            cwd=self.repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    def assert_dirty(self, value: str) -> None:
        self.assertRegex(value, rf"^{re.escape(self.head)}-dirty\.[0-9a-f]{{8}}$")

    def test_clean_and_untracked_only(self) -> None:
        self.assertEqual(GENERATOR.resolve_id(self.repo), self.head)
        (self.repo / "untracked.cpp").write_text("int ignored;\n")
        self.assertEqual(GENERATOR.resolve_id(self.repo), self.head)

    def test_staged_and_unstaged_changes_share_the_exact_snapshot(self) -> None:
        (self.repo / "tracked.bin").write_bytes(b"changed\n")
        unstaged = GENERATOR.resolve_id(self.repo)
        self.assert_dirty(unstaged)
        self.run_git("add", "tracked.bin")
        self.assertEqual(GENERATOR.resolve_id(self.repo), unstaged)

    def test_binary_content_changes_change_the_digest(self) -> None:
        (self.repo / "tracked.bin").write_bytes(b"\x00\xff\x80first")
        first = GENERATOR.resolve_id(self.repo)
        self.assert_dirty(first)
        (self.repo / "tracked.bin").write_bytes(b"\x00\xff\x80second")
        second = GENERATOR.resolve_id(self.repo)
        self.assert_dirty(second)
        self.assertNotEqual(first, second)

    def test_configured_id_is_authoritative_without_git_objects(self) -> None:
        configured = "3733a00-dirty.1234abcd"
        self.assertEqual(GENERATOR.resolve_id(self.repo, configured), configured)
        objects = self.repo / ".git" / "objects"
        for path in sorted(objects.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        objects.rmdir()
        self.assertEqual(GENERATOR.resolve_id(self.repo, configured), configured)

    def test_configured_id_rejects_shell_syntax(self) -> None:
        with self.assertRaises(ValueError):
            GENERATOR.resolve_id(self.repo, "good;touch-bad")

    def test_write_if_different_preserves_timestamp(self) -> None:
        output = self.repo / "generated.cpp"
        self.assertTrue(GENERATOR.write_if_different(output, "first\n"))
        os.utime(output, ns=(1_000_000_000, 1_000_000_000))
        before = output.stat().st_mtime_ns
        self.assertFalse(GENERATOR.write_if_different(output, "first\n"))
        self.assertEqual(output.stat().st_mtime_ns, before)
        self.assertTrue(GENERATOR.write_if_different(output, "second\n"))
        self.assertNotEqual(output.stat().st_mtime_ns, before)


if __name__ == "__main__":
    unittest.main()
