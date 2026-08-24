#!/usr/bin/env python3
"""Command-construction controls for test-linux-clang.sh cache pairing."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("test-linux-clang.sh")
ROOT = SCRIPT.parent.parent


class LinuxClangCachePairingTests(unittest.TestCase):
    def run_gate(
        self,
        *,
        platform: str = "linux/aarch64",
        mode: str = "full",
        generation: str | None = None,
    ) -> tuple[subprocess.CompletedProcess[str], str]:
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            log = temporary / "docker.log"
            docker = temporary / "docker"
            docker.write_text(
                "#!/bin/sh\n"
                "{\n"
                "  printf 'CALL'\n"
                "  for arg in \"$@\"; do printf '\\t%s' \"$arg\"; done\n"
                "  printf '\\n'\n"
                "} >> \"$FAKE_DOCKER_LOG\"\n"
            )
            docker.chmod(0o755)

            environment = os.environ.copy()
            environment.update(
                {
                    "PATH": f"{temporary}:{environment['PATH']}",
                    "FAKE_DOCKER_LOG": str(log),
                    "LINUX_CLANG_PLATFORM": platform,
                    "LINUX_CLANG_MODE": mode,
                    "LINUX_CLANG_REPEAT": "1",
                    "BUILD_CORES": "1",
                }
            )
            if generation is not None:
                environment["LINUX_CLANG_CACHE_GENERATION"] = generation

            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
            )
            return completed, log.read_text() if log.exists() else ""

    def assert_only_build_and_run(self, log: str) -> None:
        calls = [line.split("\t", 2)[1] for line in log.splitlines()]
        self.assertEqual(calls, ["build", "run"])
        self.assertNotIn("volume\trm", log)
        self.assertNotIn("volume\tprune", log)

    def test_default_generation_pairs_existing_full_caches(self) -> None:
        completed, log = self.run_gate()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn(
            "source=catalogue-tools-linux-clang20-aarch64-conan-full,target=/root/.conan2",
            log,
        )
        self.assertIn(
            "source=catalogue-tools-linux-clang20-aarch64-build-full,target=/build",
            log,
        )
        self.assertNotIn(
            "source=catalogue-tools-linux-clang20-aarch64-build,target=/build",
            log,
        )
        self.assert_only_build_and_run(log)

    def test_generation_changes_both_cache_halves(self) -> None:
        completed, log = self.run_gate(generation="probe17")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("aarch64-conan-probe17,target=/root/.conan2", log)
        self.assertIn("aarch64-build-probe17,target=/build", log)
        self.assertNotIn("conan-full", log)
        self.assertNotIn("build-full", log)
        self.assert_only_build_and_run(log)

    def test_architecture_and_tsan_remain_distinct_dimensions(self) -> None:
        arm_completed, arm_log = self.run_gate(mode="rpc-tsan")
        amd_completed, amd_log = self.run_gate(platform="linux/amd64")
        self.assertEqual(arm_completed.returncode, 0, arm_completed.stderr)
        self.assertEqual(amd_completed.returncode, 0, amd_completed.stderr)
        self.assertIn("aarch64-conan-full,target=/root/.conan2", arm_log)
        self.assertIn("aarch64-build-full-tsan,target=/build", arm_log)
        self.assertIn("amd64-conan-full,target=/root/.conan2", amd_log)
        self.assertIn("amd64-build-full,target=/build", amd_log)
        self.assert_only_build_and_run(arm_log)
        self.assert_only_build_and_run(amd_log)

    def test_invalid_generation_refuses_before_docker(self) -> None:
        for generation in ("../predecessor", "", " ", "foo/bar"):
            with self.subTest(generation=generation):
                completed, log = self.run_gate(generation=generation)
                self.assertEqual(completed.returncode, 2)
                self.assertIn(
                    "invalid LINUX_CLANG_CACHE_GENERATION", completed.stderr
                )
                self.assertEqual(log, "")


if __name__ == "__main__":
    unittest.main()
