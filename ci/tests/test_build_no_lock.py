# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "build_no_lock.py"
SPEC = importlib.util.spec_from_file_location("build_no_lock", SCRIPT)
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)


class BuildNoLockTest(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory)
        (root / "src").mkdir()
        (root / "tests").mkdir()
        (root / "benches").mkdir()
        (root / "Cargo.toml").write_text(
            '[package]\nname = "artifact_fixture"\nversion = "0.0.0"\nedition = "2024"\n'
            "[workspace]\n[features]\nprobe = []\n"
            '[[bin]]\nname = "fixture_cli"\npath = "src/main.rs"\n'
            '[[bench]]\nname = "fixture_bench"\nharness = false\n'
            '[profile.ci]\ninherits = "dev"\ndebug = 0\nincremental = false\n'
        )
        (root / "build.rs").write_text(
            'fn main() { let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());'
            ' std::fs::write(out.join("generated.rs"), "pub fn value() -> u32 { 7 }").unwrap(); }'
        )
        (root / "src/lib.rs").write_text(
            '#[cfg(not(feature = "probe"))] compile_error!("all features must be built");\n'
            'include!(concat!(env!("OUT_DIR"), "/generated.rs"));\n'
            "#[test] fn library_test() { assert_eq!(value(), 7); }\n"
        )
        (root / "src/main.rs").write_text(
            "fn main() { assert_eq!(artifact_fixture::value(), 7); }\n"
        )
        (root / "tests/retains_binary.rs").write_text(
            "#[test] fn ordinary_binary_is_available() {\n"
            ' assert!(std::process::Command::new(env!("CARGO_BIN_EXE_fixture_cli"))\n'
            " .status().unwrap().success()); }\n"
        )
        (root / "benches/fixture_bench.rs").write_text(
            "fn main() { assert_eq!(artifact_fixture::value(), 7); }\n"
        )
        return root

    def run_build(self, root):
        env = os.environ | {"CARGO_TARGET_DIR": str(root / "target")}
        return subprocess.run(
            [sys.executable, str(SCRIPT)],
            cwd=root,
            env=env,
            text=True,
            capture_output=True,
        )

    def test_links_all_targets_and_retains_compiler_inputs(self):
        # Stay below the repository so rustup keeps its configured toolchain.
        with tempfile.TemporaryDirectory(prefix=".no-lock-", dir=SCRIPT.parent) as temp:
            root = self.fixture(temp)
            result = self.run_build(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            messages = [
                json.loads(line)
                for line in result.stdout.splitlines()
                if line.startswith("{")
            ]
            removed = [item for item in messages if "removed_test_executable" in item]
            self.assertTrue(
                {"artifact_fixture", "fixture_cli", "retains_binary", "fixture_bench"}
                <= {item["target"] for item in removed}
            )
            self.assertTrue(
                all(
                    not Path(item["removed_test_executable"]).exists()
                    for item in removed
                )
            )
            target = root / "target/ci"
            self.assertTrue((target / "fixture_cli").is_file())
            # Cargo guarantees this binary for test execution, not compile-time reads.
            subprocess.run([str(target / "fixture_cli")], check=True)
            # Cargo 1.100 nests compiler inputs in per-unit build directories and
            # names the build-script executable build_script_build.
            self.assertTrue(list(target.rglob("libartifact_fixture*.rlib")))
            self.assertTrue(list((target / "build").rglob("build[-_]script[-_]build")))
            self.assertTrue(list((target / "build").rglob("out/generated.rs")))

    def test_link_failure_preserves_cargo_status_and_diagnostic(self):
        with tempfile.TemporaryDirectory(prefix=".no-lock-", dir=SCRIPT.parent) as temp:
            root = self.fixture(temp)
            with (root / "src/lib.rs").open("a") as file:
                file.write(
                    '#[cfg(test)] unsafe extern "C" { fn missing_no_lock_probe_symbol(); }\n'
                    "#[test] fn linker_must_run() { unsafe { missing_no_lock_probe_symbol() } }\n"
                )
            result = self.run_build(root)
            self.assertEqual(result.returncode, 101, result.stderr)
            self.assertIn("missing_no_lock_probe_symbol", result.stderr)
            self.assertIn("linking with", result.stderr)

    def test_never_deletes_an_executable_outside_the_target_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = root / "target"
            target.mkdir()
            outside = root / "other-owner-binary"
            outside.write_bytes(b"keep")
            message = {
                "reason": "compiler-artifact",
                "target": {"kind": ["test"], "name": "foreign"},
                "profile": {"test": True},
                "executable": str(outside),
            }
            with self.assertRaisesRegex(ValueError, "outside target directory"):
                BUILD.discard_test_executable(message, target)
            self.assertEqual(outside.read_bytes(), b"keep")


if __name__ == "__main__":
    unittest.main()
