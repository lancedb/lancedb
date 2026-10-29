import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "ci" / "check_cargo_lock.py"
CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"


def package(name, version, deps=(), source=CRATES_IO, checksum="aa"):
    text = f'[[package]]\nname = "{name}"\nversion = "{version}"\n'
    if source:
        text += f'source = "{source}"\nchecksum = "{checksum}"\n'
    if deps:
        text += f"dependencies = {list(deps)!r}\n".replace("'", '"')
    return text


LIBC = package("libc", "0.2.1")
UNTRUSTED = package("untrusted", "0.9.0")
# The prune drops napi and untrusted 0.7.1, the edge from app to it, and the
# version qualifier on app's remaining untrusted edge.
BEFORE = "version = 4\n\n" + "".join(
    [
        package(
            "app", "1.0.0", ["libc", "untrusted 0.7.1", "untrusted 0.9.0"], source=None
        ),
        package("napi", "3.0.0", ["untrusted 0.7.1"]),
        LIBC,
        package("untrusted", "0.7.1"),
        UNTRUSTED,
    ]
)
APP = package("app", "1.0.0", ["libc", "untrusted"], source=None)


class CheckCargoLockTest(unittest.TestCase):
    def run_check(self, after, header="version = 4\n\n", before=BEFORE):
        with tempfile.TemporaryDirectory() as tmp:
            before_path = Path(tmp, "before.lock")
            after_path = Path(tmp, "after.lock")
            before_path.write_text(before)
            after_path.write_text(header + after)
            return subprocess.run(
                [sys.executable, SCRIPT, before_path, after_path],
                check=False,
                capture_output=True,
                text=True,
            )

    def test_prune_passes(self):
        result = self.run_check(APP + LIBC + UNTRUSTED)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_added_entry_fails(self):
        result = self.run_check(APP + LIBC + UNTRUSTED + package("ring", "0.17.0"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ring", result.stderr)

    def test_changed_version_fails(self):
        result = self.run_check(APP + package("libc", "0.2.2") + UNTRUSTED)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("0.2.2", result.stderr)

    def test_changed_source_fails(self):
        libc = package("libc", "0.2.1", source="git+https://example.com/libc")
        result = self.run_check(APP + libc + UNTRUSTED)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("example.com", result.stderr)

    def test_changed_checksum_fails(self):
        libc = package("libc", "0.2.1", checksum="bb")
        result = self.run_check(APP + libc + UNTRUSTED)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("'bb'", result.stderr)

    def test_added_dependency_fails(self):
        libc = package("libc", "0.2.1", ["untrusted"])
        result = self.run_check(APP + libc + UNTRUSTED)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("dependencies of ('libc'", result.stderr)

    def test_removed_dependency_passes(self):
        app = package("app", "1.0.0", ["untrusted"], source=None)
        result = self.run_check(app + LIBC + UNTRUSTED)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_disabled_optional_dependency_still_used_elsewhere_passes(self):
        # Removing the CLI disables axum's tracing feature, but other SDK
        # dependencies still need tracing. Its package therefore stays pinned.
        app = package("app", "1.0.0", ["axum", "tracing"], source=None)
        tracing = package("tracing", "0.1.44")
        before = (
            "version = 4\n\n" + app + package("axum", "0.8.9", ["tracing"]) + tracing
        )
        after = app + package("axum", "0.8.9") + tracing
        result = self.run_check(after, before=before)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_dependency_redirected_to_an_existing_version_fails(self):
        before = (
            "version = 4\n\n"
            + package("app", "1.0.0", ["untrusted 0.7.1"], source=None)
            + package("untrusted", "0.7.1")
            + UNTRUSTED
        )
        after = (
            package("app", "1.0.0", ["untrusted 0.9.0"], source=None)
            + package("untrusted", "0.7.1")
            + UNTRUSTED
        )
        result = self.run_check(after, before=before)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("dependencies of ('app'", result.stderr)

    def test_changed_lock_version_fails(self):
        result = self.run_check(APP + LIBC + UNTRUSTED, header="version = 3\n\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("top-level 'version'", result.stderr)


if __name__ == "__main__":
    unittest.main()
