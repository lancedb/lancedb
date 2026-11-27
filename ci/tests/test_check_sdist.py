import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "ci" / "check_sdist.py"
ROOT = "lancedb-1.0.0"


class CheckSdistTest(unittest.TestCase):
    def run_check(self, tracked, shipped, links=(), root=ROOT):
        """Run the check in a git repo tracking `tracked`, on an sdist holding `shipped`
        as files and `links` as symlinks, under the folder `root`."""
        with tempfile.TemporaryDirectory() as tmp:
            subprocess.run(["git", "init", "-q", tmp], check=True)
            for name in tracked:
                path = Path(tmp, "python/python/lancedb", name)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("x = 1\n")
            if tracked:
                subprocess.run(["git", "-C", tmp, "add", "."], check=True)
            sdist = Path(tmp, f"{ROOT}.tar.gz")
            with tarfile.open(sdist, "w:gz") as tar:
                for name in shipped:
                    member = Path(tmp, "member")
                    member.write_text("x = 1\n")
                    tar.add(member, arcname=f"{root}/python/lancedb/{name}")
                for name in links:
                    link = tarfile.TarInfo(f"{root}/python/lancedb/{name}")
                    link.type = tarfile.SYMTYPE
                    link.linkname = "__init__.py"
                    tar.addfile(link)
            return subprocess.run(
                [sys.executable, SCRIPT, sdist],
                cwd=tmp,
                capture_output=True,
                text=True,
            )

    def test_complete_sdist_passes(self):
        result = self.run_check(["__init__.py", "db.py"], ["__init__.py", "db.py"])
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_tracked_file_fails(self):
        result = self.run_check(["__init__.py", "db.py"], ["__init__.py"])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("python/python/lancedb/db.py", result.stderr)

    def test_tracked_file_shipped_as_symlink_fails(self):
        result = self.run_check(
            ["__init__.py", "db.py"], ["__init__.py"], links=["db.py"]
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("python/python/lancedb/db.py", result.stderr)

    def test_files_outside_the_sdist_root_fail(self):
        result = self.run_check(["__init__.py"], ["__init__.py"], root="other-1.0.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("python/python/lancedb/__init__.py", result.stderr)

    def test_no_tracked_files_fails(self):
        result = self.run_check([], ["__init__.py"])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("git tracks no files", result.stderr)


if __name__ == "__main__":
    unittest.main()
