import tempfile
import unittest
from pathlib import Path

from validate_stable_lance import validate


class ValidateStableLanceTest(unittest.TestCase):
    def write_fixture(
        self,
        root: Path,
        *,
        rust: str = "=10.0.0",
        python: str = "10.0.0",
        java: str = "10.0.0",
    ) -> None:
        (root / "python").mkdir()
        (root / "java").mkdir()
        (root / "Cargo.toml").write_text(
            f'[workspace.dependencies]\nlance = "{rust}"\nlance-core = "{rust}"\n'
        )
        (root / "python" / "pyproject.toml").write_text(
            '[project.optional-dependencies]\ntests = ["pylance==' + python + '"]\n'
        )
        (root / "java" / "pom.xml").write_text(
            "<project><properties><lance-core.version>"
            + java
            + "</lance-core.version></properties></project>"
        )

    def test_accepts_matching_stable_versions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self.write_fixture(root)
            self.assertEqual(validate(root), "10.0.0")

    def test_rejects_prerelease(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self.write_fixture(root, python="10.0.0rc1")
            with self.assertRaisesRegex(ValueError, "not stable"):
                validate(root)

    def test_rejects_sdk_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self.write_fixture(root, java="9.0.0")
            with self.assertRaisesRegex(ValueError, "do not match across SDKs"):
                validate(root)

    def test_rejects_non_exact_rust_dependency(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self.write_fixture(root, rust="10.0.0")
            with self.assertRaisesRegex(ValueError, "not exact"):
                validate(root)

    def test_rejects_unpublished_rust_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self.write_fixture(root)
            (root / "Cargo.toml").write_text(
                "[workspace.dependencies]\n"
                'lance = { version = "=10.0.0", git = "https://example.com/lance", '
                'tag = "v10.0.0" }\n'
            )
            with self.assertRaisesRegex(ValueError, "unpublished source fields"):
                validate(root)


if __name__ == "__main__":
    unittest.main()
