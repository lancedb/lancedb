import tomllib
import unittest
from pathlib import Path

from packaging.requirements import Requirement
from packaging.version import Version


REPO_ROOT = Path(__file__).resolve().parents[2]


class PythonDependenciesTest(unittest.TestCase):
    def setUp(self):
        with (REPO_ROOT / "python/pyproject.toml").open("rb") as f:
            self.project = tomllib.load(f)["project"]
        self.pylance = Requirement(self.project["optional-dependencies"]["pylance"][0])

    def test_pylance_extra_excludes_older_lance_majors(self):
        with (REPO_ROOT / "Cargo.toml").open("rb") as f:
            lance = tomllib.load(f)["workspace"]["dependencies"]["lance"]
        if "version" not in lance:
            self.skipTest("Local Lance checkout has no version requirement")
        major = Version(lance["version"].lstrip("=")).major
        for older_major in range(major):
            for suffix in (".0.0", ".0.0b6", ".999.999"):
                version = f"{older_major}{suffix}"
                with self.subTest(version=version):
                    self.assertFalse(
                        self.pylance.specifier.contains(version, prereleases=True),
                        f"The pylance extra must exclude Lance {version}; "
                        f"the bundled Lance is major {major}",
                    )

    def test_pylance_extra_accepts_tested_version(self):
        requirements = [
            Requirement(dep) for dep in self.project["optional-dependencies"]["tests"]
        ]
        tested = next(dep for dep in requirements if dep.name == "pylance")
        pin = next(spec for spec in tested.specifier if spec.operator == "==")
        self.assertTrue(
            self.pylance.specifier.contains(pin.version, prereleases=True),
            f"The tests and pylance extras must be installable together: {tested}",
        )


if __name__ == "__main__":
    unittest.main()
