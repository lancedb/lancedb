# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import runpy
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

validate_wheel = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "check_python_wheel.py")
)["validate_wheel"]


class CheckPythonWheelTest(unittest.TestCase):
    def make_wheel(self, directory, tags, wheel_tags):
        wheel = Path(directory) / f"lancedb-0.1.0-{tags}.whl"
        with ZipFile(wheel, "w") as archive:
            archive.writestr("lancedb/_lancedb.abi3.so", b"fixture")
            archive.writestr(
                "lancedb-0.1.0.dist-info/METADATA",
                "Metadata-Version: 2.4\nName: lancedb\nVersion: 0.1.0\n"
                "Requires-Python: >=3.10\n",
            )
            archive.writestr(
                "lancedb-0.1.0.dist-info/WHEEL",
                "Wheel-Version: 1.0\nRoot-Is-Purelib: false\n"
                + "".join(f"Tag: {tag}\n" for tag in wheel_tags),
            )
        return wheel

    def test_manylinux_compressed_platform_tags(self):
        with tempfile.TemporaryDirectory() as directory:
            wheel = self.make_wheel(
                directory,
                "cp310-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64",
                [
                    "cp310-abi3-manylinux_2_17_x86_64",
                    "cp310-abi3-manylinux2014_x86_64",
                ],
            )
            validate_wheel(wheel, "abi3", "manylinux")

    def test_free_threaded_single_platform(self):
        with tempfile.TemporaryDirectory() as directory:
            tag = "cp315-cp315t-win_amd64"
            validate_wheel(
                self.make_wheel(directory, tag, [tag]), "cp315t", "win_amd64"
            )

    def test_rejects_missing_or_extra_metadata_tags(self):
        with tempfile.TemporaryDirectory() as directory:
            filename_tags = "cp310-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64"
            for metadata_tags in (
                ["cp310-abi3-manylinux_2_17_x86_64"],
                ["cp310-abi3-manylinux_2_17_x86_64", "cp310-abi3-win_amd64"],
            ):
                with self.subTest(metadata_tags=metadata_tags):
                    wheel = self.make_wheel(directory, filename_tags, metadata_tags)
                    with self.assertRaisesRegex(ValueError, "WHEEL metadata"):
                        validate_wheel(wheel, "abi3", "manylinux")

    def test_rejects_wrong_abi(self):
        with tempfile.TemporaryDirectory() as directory:
            tag = "cp315-abi3-win_amd64"
            wheel = self.make_wheel(directory, tag, [tag])
            with self.assertRaisesRegex(ValueError, "expected cp315-cp315t"):
                validate_wheel(wheel, "cp315t", "win_amd64")


if __name__ == "__main__":
    unittest.main()
