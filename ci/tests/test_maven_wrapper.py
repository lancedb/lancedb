import os
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path

JAVA_DIR = Path(__file__).resolve().parents[2] / "java"
PUBLISH_ARGS = [
    "--batch-mode",
    "-DskipTests",
    "deploy",
    "-pl",
    "lancedb-core",
    "-am",
    "-P",
    "deploy-to-fury",
]


class MavenWrapperTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        workdir = Path(tmp.name)
        bin_dir = workdir / "bin"
        bin_dir.mkdir()
        self.wget = bin_dir / "wget"
        self.download_log = workdir / "downloads"
        properties = JAVA_DIR / ".mvn/wrapper/maven-wrapper.properties"
        url = next(
            line.partition("=")[2]
            for line in properties.read_text().splitlines()
            if line.startswith("distributionUrl=")
        )
        artifact_path = url.split("/org/apache/maven/", 1)[1]
        mirror = workdir / "mirror"
        self.archive = mirror / "org/apache/maven" / artifact_path
        self.archive.parent.mkdir(parents=True)
        maven_home = self.archive.name.removesuffix("-bin.zip")
        launcher = zipfile.ZipInfo(f"{maven_home}/bin/mvn")
        launcher.create_system = 3
        launcher.external_attr = 0o100755 << 16
        with zipfile.ZipFile(self.archive, "w") as distribution:
            distribution.writestr(launcher, '#!/bin/sh\nprintf "%s\\n" "$@"\n')
        self.env = dict(
            os.environ,
            PATH=f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            MAVEN_USER_HOME=str(workdir / "m2"),
            MVNW_REPOURL=mirror.as_uri(),
            MVNW_USERNAME="",
            MVNW_PASSWORD="",
            MVNW_VERBOSE="",
            TEST_ARCHIVE=str(self.archive),
            TEST_DOWNLOAD_LOG=str(self.download_log),
        )

    def run_wrapper(self, wget_result):
        self.wget.write_text(
            '#!/bin/sh\necho wget >> "$TEST_DOWNLOAD_LOG"\n'
            'while [ "$1" != -O ]; do shift; done\n' + wget_result + "\n"
        )
        self.wget.chmod(0o755)
        return subprocess.run(
            ["./mvnw", *PUBLISH_ARGS],
            check=False,
            cwd=JAVA_DIR,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=20,
        )

    def test_cold_cache_falls_back_to_curl_after_wget_failure(self):
        result = self.run_wrapper('printf partial > "$2"\nexit 8')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), PUBLISH_ARGS)
        self.assertEqual(self.download_log.read_text().splitlines(), ["wget"])

        self.archive.unlink()
        cached = self.run_wrapper("exit 8")
        self.assertEqual(cached.returncode, 0, cached.stderr)
        self.assertEqual(cached.stdout.splitlines(), PUBLISH_ARGS)
        self.assertEqual(self.download_log.read_text().splitlines(), ["wget"])

    def test_cold_cache_uses_a_successful_wget_download(self):
        result = self.run_wrapper('mv "$TEST_ARCHIVE" "$2"')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), PUBLISH_ARGS)
        self.assertEqual(self.download_log.read_text().splitlines(), ["wget"])


if __name__ == "__main__":
    unittest.main()
