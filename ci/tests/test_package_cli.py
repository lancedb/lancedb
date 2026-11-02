import io
import os
import tarfile
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import patch

from ci import package_cli as cli

VERSION = "1.2.3-beta.4"


class PackageCliTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.output = self.root / "dist"
        self.write_versions(VERSION)
        for name in cli.NOTICES:
            (self.root / name).write_text(f"Test {name}\n")
        self.binary = self.root / "lancedb"
        self.binary.write_text(
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            "assert not any(k.startswith('LANCEDB_') for k in os.environ)\n"
            "assert os.environ['HOME'] != os.environ.get('ORIGINAL_HOME')\n"
            "assert not os.listdir(os.environ['HOME'])\n"
            f"print({{('--version',): 'lancedb {VERSION}', "
            "('--help',): 'Usage: lancedb', "
            "('profile', 'list'): 'Configured profiles:'}[tuple(sys.argv[1:])])\n"
        )
        # Packaging must restore executable permissions even if the input lacks them.
        self.binary.chmod(0o644)

    def write_versions(self, version):
        (self.root / ".bumpversion.toml").write_text(
            f'[tool.bumpversion]\ncurrent_version = "{version}"\n'
        )
        for manifest in ("rust/lancedb/Cargo.toml", "rust/lancedb/cli/Cargo.toml"):
            path = self.root / manifest
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f'[package]\nversion = "{version}"\n')

    def make_package(self, target=cli.TARGETS[0]):
        with patch.object(cli, "check_runtime"):
            return cli.package(self.root, self.binary, target, self.output, VERSION)

    def test_stable_and_beta_versions_and_tags(self):
        for version in ("1.2.3", VERSION):
            self.write_versions(version)
            for ref in (
                f"refs/tags/v{version}",
                "refs/heads/main",
                "refs/pull/1/merge",
            ):
                with self.subTest(version=version, ref=ref):
                    self.assertEqual(cli.release_version(self.root, ref), version)

    def test_mismatched_tag_and_manifests_fail(self):
        with self.assertRaisesRegex(ValueError, "Tag"):
            cli.release_version(self.root, "refs/tags/v1.2.3")
        for manifest in ("rust/lancedb/Cargo.toml", "rust/lancedb/cli/Cargo.toml"):
            self.write_versions(VERSION)
            (self.root / manifest).write_text('[package]\nversion = "0.1.0"\n')
            with (
                self.subTest(manifest=manifest),
                self.assertRaisesRegex(ValueError, "version does not match"),
            ):
                cli.release_version(self.root, "refs/heads/main")

    def test_cli_participates_in_repository_version_bumps(self):
        config = tomllib.loads((cli.ROOT / ".bumpversion.toml").read_text())["tool"][
            "bumpversion"
        ]
        entry = next(
            f
            for f in config["files"]
            if f.get("filename") == "rust/lancedb/cli/Cargo.toml"
        )
        manifest = (cli.ROOT / entry["filename"]).read_text()
        version = cli.release_version(cli.ROOT, "")
        self.assertIn(entry["search"].format(current_version=version), manifest)
        lock = tomllib.loads((cli.ROOT / "Cargo.lock").read_text())
        package = next(p for p in lock["package"] if p["name"] == "lancedb-cli")
        self.assertEqual(package["version"], version)

    def test_archive_layout_permissions_and_isolated_smoke_test(self):
        with patch.dict(
            os.environ,
            {"LANCEDB_API_KEY": "unused", "ORIGINAL_HOME": os.environ["HOME"]},
        ):
            archive = self.make_package()
        self.assertEqual(archive.name, f"lancedb-v{VERSION}-{cli.TARGETS[0]}.tar.gz")
        cli.verify_archive(archive, VERSION, cli.TARGETS[0])
        with tarfile.open(archive) as tar:
            for member in tar.getmembers():
                self.assertEqual(member.uid, 0)
                self.assertEqual(member.gid, 0)

    def test_wrong_binary_version_fails_smoke_test(self):
        self.binary.write_text(
            self.binary.read_text().replace(f"lancedb {VERSION}", "lancedb 0.1.0")
        )
        with self.assertRaisesRegex(ValueError, "wrong version"):
            self.make_package()

    def test_bad_archive_members_are_rejected(self):
        archive = self.make_package()
        stem = cli.archive_stem(VERSION, cli.TARGETS[0])
        for bad in (
            "missing",
            "extra",
            "duplicate",
            "symlink",
            "permissions",
            "empty",
            "traversal",
        ):
            with self.subTest(bad=bad):
                with tarfile.open(archive, "w:gz") as tar:
                    for name in ("lancedb", *cli.NOTICES):
                        if bad == "missing" and name == "LICENSE":
                            continue
                        member = tarfile.TarInfo(f"{stem}/{name}")
                        member.mode = 0o755 if name == "lancedb" else 0o644
                        member.size = 1
                        if name == "lancedb":
                            if bad == "symlink":
                                member.type = tarfile.SYMTYPE
                                member.linkname = "/usr/bin/true"
                                member.size = 0
                            elif bad == "permissions":
                                member.mode = 0o644
                            elif bad == "empty":
                                member.size = 0
                            elif bad == "traversal":
                                member.name = "../lancedb"
                        tar.addfile(member, io.BytesIO(b"x"))
                    if bad in ("extra", "duplicate"):
                        member = tarfile.TarInfo(
                            f"{stem}/{'LICENSE' if bad == 'duplicate' else 'extra'}"
                        )
                        tar.addfile(member)
                with self.assertRaises(ValueError):
                    cli.verify_archive(archive, VERSION, cli.TARGETS[0])

    def test_checksums_require_exact_target_set_and_detect_tampering(self):
        self.make_package()
        with self.assertRaisesRegex(ValueError, "exactly"):
            cli.checksum_lines(self.output, VERSION)
        for target in cli.TARGETS[1:]:
            self.make_package(target)
        manifest = self.output / "SHA256SUMS"
        lines = cli.checksum_lines(self.output, VERSION)
        manifest.write_text(lines)
        self.assertEqual(len(lines.splitlines()), 3)
        cli.verify_checksums(self.output, VERSION)
        # Still a valid archive, but a different payload from the published digest.
        self.binary.write_text(self.binary.read_text() + "# changed\n")
        self.make_package()
        with self.assertRaisesRegex(ValueError, "SHA256SUMS"):
            cli.verify_checksums(self.output, VERSION)
        (self.output / "unexpected.tar.gz").write_bytes(b"extra")
        with self.assertRaisesRegex(ValueError, "exactly"):
            cli.checksum_lines(self.output, VERSION)


class RuntimeMetadataTest(unittest.TestCase):
    def test_linux_baseline_and_architectures(self):
        for target, machine in (
            (cli.TARGETS[0], "Advanced Micro Devices X86-64"),
            (cli.TARGETS[1], "AArch64"),
        ):
            cli.check_linux_metadata(
                f"  Machine: {machine}\n",
                " (NEEDED) Shared library: [libc.so.6]\n (NEEDED) Shared library: [libgcc_s.so.1]",
                "GLIBC_2.28 GLIBCXX_3.4.25 CXXABI_1.3.11 GCC_7.0.0",
                target,
            )

    def test_linux_rejects_wrong_arch_new_symbols_and_non_system_libraries(self):
        header = "  Machine: Advanced Micro Devices X86-64\n"
        dynamic = " (NEEDED) Shared library: [libc.so.6]"
        cases = [("  Machine: AArch64\n", dynamic, "GLIBC_2.28")]
        for symbol in (
            "GLIBC_2.29",
            "GLIBCXX_3.4.26",
            "CXXABI_1.3.12",
            "GCC_12.0.0",
            "GLIBC_PRIVATE",
        ):
            cases.append((header, dynamic, symbol))
        for extra in (
            " (NEEDED) Shared library: [libssl.so.3]",
            " (RUNPATH) Library runpath: [/opt/build/lib]",
        ):
            cases.append((header, dynamic + extra, "GLIBC_2.28"))
        for args in cases:
            with self.subTest(args=args), self.assertRaises(ValueError):
                cli.check_linux_metadata(*args, cli.TARGETS[0])

    def test_macos_baseline_and_system_libraries(self):
        cli.check_macos_metadata(
            "arm64\n",
            "cli:\n\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0)",
            "    minos 14.0\n",
        )

    def test_macos_rejects_wrong_arch_version_and_homebrew_dependencies(self):
        for args in (
            ("x86_64", "", "    minos 14.0\n"),
            ("arm64", "", "    minos 15.0\n"),
            ("arm64", "", ""),
            (
                "arm64",
                "\t/opt/homebrew/lib/libssl.dylib (compatibility version 3.0.0)",
                "    minos 14.0\n",
            ),
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                cli.check_macos_metadata(*args)


if __name__ == "__main__":
    unittest.main()
