"""Package and validate standalone CLI release assets (Python 3.12+).

Run `version` before building, `package` on each native runner, then `checksums`
after gathering the archives. Run `verify` again immediately before publication.
"""

import argparse
import hashlib
import os
import re
import subprocess
import tarfile
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGETS = (
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "aarch64-apple-darwin",
)
NOTICES = ("LICENSE", "RUST_THIRD_PARTY_LICENSES.html")


def release_version(root: Path, ref: str) -> str:
    config = tomllib.loads((root / ".bumpversion.toml").read_text())["tool"][
        "bumpversion"
    ]
    version = config["current_version"]
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:-beta\.\d+)?", version):
        raise ValueError(f"Unsupported release version: {version}")
    for manifest in ("rust/lancedb/Cargo.toml", "rust/lancedb/cli/Cargo.toml"):
        package = tomllib.loads((root / manifest).read_text())["package"]
        if package["version"] != version:
            raise ValueError(f"{manifest} version does not match {version}")
    if ref.startswith("refs/tags/") and ref != f"refs/tags/v{version}":
        raise ValueError(f"Tag {ref} does not match v{version}")
    return version


def archive_stem(version: str, target: str) -> str:
    return f"lancedb-v{version}-{target}"


def command(*args: str) -> str:
    return subprocess.check_output(args, text=True)


def check_linux_metadata(header: str, dynamic: str, symbols: str, target: str):
    machine = (
        "AArch64" if target.startswith("aarch64") else "Advanced Micro Devices X86-64"
    )
    if not re.search(rf"Machine:\s+{re.escape(machine)}\s*$", header, re.MULTILINE):
        raise ValueError(f"ELF architecture does not match {target}")
    allowed = {
        "libc.so.6",
        "libdl.so.2",
        "libm.so.6",
        "libpthread.so.0",
        "librt.so.1",
        "libgcc_s.so.1",
        "libstdc++.so.6",
        "ld-linux-x86-64.so.2",
        "ld-linux-aarch64.so.1",
    }
    needed = set(re.findall(r"\(NEEDED\).*\[([^\]]+)\]", dynamic))
    if not needed or needed - allowed:
        raise ValueError(f"Unexpected runtime libraries: {sorted(needed - allowed)}")
    if re.search(r"\((?:RPATH|RUNPATH)\)", dynamic):
        raise ValueError("CLI must not depend on a build-container runtime path")
    # AlmaLinux 8 provides glibc 2.28, GCC 8's libstdc++, and libgcc.
    limits = {
        "GLIBC": (2, 28),
        "GLIBCXX": (3, 4, 25),
        "CXXABI": (1, 3, 11),
        "GCC": (7, 0, 0),
    }
    for namespace, limit in limits.items():
        for value in re.findall(rf"\b{namespace}_([\d.]+)\b", symbols):
            if tuple(map(int, value.split("."))) > limit:
                raise ValueError(
                    f"Runtime symbol {namespace}_{value} exceeds Linux baseline"
                )
    if "GLIBC_PRIVATE" in symbols:
        raise ValueError("CLI must not require private glibc symbols")


def check_macos_metadata(arches: str, libraries: str, build: str):
    if arches.strip() != "arm64":
        raise ValueError("CLI must contain exactly the arm64 architecture")
    for library in re.findall(r"^\s+(\S+) \(", libraries, re.MULTILINE):
        if not library.startswith(("/usr/lib/", "/System/Library/")):
            raise ValueError(f"Non-system macOS runtime library: {library}")
    minimum = re.findall(r"^\s*minos ([\d.]+)\s*$", build, re.MULTILINE)
    if len(minimum) != 1 or tuple(map(int, minimum[0].split("."))) != (14, 0):
        raise ValueError("CLI must target macOS 14.0")


def check_runtime(binary: Path, target: str):
    path = str(binary.resolve())
    if target.endswith("linux-gnu"):
        check_linux_metadata(
            command("readelf", "-h", path),
            command("readelf", "-d", path),
            command("readelf", "--version-info", path),
            target,
        )
    else:
        command("codesign", "--verify", "--strict", path)
        check_macos_metadata(
            command("lipo", "-archs", path),
            command("otool", "-L", path),
            command("vtool", "-show-build", path),
        )


def verify_archive(archive: Path, version: str, target: str):
    stem = archive_stem(version, target)
    expected = {f"{stem}/{name}" for name in ("lancedb", *NOTICES)}
    with tarfile.open(archive, "r:gz") as tar:
        members = tar.getmembers()
        if len(members) != len(expected) or {m.name for m in members} != expected:
            raise ValueError(f"Unexpected archive contents: {archive.name}")
        for member in members:
            mode = 0o755 if member.name.endswith("/lancedb") else 0o644
            if not member.isfile() or member.mode != mode or member.size == 0:
                raise ValueError(f"Invalid archive member: {member.name}")


def smoke_test(archive: Path, version: str, target: str):
    verify_archive(archive, version, target)
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(directory, filter="data")
        binary = directory / archive_stem(version, target) / "lancedb"
        home = directory / "home"
        home.mkdir()
        env = {k: v for k, v in os.environ.items() if not k.startswith("LANCEDB_")}
        env.update(HOME=str(home), XDG_CONFIG_HOME=str(home / ".config"))
        for args in (("--help",), ("--version",), ("profile", "list")):
            output = subprocess.run(
                [str(binary), *args],
                cwd=home,
                env=env,
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if args == ("--version",) and output.stdout.strip() != f"lancedb {version}":
                raise ValueError(
                    f"Packaged CLI reports the wrong version: {output.stdout!r}"
                )
            if args == ("--help",) and "Usage:" not in output.stdout:
                raise ValueError("Packaged CLI did not print help")
            if (
                args == ("profile", "list")
                and "Configured profiles:" not in output.stdout
            ):
                raise ValueError("Packaged CLI did not list profiles")


def package(root: Path, binary: Path, target: str, output: Path, version: str) -> Path:
    check_runtime(binary, target)
    output.mkdir(parents=True, exist_ok=True)
    stem = archive_stem(version, target)
    archive = output / f"{stem}.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for name, source in [
            ("lancedb", binary),
            *((name, root / name) for name in NOTICES),
        ]:
            member = tar.gettarinfo(str(source), arcname=f"{stem}/{name}")
            member.mode = 0o755 if name == "lancedb" else 0o644
            member.uid = member.gid = 0
            member.uname = member.gname = ""
            with source.open("rb") as file:
                tar.addfile(member, file)
    smoke_test(archive, version, target)
    return archive


def checksum_lines(output: Path, version: str) -> str:
    expected = {f"{archive_stem(version, target)}.tar.gz": target for target in TARGETS}
    found = {path.name for path in output.glob("*.tar.gz")}
    if found != expected.keys():
        raise ValueError(
            f"Expected exactly these CLI archives: {sorted(expected)}; found {sorted(found)}"
        )
    lines = []
    for name, target in sorted(expected.items()):
        archive = output / name
        verify_archive(archive, version, target)
        with archive.open("rb") as file:
            digest = hashlib.file_digest(file, "sha256").hexdigest()
        lines.append(f"{digest}  {name}\n")
    return "".join(lines)


def verify_checksums(output: Path, version: str):
    if (output / "SHA256SUMS").read_text() != checksum_lines(output, version):
        raise ValueError("SHA256SUMS does not match the CLI release archives")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("version")
    build = commands.add_parser("package")
    build.add_argument("--binary", type=Path, required=True)
    build.add_argument("--target", choices=TARGETS, required=True)
    build.add_argument("--output", type=Path, required=True)
    for name in ("checksums", "verify"):
        commands.add_parser(name).add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    version = release_version(ROOT, os.environ.get("GITHUB_REF", ""))
    if args.command == "version":
        print(version)
    elif args.command == "package":
        print(package(ROOT, args.binary, args.target, args.output, version))
    elif args.command == "checksums":
        (args.output / "SHA256SUMS").write_text(checksum_lines(args.output, version))
    else:
        verify_checksums(args.output, version)


if __name__ == "__main__":
    main()
