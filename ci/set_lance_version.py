import argparse
import sys
import json


def run_command(command: str) -> str:
    """
    Run a shell commend and return stdout as a string.
    If exit code is not 0, raise an exception with the stderr output.
    """
    import subprocess

    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Command failed with error: {result.stderr.strip()}")
    return result.stdout.strip()


def get_latest_stable_version() -> str:
    version_line = run_command("cargo info lance | grep '^version:'")
    version = version_line.split(" ")[1].strip()
    return version


def get_latest_preview_version() -> str:
    lance_tags = run_command(
        "git ls-remote --tags https://github.com/lancedb/lance.git | grep 'refs/tags/v[0-9beta.-]\\+$'"
    ).splitlines()
    lance_tags = (
        tag.split("refs/tags/")[1]
        for tag in lance_tags
        if "refs/tags/" in tag and "beta" in tag
    )
    from packaging.version import Version

    latest = max(
        (tag[1:] for tag in lance_tags if tag.startswith("v")), key=lambda t: Version(t)
    )
    return str(latest)


def extract_features(line: str) -> list:
    """
    Extracts the features from a line in Cargo.toml.
    Example: 'lance = { "version" = "=0.29.0", "features" = ["dynamodb"] }'
    Returns: ['dynamodb']
    """
    import re

    match = re.search(r'"features"\s*=\s*\[(.*?)\]', line)
    if match:
        features_str = match.group(1)
        return [f.strip('"') for f in features_str.split(",")]
    return []


def update_cargo_toml(line_updater):
    """
    Updates the Cargo.toml file by applying the line_updater function to each line.
    The line_updater function should take a line as input and return the updated line.
    """
    with open("Cargo.toml", "r") as f:
        lines = f.readlines()

    new_lines = []
    for line in lines:
        if line.startswith("lance"):
            # Update the line using the provided function
            new_lines.append(line_updater(line))
        else:
            # Keep the line unchanged
            new_lines.append(line)

    with open("Cargo.toml", "w") as f:
        f.writelines(new_lines)


def set_stable_version(version: str):
    """
    Sets lines to
    lance = { "version" = "=0.29.0", "features" = ["dynamodb"] }
    lance-io = "=0.29.0"
    ...
    """

    def line_updater(line: str) -> str:
        package_name = line.split("=", maxsplit=1)[0].strip()
        features = extract_features(line)
        if features:
            return f'{package_name} = {{ "version" = "={version}", "features" = {json.dumps(features)} }}\n'
        else:
            return f'{package_name} = "={version}"\n'

    update_cargo_toml(line_updater)


def set_preview_version(version: str):
    """
    Sets lines to
    lance = { "version" = "=0.29.0", "features" = ["dynamodb"], tag = "v0.29.0-beta.2", git="https://github.com/lancedb/lance.git" }
    lance-io = { version = "=0.29.0", tag = "v0.29.0-beta.2", git="https://github.com/lancedb/lance.git" }
    ...
    """

    def line_updater(line: str) -> str:
        package_name = line.split("=", maxsplit=1)[0].strip()
        features = extract_features(line)
        base_version = version.split("-")[0]  # Get the base version without beta suffix
        if features:
            return f'{package_name} = {{ "version" = "={base_version}", "features" = {json.dumps(features)}, "tag" = "v{version}", "git" = "https://github.com/lancedb/lance.git" }}\n'
        else:
            return f'{package_name} = {{ "version" = "={base_version}", "tag" = "v{version}", "git" = "https://github.com/lancedb/lance.git" }}\n'

    update_cargo_toml(line_updater)


def set_local_version():
    """
    Sets lines to
    lance = { path = "../lance/rust/lance", features = ["dynamodb"] }
    lance-io = { path = "../lance/rust/lance-io" }
    ...
    """

    def line_updater(line: str) -> str:
        package_name = line.split("=", maxsplit=1)[0].strip()
        features = extract_features(line)
        if features:
            return f'{package_name} = {{ "path" = "../lance/rust/{package_name}", "features" = {json.dumps(features)} }}\n'
        else:
            return f'{package_name} = {{ "path" = "../lance/rust/{package_name}" }}\n'

    update_cargo_toml(line_updater)


parser = argparse.ArgumentParser(description="Set the version of the Lance package.")
parser.add_argument(
    "version",
    type=str,
    help="The version to set for the Lance package. Use 'stable' for the latest stable version, 'preview' for latest preview version, or a specific version number (e.g., '0.1.0'). You can also specify 'local' to use a local path.",
)
args = parser.parse_args()

if args.version == "stable":
    latest_stable_version = get_latest_stable_version()
    print(
        f"Found latest stable version: \033[1mv{latest_stable_version}\033[0m",
        file=sys.stderr,
    )
    set_stable_version(latest_stable_version)
elif args.version == "preview":
    latest_preview_version = get_latest_preview_version()
    print(
        f"Found latest preview version: \033[1mv{latest_preview_version}\033[0m",
        file=sys.stderr,
    )
    set_preview_version(latest_preview_version)
elif args.version == "local":
    set_local_version()
else:
    # Parse the version number.
    version = args.version
    # Ignore initial v if present.
    if version.startswith("v"):
        version = version[1:]

    if "beta" in version:
        set_preview_version(version)
    else:
        set_stable_version(version)

print("Updating lockfiles...", file=sys.stderr, end="")
run_command("cargo metadata > /dev/null")
print(" done.", file=sys.stderr)
