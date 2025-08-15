import argparse
import sys
import json


def run_command(command: str) -> str:
    """
    Run a shell command and return stdout as a string.
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

    match = re.search(r'"features"\s*=\s*\[\s*(.*?)\s*\]', line, re.DOTALL)
    if match:
        features_str = match.group(1)
        return [f.strip('"') for f in features_str.split(",") if len(f) > 0]
    return []


def extract_default_features(line: str) -> bool:
    """
    Checks if default-features = false is present in a line in Cargo.toml.
    Example: 'lance = { "version" = "=0.29.0", default-features = false, "features" = ["dynamodb"] }'
    Returns: True if default-features = false is present, False otherwise
    """
    import re

    match = re.search(r'default-features\s*=\s*false', line)
    return match is not None


def dict_to_toml_line(package_name: str, config: dict) -> str:
    """
    Converts a configuration dictionary to a TOML dependency line.
    Dictionary insertion order is preserved (Python 3.7+), so the caller
    controls the order of fields in the output.

    Args:
        package_name: The name of the package (e.g., "lance", "lance-io")
        config: Dictionary with keys like "version", "path", "git", "tag", "features", "default-features"
                The order of keys in this dict determines the order in the output.

    Returns:
        A properly formatted TOML line with a trailing newline
    """
    # If only version is specified, use simple format
    if len(config) == 1 and "version" in config:
        return f'{package_name} = "{config["version"]}"\n'

    # Otherwise, use inline table format
    parts = []
    for key, value in config.items():
        if key == "default-features" and not value:
            parts.append("default-features = false")
        elif key == "features":
            parts.append(f'"features" = {json.dumps(value)}')
        elif isinstance(value, str):
            parts.append(f'"{key}" = "{value}"')
        else:
            # This shouldn't happen with our current usage
            parts.append(f'"{key}" = {json.dumps(value)}')

    return f'{package_name} = {{ {", ".join(parts)} }}\n'


def update_cargo_toml(line_updater):
    """
    Updates the Cargo.toml file by applying the line_updater function to each line.
    The line_updater function should take a line as input and return the updated line.
    """
    with open("Cargo.toml", "r") as f:
        lines = f.readlines()

    new_lines = []
    lance_line = ""
    is_parsing_lance_line = False
    for line in lines:
        if line.startswith("lance"):
            # Check if this is a single-line or multi-line entry
            # Single-line entries either:
            # 1. End with } (complete inline table)
            # 2. End with " (simple version string)
            # Multi-line entries start with { but don't end with }
            if line.strip().endswith("}") or line.strip().endswith('"'):
                # Single-line entry - process immediately
                new_lines.append(line_updater(line))
            elif "{" in line and not line.strip().endswith("}"):
                # Multi-line entry - start accumulating
                lance_line = line
                is_parsing_lance_line = True
            else:
                # Single-line entry without quotes or braces (shouldn't happen but handle it)
                new_lines.append(line_updater(line))
        elif is_parsing_lance_line:
            lance_line += line
            if line.strip().endswith("}"):
                new_lines.append(line_updater(lance_line))
                lance_line = ""
                is_parsing_lance_line = False
        else:
            # Keep the line unchanged
            new_lines.append(line)

    with open("Cargo.toml", "w") as f:
        f.writelines(new_lines)


def set_stable_version(version: str):
    """
    Sets lines to
    lance = { "version" = "=0.29.0", default-features = false, "features" = ["dynamodb"] }
    lance-io = { "version" = "=0.29.0", default-features = false }
    ...
    """

    def line_updater(line: str) -> str:
        package_name = line.split("=", maxsplit=1)[0].strip()

        # Build config in desired order: version, default-features, features
        config = {"version": f"={version}"}

        if extract_default_features(line):
            config["default-features"] = False

        features = extract_features(line)
        if features:
            config["features"] = features

        return dict_to_toml_line(package_name, config)

    update_cargo_toml(line_updater)


def set_preview_version(version: str):
    """
    Sets lines to
    lance = { "version" = "=0.29.0", default-features = false, "features" = ["dynamodb"], "tag" = "v0.29.0-beta.2", "git" = "https://github.com/lancedb/lance.git" }
    lance-io = { "version" = "=0.29.0", default-features = false, "tag" = "v0.29.0-beta.2", "git" = "https://github.com/lancedb/lance.git" }
    ...
    """

    def line_updater(line: str) -> str:
        package_name = line.split("=", maxsplit=1)[0].strip()
        base_version = version.split("-")[0]  # Get the base version without beta suffix

        # Build config in desired order: version, default-features, features, tag, git
        config = {"version": f"={base_version}"}

        if extract_default_features(line):
            config["default-features"] = False

        features = extract_features(line)
        if features:
            config["features"] = features

        config["tag"] = f"v{version}"
        config["git"] = "https://github.com/lancedb/lance.git"

        return dict_to_toml_line(package_name, config)

    update_cargo_toml(line_updater)


def set_local_version():
    """
    Sets lines to
    lance = { "path" = "../lance/rust/lance", default-features = false, "features" = ["dynamodb"] }
    lance-io = { "path" = "../lance/rust/lance-io", default-features = false }
    ...
    """

    def line_updater(line: str) -> str:
        package_name = line.split("=", maxsplit=1)[0].strip()

        # Build config in desired order: path, default-features, features
        config = {"path": f"../lance/rust/{package_name}"}

        if extract_default_features(line):
            config["default-features"] = False

        features = extract_features(line)
        if features:
            config["features"] = features

        return dict_to_toml_line(package_name, config)

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
