"""
Takes a list of semver strings and sorts them in ascending order.
"""

import sys
from packaging.version import parse, InvalidVersion

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("prefix", default="v")
    args = parser.parse_args()

    # Read the input from stdin
    lines = sys.stdin.readlines()

    # Parse the versions
    versions = []
    for line in lines:
        line = line.strip()
        try:
            version_str = line.removeprefix(args.prefix)
            version = parse(version_str)
        except InvalidVersion:
            # There are old tags that don't follow the semver format
            print(f"Invalid version: {line}", file=sys.stderr)
            continue
        versions.append((line, version))

    # Sort the versions
    versions.sort(key=lambda x: x[1])

    # Print the sorted versions as original strings
    for line, _ in versions:
        print(line)
