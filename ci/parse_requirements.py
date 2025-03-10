import argparse
import toml


def parse_dependencies(pyproject_path, extras=None):
    with open(pyproject_path, "r") as file:
        pyproject = toml.load(file)

    dependencies = pyproject.get("project", {}).get("dependencies", [])
    for dependency in dependencies:
        print(dependency)

    optional_dependencies = pyproject.get("project", {}).get(
        "optional-dependencies", {}
    )

    if extras:
        for extra in extras.split(","):
            for dep in optional_dependencies.get(extra, []):
                print(dep)


def main():
    parser = argparse.ArgumentParser(
        description="Generate requirements.txt from pyproject.toml"
    )
    parser.add_argument("path", type=str, help="Path to pyproject.toml")
    parser.add_argument(
        "--extras",
        type=str,
        help="Comma-separated list of extras to include",
        default="",
    )

    args = parser.parse_args()

    parse_dependencies(args.path, args.extras)


if __name__ == "__main__":
    main()
