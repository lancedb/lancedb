import tomllib

found_preview_lance = False

with open("Cargo.toml", "rb") as f:
    cargo_data = tomllib.load(f)

    for name, dep in cargo_data["workspace"]["dependencies"].items():
        if name == "lance" or name.startswith("lance-"):
            if isinstance(dep, str):
                version = dep
            elif isinstance(dep, dict):
                # Version doesn't have the beta tag in it, so we instead look
                # at the git tag.
                version = dep.get('tag', dep.get('version'))
            else:
                raise ValueError("Unexpected type for dependency: " + str(dep))

            if "beta" in version:
                found_preview_lance = True
                print(f"Dependency '{name}' is a preview version: {version}")

with open("python/pyproject.toml", "rb") as f:
    py_proj_data = tomllib.load(f)

    for dep in py_proj_data["project"]["dependencies"]:
        if dep.startswith("pylance"):
            if "b" in dep:
                found_preview_lance = True
                print(f"Dependency '{dep}' is a preview version")
            break  # Only one pylance dependency

if found_preview_lance:
    raise ValueError("Found preview version of Lance in dependencies")
