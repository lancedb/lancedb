#!/usr/bin/env bash
# Run from the repository root with bump-my-version and packaging installed.
set -euo pipefail

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/repo" "$work/bin"

# Release tagging must still include Java's versions when Maven cannot update them.
cat > "$work/bin/mvn" <<'MOCK'
#!/bin/sh
echo "Unable to run Maven versions plugin" >&2
exit 1
MOCK
chmod +x "$work/bin/mvn"
export PATH="$work/bin:$PATH"

git ls-files -z .bumpversion.toml ci/bump_version.sh ci/semver_sort.py \
    nodejs/package.json 'nodejs/npm/*/package.json' nodejs/Cargo.toml \
    rust/lancedb/Cargo.toml rust/lancedb/cli/Cargo.toml python/Cargo.toml \
    docs/src/java/java.md java/pom.xml java/lancedb-core/pom.xml |
    tar --null -T - -cf - | tar -xf - -C "$work/repo"
cd "$work/repo"

# The breaking-change check needs GitHub access and runs after the release tag is set.
printf 'pass\n' > ci/check_breaking_changes.py
git init --quiet
git config user.name "Release test"
git config user.email "release-test@example.com"
git add .
git commit --quiet --message "Release baseline"
git tag v0.0.0

current_version=$(python -c 'import tomllib; print(tomllib.load(open(".bumpversion.toml", "rb"))["tool"]["bumpversion"]["current_version"])')
git tag "v$current_version"

check_java_release() {
    local release_type=$1
    local expected_version=$2
    bash ci/bump_version.sh "$release_type" false > "$work/bump.log" 2>&1 || {
        cat "$work/bump.log"
        return 1
    }
    python - "$release_type" "$expected_version" <<'PY'
import subprocess
import sys
import xml.etree.ElementTree as ET

release_type, expected_version = sys.argv[1:]
case = f"test_java_release_version_matches_tag ({release_type})"
tag = subprocess.check_output(["git", "describe", "--tags", "--exact-match", "HEAD"], text=True).strip()
assert tag == f"v{expected_version}", f"{case}: expected v{expected_version}, got {tag}"
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
for path, version_path in (
    ("java/pom.xml", "m:version"),
    ("java/lancedb-core/pom.xml", "m:parent/m:version"),
):
    content = subprocess.check_output(["git", "show", f"{tag}:{path}"])
    version = ET.fromstring(content).findtext(version_path, namespaces=namespace)
    assert version == expected_version, f"{case}: {path}: expected {expected_version}, got {version}"
print(f"{case}: passed")
PY
}

case "$current_version" in
    *-beta.*)
        preview_version="${current_version%.*}.$((${current_version##*.} + 1))"
        ;;
    *)
        preview_version="${current_version%.*}.$((${current_version##*.} + 1))-beta.0"
        ;;
esac
check_java_release preview "$preview_version"
check_java_release stable "${preview_version%-beta.*}"
