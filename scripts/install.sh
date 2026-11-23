#!/bin/sh
# Install the public LanceDB command-line client from GitHub Releases.
set -eu

fail() {
    printf 'lancedb installer: %s\n' "$*" >&2
    exit 1
}

download() {
    curl -q --proto '=https' --proto-redir '=https' -fsSL "$@"
}

main() {
    [ "$#" -eq 0 ] || fail 'Set LANCEDB_VERSION and LANCEDB_INSTALL_DIR via the environment.'
    releases=https://github.com/lancedb/lancedb/releases
    platform=$(uname -s)-$(uname -m)
    case "$platform" in
        Darwin-arm64) target=aarch64-apple-darwin ;;
        Linux-aarch64|Linux-arm64) target=aarch64-unknown-linux-gnu ;;
        Linux-x86_64) target=x86_64-unknown-linux-gnu ;;
        *) fail "No CLI archive for $platform. Supported: Apple Silicon macOS, Linux ARM64 and Linux x86-64." ;;
    esac

    case "$target" in
        *-linux-gnu)
            libc=$(getconf GNU_LIBC_VERSION 2>/dev/null) || fail 'Linux requires glibc 2.28 or newer.'
            printf '%s\n' "$libc" | awk -F '[ .]' '$1 == "glibc" && ($2 > 2 || ($2 == 2 && $3 >= 28)) { ok=1 } END { exit !ok }' ||
                fail 'Linux requires glibc 2.28 or newer.'
            if [ "$target" = x86_64-unknown-linux-gnu ]; then
                for feature in avx2 fma f16c; do
                    grep -qw "$feature" /proc/cpuinfo || fail "The Linux x86-64 binary requires CPU feature $feature."
                done
            fi ;;
        *-apple-darwin)
            os_version=$(sw_vers -productVersion)
            [ "${os_version%%.*}" -ge 14 ] || fail 'Apple Silicon requires macOS 14 or newer.' ;;
    esac

    dir=${LANCEDB_INSTALL_DIR:-"$HOME/.local/bin"}
    case "$dir" in /*) ;; *) fail 'LANCEDB_INSTALL_DIR must be an absolute path.' ;; esac
    dest=$dir/lancedb
    if [ -e "$dest" ] || [ -L "$dest" ]; then
        fail "$dest already exists. Move it first or choose a separate LANCEDB_INSTALL_DIR."
    fi
    if existing=$(command -v lancedb); then
        if [ -z "${LANCEDB_INSTALL_DIR:-}" ]; then
            fail "Found $existing on PATH. To install alongside it, set LANCEDB_INSTALL_DIR to a separate directory."
        fi
    fi

    tag=${LANCEDB_VERSION:-}
    if [ -z "$tag" ]; then
        latest=$(download --head --output /dev/null --write-out '%{url_effective}' "$releases/latest")
        tag=${latest##*/}
    fi
    version=${tag#v}
    # Match the stable and beta tag formats supported by ci/package_cli.py.
    case "$version" in *[!0-9a-z.-]*|'') fail "Invalid release version: $tag" ;; esac
    printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-beta\.[0-9]+)?$' ||
        fail "Invalid release version: $tag"

    stem=lancedb-v$version-$target
    archive=$stem.tar.gz
    url=$releases/download/v$version
    mkdir -p "$dir"
    # Stage on the destination filesystem so the final hard link is atomic and cannot overwrite a file.
    stage=$(mktemp -d "$dir/.lancedb-install.XXXXXXXX")
    trap 'rm -rf "$stage"' EXIT
    trap 'exit 1' HUP INT TERM

    download -o "$stage/SHA256SUMS" "$url/SHA256SUMS" ||
        fail "Could not fetch CLI checksums for v$version. Choose a release with CLI assets using LANCEDB_VERSION."
    expected=$(awk -v name="$archive" '$2 == name { count++; hash=$1 } END { if (count != 1) exit 1; print hash }' "$stage/SHA256SUMS") ||
        fail "Expected exactly one checksum for $archive."
    download -o "$stage/$archive" "$url/$archive"
    if command -v sha256sum >/dev/null 2>&1; then
        actual=$(sha256sum < "$stage/$archive")
    else
        actual=$(shasum -a 256 < "$stage/$archive")
    fi
    [ "${actual%% *}" = "$expected" ] || fail "Checksum mismatch for $archive."

    tar -xzOf "$stage/$archive" "$stem/lancedb" > "$stage/lancedb"
    chmod 755 "$stage/lancedb"
    reported=$("$stage/lancedb" --version) ||
        fail "Cannot run $target on this machine. Check the CPU and OS requirements in the CLI install guide."
    [ "$reported" = "lancedb $version" ] || fail "Unexpected CLI version: $reported"
    ln "$stage/lancedb" "$dest"
    printf 'Installed %s at %s\n' "$reported" "$dest"
    case ":${PATH:-}:" in
        *:"$dir":*) ;;
        *) printf 'Add %s to PATH or invoke the full path above.\n' "$dir" ;;
    esac
}

# Keep the entry point last so a truncated download cannot start installation.
main "$@"
