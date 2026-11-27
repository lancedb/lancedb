#!/bin/sh
# Run from repository root. The fake transport serves a deliberately corrupt archive.
set -eu
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export HOME="$work/home" LANCEDB_VERSION=v0.42.0-beta.19
unset LANCEDB_INSTALL_DIR
mkdir -p "$work/bin" "$HOME"
export PATH="$work/bin:$PATH"
cat > "$work/bin/curl" <<'MOCK'
#!/bin/sh
while [ "$1" != -o ]; do shift; done
case "$3" in
    */SHA256SUMS)
        for target in x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin; do
            printf '%064d  lancedb-%s-%s.tar.gz\n' 0 "$LANCEDB_VERSION" "$target"
        done > "$2" ;;
    *) printf 'corrupt archive' > "$2" ;;
esac
MOCK
chmod +x "$work/bin/curl"
reject() {
    if sh scripts/install.sh > "$work/log" 2>&1; then
        echo "Unexpected install success" >&2
        exit 1
    fi
    grep -F "$1" "$work/log"
}

printf 'existing admin CLI\n' > "$work/bin/lancedb"
chmod +x "$work/bin/lancedb"
reject 'on PATH'
test ! -e "$HOME/.local/bin/lancedb"
export LANCEDB_INSTALL_DIR="$work/separate bin"
mkdir -p "$LANCEDB_INSTALL_DIR"
ln -s "$work/bin/lancedb" "$LANCEDB_INSTALL_DIR/lancedb"
reject 'already exists'
test -L "$LANCEDB_INSTALL_DIR/lancedb"
rm "$LANCEDB_INSTALL_DIR/lancedb"
reject 'Checksum mismatch'
test -z "$(ls -A "$LANCEDB_INSTALL_DIR")"
test "$(cat "$work/bin/lancedb")" = 'existing admin CLI'
