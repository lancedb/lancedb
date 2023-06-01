#!/bin/bash
set -e

# if cargo bump isn't installed return an error
if ! cargo set-version &> /dev/null
then
    echo "cargo-edit could not be found. Install with `cargo install cargo-edit`"
    exit
fi

BUMP_PART=${1:-patch}

# if BUMP_PART isn't patch, minor, or major return an error
if [ "$BUMP_PART" != "patch" ] && [ "$BUMP_PART" != "minor" ] && [ "$BUMP_PART" != "major" ]
then
    echo "BUMP_PART must be one of patch, minor, or major"
    exit
fi

function get_crate_version() {
    cargo pkgid -p $1 | cut -d@ -f2 | cut -d# -f2
}

# First, validate versions are starting as same
VECTORDB_VERSION=$(get_crate_version vectordb)
FFI_NODE_VERSION=$(get_crate_version vectordb-node)

# FYI, we pipe all output to /dev/null because the only thing we want to ouput
# if success is the new tag. This way it can be then used with `git tag`.
pushd node > /dev/null
NODE_VERSION=$(npm pkg get version | xargs echo)
popd > /dev/null

if [ "$VECTORDB_VERSION" != "$FFI_NODE_VERSION" ] || [ "$VECTORDB_VERSION" != "$NODE_VERSION" ]
then
    echo "Version mismatch between rust/vectordb, rust/ffi/node, and node"
    echo "rust/vectordb: $VECTORDB_VERSION"
    echo "rust/ffi/node: $FFI_NODE_VERSION"
    echo "node: $NODE_VERSION"
    exit
fi

cargo set-version --bump $BUMP_PART > /dev/null 2>&1
NEW_VERSION=$(get_crate_version vectordb)

pushd node > /dev/null
npm version $BUMP_PART > /dev/null

# Also need to update version of the native modules
NATIVE_MODULES=$(npm pkg get optionalDependencies | jq 'keys[]' | grep @vectordb/ | tr -d '"')
for module in $NATIVE_MODULES
do
    npm install $module@$NEW_VERSION --save-optional > /dev/null
done
popd > /dev/null


echo $NEW_VERSION
