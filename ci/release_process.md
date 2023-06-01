# How to release

This is for the Rust crate and Node module. For now, the Python module is
released separately.

<!--
The release is started by bumping the versions and pushing a new tag. To do this
automatically, use the `make_release_commit` GitHub action.

When the tag is pushed, GitHub actions will start building the libraries and
will upload them to a draft release.

While those jobs are running, edit the release notes as needed. For example, 
bring relevant new features and bugfixes to the top of the notes and the testing
and CI changes to the bottom.

Once the jobs have finished, the release will be marked as not draft and the
artifacts will be released to crates.io, NPM, and PyPI.

-->

## Manual process

The manual release process can be completed on a MacOS machine.

### Bump the versions

You can use the script `ci/bump_versions.sh` to bump the versions. It defaults
to a `patch` bump, but you can also pass `minor` and `major`. Once you have the
tag created, push it to GitHub.

```shell
VERSION=$(bash ci/bump_versions.sh)
git tag v$VERSION-nodejs
git push origin v$VERSION--nodejs
```

### Build the MacOS release libraries

One-time setup:

```shell
rustup target add x86_64-apple-darwin aarch64-apple-darwin
```

To build both x64 and arm64, run `ci/build_macos_artifacts.sh` without any args:

```shell
bash ci/build_macos_artifacts.sh
```

### Build the Linux release libraries

To build a Linux library, we need to use docker with a different build script:

```shell
ARCH=aarch64
docker run \
    -v $(pwd):/io -w /io \
    quay.io/pypa/manylinux2014_$ARCH \
    bash ci/build_linux_artifacts.sh $ARCH-unknown-linux-gnu
```

For x64, change `ARCH` to `x86_64`. NOTE: compiling for a different architecture
than your machine in Docker is very slow. It's best to do this on a machine with
matching architecture.


<!--
Similar script for musl binaries (not yet working):

```shell
ARCH=aarch64
docker run \
    --user $(id -u) \
    -v $(pwd):/io -w /io \
    quay.io/pypa/musllinux_1_1_$ARCH \
    bash ci/build_linux_artifacts.sh $ARCH-unknown-linux-musl
```

-->

<!--

For debugging, use these snippets:

```shell
ARCH=aarch64
docker run -it \
    -v $(pwd):/io -w /io \
    quay.io/pypa/manylinux2014_$ARCH \
    bash
```

```shell
ARCH=aarch64
docker run -it \
    -v $(pwd):/io -w /io \
    quay.io/pypa/musllinux_1_1_$ARCH \
    bash
```

Note: musllinux_1_1 is Alpine Linux 3.12
-->


### Build the npm module

To build the typescript and create a release tarball, run:

```shell
npm ci
npm tsc
npm pack
```

### Release to npm

Assuming you still have `VERSION` set from earlier:

```shell
pushd node
npm publish lancedb-vectordb-$VERSION.tgz
for tarball in ./dist/lancedb-vectordb-*-$VERSION.tgz;
    do 
    npm publish $tarball
done
popd
```

### Release to crates.io

```shell
cargo publish -p vectordb
cargo publish -p vectordb-node
```
