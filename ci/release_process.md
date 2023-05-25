
How to release the node module

### 1. Bump the versions

<!-- TODO: we also need to bump the optional dependencies for node! -->

```shell
pushd rust/vectordb
cargo bump minor
popd

pushd rust/ffi/node
cargo bump minor
popd

pushd python
cargo bump minor
popd

pushd node
npm version minor
popd

git add -u
git commit -m "Bump versions"
git push
```

### 2. Push a new tag

```shell
git tag vX.X.X
git push --tag vX.X.X
```

When the tag is pushed, GitHub actions will start building the libraries and
will upload them to a draft release.

While those jobs are running, edit the release notes as needed. For example, 
bring relevant new features and bugfixes to the top of the notes and the testing
and CI changes to the bottom.

Once the jobs have finished, the release will be marked as not draft and the
artifacts will be released to crates.io, NPM, and PyPI.

## Manual process

You can build the artifacts locally on a MacOS machine.

### Build the MacOS release libraries

One-time setup:

```shell
rustup target add x86_64-apple-darwin aarch64-apple-darwin
```

To build:

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

You can change `ARCH` to `x86_64`.

Similar script for musl binaries:

```shell
ARCH=aarch64
docker run \
    -v $(pwd):/io -w /io \
    quay.io/pypa/musllinux_1_1_$ARCH \
    bash ci/build_linux_artifacts.sh $ARCH-unknown-linux-musl
```

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

```
docker run \
    -v $(pwd):/io -w /io \
    quay.io/pypa/musllinux_1_1_aarch64 \
    bash alpine_repro.sh
```