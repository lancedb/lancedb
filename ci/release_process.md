
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
will upload them to a draft release. Wait for those jobs to complete.

### 3. Publish the release

Once the jobs are complete, you can edit the 

2. Push a tag, such as vX.X.X. Once the tag is pushrf, GitHub actions will start
   building the native libraries and uploading them to a draft release. Wait for
   those jobs to complete.
3. If the libraries are successful, edit the changelog and then publish the
   release. Once you publish, a new action will start and upload all the 
   release artifacts to npm.

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

For debugging, use this snippet:

```shell
ARCH=aarch64
docker run -it \
    -v $(pwd):/io -w /io \
    quay.io/pypa/musllinux_1_1_$ARCH \
    bash
```
-->

```
docker run \
    -v $(pwd):/io -w /io \
    quay.io/pypa/musllinux_1_1_aarch64 \
    bash alpine_repro.sh
```