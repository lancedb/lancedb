# How to release

This is for the Rust crate and Node module. For now, the Python module is
released separately.

The release is started by bumping the versions and pushing a new tag. To do this
automatically, use the `make_release_commit` GitHub action.

When the tag is pushed, GitHub actions will start building the libraries and
will upload them to a draft release.

While those jobs are running, edit the release notes as needed. For example, 
bring relevant new features and bugfixes to the top of the notes and the testing
and CI changes to the bottom.

Once the jobs have finished, the release will be marked as not draft and the
artifacts will be released to crates.io, NPM, and PyPI.

## Manual process

You can also build the artifacts locally on a MacOS machine.

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

Similar script for musl binaries (not yet working):

```shell
ARCH=aarch64
docker run \
    --user $(id -u) \
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