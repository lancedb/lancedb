# Release process

There are five total packages we release. Three are the `lancedb` packages
for Python, Rust, and Node.js. The other two are the legacy `vectordb`
packages for Rust and node.js.

The Python package is versioned and released separately from the Rust and Node.js
ones. For Rust and Node.js, the release process is shared between `lancedb` and
`vectordb` for now.

## Breaking changes

We try to avoid breaking changes, but sometimes they are necessary. When there
are breaking changes, we will increment the minor version. (This is valid 
semantic versioning because we are still in `0.x` versions.)

When a PR makes a breaking change, the PR author should mark the PR using the 
conventional commit markers: either exclamation mark after the type
(such as `feat!: change signature of func`) or have `BREAKING CHANGE` in the
body of the PR. A CI job will add a `breaking-change` label to the PR, which is
what will ultimately be used to CI to determine if the minor version should be
incremented.

A CI job will validate that if a `breaking-change` label is added, the minor
version is incremented in the `Cargo.toml` and `pyproject.toml` files. The only
exception is if it has already been incremented since the last stable release.

**It is the responsibility of the PR author to increment the minor version when
appropriate.**

Some things that are considered breaking changes:

* Upgrading `lance` to a new minor version. Minor version bumps in Lance are
  considered breaking changes during `0.x` releases. This can change behavior
  in LanceDB.
* Upgrading a dependency pin that is in the Rust API. In particular, upgrading
  `DataFusion` and `Arrow` are breaking changes. Changing dependencies that are
  not exposed in our public API are not considered breaking changes.
* Changing the signature of a public function or method.
* Removing a public function or method.

We do make exceptions for APIs that are marked as experimental. These are APIs
that are under active development and not in major use. These changes should not
receive the `breaking-change` label.
