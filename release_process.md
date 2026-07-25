# Release process

We release four `lancedb` packages: Python, Rust, Java, and Node.js.

All four share a single version number, defined by `current_version` in
`.bumpversion.toml`. One `vX.Y.Z` tag releases all of them, so a breaking change
in any SDK bumps the minor version for every SDK.

> [!NOTE]
> Python used to be versioned separately, under `python-vX.Y.Z` tags. It ran
> three minor versions ahead of the other SDKs, which made the two numbers hard
> to reason about. Both tracks were merged at `v0.37.0`: the Python line went
> `0.36` → `0.37` as usual, while Rust, Java, and Node.js jumped `0.33` → `0.37`
> to catch up. Tags before `v0.37.0` follow the old split scheme.

## Preview releases

LanceDB has full releases about every 2 weeks, but in between we make frequent
preview releases. These are released as `0.x.y.betaN` versions. They receive the
same level of testing as normal releases and let you get access to the latest
features. However, we do not guarantee that preview releases will be available
more than 6 months after they are released. We may delete the preview releases
from the packaging index after a while. Once your application is stable, we
recommend switching to full releases, which will never be removed from package
indexes.

## Making releases

The release process uses a handful of GitHub actions to automate the process.

```text
  ┌─────────────────────┐
  │Create Release Commit│
  └─┬───────────────────┘
    │                    ┌──────────────┐
    └──►(tag) vX.Y.Z ─┬─►│GitHub Release├───►GH Release
                      │  └──────────────┘
                      │  ┌────────────┐
                      ├─►│PyPI Publish├─────►Python Wheels
                      │  └────────────┘
                      │  ┌───────────┐
                      ├─►│NPM Publish├──────►NPM Packages
                      │  └───────────┘
                      │  ┌─────────────┐
                      ├─►│Cargo Publish├────►Cargo Release
                      │  └─────────────┘
                      │  ┌─────────────┐
                      └─►│Maven Publish├────►Java Maven Repo Release
                         └─────────────┘
```

To start a release, trigger a `Create Release Commit` action from
[the workflows page](https://github.com/lancedb/lancedb/actions/workflows/make-release-commit.yml)
(Click on "Run workflow").

* **For a preview release**, leave the default parameters.
* **For a stable release**, set the `release_type` input to `stable`.

> [!IMPORTANT]
> If there was a breaking change since the last stable release, and we haven't
> done so yet, we should increment the minor version. The CI will detect if this
> is needed and fail the `Create Release Commit` job. To fix, select the
> "bump minor version" option.

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

> [!IMPORTANT]
> Reviewers should check that PRs with breaking changes receive the `breaking-change`
> label. If a PR is missing the label, please add it, even if after it was merged.
> This label is used in the release process.

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
