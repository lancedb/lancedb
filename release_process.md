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

## Prepare a stable release

### Select the release baseline

Choose the SDK product commit for the planned release and record its SHA. Include
all fixes required for that release, including security fixes. Create and push a
release branch such as `release/v0.41` from that commit before opening the Lance RC
upgrade PR. For a patch to an existing stable line, reuse its release branch and
backport the required fixes.

Keep the selected product baseline while main continues development. If main
already tracks a newer Lance development version, retain that dependency on main
and prepare the stable Lance dependency on the SDK release branch. Any additional
fixes should arrive through reviewed PRs into the release branch and be validated.

### Validate the Lance RC

1. Open a **draft PR targeting the SDK release branch** to consume the exact Lance
   RC. Check both the PR base and dependency references when using automation.
   Update the Rust declarations and lockfile, Python dependency declarations and
   lockfile, and Java Maven version consistently.
2. Wait for the RC artifacts needed by each SDK to be available, then run the
   relevant Rust, Python, Node.js, Java, and packaging checks. Resolve compatibility
   failures and rerun CI after changes to the PR or its release-branch base.
3. Complete code and compatibility review on the RC pin PR. If draft status
   skips Gatekeeper or another applicable review, mark the PR ready for review or
   explicitly run the review. Read the recommendation and inline findings even
   when GitHub records them as comments. Passing CI or a skipped review does not
   clear an unresolved blocking finding.
4. Before the Lance vote is closed and Lance stable is published, the pin PR
   must be **ready to merge**: all relevant CI and reviews are complete, with no
   unresolved blocking findings at its current head and release-branch base.
   Refresh reviews and comments immediately before reporting readiness.
5. Report the exact Lance tag and peeled SHA,
   SDK release-branch base and PR head SHAs (and tested merge commit, when
   applicable), full CI results and review outcomes to the release manager.
   Label local source checks separately from checks consuming the published RC
   artifacts.

Keep the RC validation PR unmerged until Lance is approved, while completing all
substantive compatibility and review work before Lance stable publication. Once
the official stable Lance artifacts are available, only the mechanical
RC-to-stable dependency update and validation of those packages should remain.
Update the PR, rerun the relevant checks and refresh reviews. In particular,
verify that the Rust package resolves
published dependencies: Cargo only reads [`[patch]` settings at the workspace root](https://doc.rust-lang.org/cargo/reference/overriding-dependencies.html#the-patch-section),
so package consumers do not inherit a dependency's workspace override. Merge the
validated stable update into the SDK release branch.

### Publish the validated candidate

Run **Create Release Commit** from the verified SDK release-branch head. Do not
switch to a later main snapshot after validation. Start with a dry run, review the
expected version and lockfile updates, and use the same branch and release inputs
for the real run.

The workflow creates a new version commit and tag. Compare that tag with the
validated candidate and check that only the expected mechanical version and
lockfile updates were introduced. Code or dependency changes require renewed
validation. Verify all four SDK publishing workflows and the exact registry
artifacts before reporting the stable release as complete.

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
* **For a stable release**, select the verified release branch and set the `type` input to `stable`.

For example, dry-run a stable release from the selected branch:

```bash
gh workflow run make-release-commit.yml --repo lancedb/lancedb \
  --ref release/v0.41 -f type=stable -f dry_run=true -f bump-minor=false
```

After reviewing the dry run, use the same branch and inputs with `dry_run=false`.
Select `bump-minor` according to the version and breaking-change check below.

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
