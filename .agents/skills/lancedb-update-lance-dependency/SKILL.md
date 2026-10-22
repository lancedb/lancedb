---
name: lancedb-update-lance-dependency
description: Update LanceDB to a specific Lance release or tag. Use when bumping Lance dependencies in the lancedb repository, including Rust workspace Lance crates, Java lance-core, validation, branch creation, commit, push, and PR creation when requested.
---

# LanceDB Update Lance Dependency

## Scope

Use this skill in the `lancedb/lancedb` repository when updating the Lance dependency to a specific Lance version or tag.

Inputs can be a version (`7.2.0-beta.1`), a tag (`v7.2.0-beta.1`), a tag ref (`refs/tags/v7.2.0-beta.1`), or `latest`.

## Workflow

1. Confirm the worktree status with `git status --short`.
2. Resolve the target Lance version:

   - If the input is `latest`, empty, or omitted, run:

     ```bash
     python3 ci/check_lance_release.py
     ```

     Parse the JSON output. If `needs_update` is not `true`, stop without creating a PR. Otherwise use `latest_tag`.

   - If the input is explicit, use it directly.

3. Compute update metadata without changing files:

   ```bash
   python3 ci/update_lance_dependency.py "$TAG_OR_VERSION" --metadata-only
   ```

   Before making changes, check for an existing open PR with the emitted `pr_title`:

   ```bash
   gh pr list --search "\"$PR_TITLE\" in:title" --state open --limit 1 --json number,url,title
   ```

   If a matching open PR exists, stop and report it instead of creating a duplicate.

4. Run the deterministic update entrypoint:

   ```bash
   python3 ci/update_lance_dependency.py "$TAG_OR_VERSION"
   ```

   This updates the Rust workspace Lance dependencies through `ci/set_lance_version.py`, updates `java/pom.xml`, refreshes Cargo metadata, and prints JSON metadata containing `branch_name`, `commit_message`, and `pr_title`.

5. Run validation:

   ```bash
   cargo clippy --quiet --workspace --tests --all-features -- -D warnings
   cargo fmt --all --quiet
   ```

   Fix real diagnostics and rerun clippy until it succeeds. Do not skip warnings.

6. Inspect `git status --short` and `git diff` to ensure only the Lance dependency update and required compatibility fixes are present.

7. If the task only asks to prepare local changes, stop here and report the changed files and validation result.

8. If the task asks to publish the update, create a branch using the printed `branch_name`, stage all relevant files, and commit using the printed `commit_message`. Do not amend or rewrite existing commits.

9. Push to `origin`. Before creating the PR, check that the current token has push permission:

   ```bash
   gh api repos/lancedb/lancedb --jq .permissions.push
   ```

   If the remote branch already exists for the same generated branch name, delete the remote ref with `gh api -X DELETE repos/lancedb/lancedb/git/refs/heads/$BRANCH_NAME`, then push. Do not force-push.

10. Create a PR targeting `main` with the printed `pr_title`. If there is no PR template, keep the body to two or three concise sentences: state the Lance dependency bump, note any required compatibility fixes, and link the triggering Lance tag or release.

11. Read back the remote PR title after creation. If it is not a Conventional Commit title, fix it immediately.

12. When running in GitHub Actions after creating the LanceDB PR, trigger the Sophon dependency update:

    ```bash
    gh workflow run codex-bump-lancedb-lance.yml \
      --repo lancedb/sophon \
      -f lance_ref="$LANCE_TAG" \
      -f lancedb_ref="$BRANCH_NAME"
    gh run list --repo lancedb/sophon --workflow codex-bump-lancedb-lance.yml --limit 1 --json databaseId,url,displayTitle
    ```

    Use the emitted metadata `tag` value as `LANCE_TAG`. Do this only after a new LanceDB PR has been created. If the update was skipped because no update is needed or an open PR already exists, do not trigger Sophon.

## GitHub Actions

When this skill is used from GitHub Actions, `TAG`, `GH_TOKEN`, and `GITHUB_TOKEN` may already be set. Resolve `latest` first when `TAG` is empty. Once an explicit tag or version is known, use:

```bash
python3 ci/update_lance_dependency.py "$TAG" --github-output "$GITHUB_OUTPUT"
```

Then use the emitted `branch_name`, `commit_message`, and `pr_title` values for branch, commit, and PR creation.
