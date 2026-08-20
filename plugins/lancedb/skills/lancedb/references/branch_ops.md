# Branch Operations

Branches are isolated, writable lines of history forked from `main` by default (or from another branch/version via `create`'s `from_ref`/`from_version`). There is no global "switch branch" state: `branches.create(...)` / `branches.checkout(...)` return a **table handle scoped to that branch**, and every read/write on that handle lands on the branch while the original main handle is unaffected. Unpinned handles track the branch's latest version and are writable; `checkout(name, version=...)` pins the handle to that version and is read-only.

Don't work from memory — read the public docs for the current API:

- **Branching guide (concepts + Python/TypeScript examples):** <https://docs.lancedb.com/tables/branching> — covers creating, writing to, reopening, and deleting branches; applying branch-tested changes back to main; diff/merge (Enterprise only); and building indexes on a branch.
- **How branches relate to versions and tags:** <https://docs.lancedb.com/tables/versioning>
- **Python API reference** (`Table.branches`, `Table.current_branch`, `Branches`/`AsyncBranches` with `list`/`create`/`checkout`/`delete`/`diff`/`merge`): <https://lancedb.github.io/lancedb/python/python/#lancedb.table.Branches>
- **TypeScript API reference** (`Branches` class, same methods; `table.branches()` is async, `table.currentBranch()` returns `null` for main): <https://lancedb.github.io/lancedb/js/classes/Branches/>

Notes the docs may not state prominently:

- Branch lifecycle works on local/OSS and remote Cloud/Enterprise tables; **merging into main is Enterprise-only** (others raise `NotSupported`). A rejected merge is not an exception — inspect the returned `status` and `diff.mergeBlockers`.
- To verify isolation after a branch write, read through both handles: the branch handle sees the change, the main handle must not.

