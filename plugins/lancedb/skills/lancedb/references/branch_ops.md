# Branch Operations

Manage branches on a LanceDB table: list what exists, create new ones, delete stale ones, and direct read/write operations at a specific branch without touching main. Use for branch lifecycle tasks, experimental/isolated table versions, targeting an operation at a non-main branch, or confirming a mutation did not affect main.

Works on local/OSS and remote Enterprise/Cloud tables, except merging a branch into main, which is Enterprise-only.

## The branch model (important)

Branches are isolated, writable lines of history forked from another branch (or a specific version). Writes on a branch never affect `main`.

There is **no global "switch branch" state** — you never repoint the whole table at a branch. Instead, **operations are scoped by which table handle you use**:

- The handle you got from `open_table(name)` / `openTable(name)` targets `main`.
- `branches.create(...)` and `branches.checkout(...)` return a **new table handle scoped to that branch**. Every read/write on that handle (add, update, `update_field_metadata`, `create_index`, search, …) lands on the branch.
- The original main handle is unaffected — keep it around to verify isolation.

`branches.list()` returns only non-main branches. Main always exists and is not listed.

## Python

`table.branches` is a property returning the branch manager; `table.current_branch()` tells you what a handle is scoped to (`None` = main).

```python
table = db.open_table("products")            # scoped to main

# list — dict of name -> metadata (parent_branch, parent_version, ...); {} = only main
table.branches.list()

# create: forks from main by default and returns a handle scoped to the new branch
exp = table.branches.create("experiment-reindex")
exp = table.branches.create("exp2", from_ref="main", from_version=None)  # optional fork point

# checkout an existing branch -> branch-scoped handle
wip = table.branches.checkout("wip-branch")
# with version= it pins to that version (read-only detached view); omit to track latest, writable

# operate on the branch simply by using its handle
wip.update_field_metadata(
    {"path": "category", "metadata": {"lancedb:description": "Product category label."}}
)
wip.create_scalar_index("category")

# delete: removes only the branch pointer; main and row data remain intact
table.branches.delete("stale-2024")

# alternatively, open a branch handle directly from the connection
wip = db.open_table("products", branch="wip-branch")

exp.current_branch()    # "experiment-reindex"
table.current_branch()  # None (main)
```

Async: same shape — `table.branches` returns `AsyncBranches`; `await table.branches.create(...)` etc.

## TypeScript

`table.branches()` is an **async method** returning the `Branches` manager; `table.currentBranch()` returns the scoped branch or `null` for main.

```typescript
const table = await db.openTable("products"); // scoped to main
const branches = await table.branches();

// list — Record<string, BranchContents>; {} = only main
await branches.list();

// create: forks from main by default, returns a Table scoped to the new branch
const exp = await branches.create("experiment-reindex");
const exp2 = await branches.create("exp2", "main" /* fromRef */, undefined /* fromVersion */);

// checkout an existing branch -> branch-scoped Table
const wip = await branches.checkout("wip-branch");
// with a version arg it pins (read-only detached view); omit to track latest, writable

// operate on the branch simply by using its handle
await wip.updateFieldMetadata([
  { path: "category", metadata: { "lancedb:description": "Product category label." } },
]);
await wip.createIndex("category");

// delete: removes only the branch pointer; main and row data remain intact
await branches.delete("stale-2024");

// alternatively, open a branch handle directly from the connection
const wip2 = await db.openTable("products", { branch: "wip-branch" });

exp.currentBranch();   // "experiment-reindex"
table.currentBranch(); // null (main)
```

## Verifying isolation

After writing to a branch, confirm the change did NOT land on main by reading through both handles:

```python
wip = table.branches.checkout("wip-branch")
wip.update_field_metadata({"path": "category", "metadata": {"lancedb:description": "..."}})

assert b"lancedb:description" in (wip.schema.field("category").metadata or {})
assert b"lancedb:description" not in (table.schema.field("category").metadata or {})  # main untouched
```

Two handles on the same branch see each other's writes (e.g. `table.branches.create("exp")` and `db.open_table(name, branch="exp")`); main stays isolated.

## Merging a branch into main (Enterprise only)

Merge is available through the SDKs (`table.branches.merge(...)`) on **Enterprise tables only** — it is not supported on Cloud or local/OSS tables, which raise `NotSupported`.

`merge` takes the branch to merge **from** and a `dry_run` flag. Both the SDK method and the underlying REST endpoint **actually merge by default** (`dry_run=False`); pass `dry_run=True` to only preview. A rejected merge is **not an exception** — it returns a result with `status="rejected"` rather than raising, so inspect the return value. Use `branches.diff(from_branch)` to inspect a branch's pending diff without attempting a merge.

```python
exp = "experiment-reindex"

# preview only — returns status="ready" if it would merge cleanly
preview = table.branches.merge(exp, dry_run=True)

# actually merge (default)
result = table.branches.merge(exp)
if result["status"] == "merged":
    print("landed at", result["mainVersionAfter"])
elif result["status"] == "rejected":
    print(result["diff"]["mergeBlockers"])   # why it was refused

# inspect a branch's pending diff without merging
diff = table.branches.diff(exp)
```

Async: `await table.branches.merge(exp)`, `await table.branches.diff(exp)`.

```typescript
const branches = await table.branches();
const exp = "experiment-reindex";

// preview only (second arg is dryRun)
const preview = await branches.merge(exp, true);

// actually merge (default)
const result = await branches.merge(exp);
if (result.status === "merged") {
  console.log("landed at", result.mainVersionAfter);
} else if (result.status === "rejected") {
  console.log(result.diff.mergeBlockers);
}

const diff = await branches.diff(exp);
```

The result is the wire JSON, containing `status` (`ready` on a passing dry run, `merged` on success, `rejected` when refused — also `notImplemented`/`unknown`), the branch `diff` (including `mergeBlockers` explaining any rejection), a `preview` of the columns that would be promoted, and — after a real merge — `mainVersionAfter`.

### Merge preconditions

Merge only **promotes newly added columns** onto main; it does not replay arbitrary commits. Practically, a branch is mergeable only if it has **exactly one commit since it was created, and that commit added a column**. The merge is rejected (`status: "rejected"`, with `mergeBlockers` set) if:

- the branch was forked from another branch rather than directly from main
- main has advanced since the branch was forked
- the branch's rows changed since the fork (row counts must match main exactly)
- the branch removed columns or changed a column's type/nullability
- the branch added no columns (index-only changes are not merged)

### Adding a column in a single commit

Because the branch must contain just one column-adding commit, add the column with its values in one operation rather than add-then-backfill:

1. **SQL transformation** — `add_columns` with a SQL expression computed from existing columns, so the column lands populated in one commit.
2. **Precompute the values** — compute the column's values externally, then add the fully-populated column in a single operation (e.g. via `merge_insert`/`add_columns` with the data ready).
3. **Lance-format-level data evolution (pylance)** — use Lance's data evolution with backfill, documented at <https://lance.org/guide/data_evolution/#with-data-backfill>.

## Quick reference

| Goal | Python | TypeScript |
|------|--------|------------|
| List branches (non-main) | `table.branches.list()` | `await (await table.branches()).list()` |
| Create branch (off main) | `table.branches.create(name)` → branch handle | `await branches.create(name)` → branch `Table` |
| Create from a fork point | `table.branches.create(name, from_ref=..., from_version=...)` | `await branches.create(name, fromRef, fromVersion)` |
| Get a branch handle | `table.branches.checkout(name)` or `db.open_table(t, branch=name)` | `await branches.checkout(name)` or `await db.openTable(t, { branch: name })` |
| Pin to a branch version (read-only) | `table.branches.checkout(name, version=v)` | `await branches.checkout(name, v)` |
| Delete branch | `table.branches.delete(name)` | `await branches.delete(name)` |
| Which branch is this handle on? | `table.current_branch()` (`None` = main) | `table.currentBranch()` (`null` = main) |
| Target main | use the original (non-branch) handle | use the original (non-branch) handle |
| Merge branch into main (Enterprise only) | `table.branches.merge(from_branch, dry_run=False)` | `await branches.merge(fromBranch, dryRun)` |
| Preview a branch's pending diff (Enterprise only) | `table.branches.diff(from_branch)` | `await branches.diff(fromBranch)` |

Branch names must be non-empty; empty names raise a validation error.
