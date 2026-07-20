# TypeScript API Reference

Quick method reference for TypeScript LanceDB code. Cross-check source for non-trivial claims.

## Connect

```typescript
import * as lancedb from "@lancedb/lancedb";

const db = await lancedb.connect("./camelot-db");
```

**Place the local database directory next to the script/entrypoint that opens it** (resolve the path relative to the module, e.g. via `import.meta.dirname` / `__dirname`), not buried under a shared `data/` folder. The Lance dataset is the database, not a data file — keeping it beside its code makes ownership obvious and paths stable regardless of the working directory the script is launched from.

**Do not name the directory `lancedb`** (e.g. `./lancedb`, `./data/lancedb`). It collides with the imported `lancedb` package/namespace, which is confusing to read. Give it a name derived from the repo or dataset with a clear prefix/suffix — for example `./<dataset>-db`, `./<repo>_lancedb`, or `./vectordb`.

Remote connections use `db://...` plus Enterprise/Cloud credentials and deployment settings. Check current source/docs for exact connection options.

## Table Reads

| Task | Preferred API |
| --- | --- |
| Vector search | `table.search(queryVector).limit(k)` |
| Full scan with filters/projection | `table.query().where(...).select(...).limit(...)` |
| Filter | `.where("col > 10")` |
| Projection | `.select(["id", "text"])` |
| Bound result count | `.limit(20)` |
| Collect bounded result as objects | `.toArray()` on query/search result |
| Collect bounded result as Arrow | `.toArrow()` on query/search result |
| Stream result batches | `for await (const batch of table.query()...)` |

## Local vs Remote Safety

| API | Agent guidance |
| --- | --- |
| `table.search(...)` | Preferred read path |
| `table.query()` | Preferred scan/filter path |
| `await table.toArrow()` | Avoid in portable or large-table code |
| `await table.query().toArray()` with no `limit()` | Avoid; unbounded collection |
| `await table.query().toArrow()` with no `limit()` | Avoid; unbounded collection |

## Indexes

```typescript
await table.createIndex("vector");
await table.createIndex("status");
```

Use vector indexes for large vector search workloads and scalar indexes for filtered columns or merge/upsert keys. Check source/docs before specifying advanced index options.

## Filtering And Recall Knobs

```typescript
await table.search(queryVector).where("status = 'ready'").limit(10).toArray();
await table.search(queryVector).limit(10).refineFactor(20).toArray();
await table.search(queryVector).limit(10).nprobes(50).toArray();
await table.search(queryVector).limit(10).ef(100).toArray();
await table.search(queryVector).where("status = 'ready'").postfilter().limit(10).toArray();
```

Use `postfilter()` only when fewer than `limit` results are acceptable.

## Diagnostics

```typescript
console.log(await table.search(queryVector).where("year > 2000").limit(10).analyzePlan());
console.log(await table.indexStats("vector_idx"));
```

Use these before changing indexes or search tuning.

## Column (Field) Metadata

```typescript
const schema = await table.schema();
const meta = schema.fields.find((f) => f.name === "category")?.metadata; // Map<string, string>
const res = await table.updateFieldMetadata([
  { path: "category", metadata: { "lancedb:description": "...", "lancedb:tag:field_type": "label" } },
]);
res.version; // new table version
```

Merges by default; a `null` value deletes that key; `replace: true` swaps the whole map. Nested fields use dot-paths (`"a.b.c"`). See `references/column_metadata.md` for key conventions (`lancedb:description`, `lancedb:tag:<name>`, `lancedb:logical-column`) and the authoring workflow.

## Branches

```typescript
const branches = await table.branches();       // async manager
await branches.list();                         // non-main branches; {} = only main
const exp = await branches.create("exp");      // fork off main -> Table scoped to the branch
const wip = await branches.checkout("wip");    // existing branch -> scoped Table (version arg pins read-only)
const wip2 = await db.openTable("t", { branch: "wip" }); // or open scoped directly
await branches.delete("stale");                // removes only the branch pointer
table.currentBranch();                         // null = main
```

There is no global switch — scoping is per table handle: any read/write on a branch handle lands on that branch; the original handle keeps targeting main. See `references/branch_ops.md` for the model and isolation checks.

## Maintenance

```typescript
await table.optimize();
```

Call this after every successful local/OSS ingestion. It handles compaction, cleanup of old versions according to retention, and index optimization. Do not add this for LanceDB Enterprise/Cloud remote tables; Enterprise handles compaction and cleanup automatically from cluster configuration.
