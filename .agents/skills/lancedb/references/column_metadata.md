# Column Metadata Authoring

Write column-level descriptions, tags, and logical groupings onto a LanceDB table's schema. Use this when the user wants to document, annotate, tag, or classify what their table columns ARE (embeddings vs labels vs eval metrics, model provenance, version families, etc.).

Works on local/OSS and remote Enterprise/Cloud tables alike — read the schema through the table handle, write through `update_field_metadata` (Python) / `updateFieldMetadata` (TypeScript).

## Metadata key conventions

All metadata uses namespaced keys:

| Key | Purpose | Example value |
|-----|---------|---------------|
| `lancedb:description` | Human-readable explanation of what the column contains | `"CLIP ViT-L/14 image embedding, L2-normalized (768-dim)"` |
| `lancedb:tag:<name>` | Flexible key-value tag; the suffix names the tag category | `lancedb:tag:field_type: "embedding"`, `lancedb:tag:model: "clip"`, `lancedb:tag:project_id: "foo"` |
| `lancedb:logical-column` | Logical group/family this column belongs to | `"clip_features"` |

Tags are open-ended — use whatever key suffix and value make sense given the user's intent. The tag suffix should describe *what is being classified* (e.g., `field_type`, `model`, `project_id`) and the value describes *how*. Multiple tags on the same column are fine — each is a separate key. All values are strings.

## Step 1: Read the schema and existing metadata

Read existing metadata before writing, to avoid redundant updates.

Python — `table.schema` (sync property; async: `await table.schema()`) returns a `pyarrow.Schema`. **Arrow field metadata is bytes-keyed in Python**:

```python
schema = table.schema
for field in schema:
    meta = field.metadata or {}  # dict[bytes, bytes], e.g. {b"lancedb:description": b"..."}
    print(field.name, field.type, field.nullable, meta)
```

TypeScript — `await table.schema()` returns an Arrow `Schema`; field metadata is a `Map<string, string>`:

```typescript
const schema = await table.schema();
for (const field of schema.fields) {
  console.log(field.name, field.type, field.nullable, field.metadata); // Map
  // field.metadata.get("lancedb:description")
}
```

For struct/nested fields, recurse into the field's children and address them as dot-paths (e.g., `parent.child`).

If the user hasn't specified which columns to update, work with all columns.

## Step 2: Generate metadata

Decide what to generate based on the user's request.

### Descriptions (`lancedb:description`)

Base descriptions on:
- The column name and Arrow type (e.g., `FixedSizeList` of floats → likely an embedding)
- User-supplied context (upstream pipeline, sample values, domain knowledge)
- Name patterns: `_embedding`/`_vec`/`_embed` → vector; `_label`/`_class` → label; `_score`/`_eval`/`_metric` → evaluation metric

Be specific and concise. Good: `"Sentence-BERT embedding of the query text (768-dim)."` Not: `"An embedding column."`

### Tags (`lancedb:tag:<name>`)

Choose tag key names that match what the user asked to annotate. Common patterns:

- Semantic field type → `lancedb:tag:field_type: "embedding"` / `"text"` / `"image"` / `"label"` / `"eval"` / `"id"` / `"metadata"`
- Model or source → `lancedb:tag:model: "clip"` / `"bert"` / `"vit"`
- Project affiliation → `lancedb:tag:project_id: "<name>"`
- Version → `lancedb:tag:version: "v3"` (and `lancedb:tag:latest: "true"` for the newest)

Use Arrow type as a hint: `FixedSizeList` + float → embedding; `Utf8`/`LargeUtf8` → text; `Binary` → image or blob.

### Logical groupings (`lancedb:logical-column`)

Look for naming patterns across columns:
- `clip_v1`, `clip_v2`, `clip_v3` → logical column `"clip"`, latest is `v3`
- `text_embed_20240101`, `text_embed_20240601` → logical column `"text_embed"`, latest is the most recent date suffix

Write `lancedb:logical-column` on all members of a group. Mark the newest with `lancedb:tag:latest: "true"` (in addition to its version tag).

## Step 3: Write the metadata

Each update names a field by dot-path and carries a metadata map. Semantics (identical in both SDKs):

- **Merge by default** (`replace` omitted/false) — preserves existing metadata the user didn't ask to change
- `replace: true` swaps the field's entire metadata map — only if the user explicitly asks to overwrite
- A value of `None`/`null` deletes that specific key
- Batch all field updates into a single call when possible
- Returns the new table version

Python (sync and async take one dict per field, as varargs):

```python
res = table.update_field_metadata(
    {
        "path": "clip_v3",
        "metadata": {
            "lancedb:description": "CLIP ViT-L/14 image embedding, L2-normalized (1024-dim).",
            "lancedb:tag:field_type": "embedding",
            "lancedb:tag:model": "clip",
            "lancedb:tag:version": "v3",
            "lancedb:tag:latest": "true",
            "lancedb:logical-column": "clip",
        },
    },
    {
        "path": "clip_v2",
        "metadata": {
            "lancedb:description": "CLIP ViT-B/32 image embedding (768-dim), superseded by v3.",
            "lancedb:tag:field_type": "embedding",
            "lancedb:tag:model": "clip",
            "lancedb:tag:version": "v2",
            "lancedb:logical-column": "clip",
        },
    },
)
print(res.version)  # new table version

# merge semantics: add a key, delete one via None, keep the rest
table.update_field_metadata(
    {"path": "clip_v2", "metadata": {"lancedb:tag:archived": "true", "lancedb:tag:latest": None}}
)
```

(`replace_field_metadata` is deprecated — use `update_field_metadata`.)

TypeScript (takes an array of `FieldMetadataUpdate`):

```typescript
const res = await table.updateFieldMetadata([
  {
    path: "clip_v3",
    metadata: {
      "lancedb:description": "CLIP ViT-L/14 image embedding, L2-normalized (1024-dim).",
      "lancedb:tag:field_type": "embedding",
      "lancedb:tag:model": "clip",
      "lancedb:tag:version": "v3",
      "lancedb:tag:latest": "true",
      "lancedb:logical-column": "clip",
    },
  },
  {
    path: "clip_v2",
    metadata: {
      "lancedb:description": "CLIP ViT-B/32 image embedding (768-dim), superseded by v3.",
      "lancedb:tag:field_type": "embedding",
      "lancedb:tag:model": "clip",
      "lancedb:tag:version": "v2",
      "lancedb:logical-column": "clip",
    },
  },
]);
console.log(res.version); // new table version

// merge semantics: add a key, delete one via null, keep the rest
await table.updateFieldMetadata([
  { path: "clip_v2", metadata: { "lancedb:tag:archived": "true", "lancedb:tag:latest": null } },
]);
```

## Step 4: Confirm

Report back:
- Which columns were updated and what was written
- The new table version number (from the result)
- Any columns skipped (e.g., already had up-to-date metadata)

## Quick examples

**"Write descriptions for all columns in the `product_embeddings` table"**
1. Read `table.schema` → all fields + existing metadata
2. Generate a `lancedb:description` for each column based on name + type
3. One `update_field_metadata` call with all descriptions
4. Report

**"Tag the columns in `model_outputs` with their field type and model"**
1. Read the schema
2. For each field, classify by name + Arrow type → set `lancedb:tag:field_type` and `lancedb:tag:model` where applicable
3. Write in one batched call
4. Report

**"Group the feature columns in `training_features` into logical families and mark the latest version"**
1. Read the schema
2. Find version patterns → assign `lancedb:logical-column` and `lancedb:tag:version`; mark newest with `lancedb:tag:latest: "true"`
3. Write in one batched call
4. Show the grouping
