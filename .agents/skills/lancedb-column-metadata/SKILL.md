---
name: lancedb-column-metadata
description: Column metadata authoring for LanceDB tables via the REST API. This skill is required for tasks like writing field descriptions, setting tags on columns (field_type, model, project_id, version), classifying columns as embeddings vs labels vs eval metrics, or grouping versioned columns into logical families — because it has the API integration needed to read the schema and persist metadata back. Invoke whenever someone wants to document, annotate, tag, or classify what their table columns ARE. Trigger even without an explicit "LanceDB" mention, as long as the context is column-level documentation or tagging for an ML or vector database table.
metadata:
  short-description: Write column descriptions, tags, and logical groupings to a LanceDB table
---

## Overview

This skill authors column-level metadata for a LanceDB table. It connects to a LanceDB deployment over its REST API, inspects the table schema, generates appropriate metadata, and writes it back.

## Step 0: Establish the connection

Use the `lancedb-connect` skill (invoke it via the Skill tool) to resolve the base URL and auth headers (`x-api-key`, `x-lancedb-database`) for whichever deployment the user is working against — LanceDB Cloud, enterprise/self-hosted, or a local dev server. Skip it only if the connection details are already established in the conversation.

All examples below use `{base_url}` — substitute the resolved endpoint and include the resolved headers on every request.

## Metadata keys

All metadata uses namespaced keys:

| Key | Purpose | Example value |
|-----|---------|---------------|
| `lancedb:description` | Human-readable explanation of what the column contains | `"CLIP ViT-L/14 image embedding, L2-normalized (768-dim)"` |
| `lancedb:tag:<name>` | Flexible key-value tag; the suffix names the tag category | `lancedb:tag:field_type: "embedding"`, `lancedb:tag:model: "clip"`, `lancedb:tag:project_id: "foo"` |
| `lancedb:logical-column` | Logical group/family this column belongs to | `"clip_features"` |

Tags are open-ended — use whatever key suffix and value make sense given the user's intent. The tag suffix should describe *what is being classified* (e.g., `field_type`, `model`, `project_id`) and the value describes *how*.

## Step 1: Resolve the table identifier

You need:
- **Table name** (required) — e.g., `my_table` or `my_namespace.my_table`
- **Database name** — ask if not provided and not inferable from context; it goes in the `x-lancedb-database` header (or the hostname, for LanceDB Cloud), never in the URL path

The table identifier in the URL path is typically `table_name` for a top-level table, or `namespace.table_name` if the table lives in a namespace. The API accepts a `delimiter` query parameter to parse compound identifiers (default `.`).

## Step 2: Describe the table

```http
POST {base_url}/v1/table/{table_id}/describe
Content-Type: application/json

{}
```

The response contains `schema.fields` — an array of field objects:

```json
{
  "schema": {
    "fields": [
      {
        "name": "clip_embedding_v3",
        "type": { "type": "FixedSizeList", "fields": [...], "listSize": 768 },
        "nullable": true,
        "metadata": { "lancedb:description": "..." }
      }
    ]
  }
}
```

Each field has:
- `name` — field name
- `type` — Arrow data type (check `type.type` for the type string)
- `nullable` — boolean
- `metadata` — existing key-value metadata (read this before writing to avoid redundant updates)

For struct/nested fields, recurse into `type.fields` and represent them as dot-notation paths (e.g., `parent.child`).

If the user hasn't specified which columns to update, work with all columns.

## Step 3: Generate metadata

Decide what to generate based on the user's request.

### Writing descriptions (`lancedb:description`)

Base descriptions on:
- The column name and Arrow type (e.g., `FixedSizeList` of floats → likely an embedding)
- User-supplied context (upstream pipeline, sample values, domain knowledge)
- Name patterns: `_embedding`/`_vec`/`_embed` → vector; `_label`/`_class` → label; `_score`/`_eval`/`_metric` → evaluation metric

Be specific and concise. Good: `"Sentence-BERT embedding of the query text (768-dim)."` Not: `"An embedding column."`

### Tagging columns (`lancedb:tag:<name>`)

Choose tag key names that match what the user asked to annotate. Common patterns:

- Semantic field type → `lancedb:tag:field_type: "embedding"` / `"text"` / `"image"` / `"label"` / `"eval"` / `"id"` / `"metadata"`
- Model or source → `lancedb:tag:model: "clip"` / `"bert"` / `"vit"`
- Project affiliation → `lancedb:tag:project_id: "<name>"`
- Version → `lancedb:tag:version: "v3"` (and `lancedb:tag:latest: "true"` for the newest)

Use Arrow type as a hint: `FixedSizeList` + float → embedding; `Utf8`/`LargeUtf8` → text; `Binary` → image or blob.

Multiple tags on the same column are fine — each is a separate key.

### Grouping into logical columns (`lancedb:logical-column`)

Look for naming patterns across columns:
- `clip_v1`, `clip_v2`, `clip_v3` → logical column `"clip"`, latest is `v3`
- `text_embed_20240101`, `text_embed_20240601` → logical column `"text_embed"`, latest is the most recent date suffix

Write `lancedb:logical-column` on all members of a group. Mark the newest with `lancedb:tag:latest: "true"` (in addition to its version tag).

## Step 4: Write the metadata

```http
POST {base_url}/v1/table/{table_id}/update_field_metadata
Content-Type: application/json

{
  "updates": [
    {
      "path": "clip_v3",
      "metadata": {
        "lancedb:description": "CLIP ViT-L/14 image embedding, L2-normalized (1024-dim).",
        "lancedb:tag:field_type": "embedding",
        "lancedb:tag:model": "clip",
        "lancedb:tag:version": "v3",
        "lancedb:tag:latest": "true",
        "lancedb:logical-column": "clip"
      },
      "replace": false
    },
    {
      "path": "clip_v2",
      "metadata": {
        "lancedb:description": "CLIP ViT-B/32 image embedding (768-dim), superseded by v3.",
        "lancedb:tag:field_type": "embedding",
        "lancedb:tag:model": "clip",
        "lancedb:tag:version": "v2",
        "lancedb:logical-column": "clip"
      },
      "replace": false
    }
  ]
}
```

Rules:
- **Use `"replace": false`** (merge) by default — this preserves existing metadata the user didn't ask to change
- Use `"replace": true` only if the user explicitly asks to overwrite all existing metadata on a column
- Set a value to `null` to delete a specific key
- Batch all updates in a single request when possible

The response includes `version` (new table version) and `fields` (the updated metadata per field).

## Step 5: Confirm

Report back:
- Which columns were updated and what was written
- The new table version number
- Any columns skipped (e.g., already had up-to-date metadata)

---

## Quick examples

**"Write descriptions for all columns in the `product_embeddings` table"**
1. POST `/v1/table/product_embeddings/describe` → get all fields
2. Generate a `lancedb:description` for each column based on name + type
3. POST `update_field_metadata` with descriptions
4. Report

**"Tag the columns in `model_outputs` with their field type and model"**
1. Describe `model_outputs`
2. For each field, classify by name + Arrow type → set `lancedb:tag:field_type` and `lancedb:tag:model` where applicable
3. POST `update_field_metadata`
4. Report

**"Group the feature columns in `training_features` into logical families and mark the latest version"**
1. Describe the table
2. Find version patterns → assign `lancedb:logical-column` and `lancedb:tag:version`; mark newest with `lancedb:tag:latest: "true"`
3. POST `update_field_metadata`
4. Show the grouping
