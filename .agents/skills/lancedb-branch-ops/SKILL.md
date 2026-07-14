---
name: lancedb-branch-ops
description: Branch management for LanceDB tables via the REST API. Use this skill whenever someone wants to create, delete, list, or switch branches on a LanceDB table — or needs to make sure a write (metadata update, index build, etc.) lands on a specific branch instead of main. Invoke it even without the word "branch" if context makes clear they want an experimental copy of a table, want to isolate changes, or want to confirm a mutation didn't touch main. Covers: branches/list, branches/create, branches/delete, and passing "branch" in describe/update_field_metadata/create_index to target a non-main version.
---

## Goal

Manage branches on a LanceDB table: list what exists, create new ones, delete stale ones, and direct read/write operations at a specific branch without touching main.

## Step 0: Establish the connection

Use the `lancedb-connect` skill to resolve the base URL and auth headers (`x-api-key`, `x-lancedb-database`). Skip this only if the connection is already known from the current conversation.

All examples below use `{base_url}` — substitute the resolved endpoint and include the auth headers on every request.

## The branch model (important)

LanceDB branches are named snapshots that diverge from the table's current state at creation time. There is **no checkout command** — you never switch the whole table to a branch. Instead, you **pass `"branch": "<name>"` in the request body** of any operation to target that branch. Omitting the key (or sending an empty body) always targets main.

`branches/list` returns only non-main branches. Main always exists and is not listed.

## List branches

```http
POST {base_url}/v1/table/{table_id}/branches/list
Content-Type: application/json

{}
```

Response:
```json
{
  "branches": {
    "experiment-reindex": {"parentVersion": 1, "createAt": 1782506085, "manifestSize": 1029}
  }
}
```

If `branches` is `{}`, the table has no branches besides main.

## Create a branch

```http
POST {base_url}/v1/table/{table_id}/branches/create
Content-Type: application/json

{"name": "experiment-reindex"}
```

HTTP 200 with `{}` body = success. The branch is created off the table's current state on main.

Verify by calling `branches/list` and confirming the new name appears.

## Delete a branch

```http
POST {base_url}/v1/table/{table_id}/branches/delete
Content-Type: application/json

{"name": "stale-2024"}
```

HTTP 200 with `{}` body = success. Only the branch pointer is removed — main and all row data remain intact.

Verify by calling `branches/list` (name gone) and `describe` with no branch param (main still responds).

## Operate on a specific branch

Pass `"branch": "<name>"` in the body of any operation to scope it to that branch:

**Read schema on a branch:**
```http
POST {base_url}/v1/table/{table_id}/describe
Content-Type: application/json

{"branch": "wip-branch"}
```

**Write metadata to a branch (not main):**
```http
POST {base_url}/v1/table/{table_id}/update_field_metadata
Content-Type: application/json

{
  "branch": "wip-branch",
  "updates": [
    {
      "path": "category",
      "metadata": {"lancedb:description": "Product category label."},
      "replace": false
    }
  ]
}
```

**Build an index on a branch:**
```http
POST {base_url}/v1/table/{table_id}/create_index
Content-Type: application/json

{
  "branch": "wip-branch",
  "column": "category",
  "index_type": "BTREE"
}
```

## Verifying isolation

After writing to a branch, always confirm the change did NOT land on main:

```bash
# Should show the new metadata
curl -s -X POST {base_url}/v1/table/{table_id}/describe \
  -H "x-api-key: <key>" -H "x-lancedb-database: <db>" \
  -H "content-type: application/json" \
  -d '{"branch": "wip-branch"}'

# Should NOT show the new metadata
curl -s -X POST {base_url}/v1/table/{table_id}/describe \
  -H "x-api-key: <key>" -H "x-lancedb-database: <db>" \
  -H "content-type: application/json" \
  -d '{}'
```

## Quick reference

| Goal | Endpoint | Body |
|------|----------|------|
| List all branches | `branches/list` | `{}` |
| Create a branch | `branches/create` | `{"name": "..."}` |
| Delete a branch | `branches/delete` | `{"name": "..."}` |
| Read schema on branch | `describe` | `{"branch": "..."}` |
| Write metadata on branch | `update_field_metadata` | `{"branch": "...", "updates": [...]}` |
| Build index on branch | `create_index` | `{"branch": "...", "column": ..., "index_type": ...}` |
| Target main (default) | any endpoint | omit `"branch"` key |
