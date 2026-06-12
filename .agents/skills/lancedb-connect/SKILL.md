---
name: lancedb-connect
description: Resolve how to connect to a LanceDB deployment over the REST API — figure out the base URL, API key, and database header. Use this before making any REST requests to a LanceDB table, whenever the endpoint or auth setup is not already known. Also useful on its own when someone asks how to connect, authenticate, or curl their LanceDB instance.
metadata:
  short-description: Resolve the base URL and auth headers for a LanceDB deployment
---

## Goal

Produce two things every REST request needs:

1. **Base URL** — the endpoint
2. **Headers** — `x-api-key`, and usually `x-lancedb-database`

## Resolution steps

1. If the user already gave a URL and API key (or said which environment they're working against), use that.
2. Otherwise, look for credentials already available in the environment:
   - Env vars like `LANCEDB_URI` / `LANCEDB_HOST` / `LANCEDB_API_KEY`
   - A LanceDB endpoint already running or port-forwarded locally (the REST default port is 2333, i.e. `http://localhost:2333`)
3. If you didn't find both pieces, ask the user directly: **"What's your LanceDB endpoint's URL, and what's your API key?"** Also ask which database to use if it isn't obvious. Don't guess or probe further — the user knows their deployment.

## Validating the connection

Make a cheap authenticated request and check the status:

```bash
curl -s -w "\n%{http_code}" "{base_url}/v1/table/?limit=1" \
  -H "x-api-key: <key>" \
  -H "x-lancedb-database: <database>"
```

- `200` — connection, key, and database header all good
- `401` — API key missing or wrong
- `400` mentioning a database header — this deployment expects `x-lancedb-database`

## Non-REST equivalents

If the caller would rather use the SDK or CLI than raw REST, the same credentials work:

- Python SDK: `lancedb.connect("db://<database>", api_key="<key>", host_override="<base_url>")`
- `lancedb` CLI: a `[profiles.<name>]` entry in `~/.lancedb/config.toml` with `http_server_url`, `api_key`, `database`
