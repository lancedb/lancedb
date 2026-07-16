# Connecting to a LanceDB remote server

LanceDB Enterprise/Cloud deployments are served by a server implementing the
lance-namespace OpenAPI spec
(<https://github.com/lance-format/lance-namespace/blob/main/docs/src/spec.yaml>).
Every remote (`db://...`) connection talks to such a server, and some operations
exist only there. In particular, all operations around jobs (listing, inspecting,
creating, or canceling jobs) run server-side — there is no local/OSS equivalent, so
resolve a server connection before attempting any job work. The job REST methods
themselves are documented in `references/remote_jobs.md`.

Every request needs two things:

1. **Base URL** — the server endpoint
2. **Credentials** — an API key (`x-api-key` header over REST), and usually a database name (`x-lancedb-database` header)

## Resolution steps

1. If the user already gave a URL and API key (or said which environment they're working against), use that.
2. Otherwise, look for credentials already available in the environment:
   - Env vars like `LANCEDB_URI` / `LANCEDB_HOST` / `LANCEDB_API_KEY`
   - A server endpoint already running or port-forwarded locally (the REST default port is 2333, i.e. `http://localhost:2333`)
3. If you didn't find both pieces, ask the user directly: **"What's your LanceDB endpoint's URL, and what's your API key?"** Also ask which database to use if it isn't obvious. Don't guess or probe further — the user knows their deployment.

## Validating the connection

Make a cheap authenticated request and check the status before starting real work:

```bash
curl -s -w "\n%{http_code}" "{base_url}/v1/table/?limit=1" \
  -H "x-api-key: <key>" \
  -H "x-lancedb-database: <database>"
```

- `200` — connection, key, and database header all good
- `401` — API key missing or wrong
- `400` mentioning a database header — this deployment expects `x-lancedb-database`

## Non-REST equivalents

The same credentials work through the SDKs and CLI:

- Python SDK: `lancedb.connect("db://<database>", api_key="<key>", host_override="<base_url>")`
- TypeScript SDK: `await lancedb.connect("db://<database>", { apiKey: "<key>", hostOverride: "<base_url>" })`
- `lancedb` CLI: a `[profiles.<name>]` entry in `~/.lancedb/config.toml` with `http_server_url`, `api_key`, `database`
