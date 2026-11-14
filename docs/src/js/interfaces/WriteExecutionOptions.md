[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / WriteExecutionOptions

# Interface: WriteExecutionOptions

## Properties

### retries?

```ts
optional retries: number;
```

Override the remote retry budget for this execution only (an integer from
0 to 255). Use `execute(data, { retries: 0 })` to disable even retries after
precommit conflicts (409) or rate limits (429). This does not change
connection-wide/read budgets or local transaction conflict retries.

By default, remote calls use the connection's `clientConfig.retryConfig`
([RetryConfig](RetryConfig.md)). Its `statuses` (also set through
`LANCE_CLIENT_RETRY_STATUSES`) selects candidates within the retry budget;
it never permits replay of ambiguous merge outcomes. In particular, 502,
ordinary 503, and 504 are never replayed, even if configured. Reconcile table
contents before manually repeating a merge whose commit outcome is unknown.

***

### timeoutMs?

```ts
optional timeoutMs: number;
```

Maximum time to run the operation before cancelling it.

By default, there is a 30-second timeout that is only enforced after the
first attempt. This is to prevent spending too long retrying to resolve
conflicts. For example, if a write attempt takes 20 seconds and fails,
the second attempt will be cancelled after 10 seconds, hitting the
30-second timeout. However, a write that takes one hour and succeeds on the
first attempt will not be cancelled.

When this is set, the timeout is enforced on all attempts, including the first.
For remote tables this is sent as a server execution timeout. It does not
extend HTTP read or proxy idle timeouts, or prove that a failed request did
not commit.
