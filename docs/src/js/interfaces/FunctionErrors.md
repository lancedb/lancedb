[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionErrors

# Interface: FunctionErrors

A table's per-row Function errors.

## Properties

### errorGroups?

```ts
optional errorGroups: FunctionErrorGroup[];
```

Grouped causes over the returned records, if the server supports the workflow.
Counts can be incomplete. Check `truncated` and `fragments` before using them as totals.

***

### fragments

```ts
fragments: FunctionErrorFragment[];
```

Fragments whose detail was capped.

***

### pageToken?

```ts
optional pageToken: string;
```

Continue the same job/filter. Absent for complete pages and older servers.

***

### records

```ts
records: FunctionErrorRecord[];
```

The recorded rows, newest job first.

***

### truncated

```ts
truncated: boolean;
```

Whether the listing stopped at its limit.

***

### workflowVersion?

```ts
optional workflowVersion: number;
```

Version of the Enterprise row-failure workflow, if advertised by the server.
