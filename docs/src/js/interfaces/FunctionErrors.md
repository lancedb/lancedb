[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionErrors

# Interface: FunctionErrors

A table's per-row Function errors.

## Properties

### fragments

```ts
fragments: FunctionErrorFragment[];
```

Fragments whose detail was capped.

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
