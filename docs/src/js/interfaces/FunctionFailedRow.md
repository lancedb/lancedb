[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionFailedRow

# Interface: FunctionFailedRow

A failed Function-column row and its source data at the table's read snapshot.

## Properties

### error

```ts
error: FunctionErrorRecord;
```

The failure for this row and Function column.

***

### input

```ts
input: Record<string, unknown>;
```

Values of the requested source columns.
