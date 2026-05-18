[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / AddDataOptions

# Interface: AddDataOptions

Options for adding data to a table.

## Properties

### mode

```ts
mode: "append" | "overwrite";
```

If "append" (the default) then the new data will be added to the table

If "overwrite" then the new data will replace the existing data in the table.

***

### progress()

```ts
progress: (progress) => void;
```

Optional callback invoked periodically with write progress.

The callback is fired once per batch written and once more with
`done: true` when the write completes. Calls are non-blocking — if the
callback is slow, intermediate updates may be dropped to avoid stalling
the write.

Errors thrown from the callback are logged with `console.warn` and
swallowed — they do not abort the write.

#### Parameters

* **progress**: [`WriteProgress`](WriteProgress.md)

#### Returns

`void`

#### Example

```ts
await table.add(data, {
  progress: (p) => {
    console.log(`${p.outputRows}/${p.totalRows ?? "?"} rows`);
  },
});
```
