[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / AddDataOptions

# Interface: AddDataOptions

Options for adding data to a table.

## Properties

### allowExternalBlobOutsideBases?

```ts
optional allowExternalBlobOutsideBases: boolean;
```

Whether blob URIs outside registered external bases may be written.

Defaults to `false`. This option is supported only for local/native
tables; remote tables return an error when it is enabled. An enabled write
stores the absolute URI reference without registering a base or copying
the external object into the database. The object must remain accessible
when the blob is read later.

***

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
`done: true` when the write completes. Calls are dispatched
asynchronously to the JS event loop and never block the write — a slow
callback will queue events rather than back-pressure the writer.

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
