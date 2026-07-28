[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CustomStopWordsSource

# Type Alias: CustomStopWordsSource

```ts
type CustomStopWordsSource: string[] | FtsStopWordsFileSource | FtsStopWordsTableSource;
```

A source for custom full-text-search stop words.

An inline array supplies the entries directly. A file is read as UTF-8 with
one stop word per line. A table source reads stop words from the selected
string column of a local/native LanceDB table. Remote table sources are
rejected because the client cannot currently guarantee a complete snapshot.

```ts
const inline: CustomStopWordsSource = ["copyright", "reserved"];
const file: CustomStopWordsSource = {
  source: "file",
  path: "./stop-words.txt",
};
const tableColumn: CustomStopWordsSource = {
  source: "table",
  table: stopWordsTable,
  column: "word",
};
```

Empty strings are ignored and exact duplicates are removed while preserving
the first occurrence. Values are otherwise preserved exactly: LanceDB does
not trim them, lowercase them, or otherwise normalize their contents.
Embedded/local fuzzy queries fail closed when `fuzziness` is greater than
zero and a custom snapshot is active; `fuzziness: 0` and indexes without a
custom snapshot continue to work normally. Remote tables currently reject
every explicit `fuzziness > 0` query because the server protocol does not
declare tokenizer-snapshot-safe fuzzy search; omit `fuzziness` or use
`fuzziness: 0`.

The source is resolved when the index is created, and the resulting list is
stored as a stable index snapshot. Standalone `tokenize` resolves the same
kind of one-call snapshot. File paths are always read by this client, never
by a remote LanceDB service.
