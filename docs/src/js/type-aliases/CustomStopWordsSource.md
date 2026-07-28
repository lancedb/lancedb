[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CustomStopWordsSource

# Type Alias: CustomStopWordsSource

```ts
type CustomStopWordsSource: object | object | object;
```

A request-only source for custom FTS stop words.

Remote services resolve file and table descriptors into a stable snapshot.
Local native tables reject source descriptors.

## Example

```ts
const inline = {
  type: "inline",
  words: ["the", "a"],
} satisfies CustomStopWordsSource;
const file = {
  type: "file",
  uri: "s3://bucket/stop-words.txt",
} satisfies CustomStopWordsSource;
const table = {
  type: "table",
  table: "catalog.schema.stop_words",
  column: "word",
} satisfies CustomStopWordsSource;
```
