[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FtsStopWordsTableSource

# Interface: FtsStopWordsTableSource

A custom stop-words snapshot read from a LanceDB table column.

## Properties

### column

```ts
column: string;
```

Name of the string column containing the stop words.

***

### source

```ts
source: "table";
```

Select a LanceDB table column.

***

### table

```ts
table: Table;
```

Local/native table containing the stop words.

A remote table cannot be used as the source. Materialize its stop-word
column locally first, or use an inline list or UTF-8 file.
