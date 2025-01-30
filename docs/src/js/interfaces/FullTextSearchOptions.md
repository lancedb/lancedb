[**@lancedb/lancedb**](../README.md) • **Docs**
***
[@lancedb/lancedb](../globals.md) / FullTextSearchOptions
# Interface: FullTextSearchOptions
Options that control the behavior of a full text search
## Properties
### columns?
```ts
optional columns: string | string[];
```
The columns to search
If not specified, all indexed columns will be searched.
For now, only one column can be searched.
