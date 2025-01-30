[**@lancedb/lancedb**](../README.md) • **Docs**
***
[@lancedb/lancedb](../globals.md) / AddColumnsSql
# Interface: AddColumnsSql
A definition of a new column to add to a table.
## Properties
### name
```ts
name: string;
```
The name of the new column.
***
### valueSql
```ts
valueSql: string;
```
The values to populate the new column with, as a SQL expression.
The expression can reference other columns in the table.
