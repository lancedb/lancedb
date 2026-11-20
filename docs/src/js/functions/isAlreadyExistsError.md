[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / isAlreadyExistsError

# Function: isAlreadyExistsError()

```ts
function isAlreadyExistsError(error): error is TableAlreadyExistsError | DatabaseAlreadyExistsError | ColumnAlreadyExistsError
```

True for table, database, and column create conflicts.

## Parameters

* **error**: `unknown`

## Returns

error is TableAlreadyExistsError \| DatabaseAlreadyExistsError \| ColumnAlreadyExistsError
