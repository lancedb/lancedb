[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / RenameTableOptions

# Interface: RenameTableOptions

## Properties

### namespacePath?

```ts
optional namespacePath: string[];
```

The namespace path of the table being renamed. Defaults to the root
namespace (`[]`) when omitted.

***

### newNamespacePath?

```ts
optional newNamespacePath: string[];
```

The namespace path to move the table to as part of the rename. When
omitted the table stays in `namespacePath`.
