[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CreateBranchOptions

# Interface: CreateBranchOptions

Options for creating a branch from a named source.

## Properties

### fromTag

```ts
fromTag: string;
```

Tag to fork from. The tag keeps the branch and version it points to.
Cannot be combined with `fromRef` or `fromVersion`.
