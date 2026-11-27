[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BranchContents

# Interface: BranchContents

Contents of a table branch.

This is the shape released in 0.41.0. Newer fields exposed by
[Branches.list](../classes/Branches.md#list) are declared on [ListedBranchContents](ListedBranchContents.md) so that
values and interfaces written against the released shape remain assignable.

## Extended by

- [`ListedBranchContents`](ListedBranchContents.md)

## Properties

### manifestSize

```ts
manifestSize: number;
```

Size in bytes of the branch's manifest file.

***

### parentBranch?

```ts
optional parentBranch: string;
```

Branch this branch was forked from. Absent for direct forks of `main`.

***

### parentVersion

```ts
parentVersion: number;
```

Version on the parent branch this branch was forked from.
