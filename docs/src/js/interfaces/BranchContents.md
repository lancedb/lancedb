[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BranchContents

# Interface: BranchContents

Contents of a table branch.

`createAt` and `metadata` are optional on this type so that values written
against releases that did not expose them remain assignable. Both are
always set on the instances returned by [Branches.list](../classes/Branches.md#list).

## Extended by

- [`ListedBranchContents`](ListedBranchContents.md)

## Properties

### createAt?

```ts
optional createAt: number;
```

Branch creation time in Unix seconds.

***

### manifestSize

```ts
manifestSize: number;
```

Size in bytes of the branch's manifest file.

***

### metadata?

```ts
optional metadata: Record<string, string>;
```

Application-defined string metadata.

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
