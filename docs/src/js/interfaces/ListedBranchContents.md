[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ListedBranchContents

# Interface: ListedBranchContents

Branch contents as returned by [Branches.list](../classes/Branches.md#list), with `createAt` and
`metadata` guaranteed to be present.

## Extends

- [`BranchContents`](BranchContents.md)

## Properties

### createAt

```ts
createAt: number;
```

Branch creation time in Unix seconds.

#### Overrides

[`BranchContents`](BranchContents.md).[`createAt`](BranchContents.md#createat)

***

### manifestSize

```ts
manifestSize: number;
```

Size in bytes of the branch's manifest file.

#### Inherited from

[`BranchContents`](BranchContents.md).[`manifestSize`](BranchContents.md#manifestsize)

***

### metadata

```ts
metadata: Record<string, string>;
```

Application-defined string metadata.

#### Overrides

[`BranchContents`](BranchContents.md).[`metadata`](BranchContents.md#metadata)

***

### parentBranch?

```ts
optional parentBranch: string;
```

Branch this branch was forked from. Absent for direct forks of `main`.

#### Inherited from

[`BranchContents`](BranchContents.md).[`parentBranch`](BranchContents.md#parentbranch)

***

### parentVersion

```ts
parentVersion: number;
```

Version on the parent branch this branch was forked from.

#### Inherited from

[`BranchContents`](BranchContents.md).[`parentVersion`](BranchContents.md#parentversion)
