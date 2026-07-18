[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Branches

# Class: Branches

Branch manager for a [Table](Table.md).

Unlike tags, `create` and `checkout` return a new [Table](Table.md) handle scoped
to the branch; writes on it do not affect `main`.

## Methods

### checkout()

```ts
checkout(name, version?): Promise<Table>
```

Check out an existing branch and return a handle scoped to it.

With `version` set, the returned handle is pinned to that version of the
branch (a read-only, detached view); otherwise it tracks the branch's
latest and stays writable.

#### Parameters

* **name**: `string`

* **version?**: `number`

#### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### create()

```ts
create(
   name,
   fromRef?,
   fromVersion?): Promise<Table>
```

Create a branch and return a handle scoped to it.

#### Parameters

* **name**: `string`
    Name of the new branch.

* **fromRef?**: `string`
    Source branch to fork from. Defaults to `main`.

* **fromVersion?**: `number`
    A specific version on `fromRef`. Defaults to latest.

#### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### delete()

```ts
delete(name): Promise<void>
```

Delete a branch.

#### Parameters

* **name**: `string`

#### Returns

`Promise`&lt;`void`&gt;

***

### diff()

```ts
diff(fromBranch): Promise<BranchDiff>
```

Compare a branch against main without modifying either branch.

#### Parameters

* **fromBranch**: `string`

#### Returns

`Promise`&lt;[`BranchDiff`](../interfaces/BranchDiff.md)&gt;

***

### list()

```ts
list(): Promise<Record<string, BranchContents>>
```

List all branches, mapping name to branch metadata.

#### Returns

`Promise`&lt;`Record`&lt;`string`, [`BranchContents`](BranchContents.md)&gt;&gt;

***

### merge()

```ts
merge(fromBranch, dryRun): Promise<MergeBranchResult>
```

Merge a branch into main.

Set `dryRun` to `true` to preview the merge. A rejected merge resolves
with `status: "rejected"` instead of throwing.

#### Parameters

* **fromBranch**: `string`
    Branch to merge from.

* **dryRun**: `boolean` = `false`
    When true, only preview the merge. Defaults to false.

#### Returns

`Promise`&lt;[`MergeBranchResult`](../interfaces/MergeBranchResult.md)&gt;
