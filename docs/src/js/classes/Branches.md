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
checkout(name): Promise<Table>
```

Check out an existing branch and return a handle scoped to it.

#### Parameters

* **name**: `string`

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

### list()

```ts
list(): Promise<Record<string, BranchContents>>
```

List all branches, mapping name to branch metadata.

#### Returns

`Promise`&lt;`Record`&lt;`string`, [`BranchContents`](BranchContents.md)&gt;&gt;
