[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Tags

# Class: Tags

Manager for the table's version tags, obtained via [Table.tags](Table.md#tags).

A tag is a stable name pinned to a table version.

## Methods

### create()

```ts
create(tag, version): Promise<void>
```

Create a tag pointing at the given table version.

#### Parameters

• **tag**: `string`

• **version**: `number`

#### Returns

`Promise`&lt;`void`&gt;

***

### delete()

```ts
delete(tag): Promise<void>
```

Delete a tag.

#### Parameters

• **tag**: `string`

#### Returns

`Promise`&lt;`void`&gt;

***

### getVersion()

```ts
getVersion(tag): Promise<number>
```

Get the version a tag currently points to.

#### Parameters

• **tag**: `string`

#### Returns

`Promise`&lt;`number`&gt;

***

### list()

```ts
list(): Promise<Record<string, TagContents>>
```

List all tags, mapping name to tag metadata.

#### Returns

`Promise`&lt;`Record`&lt;`string`, [`TagContents`](../interfaces/TagContents.md)&gt;&gt;

***

### update()

```ts
update(tag, version): Promise<void>
```

Move a tag to the given table version.

#### Parameters

• **tag**: `string`

• **version**: `number`

#### Returns

`Promise`&lt;`void`&gt;
