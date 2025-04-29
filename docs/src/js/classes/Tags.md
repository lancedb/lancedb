[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Tags

# Class: Tags

## Constructors

### new Tags()

```ts
new Tags(): Tags
```

#### Returns

[`Tags`](Tags.md)

## Methods

### create()

```ts
create(tag, version): Promise<void>
```

#### Parameters

* **tag**: `string`

* **version**: `number`

#### Returns

`Promise`&lt;`void`&gt;

***

### delete()

```ts
delete(tag): Promise<void>
```

#### Parameters

* **tag**: `string`

#### Returns

`Promise`&lt;`void`&gt;

***

### getVersion()

```ts
getVersion(tag): Promise<number>
```

#### Parameters

* **tag**: `string`

#### Returns

`Promise`&lt;`number`&gt;

***

### list()

```ts
list(): Promise<Record<string, TagContents>>
```

#### Returns

`Promise`&lt;`Record`&lt;`string`, [`TagContents`](TagContents.md)&gt;&gt;

***

### update()

```ts
update(tag, version): Promise<void>
```

#### Parameters

* **tag**: `string`

* **version**: `number`

#### Returns

`Promise`&lt;`void`&gt;
