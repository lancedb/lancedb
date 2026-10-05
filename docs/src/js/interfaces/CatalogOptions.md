[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CatalogOptions

# Interface: CatalogOptions

Options shared by a catalog and the database connections it returns.

## Extends

- `Omit`&lt;`NativeCatalogOptions`, `"oauthConfig"`&gt;

## Properties

### apiKey?

```ts
optional apiKey: string;
```

#### Inherited from

`Omit.apiKey`

***

### clientConfig?

```ts
optional clientConfig: ClientConfig;
```

#### Inherited from

`Omit.clientConfig`

***

### headerProvider?

```ts
optional headerProvider: HeaderProvider | () => Record<string, string> | Promise<Record<string, string>>;
```

Called for each request to supply authentication headers.

***

### oauthConfig?

```ts
optional oauthConfig: OAuthConfig;
```

***

### readConsistencyInterval?

```ts
optional readConsistencyInterval: number;
```

#### Inherited from

`Omit.readConsistencyInterval`

***

### sqlHostOverride?

```ts
optional sqlHostOverride: string;
```

SQL service endpoint inherited by database connections.

#### Inherited from

`Omit.sqlHostOverride`
