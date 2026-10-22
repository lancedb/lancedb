[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / DirNamespaceConfig

# Interface: DirNamespaceConfig

Configuration for the built-in directory namespace (`"dir"`).

The directory namespace stores tables under a single root path (local
filesystem or object storage URI). See
[https://docs.lancedb.com/namespaces](https://docs.lancedb.com/namespaces) for the documented surface;
less-common knobs live under [DirNamespaceConfig.extraProperties](DirNamespaceConfig.md#extraproperties).

## Properties

### extraProperties?

```ts
optional extraProperties: Record<string, string>;
```

Additional raw properties passed verbatim to the namespace
implementation (e.g. `storage.*`, `credential_vendor.*`). Typed
fields above take precedence on key collision.

***

### manifestEnabled?

```ts
optional manifestEnabled: boolean;
```

Whether to maintain a namespace manifest at the root. Required for
child namespaces. Defaults to true on the impl side.

***

### root

```ts
root: string;
```

Root path or URI containing the LanceDB tables.
