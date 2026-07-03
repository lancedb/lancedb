[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / RestNamespaceConfig

# Interface: RestNamespaceConfig

Configuration for the built-in REST namespace (`"rest"`).

The REST namespace talks to a remote catalog server over HTTP. See
[https://docs.lancedb.com/namespaces](https://docs.lancedb.com/namespaces) for the documented surface;
less-common knobs (TLS, metrics) live under
[RestNamespaceConfig.extraProperties](RestNamespaceConfig.md#extraproperties).

## Properties

### extraProperties?

```ts
optional extraProperties: Record<string, string>;
```

Additional raw properties passed verbatim to the namespace
implementation (e.g. `tls.*`, `ops_metrics_enabled`, `delimiter`).
Typed fields above take precedence on key collision.

***

### headers?

```ts
optional headers: Record<string, string>;
```

HTTP headers forwarded with each request. Keys are passed through
as-is (e.g. `"x-api-key"`, `"Authorization"`).

***

### uri

```ts
uri: string;
```

Catalog endpoint URL.
