[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TlsConfig

# Interface: TlsConfig

TLS/mTLS configuration for the remote HTTP client.

## Properties

### assertHostname?

```ts
optional assertHostname: boolean;
```

Whether to verify the hostname in the server's certificate.

***

### certFile?

```ts
optional certFile: string;
```

Path to the client certificate file (PEM format) for mTLS authentication.

***

### keyFile?

```ts
optional keyFile: string;
```

Path to the client private key file (PEM format) for mTLS authentication.

***

### sslCaCert?

```ts
optional sslCaCert: string;
```

Path to the CA certificate file (PEM format) for server verification.
