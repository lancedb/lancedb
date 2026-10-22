[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / OAuthFlowType

# Enumeration: OAuthFlowType

OAuth authentication flow types.

## Enumeration Members

### AuthorizationCode

```ts
AuthorizationCode: "authorization_code";
```

Interactive Authorization Code grant, using PKCE by default.

***

### AzureManagedIdentity

```ts
AzureManagedIdentity: "azure_managed_identity";
```

Azure Managed Identity via IMDS.

***

### ClientCredentials

```ts
ClientCredentials: "client_credentials";
```

Client Credentials grant (service-to-service / M2M).

***

### DeviceCode

```ts
DeviceCode: "device_code";
```

Device Authorization grant for CLI and headless environments.
