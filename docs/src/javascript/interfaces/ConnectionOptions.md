[vectordb](../README.md) / [Exports](../modules.md) / ConnectionOptions

# Interface: ConnectionOptions

## Table of contents

### Properties

- [apiKey](ConnectionOptions.md#apikey)
- [awsCredentials](ConnectionOptions.md#awscredentials)
- [awsRegion](ConnectionOptions.md#awsregion)
- [hostOverride](ConnectionOptions.md#hostoverride)
- [region](ConnectionOptions.md#region)
- [uri](ConnectionOptions.md#uri)

## Properties

### apiKey

• `Optional` **apiKey**: `string`

#### Defined in

[index.ts:81](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L81)

___

### awsCredentials

• `Optional` **awsCredentials**: [`AwsCredentials`](AwsCredentials.md)

User provided AWS crednetials.

If not provided, LanceDB will use the default credentials provider chain.

#### Defined in

[index.ts:75](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L75)

___

### awsRegion

• `Optional` **awsRegion**: `string`

AWS region to connect to. Default is defaultAwsRegion.

#### Defined in

[index.ts:78](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L78)

___

### hostOverride

• `Optional` **hostOverride**: `string`

Override the host URL for the remote connections.

This is useful for local testing.

#### Defined in

[index.ts:91](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L91)

___

### region

• `Optional` **region**: `string`

Region to connect

#### Defined in

[index.ts:84](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L84)

___

### uri

• **uri**: `string`

LanceDB database URI.

- `/path/to/database` - local database
- `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
- `db://host:port` - remote database (SaaS)

#### Defined in

[index.ts:69](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L69)
