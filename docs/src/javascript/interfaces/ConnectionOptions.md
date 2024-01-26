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

API key for the remote connections

Can also be passed by setting environment variable `LANCEDB_API_KEY`

#### Defined in

[index.ts:88](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L88)

___

### awsCredentials

• `Optional` **awsCredentials**: [`AwsCredentials`](AwsCredentials.md)

User provided AWS crednetials.

If not provided, LanceDB will use the default credentials provider chain.

#### Defined in

[index.ts:78](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L78)

___

### awsRegion

• `Optional` **awsRegion**: `string`

AWS region to connect to. Default is defaultAwsRegion.

#### Defined in

[index.ts:81](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L81)

___

### hostOverride

• `Optional` **hostOverride**: `string`

Override the host URL for the remote connection.

This is useful for local testing.

#### Defined in

[index.ts:98](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L98)

___

### region

• `Optional` **region**: `string`

Region to connect

#### Defined in

[index.ts:91](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L91)

___

### uri

• **uri**: `string`

LanceDB database URI.

- `/path/to/database` - local database
- `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
- `db://host:port` - remote database (LanceDB cloud)

#### Defined in

[index.ts:72](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L72)
