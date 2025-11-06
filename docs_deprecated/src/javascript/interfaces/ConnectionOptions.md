[vectordb](../README.md) / [Exports](../modules.md) / ConnectionOptions

# Interface: ConnectionOptions

## Table of contents

### Properties

- [apiKey](ConnectionOptions.md#apikey)
- [awsCredentials](ConnectionOptions.md#awscredentials)
- [awsRegion](ConnectionOptions.md#awsregion)
- [hostOverride](ConnectionOptions.md#hostoverride)
- [readConsistencyInterval](ConnectionOptions.md#readconsistencyinterval)
- [region](ConnectionOptions.md#region)
- [storageOptions](ConnectionOptions.md#storageoptions)
- [timeout](ConnectionOptions.md#timeout)
- [uri](ConnectionOptions.md#uri)

## Properties

### apiKey

• `Optional` **apiKey**: `string`

API key for the remote connections

Can also be passed by setting environment variable `LANCEDB_API_KEY`

#### Defined in

[index.ts:112](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L112)

___

### awsCredentials

• `Optional` **awsCredentials**: [`AwsCredentials`](AwsCredentials.md)

User provided AWS crednetials.

If not provided, LanceDB will use the default credentials provider chain.

**`Deprecated`**

Pass `aws_access_key_id`, `aws_secret_access_key`, and `aws_session_token`
through `storageOptions` instead.

#### Defined in

[index.ts:92](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L92)

___

### awsRegion

• `Optional` **awsRegion**: `string`

AWS region to connect to. Default is defaultAwsRegion

**`Deprecated`**

Pass `region` through `storageOptions` instead.

#### Defined in

[index.ts:98](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L98)

___

### hostOverride

• `Optional` **hostOverride**: `string`

Override the host URL for the remote connection.

This is useful for local testing.

#### Defined in

[index.ts:122](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L122)

___

### readConsistencyInterval

• `Optional` **readConsistencyInterval**: `number`

(For LanceDB OSS only): The interval, in seconds, at which to check for
updates to the table from other processes. If None, then consistency is not
checked. For performance reasons, this is the default. For strong
consistency, set this to zero seconds. Then every read will check for
updates from other processes. As a compromise, you can set this to a
non-zero value for eventual consistency. If more than that interval
has passed since the last check, then the table will be checked for updates.
Note: this consistency only applies to read operations. Write operations are
always consistent.

#### Defined in

[index.ts:140](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L140)

___

### region

• `Optional` **region**: `string`

Region to connect. Default is 'us-east-1'

#### Defined in

[index.ts:115](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L115)

___

### storageOptions

• `Optional` **storageOptions**: `Record`\<`string`, `string`\>

User provided options for object storage. For example, S3 credentials or request timeouts.

The various options are described at https://lancedb.github.io/lancedb/guides/storage/

#### Defined in

[index.ts:105](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L105)

___

### timeout

• `Optional` **timeout**: `number`

Duration in milliseconds for request timeout. Default = 10,000 (10 seconds)

#### Defined in

[index.ts:127](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L127)

___

### uri

• **uri**: `string`

LanceDB database URI.

- `/path/to/database` - local database
- `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
- `db://host:port` - remote database (LanceDB cloud)

#### Defined in

[index.ts:83](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L83)
