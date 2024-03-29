[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / ConnectionOptions

# Interface: ConnectionOptions

## Table of contents

### Properties

- [apiKey](ConnectionOptions.md#apikey)
- [hostOverride](ConnectionOptions.md#hostoverride)
- [readConsistencyInterval](ConnectionOptions.md#readconsistencyinterval)

## Properties

### apiKey

• `Optional` **apiKey**: `string`

#### Defined in

native.d.ts:51

___

### hostOverride

• `Optional` **hostOverride**: `string`

#### Defined in

native.d.ts:52

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

native.d.ts:64
