# Apache OpenDAL™ Rust Core: One Layer, All Storage.

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal/ci_core.yml?branch=main
[actions]: https://github.com/apache/opendal/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/opendal.svg
[crates.io]: https://crates.io/crates/opendal
[crate downloads]: https://img.shields.io/crates/d/opendal.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

Apache OpenDAL™ is an Open Data Access Layer that enables seamless interaction with diverse storage services.

<img src="https://opendal.apache.org/img/architectural.png" alt="OpenDAL Architectural" width="61.8%" />

## Useful Links

- **User guide**: [opendal.apache.org/docs/core](https://opendal.apache.org/docs/core) — install, operations, layers, idioms, and extending the core.
- **API reference**: [docs.rs/opendal](https://docs.rs/opendal/) (release) | [dev](https://opendal.apache.org/docs/rust/opendal/)
- **Services & configuration**: [opendal.apache.org/services](https://opendal.apache.org/services)
- **Concepts**: [opendal.apache.org/docs/concepts](https://opendal.apache.org/docs/concepts)
- [Upgrade Guide](https://github.com/apache/opendal/blob/main/core/core/src/docs/upgrade.md) | [Release Notes](https://github.com/apache/opendal/blob/main/CHANGELOG.md) | [RFCs](https://github.com/apache/opendal/tree/main/core/core/src/docs/rfcs)
- [Examples](https://github.com/apache/opendal/tree/main/core/examples)

## Installation

```shell
cargo add opendal
```

Each service is a feature flag. The in-memory service is always available; enable
any other service with its `services-*` feature, e.g. `features = ["services-s3"]`.

## Quickstart

```rust
use opendal::services;
use opendal::Operator;
use opendal::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure a service, then build an operator from it.
    let op = Operator::new(services::Memory::default())?;

    // The same verbs work on every service.
    op.write("hello.txt", "Hello, World!").await?;
    let bytes = op.read("hello.txt").await?;
    let meta = op.stat("hello.txt").await?;
    op.delete("hello.txt").await?;

    println!("read {} bytes", meta.content_length());
    Ok(())
}
```

To use a real backend, swap `Memory` for another service and configure it — the
operations stay identical. See [Getting started](https://opendal.apache.org/docs/core/getting-started)
and [Connecting to your storage](https://opendal.apache.org/docs/core/connecting).

## Services

OpenDAL talks to 50+ backends through one API. A selection by category:

| Type                       | Services                                                        |
|----------------------------|-----------------------------------------------------------------|
| Standard Protocols         | ftp, http, sftp, webdav                                         |
| Object Storage             | s3, gcs, azblob, oss, cos, obs, b2, vercel-blob, …             |
| File Storage               | fs, hdfs, azdls, azfile, webhdfs, ipfs, …                      |
| Consumer Cloud Storage     | gdrive, onedrive, dropbox, aliyun-drive, koofr, …             |
| Key-Value & Database       | redis, etcd, tikv, rocksdb, sqlite, postgresql, mongodb, …    |
| Cache                      | memcached, moka, mini-moka, ghac, vercel-artifacts            |

See [Services](https://opendal.apache.org/services) for the full list and each
service's configuration keys.

## Layers

Wrap an operator with layers to add retry, logging, timeout, metrics, and other
cross-cutting behavior without touching your storage code. Retry, logging,
timeout, and concurrency limit are built in:

```rust
use opendal::layers::RetryLayer;
use opendal::services::Memory;
use opendal::{Operator, Result};

fn build_operator() -> Result<Operator> {
    let op = Operator::new(Memory::default())?;
    Ok(op.layer(RetryLayer::new()))
}
```

See [Going to production](https://opendal.apache.org/docs/core/production) and the
[`layers` module](https://docs.rs/opendal/latest/opendal/layers/index.html).

## Contributing

Check out the [CONTRIBUTING](https://github.com/apache/opendal/blob/main/CONTRIBUTING.md) guide for building, testing, and
submitting changes to the core.

## Used by

Check out the [users](https://github.com/apache/opendal/blob/main/core/users.md) list for more details on who is using OpenDAL.

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: <http://www.apache.org/licenses/LICENSE-2.0>

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
