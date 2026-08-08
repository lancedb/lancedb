# Upgrade to v0.58

## Public API

### `Operator::new` returns a finished operator

`Operator::new(builder)` now returns `Result<Operator>` directly. Remove the old `finish()` call from construction code:

```diff
- let op = Operator::new(builder)?.finish();
+ let op = Operator::new(builder)?;
```

### Runtime composition APIs consolidated

Operator runtime resources now live in `OperationContext`, which is exported from the crate root instead of `raw`.

- Replace `raw::OperationContext` imports with `opendal::OperationContext`.
- Replace `Operator::http_transport(...)` and `Operator::executor(...)` with `Operator::with_context(...)`.
- Replace `Operator::from_service(...)` with `Operator::from_parts(ctx, service)`.
- Replace `Operator::inner()`, `Operator::into_inner()`, and `Operator::executor_ref()` with `service()`, `context()`, or `into_parts()` depending on whether you need borrowed or owned composed state.
- `Operator::from_inner(...)` is deprecated; use `Operator::from_parts(...)`.

### Native and full capability APIs removed

`OperatorInfo::native_capability()` has been removed. `OperatorInfo::capability()` is now the public availability contract for the composed operator stack.

### Suffix reads use public `BytesRange`

`BytesRange` is now part of the public type layer. Reader APIs accept values convertible into `BytesRange`, and suffix reads can be requested with `BytesRange::suffix(size)`. Code that imported raw HTTP range helpers should use `opendal::BytesRange` instead.

## Raw API

### Services and layers use the new composition boundary

Out-of-tree services must implement `raw::Service` instead of the old accessor pipeline. Services now return immutable identity through `ServiceInfo`, report availability through `capability()`, and receive `&OperationContext` at each operation boundary.

Out-of-tree layers must implement `Layer::apply_service` and, when they replace runtime resources, `Layer::apply_context`. The old typed layer pipeline, including `TypedLayer`, `RuntimeResourceLayer`, `TypeEraseLayer`, `OperatorBuilder<S>`, and `typed_layer`, has been removed.

### Stateful operation factories are synchronous

`Service` and `ServiceDyn` factories for `read`, `write`, `delete`, `list`, and `copy` now return OIO bodies synchronously. Move asynchronous work into the returned body implementation instead of returning `(Rp*, body)` futures from the factory.

### Removed raw helpers

The unused `raw::AtomicContentLength`, `raw::PathCacher`, and `raw::PathQuery` helpers have been removed. The `internal-path-cache` feature has also been removed.

# Upgrade to v0.57

## Public API

### Read response metadata

Read operations can now expose metadata returned by the underlying service while opening the read response. `Reader::metadata()` returns the complete object metadata already observed by this reader, and `BufferStream::metadata()` can open the read response and return metadata before the response body is consumed.

`Operator::read`, `read_with`, and `read_options` still return `Buffer`.

### Copy APIs return `Metadata`

`Operator::copy`, `copy_options`, `copy_with`, and their blocking equivalents now return `Metadata` instead of `()`, matching write completion behavior.

Out-of-tree raw services and layers that implement copy must migrate to the new copier flow:

- `Access::copy` returns `(RpCopy, Self::Copier)`.
- `Access` implementations define a `type Copier`.
- `oio::Copy::close` returns the server-side completion `Metadata`.

### `RetryInterceptor::intercept` takes `RetryEvent`

`RetryInterceptor::intercept` now receives a single `RetryEvent<'_>` argument instead of `(&Error, Duration)`. The event carries the operation being retried and a 1-based retry attempt counter, and is `#[non_exhaustive]` so future fields can be added without another break.

### HTTP metrics include `service_operation`

HTTP metrics emitted by the metrics layers now include a `service_operation` label for backend-specific request names such as `GetObject` or `UploadPart`. Metrics consumers and dashboards that assume the previous HTTP metric label set must be updated.

## Raw API

### `RpRead` carries optional `Metadata`

`RpRead` now carries optional complete object `Metadata` observed while opening a read operation. The old size/range fields have been removed.

Out-of-tree services should return:

- `RpRead::new(metadata)` when the read response natively provides metadata.
- `RpRead::default()` when the service does not provide read metadata.

The `metadata.content_length()` value must describe the full object size, even for range reads.

## Services

### `allow_anonymous` renamed to `skip_signature`

S3-compatible services now use `skip_signature` to describe requests that bypass credential loading and request signing:

- For S3, OSS, and GCS, `allow_anonymous` is kept as a deprecated compatibility alias. New code should use `skip_signature()` on builders or `skip_signature = true` in config.
- For TOS, migrate from `allow_anonymous(true)` / `allow_anonymous = true` to `skip_signature()` / `skip_signature = true`.
- For GCS, the behavior changed from fallback-on-credential-error to an unconditional signing bypass. If requests should be signed, do not set either `skip_signature` or the deprecated `allow_anonymous` alias.

### Hugging Face `repo_type` is required

The Hugging Face service no longer defaults `repo_type` to `model`. Set it explicitly when constructing the service:

```diff
 let builder = opendal_service_hf::Hf::default()
+    .repo_type("model")
     .repo_id("username/repo");
```

For config maps and environment-driven setup, add `repo_type = "model"` or the correct repository type: `model`, `dataset`, `space`, or `bucket`.

The service also adds a `download_mode` option. The default is `xet`; set `download_mode = "http"` to force plain HTTP downloads.

### TOS versioning option removed

`TosBuilder::enable_versioning` and `TosConfig::enable_versioning` have been removed. TOS now declares versioned stat/read/delete capabilities natively.

# Upgrade to v0.56

## Public API

### Deprecated HTTP client builder hooks removed

The long-deprecated `http_client` customization hooks on service builders have been removed. Configure custom HTTP behavior with the layered HTTP client APIs instead of mutating service builders directly.

### `TimeoutLayer::with_speed` removed

`TimeoutLayer::with_speed` has been removed after a deprecation cycle. Use `with_io_timeout` to enforce per-IO deadlines instead.

## Raw API

### Test helpers moved to `opendal_testkit`

The old `opendal::raw::tests` module has been split into the standalone `opendal-testkit` crate. If you maintain out-of-tree services or integrations, replace imports from `opendal::raw::tests` with `opendal::tests` or depend on `opendal-testkit` directly.

# Upgrade to v0.55

## Public API

### Timestamp types now come from `jiff`

All public metadata APIs that previously exposed `chrono::DateTime<Utc>` now use `jiff::Timestamp`. For example, `Metadata::last_modified()` and related setters return/accept `Timestamp` values (`core/src/types/metadata.rs`). Update downstream crates to depend on `jiff` if they manipulate these timestamps or convert them to other formats.

### Scheme handling is string-based

`OperatorInfo::scheme()` now returns `&'static str` instead of `Scheme`, and `Operator::via_iter` accepts `impl AsRef<str>` (typically the `services::*_SCHEME` constants). Additionally, the deprecated constructors `Operator::from_map` and `Operator::via_map` have been removed. Migrate any code that relied on the enum variants or the removed constructors to the new string-based constants and `from_iter`/`via_iter`.

### List APIs only support `versions`

`OpList::with_version()`/`version()` and `Capability::list_with_version` have been removed after a long deprecation cycle. Use `with_versions()`/`versions()` on `OpList` and read `Capability::list_with_versions` instead.

### `S3Builder::security_token` removed

`S3Builder` no longer exposes the deprecated `security_token()` helper. Use `session_token()` exclusively when configuring temporary credentials.

### KV-style services no longer pretend to support `list`

Services that never returned meaningful results for `Operator::list` (such as D1, FoundationDB, GridFS, Memcached, MongoDB, MySQL, Persy, PostgreSQL, Redb, Redis, SurrealDB, TiKV, etc.) now return explicit `Unsupported` errors from their service implementations. Those features will be implemented later.

## Raw API

### Deprecated KV adapters removed

The legacy `opendal::raw::adapters::{kv, typed_kv}` modules have been deleted. Services should directly implement `Access` instead of depending on the adapters. Remove the corresponding imports and shim layers from any out-of-tree services.

# Upgrade to v0.54

## Public API

### RFC-6189: Remove Native Blocking Support

OpenDAL v0.54 implements [RFC-6189](https://opendal.apache.org/docs/rust/opendal/docs/rfcs/rfc_6189_remove_native_blocking/index.html), which removes all native blocking support in favor of using `block_on` from async runtimes.

The following breaking changes have been made:

- `blocking::Operator` can no longer be used within async contexts
- Using blocking APIs now requires an async runtime
- All `Blocking*` types have been moved to the `opendal::blocking` module

To migrate:

```diff
- use opendal::BlockingOperator;
+ use opendal::blocking::Operator;
```

### RFC-6213: Options Based API

OpenDAL v0.54 implements [RFC-6213](https://opendal.apache.org/docs/rust/opendal/docs/rfcs/rfc_6213_options_api/index.html), which introduces options-based APIs for more structured and extensible operation configuration.

New APIs added:

- `read_options(path, ReadOptions)`
- `write_options(path, data, WriteOptions)`
- `list_options(path, ListOptions)`
- `stat_options(path, StatOptions)`
- `delete_options(path, DeleteOptions)`

Example usage:

```rust
// Read with options
let options = ReadOptions::new()
    .range(0..1024)
    .if_match("etag");
let data = op.read_options("path/to/file", options).await?;

// Write with options
let options = WriteOptions::new()
    .content_type("text/plain")
    .cache_control("max-age=3600");
op.write_options("path/to/file", data, options).await?;
```

### Remove `stat_has_xxx` and `list_has_xxx` APIs

All `stat_has_*` and `list_has_*` capability check APIs have been removed. Instead, check capabilities directly on the `Capability` struct:

```diff
- if op.info().capability().stat_has_content_length() {
+ if op.info().capability().stat.content_length {
    // ...
}
```

### Fix `with_user_metadata` signature

The signature of `with_user_metadata` has been changed. Please update your code accordingly if you use this method.

### Services removed due to lack of maintainer

The following services have been removed due to lack of maintainers:

- `atomicserver`
- `icloud`
- `nebula-graph`

If you need these services, please consider maintaining them or use alternative services.

### `Operator::http_client` replaces `update_http_client`

The `Operator::update_http_client()` method has been replaced by `Operator::http_client`:

```diff
- op.update_http_client(client);
+ op = op.http_client(client);
```

### Expose `presign_xxx_options` API

New options-based presign APIs have been exposed:

```rust
let options = PresignOptions::new()
    .expire(Duration::from_secs(3600));

let url = op.presign_read_options("path/to/file", options).await?;
```

## Raw API

### Remove native blocking support

All native blocking implementations have been removed from the raw API. Services and layers no longer need to implement blocking-specific methods.

# Upgrade to v0.53

## Public API

### Supabase service is now an S3-compatible servcice

Supabase Storage is now an S3-compatible service instead: https://github.com/supabase/storage.

We removed the supabase native service support in OpenDAL v0.53. Users who want to access Supabase Storage can use the S3 service instead.

### All metrics related layers have been refactored

All metrics layers have been refactored:

- `PrometheusLayer`
- `PrometheusClientLayer`
- `MetricsLayer`

They are now provides more metrics and more detailed information. All their public API have been redesigned.

For more details, please refer to `opendal::layers::observe`'s module documentation.

### `Operator::default_executor` has been replaced by `Operator::executor`

In opendal v0.53, we introduced a new concept of `Context` which is used to store the context of the current operator. Thanks to this design, we can now get and set the `executor` and `http_client` for given Operator instead.

All services `http_client` API has been deprecated and replaced by `Operator::update_http_client` API.

### OpenDAL MSRV bumped to `1.82`

Since v0.53, OpenDAL will require Rust 1.82.0 or later to build.

## Raw API

### Operation enum merge

To reduce the complexity of the `Operation`, we have merged the duplicated `Operation`.

For example:

- `Operation::ReaderRead` has been merged into `Operation::Read`
- `Operation::BlockingRead` has been merged into `Operation::Read`

# Upgrade to v0.52

## Public API

### RFC-5556: Write Returns Metadata

Since v0.52, all write APIs in OpenDAL have been updated to return `Metadata` instead of `()`. This metadata includes useful information provided by the service, such as `content-length`, `etag`, `version`, and `last-modified`.

This feature is not fully ready yet, and many available metadata fields are still not returned. Please visit [Tracking Issues of RFC-5556: Write Returns Metadata](https://github.com/apache/opendal/issues/5557) for progress and contributions.

Affected API:

- `opendal::Operator::write`
- `opendal::Operator::write_with`
- `opendal::Operator::writer::close`
- `opendal::raw::oio::Write::close`

### Github Actions Cache (ghac) service v2

As [requested](https://github.com/apache/opendal/issues/5620) by GitHub, we have upgraded our GHAC service to ensure compatibility with the latest GitHub Actions cache API.

By upgrading to OpenDAL v0.52, your services will continue functioning after the deprecation of the legacy service (2025/03/01). GHES does not yet support GHAC v2, but OpenDAL has handled this properly to prevent any disruptions.

ghac service doesn't support `delete` anymore, please use github's API to delete cache instead.

This upgrade is mandatory and enabled by default using an environment variable in the GitHub CI environment. No changes are required at the code level.

### Breaking Changes in Dependencies

- `OtelTraceLayer` and `OtelMetricsLayer`'s dependence `opentelemetry` bumped to `0.28`
- `PrometheusClientLayer`'s dependence `prometheus-client` bumped to `0.23.1`

# Upgrade to v0.51

## Public API

### New VISION: One Layer, All Storage

OpenDAL has refined its vision to **One Layer, All Storage**, driven by the following core principles: **Open Community**, **Solid Foundation**, **Fast Access**, **Object Storage First**, and **Extensible Architecture**.

Explore the detailed vision at [OpenDAL Vision](https://opendal.apache.org/vision).

### RFC-5313: Remove Metakey

OpenDAL v0.51 implements [RFC-5313](https://opendal.apache.org/docs/rust/opendal/docs/rfcs/rfc_5314_remove_metakey/index.html), which removes the concept of metakey.

The following structs have been removed:

- `Metakey`

The following APIs have been removed:

- `list_with(path).metakey()`

Users no longer need to pass the metakey into the list. Instead, services will make their best effort to return as much metadata as possible. Users can check items like `Capability::list_has_etag` before making a call.

### Remove not used capability: `write_multi_align_size`

The capability `write_multi_align_size` is not utilized by any services, and we have no plans to support it in the future; therefore, we have removed it.

### CapabilityCheckLayer and CorrectnessCheckLayer

OpenDAL used to perform capability checks for all services, but since v0.51, it only conducts checks that impact data correctness like `write_with_if_not_exists` or `delete_with_version` by default in the `CorrectnessCheckLayer`. If users wish to verify other non-critical capabilities like `write_with_content_type` or `write_with_cache_control`, they should manually enable the `CapabilityCheckLayer`.

### RFC-3911: Deleter API

OpenDAL v0.51 implements [RFC-3911](https://opendal.apache.org/docs/rust/opendal/docs/rfcs/rfc_3911_deleter_api/index.html), which adds `Deleter` in OpenDAL to replace `batch` operation.

The following new APIs have been added:

- [`Operator::delete_iter`]
- [`Operator::delete_try_iter`]
- [`Operator::delete_stream`]
- [`Operator::delete_try_stream`]
- [`Operator::deleter`]
- [`Deleter::delete`]
- [`Deleter::delete_iter`]
- [`Deleter::delete_try_iter`]
- [`Deleter::delete_stream`]
- [`Deleter::delete_try_stream`]
- [`Deleter::flush`]
- [`Deleter::close`]
- [`Deleter::into_sink`]
- [`DeleteInput`]
- [`IntoDeleteInput`]
- [`FuturesDeleteSink`]

The following APIs have been deprecated and will be removed in the future releases:

- `Operator::remove` (replace with [`Operator::delete_iter`])
- `Operator::remove_via` (replace with [`Operator::delete_stream`])

As a result of this change, the `limit` and `with_limit` APIs on `Operator` have also been deprecated; they are currently no-ops.

## Raw API

### `adapter::kv` now returns `Scanner` instead of `Vec<String>`

To support returning key-value entries in a streaming manner instead of loading them all into memory, OpenDAL updated its adapter API to return a `Scanner` instead of a `Vec<String>`.

```diff
- async fn scan(&self, path: &str) -> Result<Vec<String>>
+ async fn scan(&self, path: &str) -> Result<Self::Scanner>
```

All services intending to implement `kv::Adapter` should adhere to this API change.

## Align `metadata` API to `info`

OpenDAL changes it's old `metadata` API to `info` to align with the new `AccessorInfo` struct.

```diff
- fn metadata(&self) -> Arc<AccessorInfo>
+ fn info(&self) -> Arc<AccessorInfo>
```

### Remove not used struct: `RangeWriter`

The struct `RangeWriter` is not utilized by any services, and we have no plans to support it in the future; therefore, we have removed it.

# Upgrade to v0.50

## Public API

### `services-postgresql`'s connect string now supports only URL format

Previously, it supports both URL format and key-value format. After switching the implementation from `tokio-postgres` to `sqlx`, the service now supports only the URL format.

### `list` now returns path itself

Previously, `list("a/b")` would not return `a/b` even if it does exist. Since v0.50.0, this behavior has been changed. OpenDAL will now return the path itself if it exists. This change applies to all cases, whether the path is a directory or a file.

### Refactoring of the metrics-related layer

In OpenDAL v0.50.0, we did a refactor on all metrics-related layers. They are now sharing the same underlying implementations. `PrometheusLayer`, `PrometheusClientLayer` and `MetricsLayer` are now have similar public APIs and exactly the same metrics value.

# Upgrade to v0.49

## Public API

### `Configurator` now returns associated builder instead

`Configurator` used to return `impl Builder`, but now it returns associated builder type directly. This will allow users to use the builder in a more flexible way.

```diff
impl Configurator for MemoryConfig {
-    fn into_builder(self) -> impl Builder {
+    type Builder = MemoryBuilder;
+    fn into_builder(self) -> Self::Builder {
        MemoryBuilder { config: self }
    }
}
```

### `LoggingLayer` now accepts `LoggingInterceptor`

`LoggingLayer` now accepts `LoggingInterceptor` trait instead of configuration. This change will allow users to customize the logging behavior more flexibly.

```diff
pub trait LoggingInterceptor: Debug + Clone + Send + Sync + Unpin + 'static {
    fn log(
        &self,
        info: &AccessorInfo,
        operation: Operation,
        context: &[(&str, &str)],
        message: &str,
        err: Option<&Error>,
    );
}
```

Users can now implement the log in the way they want.

# Upgrade to v0.48

## Public API

### Typo in `customized_credential_load`

Since v0.48, the `customed_credential_load` function has been renamed to `customized_credential_load` to fix the typo of `customized`.

```diff
- builder.customed_credential_load(v);
+ builder.customized_credential_load(v);
```

### S3 service rename `security_token` to `session_token`

[In 2014 Amazon switched](https://aws.amazon.com/blogs/security/a-new-and-standardized-way-to-manage-credentials-in-the-aws-sdks/) from `AWS_SECURITY_TOKEN` to `AWS_SESSION_TOKEN`. To be consistent with the naming of AWS STS, we have renamed the `security_token` field to `session_token` in the S3 service.

```diff
- builder.security_token(v);
+ builder.session_token(v);
```

### Operator `from_iter` and `via_iter` replaces `from_map` and `via_map`

Since v0.48, Operator's new APIs `from_iter` and `via_iter` methods have deprecated the `from_map` and `via_map` methods.

```diff
- Operator::from_map::<Fs>(map)?;
+ Operator::from_iter::<Fs>(map)?;
```

New API `from_iter` and `via_iter` should cover all use cases of `from_map` and `via_map`.

### Service builder now takes ownership

Since v0.48, all service builder now takes ownership `self` instead of `&mut self`. This change will allow users to configure the service in a more flexible way.

```diff
- let mut builder = S3::default();
- builder.bucket("test");
- builder.root("/path/to/root");
+ let builder = S3::default().bucket("test").root("/path/to/root");
  let op = Operator::new(builder)?;
```

## Raw API

### `oio::Write::write` will write the whole buffer

Starting from version 0.48, `oio::Write::write` now writes the entire buffer. This update aligns the API more closely with `oio::Read::read` and simplifies the implementation of concurrent writing.

```diff
  trait Write {
-     fn write(&mut self, bs: Buffer) -> impl Future<Output = Result<usize>>;
+     fn write(&mut self, bs: Buffer) -> impl Future<Output = Result<()>>;
  }
```

`write` will now return `Result<()>` instead of `Result<usize>`. The number of bytes written can be obtained from the buffer's length.

### `Access::metadata()` will return `Arc<AccessInfo>`

Starting from version 0.48, `Access::metadata()` will return `Arc<AccessInfo>` instead of `AccessInfo`. This change is intended to improve performance and reduce memory usage.

```diff
  trait Access {
-     fn metadata(&self) -> AccessInfo;
+     fn metadata(&self) -> Arc<AccessInfo>;
  }
```

### `MinitraceLayer` renamed to `FastraceLayer`

The `MinitraceLayer` has been renamed to `FastraceLayer` to respond to the [transition from `minitrace` to `fastrace`](https://github.com/tikv/minitrace-rust/issues/229).

```diff
- use opendal::layers::MinitraceLayer;
+ use opendal::layers::FastraceLayer;
```

### Use `Configurator` to replace `Builder::from_config`

Since v0.48, the `Builder::from_config` and `Builder::from_map` method has been replaced by the `Configurator` trait. The `Configurator` trait provides a more flexible and extensible way to configure OpenDAL.

Service implementers should update their code to use the `Configurator` trait instead:

```rust
impl Configurator for MemoryConfig {
    type Builder = MemoryBuilder;
    fn into_builder(self) -> Self::Builder {
        MemoryBuilder { config: self }
    }
}

impl Builder for MemoryBuilder {
    const SCHEME: Scheme = Scheme::Memory;
    type Config = MemoryConfig;

    fn build(self) -> Result<impl Access> {
        ...
    }
}
```

# Upgrade to v0.47

## Public API

### Reader `into_xxx` APIs

Since v0.47, `Reader`'s `into_xxx` APIs requires `async` and returns `Result` instead.

```diff
- let r = op.reader("test.txt").await?.into_futures_async_read(1024..2048);
+ let r = op.reader("test.txt").await?.into_futures_async_read(1024..2048).await?;
```

Affected API includes:

- `Reader::into_futures_async_read`
- `Reader::into_bytes_stream`
- `BlockingReader::into_std_read`
- `BlockingReader::into_bytes_iterator`

## Raw API

### Bring Streaming Read Back

As explained in [core: Bring Streaming Read Back](https://github.com/apache/opendal/issues/4672), we do need read streaming back for better performance and low memory usage.

So our `oio::Read` changed back to streaming read instead:

```diff
trait Read {
-  async fn read(&self, offset: u64, size: usize) -> Result<Buffer>;
+  async fn read(&mut self) -> Result<Buffer>;
}
```

All services and layers should be updated to meet this change.

# Upgrade to v0.46

## Public API

### MSRV Changed to 1.75

Since 0.46, OpenDAL requires Rust 1.75.0 or later to use features like [`RPITIT`](https://rust-lang.github.io/rfcs/3425-return-position-impl-trait-in-traits.html) and [`AFIT`](https://rust-lang.github.io/rfcs/3185-static-async-fn-in-trait.html).

### Services Feature Flag

Starting with version 0.46, OpenDAL only includes the memory service by default to prevent compiling unnecessary service code. To use other services, please activate their respective feature flags.

Additionally, we have removed all `reqwest`-related feature flags:

- Users must now directly use `reqwest`'s feature flags for options like `rustls`, `native-tls`, etc.
- The `rustls` feature is no longer enabled by default; it must be activated manually.
- OpenDAL no longer offers the `trust-dns` option; users should configure the client builder directly.

### Range Based Read

Since v0.46, OpenDAL transformed it's Read IO trait to range based instead of stateful poll based IO. This change will make the IO more efficient, easier for concurrency and ready for completion based IO.

`opendal::Reader` now have APIs like:

```rust
let r = op.reader("test.txt").await?;
let buf = r.read(1024..2048).await?;
```

### Buffer Based IO

Since version 0.46, OpenDAL features a native `Buffer` struct that supports both contiguous and non-contiguous buffers. This update enhances IO efficiency by minimizing unnecessary byte copying and enabling vectored IO.

OpenDAL's `Reader` will return `Buffer` and `Writer` will accept `Buffer` as input. Users who have implemented their own IO traits should update their code to use the new `Buffer` struct.

```rust
let r = op.reader("test.txt").await?;
// read returns `Buffer`
let buf: Buffer = r.read(1024..2048).await?;

let w = op.writer("test.txt").await?;

// Buffer can be created from continues bytes.
w.write("hello, world").await?;
// Buffer can also be created from non-continues bytes.
w.write(vec![Bytes::from("hello,"), Bytes::from("world!")]).await?;

// Make sure file has been written completely.
w.close().await?;
```

To enhance usability, we've integrated bridges into `bytes::Buf` and `bytes::BufMut`, allowing users to directly interact with the bytes API.

```rust
let r = op.reader("test.txt").await?;
let mut bs = vec![];
// read_into accepts bytes::BufMut
let buf: Buffer = r.read_into(&mut bs, 1024..2048).await?;

let w = op.writer("test.txt").await?;

// write_from accepts bytes::Buf
w.write_from("hello, world".as_bytes()).await?;

// Make sure file has been written completely.
w.close().await?;
```

### Bridge API

OpenDAL's `Reader` and `Writer` previously implemented APIs such as `AsyncRead` and `AsyncWrite` directly. This design was not user-friendly, as it could lead to unexpected costs that users were unaware of in advance.

Since v0.46, OpenDAL provides bridge APIs for `Reader` and `Writer` instead.

```rust
let r = op.reader("test.txt").await?;

// Convert into futures AsyncRead + AsyncSeek.
let reader = r.into_futures_async_read(1024..2048);
// Convert into futures bytes stream.
let stream = r.into_bytes_stream(1024..2048);

let w = op.writer("test.txt").await?;

// Convert into futures AsyncWrite
let writer = w.into_futures_async_write();
// Convert into futures bytes sink;
let sink = w.into_bytes_sink();
```

## Raw API

### Async in IO trait

Since version 0.46, OpenDAL has adopted Rust's native [`async_in_trait`](https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits.html) for our core IO traits, including `oio::Read`, `oio::Write`, and `oio::List`.

This update eliminates the need for manually written, poll-based state machines and simplifies the codebase. Consequently, OpenDAL now requires Rust version 1.75.0 or later.

Users who have implemented their own IO traits should update their code to use the new async trait syntax.

# Upgrade to v0.45

## Public API

### BlockingLayer is not enabled by default

To further enhance the optionality of `tokio`, we have introduced a new feature called `layers-blocking`. The default usage of the blocking layer has been disabled. To utilize the `BlockingLayer`, please enable the `layers-blocking` feature.

### TimeoutLayer deprecated `with_speed`

The `with_speed` API has been deprecated. Please use `with_io_timeout` instead.

## Raw API

No raw API changes.

# Upgrade to v0.44

## Public API

### Moka Service Configuration

- The `thread_pool_enabled` option has been removed.

### List Prefix Supported

After [RFC: List Prefix](crate::docs::rfcs::rfc_3243_list_prefix) landed, we have changed the behavior of `list` a path without `/`. OpenDAL used to return `NotADirectory` error, but now we will return the list of entries that start with given prefix instead.

# Upgrade to v0.43

## Public API

### List Recursive

After [RFC-3526: List Recursive](crate::docs::rfcs::rfc_3526_list_recursive) landed, we have changed the `list` API to accept `recursive` instead of `delimiter`:

Users will need to change the following usage:

- `op.list_with(path).delimiter("")` -> `op.list_with(path).recursive(true)`
- `op.list_with(path).delimiter("/")` -> `op.list_with(path).recursive(false)`

`delimiter` other than `""` and `"/"` is not supported anymore.

### Stat a dir path

After [RFC: List Prefix](crate::docs::rfcs::rfc_3243_list_prefix) landed, we have changed the behavior of `stat` a dir path:

Here are the behavior list:

| Case                   | Path            | Result                                     |
| ---------------------- | --------------- | ------------------------------------------ |
| stat existing dir      | `abc/`          | Metadata with dir mode                     |
| stat existing file     | `abc/def_file`  | Metadata with file mode                    |
| stat dir without `/`   | `abc/def_dir`   | Error `NotFound` or metadata with dir mode |
| stat file with `/`     | `abc/def_file/` | Error `NotFound`                           |
| stat not existing path | `xyz`           | Error `NotFound`                           |

Services like s3, azblob can handle `stat("abc/")` correctly by check if there are objects with prefix `abc/`.

## Raw API

### Lister Align

We changed our internal `lister` implementation to align with the `list` public API for better performance and readability.

- trait `Page` => `List`
- struct `Pager` => `Lister`
- trait `BlockingPage` => `BlockingList`
- struct `BlockingPager` => `BlockingLister`

Every call to `next` will return an entry instead a page of entries. Also, we changed our async list api into poll based instead of `async_trait`.

# Upgrade to v0.42

## Public API

### MSRV Changed

OpenDAL bumps it's MSRV to 1.67.0.

### S3 Service Configuration

- The `enable_exact_buf_write` option has been deprecated and is superseded by `BufferedWriter`, introduced in version 0.40.

### Oss Service Configuration

- The `write_min_size` option has been deprecated and replaced by `BufferedWriter`, also introduced in version 0.40.
- A new setting, `allow_anonymous`, has been added. Since v0.41, OSS will now return an error if credential loading fails. Enabling `allow_anonymous` to fallback to request without credentials.

### Ghac Service Configuration

- The `enable_create_simulation` option has been removed. We add this option to allow ghac simulate create empty file, but it could result in unexpected behavior when users create a file with content length `1`. So we remove it.

### Wasabi Service Removed

`wasabi` service native support has been removed. Users who want to access wasabi can use our `s3` service instead.

# Upgrade to v0.41

There is no public API and raw API changes.

# Upgrade to v0.40

## Public API

### RFC-2578 Merge Append Into Write

[RFC-2578](crate::docs::rfcs::rfc_2758_merge_append_into_write) merges `append` into `write` and removes `append` API.

- For writing a file at once, please use `op.write()` for convenience.
- For appending a file, please use `op.write_with().append(true)` instead of `op.append()`.

The same rule applies to `writer()` and `writer_with()`.

### RFC-2774 Lister API

[RFC-2774](crate::docs::rfcs::rfc_2774_lister_api) proposes a new `lister` API to replace current `list` and `scan`. And we add a new API `list` to return entries directly.

- For listing a directory at once, please use `list()` for convenience.
- For listing a directory recursively, please use `list_with().delimiter("")` or `lister_with().delimiter("")` instead of `scan()`.
- For listing in streaming, please use `lister()` or `lister_with()` instead.

### RFC-2779 List With Metakey

[RFC-2779](crate::docs::rfcs::rfc_2779_list_with_metakey) proposes a new `op.list_with().metakey()` API to allow list with metakey and removes `op.metadata(&entry)` API.

Please use `op.list_with().metakey()` instead of `op.metadata(&entry)`, for example:

```rust
// Before
let entries: Vec<Entry> = op.list("dir/").await?;
for entry in entries {
  let meta = op.metadata(&entry, Metakey::ContentLength | Metakey::ContentType).await?;
  println!("{} {}", entry.name(), entry.metadata().content_length());
}

// After
let entries: Vec<Entry> = op
  .list_with("dir/")
  .metakey(Metakey::ContentLength | Metakey::ContentType).await?;
for entry in entries {
  println!("{} {}", entry.name(), entry.metadata().content_length());
}
```

### RFC-2852: Native Capability

[RFC-2852](crate::docs::rfcs::rfc_2852_native_capability) proposes new `native_capability` and `full_capability` API to allow users to check if the underlying service supports a capability natively.

- `native_capability` returns `true` if the capability is supported natively.
- `full_capability` returns `true` if the capability is supported, maybe via a layer.

Most of time, you can use `full_capability` to replace `capability` call. But to check if the capability is supported natively for better performance design, please use `native_capability` instead.

### Buffered Writer

OpenDAL v0.40 added buffered writer support!

Users don't need to specify the `content_length()` for writer anymore!

```diff
- let mut w = op.writer_with("path/to/file").content_length(1024).await?;
+ let mut w = op.writer_with("path/to/file").await?;
```

Users can specify the `buffer()` to control the size we call underlying storage:

```rust
let mut w = op.writer_with("path/to/file").buffer(8 * 1024 * 1024).await?;
```

If buffer is not specified, we will call underlying storage everytime we call `write`. Otherwise, we will make sure to call underlying storage when buffer is full or `close` is called.

### RangeRead and RangeReader

OpenDAL v0.40 removed the origin `range_read` and `range_reader` interfaces, please use `read_with().range()` or `reader_with().range()`.

```diff
- op.range_read(path, range_start..range_end).await?;
+ op.read_with(path).range(range_start..range_end).await?;
```

```diff
- let reader = op.range_reader(path, range_start..range_end).await?;
+ let reader = op.reader_with(path).range(range_start..range_end).await?;
```

## Raw API

### RFC-3017 Remove Write Copy From

[RFC-3017](crate::docs::rfcs::rfc_3017_remove_write_copy_from) removes `copy_from` API from the `oio::Write` trait. Users who implements services and layers by hand should remove this API.

# Upgrade to v0.39

## Public API

### Service S3 Role Arn Behavior

In PR #2687, OpenDAL changed the behavior when `role_arn` has been specified.

OpenDAL used to override role_arn simply. But since this version, OpenDAL will make sure to use assume_role with specified `role_arn` and `external_id` (if supplied).

### RetryLayer supports RetryInterceptor

In PR #2666, `RetryLayer` supports `RetryInterceptor`. To implement this change, `RetryLayer` changed it's in-memory layout by adding a new generic parameter `I` to `RetryLayer<I>`.

Users who stores `RetryLayer` in struct or enum will need to change the type if they don't want to use default behavior.

## Raw API

In PR #2698, OpenDAL re-org the internal structure of `opendal::raw::oio` and changed some APIs name.

# Upgrade to v0.38

There are no public API changes.

## Raw API

OpenDAL add the `Write::sink` API to enable streaming writing. This is a breaking change for users who depend on the raw API.

For a quick fix, users who have implemented `opendal::raw::oio::Write` can return an `Unsupported` error for `Write::sink()`.

More details could be found at [RFC: Writer `sink` API][crate::docs::rfcs::rfc_2083_writer_sink_api].

# Upgrade to v0.37

In v0.37.0, OpenDAL bump the version of `reqsign` to v0.13.0.

There are no public API and raw API changes.

# Upgrade to v0.36

## Public API

In v0.36, OpenDAL improving the `xxx_with` API by allow it to be called in chain:

After this change, all `xxx_with` alike call will be changed from

```rust
let bs = op.read_with(
  "path/to/file",
  OpRead::new()
    .with_range(0..=1024)
    .with_if_match("<etag>")
    .with_if_none_match("<etag>")
    .with_override_cache_control("<cache_control>")
    .with_override_content_disposition("<content_disposition>")
  ).await?;
```

to

```rust
let bs = op.read_with("path/to/file")
  .range(0..=1024)
  .if_match("<etag>")
  .if_none_match("<etag>")
  .override_cache_control("<cache_control>")
  .override_content_disposition("<content_disposition>")
  .await?;
```

For blocking API calls, we will need a `call()` at the end:

```rust
let bs = bop.read_with("path/to/file")
  .range(0..=1024)
  .if_match("<etag>")
  .if_none_match("<etag>")
  .override_cache_control("<cache_control>")
  .override_content_disposition("<content_disposition>")
  .call()?;
```

Along with this change, users don't need to call `OpXxx` anymore so we moved it to `raw` API.

More details could be found at [RFC: Chain Based Operator API][crate::docs::rfcs::rfc_2299_chain_based_operator_api].

## Raw API

Migrated `opendal::ops` to `opendal::raw::ops`.

# Upgrade to v0.35

## Public API

- OpenDAL removes rarely used `Operator::from_env` and `Operator::from_iter` APIs
  - Users can use `Operator::via_map` instead.

## Raw API

- OpenDAL adds `append` support with could break existing layers. Please make sure `append` requests have been forward correctly.
- After the merging of `scan` and `list`, OpenDAL removes the `scan` from raw API. Please use `list_without_delimiter` instead.

# Upgrade to v0.34

## Public API

- OpenDAL raises it's MSRV to 1.65 for dependencies changes
- `OperatorInfo::can_scan` has been removed, to check if underlying services support scan a dir natively, please use `Capability::list_without_delimiter` instead.

## Raw API

### Merged `scan` into `list`

After `Capability` introduced, we have added `delimiter` in `OpList`. Users can specify the delimiter to `""` or `"/"` to control the list behavior.

Along with this change, `Operator::scan()` becomes a short alias of `Operator::list_with(OpList::new().with_delimiter(""))`.

### Typed Kv Adapter

In v0.34, OpenDAL adds a typed kv adapter for zero-copy read and write. If you are implemented kv adapter for a rust in-memory data struct, please consider migrate.

# Upgrade to v0.33

## Public API

OpenDAL 0.33 has redesigned the `Writer` API, replacing all instances of `writer.append()` with `writer.write()`. For more information, please refer to [`Writer`](crate::Writer).

## Raw API

In addition to the redesign of the `Writer` API, we have removed `append` from `oio::Write`. Therefore, users who implement services and layers should also remove it.

After v0.33 landing, services should handle `OpWrite::content_length` correctly by following these guidelines:

- If the writer does not support uploading unsized data, return a response of `NotSupported` if content length is `None`.
- Otherwise, continue writing data until either `close` or `abort` has been called.

Furthermore, OpenDAL 0.33 introduces a new concept called `Capability` which replaces `AccessorCapability`. Services must adapt to this change.

# Upgrade to v0.32

OpenDAL 0.32 doesn't have much breaking changes.

We changed `Accessor::create` into `Accessor::create_dir`. Only users who implement `Layer` need to change.

# Upgrade to v0.31

In version v0.31 of OpenDAL, we made some internal refactoring to improve its compatibility with the ecosystem.

## MSRV Bump

We increased the MSRV to 1.64 from v0.31 onwards. Although it is still possible to build OpenDAL under older rustc versions, we cannot guarantee that any issues related to them will be fixed.

## Accept `std::time::Duration` instead

Previously, OpenDAL accepted `time::Duration` as input for `presign_xxx`. However, since v0.31, we have changed this to accept `std::time::Duration` so that users do not need to depend on `time`. Internally, we migrated from `time` to `chrono` for better integration with other parts of the ecosystem.

## `disable_ec2_metadata` for services s3

We have added a new configuration option called `disable_ec2_metadata` for the S3 service in response to a mistake where it was mixed up with another option called `disable_config_load`. Users who want to disable loading credentials from EC2 metadata should set this option instead.

## Services Feature Flag

Starting from v0.31, all services in OpenDAL are split into different feature flags. To enable only S3 support, use the following TOML configuration:

```toml
opendal = {
    version = "0.31",
    default-features = false,
    features = ["services-s3"]
}
```

# Upgrade to v0.30

In version 0.30, we made significant breaking changes by removing objects. Our goal in doing so was to provide our users with APIs that are easier to understand and maintain.

More details could be found at [RFC: Remove Object Concept][crate::docs::rfcs::rfc_1477_remove_object_concept].

To upgrade to OpenDAL v0.30, users need to make the following changes:

- regex replace `object\((.*)\).reader\(\)` to `reader($1)`
  - replace the function on your case, it's recommended to do it one by one
- rename `ObjectMetakey` => `Metakey`
- rename `ObjectMode` => `EntryMode`
- replace `ErrorKind::ObjectXxx` to `ErrorKind::Xxx`
- rename `AccessorMetadata` => `AccessorInfo`
- rename `ObjectMetadata` => `Metadata`
- replace `operator.metadata()` => `operator.info()`

# Upgrade to v0.29

In v0.29, we introduced [Object Writer][crate::docs::rfcs::rfc_1420_object_writer] to replace existing Multipart related APIs.

Users can now append multiparts bytes into object via:

```rust
let mut w = o.writer().await?;
w.write(bs1).await?;
w.write(bs2).await?;
w.close()
```

Along with this change, we cleaned up a lot of internal structs and traits. Users who used to depend on `opendal::raw::io::{input,output}` should use `opendal::raw::oio` instead.

Also, decompress related feature also removed. Users can use `async-compression` with `ObjectReader` directly.

# Upgrade to v0.28

In v0.28, we introduced [Query Based Metadata][crate::docs::rfcs::rfc_1398_query_based_metadata]. Users can query cached metadata with `ObjectMetakey` to make sure that OpenDAL always makes the best decision.

```diff
- pub async fn metadata(&self) -> Result<ObjectMetadata>;
+ pub async fn metadata(
+        &self,
+        flags: impl Into<FlagSet<ObjectMetakey>>,
+    ) -> Result<Arc<ObjectMetadata>>;
```

Please visit `Object::metadata()`'s example for more details.

# Upgrade to v0.27

In v0.27, we refactored our `list` related logic and added `scan` support. So make `Pager` and `BlockingPager` associated types in `Accessor` too!

```diff
pub trait Accessor: Send + Sync + Debug + Unpin + 'static {
    type Reader: output::Read;
    type BlockingReader: output::BlockingRead;
+    type Pager: output::Page;
+    type BlockingPager: output::BlockingPage;
}
```

## User defined layers

Due to this change, all layers implementation should be changed. If there is not changed over pager, they can be changed like the following:

```diff
impl<A: Accessor> LayeredAccessor for MyAccessor<A> {
    type Inner = A;
    type Reader = MyReader<A::Reader>;
    type BlockingReader = MyReader<A::BlockingReader>;
+    type Pager = A::Pager;
+    type BlockingPager = A::BlockingPager;

+    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
+        self.inner.list(path, args).await
+    }

+    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
+        self.inner.scan(path, args).await
+    }

+    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
+        self.inner.blocking_list(path, args)
+    }

+    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
+        self.inner.blocking_scan(path, args)
+    }
}
```

## Usage of ops

To reduce the understanding overhead, we move all `OpXxx` into `opendal::ops` now. User may need to change:

```diff
- use opendal::OpWrite;
+ use opendal::ops::OpWrite;
```

## Usage of RetryLayer

`backon` is the implementation detail of our `RetryLayer`, so we hide it from our public API. Users of `RetryLayer` need to change the code like:

```diff
- RetryLayer::new(backon::ExponentialBackoff::default())
+ RetryLayer::new()
```

# Upgrade to v0.26

In v0.26 we have replaced all internal dynamic dispatch usage with static dispatch. With this change, we can ensure that all operations performed inside OpenDAL are zero cost.

Due to this change, we had to refactor the logic of `Operator`'s init logic. In v0.26, we added `opendal::Builder` trait and a generic operator build step. The public `Operator::new` API now returns a complete operator directly:

```diff
- let op = Operator::new(builder.build()?);
+ let op = Operator::new(builder)?;
```

No `finish()` call is required.

## Accessor

In v0.26, `Accessor` has been changed into trait with associated types.

All services need to declare the types returned as `Reader` or `BlockingReader`:

```rust
pub trait Accessor: Send + Sync + Debug + Unpin + 'static {
    type Reader: output::Read;
    type BlockingReader: output::BlockingRead;
}
```

If your service doesn't support `read` or `blocking_read`, we can use `()` to represent a dummy reader:

```rust
impl Accessor for MyDummyAccessor {
    type Reader = ();
    type BlockingReader = ();
}
```

## Layer

As described before, OpenDAL prefer to use static dispatch. Layers are required to implement the new `Layer` and `LayeredAccessor` trait:

```rust
pub trait Layer<A: Accessor> {
    type LayeredAccessor: Accessor;

    fn layer(&self, inner: A) -> Self::LayeredAccessor;
}

#[async_trait]
pub trait LayeredAccessor: Send + Sync + Debug + Unpin + 'static {
    type Inner: Accessor;
    type Reader: output::Read;
    type BlockingReader: output::BlockingRead;
}
```

`LayeredAccessor` is a wrapper of `Accessor` with the typed `Innder`. All methods that not implemented will be forward to inner instead.

## Builder

Since v0.26, we implement `opendal::Builder` for all services, and services' mod will not be exported.

```diff
- use opendal::services::s3::Builder;
+ use opendal::services::S3;
```

## Conclusion

Sorry again for the big changes in this release. It's a big step for OpenDAL to work in more critical systems.

# Upgrade to v0.25

In v0.25, we bring the same feature sets from `ObjectReader` to `BlockingObjectReader`.

Due to this change, all code that depends on `BlockingBytesReader` should be refactored.

- `BlockingBytesReader` => `input::BlockingReader`
- `BlockingOutputBytesReader` => `output::BlockingReader`

Most changes only happen inside. Users not using `opendal::raw::*` will not be affected.

Apart from this change, we refactored s3 credential loading logic. After this change, we can disable the config load instead of the credential methods.

- `builder.disable_credential_loader` => `builder.disable_config_load`

# Upgrade to v0.24

In v0.24, we made a big refactor on our internal IO-related traits. In this version, we split our IO traits into `input` and `output` versions:

Take `Reader` as an example:

`input::Reader` is the user input reader, which only requires `futures::AsyncRead + Send`.

`output::Reader` is the reader returned by `OpenDAL`, which implements `futures::AsyncRead`, `futures::AsyncSeek`, and `futures::Stream<Item=io::Result<Bytes>>`. Besides, `output::Reader` also implements `Send + Sync`, which makes it useful for users.

Due to this change, all code that depends on `BytesReader` should be refactored.

- `BytesReader` => `input::Reader`
- `OutputBytesReader` => `output::Reader`

Thanks to the change of IO trait split, we make `ObjectReader` implements all needed traits:

- `futures::AsyncRead`
- `futures::AsyncSeek`
- `futures::Stream<Item=io::Result<Bytes>>`

Thus, we removed the `seekable_reader` API. They can be replaced by `range_reader`:

- `o.seekable_reader` => `o.range_reader`

Most changes only happen inside. Users not using `opendal::raw::*` will not be affected.

Sorry for the inconvenience. I think those changes are required and make OpenDAL better! Welcome any comments at [Discussion](https://github.com/apache/opendal/discussions).

# Upgrade to v0.21

v0.21 is an internal refactor version of OpenDAL. In this version, we refactored our error handling and our `Accessor` APIs. Thanks to those internal changes, we added an object-level metadata cache, making it nearly zero cost to reuse existing metadata continuously.

Let's start with our errors.

## Error Handling

As described in [RFC-0977: Refactor Error](https://opendal.apache.org/rfcs/0977-refactor-error.html), we refactor opendal error by a new error
called [`opendal::Error`](https://opendal.apache.org/opendal/struct.Error.html).

This change will affect all APIs that are used to return `io::Error`.

To migrate this, please replace `std::io::Error` with `opendal::Error`:

```diff
- use std::io::Result;
+ use opendal::Result;
```

And the following error kinds should be updated:

- `std::io::ErrorKind::NotFound` => `opendal::ErrorKind::ObjectNotFound`
- `std::io::ErrorKind::PermissionDenied` => `opendal::ErrorKind::ObjectPermissionDenied`

And since v0.21, we will return errors `ObjectIsADirectory` and `ObjectNotADirectory` instead of `anyhow::Error`.

## Accessor API

In v0.21, we refactor the whole `Accessor`'s API:

```diff
- async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64>
+ async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite>
```

Since v0.21, we will return a reply struct for different operations called `RpWrite` instead of an exact type. We can split OpenDAL's public API and raw API with this change.

## ObjectList and Page

Since v0.21, `Accessor` will return `Pager` for `List`:

```diff
- async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer>
+ async fn list(&self, path: &str, args: OpList) -> Result<(RpList, output::Pager)>
```

And `Object` will return an `ObjectLister` which is built upon `Page`:

```rust
pub async fn list(&self) -> Result<ObjectLister> { ... }
```

`ObjectLister` can be used as an object stream as before. It also provides the function `next_page` to get the underlying pages directly:

```rust
impl ObjectLister {
    pub async fn next_page(&mut self) -> Result<Option<Vec<Object>>>;
}
```

## Code Layout

Since v0.21, we have categorized all APIs into `public` and `raw`.

Public APIs are exposed under `opendal::Xxx`; they are user-face APIs that are easy to use and understand.

Raw APIs are exposed under `opendal::raw::Xxx`; they are implementation details for underlying services and layers.

Please replace all usage of `opendal::io_util::*` and `opendal::http_util::*` to `opendal::raw::*` instead.

With this change, new users of OpenDAL maybe be it easier to get started.

## Summary

Sorry for introducing too much breaking change in a single version. This version can be a solid version for preparing OpenDAL v1.0.

# Upgrade to v0.20

v0.20 is a big release that we introduce a lot of performance related changes.

To make the best of information from `read` operation, we propose and implemented [RFC-0926: Object Reader](https://opendal.apache.org/rfcs/0926-object-reader.html). By this RFC, we can fetch content length from `ObjectReader` now!

```rust
pub struct ObjectReader {
    inner: BytesReader
    meta: ObjectMetadata,
}

impl ObjectReader {
    pub fn content_length(&self) -> u64 {}
    pub fn last_modified(&self) -> Option<OffsetDateTime> {}
    pub fn etag(&self) -> Option<String> {}
}
```

To make this happen, we changed our `Accessor` API:

```diff
- async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {}
+ async fn read(&self, path: &str, args: OpRead) -> Result<ObjectReader> {}
```

All layers should be updated to meet this change. Also, it's required to return `content_length` while building `ObjectReader`. Please make sure the returning `ObjectMetadata` is used correctly.

# Upgrade to v0.19

OpenDAL deprecate some features:

- `serde`: We will enable it by default.
- `layers-retry`: We will enable retry support by default.
- `layers-metadata-cache`: We will enable it by default.

Deprecated types like `DirEntry` has been removed.

# Upgrade to v0.18

OpenDAL v0.18 introduces the following breaking changes:

- Deprecated feature flag `services-http` has been removed.
- All `DirXxx` items have been renamed to `ObjectXxx` to make them more consistent.
  - `DirEntry` -> `Entry`
  - `DirStream` -> `ObjectStream`
  - `DirStreamer` -> `ObjectStream`
  - `DirIterate` -> `ObjectIterate`
  - `DirIterator` -> `ObjectIterator`

Besides, we also make a big change to our `Entry` API. Since v0.18, we can fully reuse the metadata that fetched during `list`. Take `entry.content_length()` for example:

- If `content_length` is already known, we will return directly.
- If not, we will check if the object entry is `complete`:
  - If `complete`, the entry already fetched all metadata that it could have, return directly.
  - If not, we will send a `stat` call to get the `metadata` and refresh our cache.

This change means:

- All API like `content_length` will be changed into async functions.
- `metadata` and `blocking_metadata` will not return errors anymore.
- To retrieve the latest meta, please use `entry.into_object().metadata()` instead.

# Upgrade to v0.17

OpenDAL v0.17 refactor the `Accessor` to make space for future features.

We move `path String` out of the `OpXxx` to function args so that we don't need to clone twice.

```diff
- async fn read(&self, args: OpRead) -> Result<BytesReader>
+ async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader>
```

For more information about this change, please refer to [RFC-0661: Path In Accessor](https://opendal.apache.org/rfcs/0661-path-in-accessor.html).

And since OpenDAL v0.17, we will use `rustls` as default tls engine for our underlying http client. Since this release, we will not depend on `openssl` anymore.

# Upgrade to v0.16

OpenDAL v0.16 refactor the internal implementation of `http` service. Since v0.16, http service can be used directly without enabling `services-http` feature. Accompany by these changes, http service has the following breaking changes:

- `services-http` feature has been deprecated. Enabling `services-http` is a no-op now.
- http service is read only services and can't be used to `list` or `write`.

OpenDAL introduces a new layer `ImmutableIndexLayer` that can add `list` capability for services:

```rust
use opendal::layers::ImmutableIndexLayer;
use opendal::Operator;
use opendal::Scheme;

async fn main() {
    let mut iil = ImmutableIndexLayer::default();

    for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
        iil.insert(i.to_string())
    }

    let op = Operator::from_env(Scheme::Http)?.layer(iil);
}
```

For more information about this change, please refer to [RFC-0627: Split Capabilities](https://opendal.apache.org/rfcs/0627-split-capabilities.html).

# Upgrade to v0.14

OpenDAL v0.14 removed all deprecated APIs in previous versions, including:

- `Operator::with_backoff` in v0.13
- All services `Builder::finish()` in v0.12
- All services `Backend::build()` in v0.12

Please visit related version's upgrade guide for migration.

And in OpenDAL v0.14, we introduce a break change for `write` operations.

```diff
pub trait Accessor {
    - async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {}
    + async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {}
}
```

The following APIs have affected by this change:

- `Object::write` now accept `impl Into<Vec<u8>>` instead of `AsRef<&[u8]>`
- `Object::writer` has been removed.
- `Object::write_from` has been added to support write from a reader.
- All layers should be refactored to adapt new `Accessor` trait.

For more information about this change, please refer to [RFC-0554: Write Refactor](https://opendal.apache.org/rfcs/0554-write-refactor.html).

# Upgrade to v0.13

OpenDAL deprecate `Operator::with_backoff` since v0.13.

Please use [`RetryLayer`](https://opendal.apache.org/opendal/layers/struct.RetryLayer.html) instead:

```rust
use anyhow::Result;
use backon::ExponentialBackoff;
use opendal::layers::RetryLayer;
use opendal::Operator;
use opendal::Scheme;

let _ = Operator::from_env(Scheme::Fs)
    .expect("must init")
    .layer(RetryLayer::new(ExponentialBackoff::default()));
```

# Upgrade to v0.12

OpenDAL introduces breaking changes for services initiation.

Since v0.12, `Operator::new` will accept `impl Accessor + 'static` instead of `Arc<dyn Accessor>`:

```rust
impl Operator {
    pub fn new(accessor: impl Accessor + 'static) -> Self { .. }
}
```

Every service's `Builder` now have a `build()` API which can be run without async:

```rust
let mut builder = fs::Builder::default();
let op: Operator = Operator::new(builder.build()?);
```

Along with these changes, `Operator::from_iter` and `Operator::from_env` now is a blocking API too.

For more information about this change, please refer to [RFC-0501: New Builder](https://opendal.apache.org/rfcs/0501-new-builder.html).

The following APIs have been deprecated:

- All services `Builder::finish()` (replaced by `Builder::build()`)
- All services `Backend::build()` (replace by `Builder::default()`)

The following APIs have been removed:

- public struct `Metadata` (deprecated in v0.8, replaced by `ObjectMetadata`)

# Upgrade to v0.8

OpenDAL introduces a breaking change of `list` related operations in v0.8.

Since v0.8, `list` will return `DirStreamer` instead:

```rust
pub trait Accessor: Send + Sync + Debug {
    async fn list(&self, args: &OpList) -> Result<DirStreamer> {}
}
```

`DirStreamer` streams `DirEntry` which carries `ObjectMode`, so that we don't need an extra call to get object mode:

```rust
impl DirEntry {
    pub fn mode(&self) -> ObjectMode {
        self.mode
    }
}
```

And `DirEntry` can be converted into `Object` without overhead:

```rust
let o: Object = de.into()
```

Since `v0.8`, `opendal::Metadata` has been deprecated by `opendal::ObjectMetadata`.

# Upgrade to v0.7

OpenDAL introduces a breaking change of `decompress_read` related in v0.7.

Since v0.7, `decompress_read` and `decompress_reader` will return `Ok(None)` while OpenDAL can't detect the correct compress algorithm.

```rust
impl Object {
    pub async fn decompress_read(&self) -> Result<Option<Vec<u8>>> {}
    pub async fn decompress_reader(&self) -> Result<Option<impl BytesRead>> {}
}
```

So users should match and check the `None` case:

```rust
let bs = o.decompress_read().await?.expect("must have valid compress algorithm");
```

# Upgrade to v0.4

OpenDAL introduces many breaking changes in v0.4.

## Object::reader() is not `AsyncSeek` anymore

Since v0.4, `Object::reader()` will return `impl BytesRead` instead of `Reader` that implements `AsyncRead` and `AsyncSeek`. Users who want `AsyncSeek` please wrapped with `opendal::io_util::seekable_read`:

```rust
use opendal::io_util::seekable_read;

let o = op.object("test");
let mut r = seekable_read(&o, 10..);
r.seek(SeekFrom::Current(10)).await?;
let mut bs = vec![0;10];
r.read(&mut bs).await?;
```

## Use RangeBounds instead

Since v0.4, the following APIs will be removed.

- `Object::limited_reader(size: u64)`
- `Object::offset_reader(offset: u64)`
- `Object::range_reader(offset: u64, size: u64)`

Instead, OpenDAL is providing a more general `range_reader` powered by `RangeBounds`:

```rust
pub async fn range_reader(&self, range: impl RangeBounds<u64>) -> Result<impl BytesRead>
```

Users can use their familiar rust range syntax:

```rust
let r = o.range_reader(1024..2048).await?;
```

## Return io::Result instead

Since v0.4, all functions in OpenDAL will return `std::io::Result` instead.

Please check via `std::io::ErrorKind` directly:

```rust
use std::io::ErrorKind;

if let Err(e) = op.object("test_file").metadata().await {
    if e.kind() == ErrorKind::NotFound {
        println!("object not exist")
    }
}
```

## Removing Credential

Since v0.4, `Credential` has been removed, please use the API provided by `Builder` directly.

```rust
builder.access_key_id("access_key_id");
builder.secret_access_key("secret_access_key");
```

## Write returns `BytesWriter` instead

Since v0.4, `Accessor::write` will return a `BytesWriter` instead accepting a `BoxedAsyncReader`.

Along with this change, the old `Writer` has been replaced by a new set of write functions:

```rust
pub async fn write(&self, bs: impl AsRef<[u8]>) -> Result<()> {}
pub async fn writer(&self, size: u64) -> Result<impl BytesWrite> {}
```

Users can write into an object more easily:

```rust
let _ = op.object("path/to/file").write("Hello, World!").await?;
```

## `io_util` replaces `readers`

Since v0.4, mod `io_util` will replace `readers`. In `io_utils`, OpenDAL provides helpful functions like:

- `into_reader`: Convert `BytesStream` into `BytesRead`
- `into_sink`: Convert `BytesWrite` into `BytesSink`
- `into_stream`: Convert `BytesRead` into `BytesStream`
- `into_writer`: Convert `BytesSink` into `BytesWrite`
- `observe_read`: Add callback for `BytesReader`
- `observe_write`: Add callback for `BytesWrite`

## New type alias

For better naming, types that OpenDAL returns have been renamed:

- `AsyncRead + Unpin + Send` => `BytesRead`
- `BoxedAsyncReader` => `BytesReader`
- `AsyncWrite + Unpin + Send` => `BytesWrite`
- `BoxedAsyncWriter` => `BytesWriter`
- `ObjectStream` => `ObjectStreamer`
