# LanceDB Rust

<a href="https://crates.io/crates/vectordb">![img](https://img.shields.io/crates/v/vectordb)</a>
<a href="https://docs.rs/vectordb/latest/vectordb/">![Docs.rs](https://img.shields.io/docsrs/vectordb)</a>

LanceDB Rust SDK, a serverless vector database.

Read more at: https://lancedb.com/

> [!TIP]
> A transitive dependency of `lancedb` is `lzma-sys`, which uses dynamic linking
> by default. If you want to statically link `lzma-sys`, you should activate it's
> `static` feature by adding the following to your dependencies:
>
> ```toml
> lzma-sys = { version = "*", features = ["static"] }
> ```
