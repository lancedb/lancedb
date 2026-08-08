# OpenDAL Benchmarks

Running benchmark

```shell
OPENDAL_TEST=memory cargo bench
```

Build flamegraph:

```shell
cargo flamegraph --bench io -- seek --bench
```

- `--bench io` pick the `io` target.
- `-- seek --bench`: chose the benches with `seek` and `--bench` is required by `criterion`

After `flamegraph.svg` has been generated, use browser to visit it.
