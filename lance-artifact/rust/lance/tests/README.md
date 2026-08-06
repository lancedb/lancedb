Tests for memory and IO usage.

## Debugging memory usage

Once you've identified a test that is using too much memory, you can use
bytehound to find the source of the memory usage. (Note: we need to run
bytehound on the binary, not on cargo, so we have to extract the test binary path.)

The `RUST_ALLOC_TIMINGS` environment variable tells the tracking allocator
to logs the start and end of each allocation tracking session, which makes it
easier to correlate the bytehound output with the code.

```shell
TEST_BINARY=$(cargo test --test resource_tests --no-run 2>&1 | tail -n1 | sed -n 's/.*(\([^)]*\)).*/\1/p')
LD_PRELOAD=/usr/local/lib/libbytehound.so \
    RUST_ALLOC_TIMINGS=true \
    $TEST_BINARY resource_test::write::test_memory_usage_write \
bytehound server memory-profiling_*.dat
```
