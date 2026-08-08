# Ops Benchmark Tests

Ops Benchmark Tests measure every operation's performance on the target platform.

To support benching different backends simultaneously, we use `environment value` to carry the backend config.

## Setup

Please copy `.env.example` to `.env` and change the values on need.

Take `fs` for example, we add config on `fs` on `/tmp`.

```dotenv
OPENDAL_FS_ROOT=/tmp
```

Notice: The default will skip benches if the env is not set.

## Run

Test specific backend, take s3 for example, first set the corresponding environment variables of s3, then:

```shell
OPENDAL_TEST=s3
cargo bench ops --features tests
```
