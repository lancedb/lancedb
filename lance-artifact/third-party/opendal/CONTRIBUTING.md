# Contributing

## Get Started

- `cargo check` to analyze the current package and report errors.
- `cargo build` to compile the current package.
- `cargo clippy` to catch common mistakes and improve code.
- `cargo test --features tests` to run unit tests.
- `cargo test` to run unit tests include doc tests.
- `cargo bench` to run benchmark tests.

Useful tips:

- Check/Build/Test/Clippy all code: `cargo <cmd> --tests --benches --examples`
- Test specific function: `cargo test tests::it::services::fs`

## Tests

We have unit tests and behavior tests.

### Unit Tests

Unit tests are placed under `src/tests`, organized by mod.

To run unit tests:

```shell
cargo test --features tests
```

To run unit tests include doc tests:

```shell
cargo test
```

### Behavior Tests

Behavior Tests are used to make sure every service works correctly.

```shell
# Setup env
cp .env.example .env
# Run tests
cargo test
```

Please visit [Behavior Test README](./tests/behavior/README.md) for more details.

## Benchmark

We use Ops Benchmark Tests to measure every operation's performance on the target platform.

```shell
# Setup env
cp .env.example .env
# Run benches
cargo bench
```

Please visit [Ops Benchmark README](./benches/ops/README.md) for more details.
