# LanceDB Python SDK

A Python library for [LanceDB](https://github.com/lancedb/lancedb).

## Installation

```bash
pip install lancedb
```

### Pre-Haswell x86_64 hosts: `lancedb-compat`

The default `lancedb` wheel targets `x86-64-haswell` (AVX2 + FMA + F16C) for full performance on modern hardware. Pre-Haswell hosts — Intel Sandy Bridge / Ivy Bridge / Westmere; AMD Bulldozer / Piledriver / Steamroller — don't have AVX2 and crash with `Illegal instruction` at `import lancedb`.

For those hosts, install the `lancedb-compat` package instead:

```bash
pip install lancedb-compat
```

Same Python API (`import lancedb` works as usual). The compat wheel is compiled at the `x86-64-v2` baseline (Nehalem-class) and uses runtime SIMD dispatch in the embedded lance crate to pick the right kernel tier (scalar / AVX / AVX+FMA / AVX2+FMA / AVX-512) at load time, so it still goes fast on modern hardware while running cleanly on the pre-Haswell silicon. Use `lance.simd_info()` from Python to verify which tier was selected.

`lancedb` and `lancedb-compat` install to the same `lancedb/` namespace and conflict at install time. Pick one. To switch, `pip uninstall lancedb` first, then `pip install lancedb-compat` (or vice-versa).

If you need a custom baseline (or `lancedb-compat` isn't yet published for your platform), build from source with the override:

```bash
RUSTFLAGS="-C target-cpu=x86-64-v2" maturin build --release
pip install ./target/wheels/lancedb-*.whl
```

### Preview Releases

Stable releases are created about every 2 weeks. For the latest features and bug fixes, you can install the preview release. These releases receive the same level of testing as stable releases, but are not guaranteed to be available for more than 6 months after they are released. Once your application is stable, we recommend switching to stable releases.


```bash
pip install --pre --extra-index-url https://pypi.fury.io/lancedb/ lancedb
```

## Usage

### Basic Example

```python
import lancedb
db = lancedb.connect('<PATH_TO_LANCEDB_DATASET>')
table = db.open_table('my_table')
results = table.search([0.1, 0.3]).limit(20).to_list()
print(results)
```

### Development

See [CONTRIBUTING.md](./CONTRIBUTING.md) for information on how to contribute to LanceDB.
