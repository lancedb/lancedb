# Exact File-Version Compatibility Fixtures

These files were generated with the writers at baseline commit
`3a72f8a61e14613f517dded6816d4bfc77817c93`. The deterministic input batch is
defined by `compatibility_fixture_batch` in `src/compatibility_tests.rs` and
covers primitive, nullable UTF-8, nullable list, nullable dictionary, blob,
multiple input batches, and multiple pages. The V2.0 embedded fixtures use the
primitive and nullable UTF-8 columns from the first 257 rows of the same batch.

`datagen.py` copies the baseline-compatible `datagen.rs` into a clean checkout
of that commit, runs it twice in separate processes with an isolated Cargo
target directory, and verifies that both runs produce identical bytes:

```shell
git worktree add --detach /tmp/lance-exact-version-baseline \
    3a72f8a61e14613f517dded6816d4bfc77817c93
python3 rust/lance-file/test_data/exact_versions/datagen.py \
    --source /tmp/lance-exact-version-baseline
git worktree remove /tmp/lance-exact-version-baseline
```

Pass `--write` only when intentionally restoring these files from the locked
baseline.

| File | SHA-256 |
| --- | --- |
| `v1.lance` | `fa8b3d81b9d4fd4ade5a7c3d077ebf2155664e12b9335e26fac1c0d0774e916c` |
| `v2_0.lance` | `073c8c24eb4433b83d0dda95bf7a731a9f5d8f32d78440f2f391474e99b9c49a` |
| `v2_1.lance` | `3af97ba176b72c7e00a248b4a270a53402a72e594631950f76eb3daab45c50ce` |
| `v2_2.lance` | `8298cd9301e657417b0725461345c27cf46515529d2a8b35824be139e3466a14` |
| `v2_0_self_described.lance` | `6a3a9ce8ef56f058d1d105e7f4494ce35a9026479e2f76fc6f26c04b3201a406` |
| `v2_0_mini.lance` | `5e3fc99b01a4d2f5d16a2fb051dacb49b4a736428b1494715cd83633ad142a63` |

The compatibility tests require each stable writer to reproduce its fixture
byte-for-byte and each reader to open and read the baseline file. The V2.0
standard fixture preserves footer `(0, 3)` while its self-described and
mini-Lance fixtures preserve `(2, 0)`. V2.3 is unstable, so it has deterministic
current-revision tests instead of a checked-in compatibility fixture.

Regenerate these fixtures only from the baseline writer APIs. Files generated
with the implementation under test are not independent compatibility evidence.
