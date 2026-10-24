# REPORT — lancedb/lancedb candidate fix (account `simpleqt`)

Prepared from `G:\pr\r52\lancedb` at `main` = `0ea932d` (python package version `0.41.0-beta.13`).
Nothing was pushed, commented, or opened; the change exists only as a local commit.

**Verdict: a qualifying behavioral bug was found and fixed.** Sync hybrid search rejects two
documented query-vector containers (`tuple`, `pyarrow.Array`/`ChunkedArray`) that every other
entry point accepts, so valid input raises `ValueError`.

---

## 1. Policy check (done first)

| Source | Finding |
| --- | --- |
| `CONTRIBUTING.md` | No AI/LLM/agent clause, no CLA, no DCO/sign-off requirement, no PR template reference. Requires tests for bug fixes: *"Unit test all bug fixes and new features. Your code will not be merged if it doesn't have tests."* (`CONTRIBUTING.md:48-49`), scope discipline: *"Aim to minimize the number of changes in each pull request. Keep to solving one problem at a time, when possible."* (`:51-52`), and error-case tests: *"When writing tests, test the error cases. Make sure they have understandable error messages."* (`:61-62`). |
| `AGENTS.md` (root) | Requires `ruff format .` / `ruff check .` for Python changes and conventional-commit PR titles: the semantic-release check uses the PR title as the squash-commit message. |
| `python/AGENTS.md` | Adds `pyright` and `_lancedb.pyi` upkeep when exporting new Rust types. Not needed here (pure Python change, no binding change). |
| `REVIEW.md` | Review guidelines: bug fixes need tests; new public APIs need docs. |
| `.github/` | **No PR template** (only `ISSUE_TEMPLATE/` and workflows). |
| `.github/workflows/codex-fix-ci.yml`, `codex-update-lance-dependency.yml`, `.agents/skills/` | The project itself runs agent-driven automation, so agent participation is not forbidden. |
| DCO / sign-off | No requirement found anywhere in the repo → the commit is **not** signed off (`-s` deliberately omitted). |

**Policy verdict: submission is allowed.** Requirements to satisfy: regression test (present),
minimal scope (one source file + one test file), conventional-commit PR title (present below).

---

## 2. The defect

`LanceHybridQueryBuilder._validate_query` accepted only `str | list | np.ndarray` for the query
vector and raised otherwise:

```python
# python/python/lancedb/query.py:2450-2452 on main (0ea932d)
vector_query = vector if vector is not None else query
if not isinstance(vector_query, (str, list, np.ndarray)):
    raise ValueError("Vector query must be either a string or a vector")
```

`tuple`, `pa.Array` and `pa.ChunkedArray` are documented, first-class query-vector containers —
`VEC = Union[list, np.ndarray, pa.Array, pa.ChunkedArray]` in `lancedb/common.py`, with `Tuple`
also spelled out in the public signatures (`Table.search`, `hybrid_search`, `nearest_to`) — and
they work everywhere else:

* `Table.search(tuple_or_pa_array)` → vector search normalizes them in
  `LanceQueryBuilder.create` (`query.py:1028-1033`);
* `AsyncTable.query().nearest_to(tuple_or_pa_array).nearest_to_text(...)` → async hybrid accepts
  them (`AsyncQuery._query_vec_to_array`);
* only the sync hybrid builder (`Table.search(query_type="hybrid").vector(...).text(...)`)
  rejects them.

Impact: a documented input raises `ValueError: Vector query must be either a string or a vector`;
the same value works for vector search and for the async hybrid API, so the sync/async surfaces
disagree and users must special-case an otherwise valid container.

**Defect locations (main):** `python/python/lancedb/query.py:2451` (check),
`query.py:2452` (raise) — inside `LanceHybridQueryBuilder._validate_query`.

---

## 3. Minimal repro

```python
import lancedb, pyarrow as pa

db = lancedb.connect("memory://")
table = db.create_table("t", [
    {"vector": [1.0, 2.0], "text": "hello world"},
    {"vector": [3.0, 4.0], "text": "goodbye world"},
])
table.create_fts_index("text", replace=True)

# works
table.search(query_type="hybrid").vector([1.0, 2.0]).text("hello").limit(1).to_arrow()

# same vector, same query — rejected
table.search(query_type="hybrid").vector((1.0, 2.0)).text("hello").limit(1).to_arrow()
# ValueError: Vector query must be either a string or a vector

table.search(query_type="hybrid").vector(
    pa.array([1.0, 2.0], type=pa.float32())
).text("hello").limit(1).to_arrow()
# ValueError: Vector query must be either a string or a vector
```

Measured parity probe on unmodified `main` (`G:\pr\r52\repro_hybrid_vector.py`):

```
== vector search (control) ==
OK   vector list       -> 1 rows
OK   vector tuple      -> 1 rows
OK   vector np.ndarray -> 1 rows
OK   vector pa.Array   -> 1 rows

== hybrid search, same vector containers ==
OK   hybrid list       -> 1 rows
FAIL hybrid tuple      -> ValueError: Vector query must be either a string or a vector
OK   hybrid np.ndarray -> 1 rows
FAIL hybrid pa.Array   -> ValueError: Vector query must be either a string or a vector

== async hybrid search (nearest_to) ==
OK   async hybrid list       -> 1 rows
OK   async hybrid tuple      -> 1 rows
OK   async hybrid np.ndarray -> 1 rows
OK   async hybrid pa.Array   -> 1 rows
```

### RED — regression test on unmodified `main`

`git stash push -- python/python/lancedb/query.py`, then

```
python/tests/test_hybrid_query.py::test_hybrid_query_accepts_every_vector_container
3 failed, 3 warnings in 1.70s
  FAILED ...::test_hybrid_query_accepts_every_vector_container[pa-chunked-array]
  FAILED ...::test_hybrid_query_accepts_every_vector_container[pa-array]
  FAILED ...::test_hybrid_query_accepts_every_vector_container[tuple]
E   ValueError: Vector query must be either a string or a vector
python\lancedb\query.py:2452: ValueError
```

(full log: `G:\pr\r52\red_hybrid.log`)

---

## 4. The fix — one source file

`python/python/lancedb/query.py`, `LanceHybridQueryBuilder._validate_query` (now lines 2450-2460).
The hybrid builder now normalizes the query vector exactly like the vector search path does in
`LanceQueryBuilder.create`:

```diff
         vector_query = vector if vector is not None else query
-        if not isinstance(vector_query, (str, list, np.ndarray)):
+        # Accept the same containers as the vector search path, which
+        # normalizes them in `LanceQueryBuilder.create`.
+        if isinstance(vector_query, (list, tuple)):
+            vector_query = np.array(vector_query, dtype=np.float32)
+        elif isinstance(vector_query, np.ndarray):
+            vector_query = vector_query.astype(np.float32)
+        elif isinstance(vector_query, (pa.Array, pa.ChunkedArray)):
+            vector_query = np.array(vector_query.to_pylist(), dtype=np.float32)
+        elif not isinstance(vector_query, str):
             raise ValueError("Vector query must be either a string or a vector")
```

Why this is safe / minimal:

* it mirrors `query.py:1028-1033` line for line, so hybrid and vector search can no longer
  diverge for the same value;
* the already-working case (`list`) is only converted to the same `np.float32` form the vector
  path produces, and `LanceVectorQueryBuilder.to_query_object()` immediately calls `.tolist()`
  again, so nested (multivector) lists arrive at the Rust core in exactly the same shape as
  before — `test_multivector` still passes;
* strings are untouched (the embedding-function path is unchanged);
* invalid input still raises the same `ValueError` (e.g. an `int` or `None`).
  The error-case requirement of `CONTRIBUTING.md:61-62` is preserved.

---

## 5. Regression test

`python/python/tests/test_hybrid_query.py` (`test_hybrid_query_accepts_every_vector_container`),
parametrized over `tuple`, `pa.Array` and `pa.ChunkedArray`; it asserts that each container returns
the same rows as the list form of the same hybrid query, so the test pins behavior and not just the
absence of an exception.

### GREEN — same test with the fix

```
python/tests/test_hybrid_query.py::test_hybrid_query_accepts_every_vector_container
3 passed, 3 warnings in 0.39s
```

(full log: `G:\pr\r52\green_hybrid.log`)

### Regression sweep with the fix applied

```
pytest python/tests/test_hybrid_query.py python/tests/test_query.py python/tests/test_rerankers.py \
  -m "not slow" -k "not huggingface and not cross_encoder and not reranked and not model_reranker"
220 passed, 9 skipped, 14 deselected
```

```
pytest python/tests/test_table.py python/tests/test_remote_db.py python/tests/test_fts.py \
  python/tests/test_index.py python/tests/test_permutation.py
724 passed, 5 skipped   (exit code 0)
```

The full suite (`python/tests`, excluding `slow`/`s3_test`) was also run on unmodified `main` for a
baseline: 2565 passed, 31 failed — every failure was environmental (namespace server, model
downloads, torch/multiprocessing, docs fixtures) except `test_table.py::test_merge_insert`, which is
a pre-existing flaky timing assertion (a zero-length merge timeout is only enforced on ~70-80 % of
runs because `tokio::time::timeout(Duration::ZERO, ...)` polls the future before the timer; measured
7/10 sync and 8/10 async). That is a Rust/timing issue in an existing test, unrelated to this change
and out of scope here.

---

## 6. Lint / format (pinned toolchain)

`pre-commit` pins `ruff v0.9.9`; that exact version was installed and used:

```
$ ruff --version            # G:\pr\r52\ruff099\bin\ruff.exe
ruff 0.9.9
$ ruff check python/python/lancedb/query.py python/tests/test_hybrid_query.py
All checks passed!
$ ruff format --check --diff python/python/lancedb/query.py python/tests/test_hybrid_query.py
2 files already formatted
```

---

## 7. Duplicate check

* `gh search prs --author=simpleqt --repo lancedb/lancedb` → the account's history is
  4202, 4201, 4146, 4145 (merged) and 4154, 4153, 4128, 4127 (open). None of them touches
  hybrid query-vector validation.
* `gh pr list --repo lancedb/lancedb --state open --limit 100`: open PRs that touch this area are
  **#4659** (async query serialization of `fast_search`/`postfilter`), **#4653** (phrase queries
  without a column), #4650, #4649, #4647, #4585, #4571, #4503, #4498, #4486, #4485, #4473, #4608 —
  none of them about hybrid vector containers.
* Keyword searches (`gh search prs` / `gh search issues`) for `hybrid vector tuple`,
  `hybrid search vector validation`, `hybrid vector query container`, and the exact error string
  `"Vector query must be either a string or a vector"` → **no results** (the same searches were
  sanity-checked against the bare word `hybrid`, which does return results, so the empty result is
  real).
* Nearest open issue in this area, #3001 (*"Hybrid search gives contradictory error messages about
  _score vs _distance fields"*), is a different defect and is not addressed here.

Conclusion: no duplicate found — this is not reproduced by any open PR or issue.

### An abandoned earlier candidate (for the record)

The async serialization bug I found first (`Query.from_inner` dropped `fast_search`/`postfilter`
for async builders) is **already covered by open PR #4659**, whose body states *"Async
`to_query_object()` dropped enabled `fast_search` on every query type and `postfilter` on plain and
full-text queries"* and which patches `query.py`, `test_query.py` **and** `src/query.rs`. It was
dropped from this submission (my one-line Python version also broke
`test_query.py::test_query_serialization_async`, which asserts that unset properties stay `None` —
confirming the duplicate is fixed properly only in the Rust binding).

---

## 8. Draft PR (title + body, ready to submit; nothing was pushed)

**Title**

```
fix(python): accept tuples and pyarrow arrays as hybrid search vectors
```

**Body**

> ## Summary
>
> Sync hybrid search rejected query-vector containers that the rest of the SDK accepts.
> `LanceHybridQueryBuilder._validate_query` only allowed `str`, `list` and `np.ndarray`, so a
> `tuple` or a `pyarrow.Array`/`ChunkedArray` raised
> `ValueError: Vector query must be either a string or a vector`.
>
> Those containers are documented (`VEC` in `lancedb/common.py`, and `Tuple` in the public
> signatures) and already work for:
>
> * `Table.search(query_vector)` — normalized by `LanceQueryBuilder.create`
> * `AsyncTable.query().nearest_to(query_vector).nearest_to_text(text)` — the async hybrid API
>
> ## What changed
>
> `LanceHybridQueryBuilder._validate_query` now normalizes the query vector exactly like
> `LanceQueryBuilder.create`: `list`/`tuple` → `np.float32` array, `ndarray` → `astype(float32)`,
> pyarrow arrays → `np.float32` array, strings pass through unchanged, everything else keeps
> raising the same `ValueError`.
>
> ## Test
>
> `test_hybrid_query_accepts_every_vector_container` (parametrized over `tuple`, `pa.Array`,
> `pa.ChunkedArray`) asserts the hybrid query returns the same rows as the equivalent list form.
> Fails on `main`, passes with the change.
>
> ## Notes
>
> * one source file changed, no Rust/binding changes
> * `ruff 0.9.9` (the pre-commit pin): `ruff check` clean, `ruff format --check` clean
> * checked for duplicates: no open PR or issue covers this

---

## 9. Environment used for the runs (reproducibility)

* Repo: `G:\pr\r52\lancedb`, `main` @ `0ea932d`; local core built from the same revision with
  `maturin build --profile release-no-lto` (rustc 1.97.0 from `rust-toolchain.toml`), wheel
  `G:\pr\r52\wheels\lancedb-0.41.0b13-cp310-abi3-win_amd64.whl`, `_lancedb.pyd` copied into
  `python/python/lancedb/`.
* Test interpreter: `F:\python311\python.exe` with
  `PYTHONPATH=G:\pr\r52\lancedb\python\python;G:\pr\r52\metainfo;G:\pr\r52\pure`
  (`lancedb.__version__ == "0.41.0b13"`, imported from the repo source).
* The uv-managed environment declared by `python/uv.lock` could not be used offline here (no PyPI
  access for the lockfile's full dependency set), so the same pytest suite was run with the system
  interpreter; the failures that remain in the full suite are all dependency/network artifacts, not
  code failures, and are listed in §5.

## 10. Local commit

One commit, author/committer `simpleqt <89645338+simpleqt@users.noreply.github.com>`, **no**
`Signed-off-by` (no DCO requirement exists in this repository):

```
fix(python): accept tuples and pyarrow arrays as hybrid search vectors
```
