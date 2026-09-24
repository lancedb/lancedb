# TypeSafe request batching

## Available evidence

The focused tests exercise the native reranker with a mocked API. For 80 non-null
candidates, `batch_size=1` makes 80 `system_one` calls and `batch_size=40` makes
exactly two. Boundary tests cover 0, 1, 39, 40, 41, 80, and 81 candidates, including
a partially filled last batch. These are request-count checks, not latency or
ranking-quality measurements.

Validation on this patch: **124 tests passed**, with the credential-dependent live
test skipped. This includes real local vector/FTS/hybrid searches, async queries,
and TypeSafe SDK 0.7.1 over a mock HTTP transport (including SDK-managed retries).
Repository-wide Ruff formatting/lint, the benchmark CLI, and the MkDocs build
also passed.

### Live subset test

A live test on the first 100 cached GooAQ queries (6,851 candidate pairs) passed
with `jev-1.13.0`, SDK 0.7.1, and concurrency 32 in both modes. Candidate IDs,
probabilities, resolved models, and request counts were checked for every record.
All three 80-candidate queries made exactly two batched calls. Live vector, FTS,
hybrid, empty-FTS, and multivector integration checks also passed in both modes.
No implementation changes were needed.

| Batch size | Subset SDK calls | Median | p95 |
| --- | ---: | ---: | ---: |
| 1 | 6,851 | 877 ms | 9,446 ms |
| 40 | 199 | 351 ms | 598 ms |

| Search | Unbatched Hit@5 / Hit@10 | Batched Hit@5 / Hit@10 |
| --- | ---: | ---: |
| Vector | 82% / 88% | 84% / 89% |
| FTS | 72% / 77% | 73% / 78% |
| Hybrid | 83% / 89% | 84% / 90% |

These are **subset-test observations, not full benchmark results**. The full run
was manually stopped after 494 matched queries; this summary uses the first 100
input queries without selection by latency or scores. Counts above exclude other
queries completed before stopping. The high unbatched p95 is retained as observed;
it is not attributed to a specific cause. A complete 2,000-query paired benchmark
is still needed before making a general performance or quality claim. Exact values
and configuration are in [the subset test report](typesafe_subset_test.json).

The historical GooAQ results motivated this patch: the earlier custom adapter
reported 186 ms median / 387 ms p95; native unbatched scoring at concurrency 32
reported 805 ms / 1,193 ms for 2,000 queries and 136,586 candidate pairs using
`jev-1.13.0`. Those separate runs are not measurements of this patch. See the
[research results](https://github.com/lancedb/research/blob/codex/use-native-typesafe-reranker/reranking/results/README.md)
and [earlier adapter](https://github.com/lancedb/research/blob/412a522/reranking/jev_reranker.py).

## Run the paired benchmark

Use the exact `candidates.json` from the research benchmark cache. It is a JSON
array of records with `query`, expected `answer`, ordered `vector` and `fts` ID
lists, and a `documents` mapping from string IDs to text. Distinct IDs may have
identical text. To prepare candidates without scoring models, use the research
branch's [benchmark](https://github.com/lancedb/research/blob/codex/use-native-typesafe-reranker/reranking/compare_jev.py)
with `--models none`; its default corpus and query counts reproduce the protocol.

After bootstrapping this checkout's Python development environment, run from
`python/` (set `TYPESAFE_API_KEY` or pass `--api-key-file`):

```sh
uv run --extra tests --with typesafe-sdk==0.7.1 benchmarks/bench_typesafe.py \
  --candidates /path/to/research/reranking/.benchmark-cache/candidates.json \
  --model jev-1.13.0 --max-concurrency 32 \
  --cache /tmp/typesafe-batching-fresh \
  --output /tmp/typesafe-batching-results.json
```

Both modes use the same installed SDK, model, prompt, candidates, and concurrency.
They score each query's candidate union once, alternating which mode runs first.
Timings include native reranking and SDK retries, exclude retrieval, and report
median/p95 milliseconds. Request counts mean logical SDK calls, excluding
SDK-internal retry attempts. The output includes resolved model IDs, package
versions, implementation and candidate hashes, and vector/FTS/hybrid Hit@5 and
Hit@10. As in the research protocol, each metric uses 4× overfetch and a hit means
an exact match to the expected answer text. Ties retain retrieval order.

Scores and timings can resume from a cache. Cache identities separate batch size,
request format, model, SDK version, concurrency, prompt, implementation, and input
candidate bytes. Use a new cache directory for fresh timings. The report identifies
reused queries; don't compare runs with different resolved models or environments.
API errors and malformed responses abort the benchmark without fallback scores.

Offline validation from `python/`:

```sh
uv run --extra tests --with typesafe-sdk==0.7.1 pytest \
  python/tests/test_typesafe_reranker.py \
  python/tests/test_rerankers.py benchmarks/test_bench_typesafe.py -k typesafe -q
```
