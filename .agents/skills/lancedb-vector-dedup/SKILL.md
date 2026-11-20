---
name: lancedb-vector-dedup
description: Plan and run Enterprise IVF vector deduplication with dedupe and duplicate_pairs, including sampling, index coverage, recall and resource limits. Use for near-duplicate embedding or perceptual-hash records; use MinHash guidance for text shingles.
---

# Vector deduplication

Read the maintained [workflow](../../../docs/web/curation/dedupe.mdx),
[DEDUPE reference](../../../docs/web/sql/dql/dedupe.mdx) and
[DUPLICATE_PAIRS reference](../../../docs/web/sql/dql/duplicate-pairs.mdx)
before constructing SQL. In a source checkout these paths are relative to this
skill directory. When using an installed copy without the docs, read
`https://docs.lancedb.com/curation/dedupe` and the linked SQL references.

## Establish the comparison contract

- Confirm an Enterprise deployment that supports these table functions. Use
    `dedupe` and `duplicate_pairs`; legacy `vector_dedup` names are rejected.
- Inspect the vector type, metric, compatible index, indexed fragments and
    source version. Wait for index coverage after writes. Do not run a full
    scan over unindexed fragments or silently change the index to make it run.
- Explain that candidates are compared only within physical IVF partitions
    and segments. Encoded/quantized distances are not an exact raw-vector
    comparison. L2 thresholds are squared distances; Hamming counts bits.
- Use `id_column` on pair queries for business IDs. Retain `dataset_version`,
    segment and partition identities. Do not decode internal row IDs or assume
    that representatives stay the same after later data/index changes.

## Bound the first experiment

Use a reproducible partition sample before a full scan:

```sql
SELECT id_a, id_b, distance, dataset_version, segment_id, partition_id
FROM duplicate_pairs(
    'frames', 'embedding',
    distance_threshold => 0.02,
    id_column => 'frame_id',
    selection => 'random', count => 10, seed => 42
)
```

Adapt the threshold and sample size to the metric and actual partition count.
Sampling partitions is not uniform sampling of rows. Inspect known duplicate
and distinct examples, pair density, partition skew, memory, spill space and
elapsed time. Do not infer global recall or ten-billion-row qualification
from a small or unversioned benchmark. Pair work and output can be quadratic
within a partition; an outer SQL `LIMIT` is not an upstream work budget.

## Choose the result

`dedupe` returns original representative rows and always scans all partitions.
Use its default `connected_components` for transitive groups; choose
`direct_representative` when chains should not merge transitively. It does not
accept `id_column`, `selection`, `count` or `seed`; `duplicate_pairs` does not
accept `resolution`.

Keep pair data on the server when possible; avoid collecting an unbounded
edge set in client memory. Persist a new table/view only within the user's
authorized scope, preserving source rows unless deletion was requested.
Do not register a Python Function or fetch/rewrite Blob payloads to invoke
these built-in table functions. For MinHash/text deduplication, follow the
separate `https://docs.lancedb.com/enterprise/sql-minhash` workflow.
