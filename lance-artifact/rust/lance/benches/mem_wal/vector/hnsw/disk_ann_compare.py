#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors
"""Disk-ANN comparison: Lance on-disk IVF_HNSW_SQ (the SSTable index)
vs DiskANN vs FAISS, all backed by local NVMe.

faiss/diskannpy/lance bundle conflicting tcmalloc/MKL/OpenMP and crash if
imported in the same process, so each system runs in its OWN process:

    python disk_ann_compare.py prepare --rows N --base /mnt/nvme/anncmp
    python disk_ann_compare.py run --system lance  --rows N --base ...
    python disk_ann_compare.py run --system faiss  --rows N --base ...
    python disk_ann_compare.py run --system diskann --rows N --base ...

prepare loads dbpedia-1M (1536-d OpenAI, cosine), normalizes, samples a fixed
query holdout, computes exact top-k ground truth with numpy, and saves .npy.
Each run loads those .npy, sweeps the system's search param, and reports
recall@10 vs p50/p99 latency and QPS. The Lance index is served fully cached
(large index_cache_size_bytes).
"""

import argparse
import json
import os
import time

import numpy as np

K = 10
NUM_QUERIES = 1000
SEED = 42
DIM = 1536
EF_SWEEP = [16, 32, 64, 128, 256]
HF_TREE = "https://huggingface.co/api/datasets/KShivendu/dbpedia-entities-openai-1M/tree/main/data"
HF_BASE = (
    "https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M/resolve/main/"
)


def data_dir(base, rows):
    return os.path.join(base, f"data_{rows}")


def normalize(x):
    nrm = np.linalg.norm(x, axis=1, keepdims=True)
    nrm[nrm == 0] = 1.0
    return (x / nrm).astype(np.float32)


# ---------------- prepare ----------------
def load_corpus(cache_dir, needed):
    import pyarrow.parquet as pq
    import requests

    os.makedirs(cache_dir, exist_ok=True)
    shards = sorted(
        e["path"]
        for e in requests.get(HF_TREE, timeout=60).json()
        if e["type"] == "file" and e["path"].endswith(".parquet")
    )
    out = np.empty((needed, DIM), dtype=np.float32)
    n = 0
    for rel in shards:
        if n >= needed:
            break
        local = os.path.join(cache_dir, os.path.basename(rel))
        if not os.path.exists(local):
            r = requests.get(HF_BASE + rel, timeout=600)
            r.raise_for_status()
            with open(local, "wb") as f:
                f.write(r.content)
        col = pq.read_table(local, columns=["openai"]).column("openai")
        arr = np.stack(col.to_pylist()).astype(np.float32)
        take = min(len(arr), needed - n)
        out[n : n + take] = arr[:take]
        n += take
        print(f"  shard {os.path.basename(rel)} -> {take} (cum {n})", flush=True)
    assert n == needed, f"only got {n}/{needed}"
    return out


def numpy_ground_truth(corpus, queries):
    gt = np.empty((len(queries), K), dtype=np.int64)
    # corpus is normalized -> cosine == inner product; chunk over corpus.
    chunk = 200_000
    # Compute full similarity in query-major chunks to bound memory.
    sim = np.zeros((len(queries), len(corpus)), dtype=np.float32)
    for s in range(0, len(corpus), chunk):
        e = min(s + chunk, len(corpus))
        sim[:, s:e] = queries @ corpus[s:e].T
    for i in range(len(queries)):
        idx = np.argpartition(-sim[i], K)[:K]
        gt[i] = idx[np.argsort(-sim[i][idx])]
    return gt


def cmd_prepare(args):
    d = data_dir(args.base, args.rows)
    os.makedirs(d, exist_ok=True)
    if os.path.exists(os.path.join(d, "gt.npy")):
        print("already prepared", flush=True)
        return
    cache = os.path.join(args.base, "dbpedia_cache")
    needed = args.rows if args.rows >= 1_000_000 else args.rows
    raw = load_corpus(cache, needed)
    corpus = normalize(raw)
    rng = np.random.default_rng(SEED)
    qidx = rng.choice(args.rows, size=NUM_QUERIES, replace=False)
    queries = corpus[qidx].copy()
    print(
        f"corpus={len(corpus)} queries={len(queries)} dim={DIM}; computing GT...",
        flush=True,
    )
    t = time.perf_counter()
    gt = numpy_ground_truth(corpus, queries)
    print(f"  GT in {time.perf_counter() - t:.1f}s", flush=True)
    np.save(os.path.join(d, "corpus.npy"), corpus)
    np.save(os.path.join(d, "queries.npy"), queries)
    np.save(os.path.join(d, "gt.npy"), gt)
    print(f"=== prepared {d} ===", flush=True)


# ---------------- shared run helpers ----------------
def recall_at_k(gt, got):
    return sum(len(set(g.tolist()) & set(r.tolist())) for g, r in zip(gt, got)) / (
        len(gt) * K
    )


def latency_qps(query_fn, queries, repeats=3):
    lat = []
    for _ in range(repeats):
        for q in queries:
            t = time.perf_counter()
            query_fn(q)
            lat.append((time.perf_counter() - t) * 1e6)
    lat.sort()
    return lat[len(lat) // 2], lat[int(len(lat) * 0.99)], 1e6 / (sum(lat) / len(lat))


def sweep(name, make_q, params, queries, gt):
    rows = []
    for p in EF_SWEEP:
        qf = make_q(p)
        for v in queries[:50]:
            qf(v)
        got = np.stack([qf(v) for v in queries])
        rec = recall_at_k(gt, got)
        p50, p99, qps = latency_qps(qf, queries)
        rows.append(
            {"param": p, "recall": rec, "p50_us": p50, "p99_us": p99, "qps": qps}
        )
        print(
            f"  {name} param={p} recall={rec:.4f} "
            f"p50={p50:.0f}us p99={p99:.0f}us qps={qps:.0f}",
            flush=True,
        )
    return rows


# ---------------- systems ----------------
def run_lance(base, rows, corpus, queries, gt):
    import shutil

    import lance
    import pyarrow as pa

    uri = os.path.join(base, f"lance_{rows}")
    shutil.rmtree(uri, ignore_errors=True)
    vecs = pa.FixedSizeListArray.from_arrays(
        pa.array(corpus.reshape(-1), type=pa.float32()), DIM
    )
    tbl = pa.table({"id": pa.array(np.arange(rows, dtype=np.int64)), "vec": vecs})
    ds = lance.write_dataset(tbl, uri, mode="overwrite")
    # The SSTable index is a SINGLE-partition HNSW+SQ, so model it with
    # num_partitions=1 (nprobes=1); ef is the search knob, like DiskANN/FAISS.
    t = time.perf_counter()
    ds.create_index(
        "vec",
        "IVF_HNSW_SQ",
        metric="cosine",
        num_partitions=1,
        m=20,
        ef_construction=150,
    )
    build_s = time.perf_counter() - t
    ds = lance.dataset(uri, index_cache_size_bytes=48 * 1024**3)

    def make_q(ef):
        def q(v):
            return (
                ds.to_table(
                    nearest={"column": "vec", "q": v, "k": K, "nprobes": 1, "ef": ef},
                    columns=["id"],
                )
                .column("id")
                .to_numpy()
            )

        return q

    return {
        "build_s": build_s,
        "nlist": 1,
        "sweep": sweep("lance", make_q, None, queries, gt),
    }


def run_lance_sstable(base, rows, corpus, queries, gt, lance_path, id_offset, column):
    # Open an SSTable generation directly from its dataset path and
    # benchmark its on-disk IVF_HNSW_SQ index (single partition), fully cached.
    import lance

    ds = lance.dataset(lance_path, index_cache_size_bytes=48 * 1024**3)

    def make_q(ef):
        def q(v):
            ids = (
                ds.to_table(
                    nearest={"column": column, "q": v, "k": K, "nprobes": 1, "ef": ef},
                    columns=["id"],
                )
                .column("id")
                .to_numpy()
            )
            return ids - id_offset  # map SSTable id -> corpus index

        return q

    return {
        "lance_path": lance_path,
        "id_offset": id_offset,
        "sweep": sweep("lance_sstable", make_q, None, queries, gt),
    }


def run_faiss(base, rows, corpus, queries, gt):
    # Full-precision HNSW reference (shows what no quantization buys).
    import faiss

    index = faiss.IndexHNSWFlat(DIM, 32, faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction = 200
    t = time.perf_counter()
    index.add(corpus)
    build_s = time.perf_counter() - t
    faiss.write_index(index, os.path.join(base, f"faiss_{rows}.index"))

    def make_q(ef):
        def q(v):
            index.hnsw.efSearch = ef
            return index.search(v.reshape(1, -1), K)[1][0]

        return q

    return {"build_s": build_s, "sweep": sweep("faiss", make_q, None, queries, gt)}


def run_faiss_sq(base, rows, corpus, queries, gt):
    # HNSW + 8-bit scalar quantization — apples-to-apples with Lance IVF_HNSW_SQ.
    import faiss

    try:
        index = faiss.IndexHNSWSQ(
            DIM, faiss.ScalarQuantizer.QT_8bit, 32, faiss.METRIC_INNER_PRODUCT
        )
    except Exception:
        # Fall back to L2; on unit-normalized vectors L2 ranking == cosine.
        index = faiss.IndexHNSWSQ(DIM, faiss.ScalarQuantizer.QT_8bit, 32)
    index.hnsw.efConstruction = 200
    t = time.perf_counter()
    index.train(corpus)
    index.add(corpus)
    build_s = time.perf_counter() - t
    faiss.write_index(index, os.path.join(base, f"faiss_sq_{rows}.index"))

    def make_q(ef):
        def q(v):
            index.hnsw.efSearch = ef
            return index.search(v.reshape(1, -1), K)[1][0]

        return q

    return {"build_s": build_s, "sweep": sweep("faiss_sq", make_q, None, queries, gt)}


def run_diskann(base, rows, corpus, queries, gt):
    import diskannpy as dap

    idx_dir = os.path.join(base, f"diskann_{rows}")
    os.makedirs(idx_dir, exist_ok=True)
    t = time.perf_counter()
    dap.build_memory_index(
        data=corpus,
        distance_metric="cosine",
        index_directory=idx_dir,
        index_prefix="ann",
        complexity=150,
        graph_degree=64,
        num_threads=0,
        alpha=1.2,
        use_pq_build=False,
        num_pq_bytes=0,
    )
    build_s = time.perf_counter() - t
    idx = dap.StaticMemoryIndex(
        index_directory=idx_dir,
        index_prefix="ann",
        num_threads=0,
        initial_search_complexity=256,
    )

    def make_q(L):
        def q(v):
            return idx.search(v, k_neighbors=K, complexity=max(L, K)).identifiers

        return q

    return {"build_s": build_s, "sweep": sweep("diskann", make_q, None, queries, gt)}


def cmd_run(args):
    d = data_dir(args.base, args.rows)
    corpus = np.load(os.path.join(d, "corpus.npy"))
    queries = np.load(os.path.join(d, "queries.npy"))
    gt = np.load(os.path.join(d, "gt.npy"))
    print(f"=== {args.system} rows={args.rows} corpus={len(corpus)} ===", flush=True)
    if args.system == "lance_sstable":
        res = run_lance_sstable(
            args.base,
            args.rows,
            corpus,
            queries,
            gt,
            args.lance_path,
            args.id_offset,
            args.column,
        )
    else:
        fn = {
            "lance": run_lance,
            "faiss": run_faiss,
            "faiss_sq": run_faiss_sq,
            "diskann": run_diskann,
        }[args.system]
        res = fn(args.base, args.rows, corpus, queries, gt)
    res["rows"] = args.rows
    res["system"] = args.system
    out = os.path.join(args.base, f"result_{args.rows}_{args.system}.json")
    with open(out, "w") as f:
        json.dump(res, f, indent=2)
    print(f"=== wrote {out} ===", flush=True)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--rows", type=int, required=True)
    p.add_argument("--base", required=True)
    r = sub.add_parser("run")
    r.add_argument("--rows", type=int, required=True)
    r.add_argument("--base", required=True)
    r.add_argument("--system", required=True)
    r.add_argument("--lance-path", default=None)
    r.add_argument("--id-offset", type=int, default=0)
    r.add_argument("--column", default="vector")
    args = ap.parse_args()
    (cmd_prepare if args.cmd == "prepare" else cmd_run)(args)


if __name__ == "__main__":
    main()
