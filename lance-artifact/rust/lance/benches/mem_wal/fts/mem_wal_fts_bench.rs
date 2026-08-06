// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance `FtsMemIndex` microbenchmark — the Lance side of the FTS-vs-Lucene
//! comparison (sibling of `mem_wal_hnsw_bench` for the HNSW-vs-hnswlib one).
//!
//! Two sub-commands:
//!
//!   gen   — slice a FineWeb corpus and write the shared comparison inputs:
//!             corpus.txt        one raw document per line
//!             corpus_tok.txt    one canonically-tokenized document per line
//!             queries.txt       <type>\t<raw>\t<tok> per line (term|phrase)
//!             truth.txt         exact-BM25 top-k doc ids per query (Run A)
//!           Both this bench and the Lucene bench consume these files, so the
//!           inputs are bit-identical.
//!
//!   bench — build a `FtsMemIndex` over the corpus and measure build
//!           throughput, term/phrase query latency + QPS, recall@k, memory.
//!           --run a  pre-tokenized: corpus_tok.txt + a whitespace tokenizer
//!                    (isolates the inverted index + BM25 scorer).
//!           --run b  native: corpus.txt + the default Lance tokenizer.
//!
//! Emits one `result ...` human line and one JSON line tagged
//! `impl=lance_fts`, matching the `mem_wal_hnsw_bench` output convention.

#![recursion_limit = "256"]
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::HashMap;
use std::io::{BufRead, BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use lance::dataset::mem_wal::index::{FtsMemIndex, FtsQueryExpr, SearchOptions};
use lance_core::Result;
use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;
use lance_tokenizer::TokenStream;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use rayon::prelude::*;

const TEXT_COL: &str = "text";
const HF_API_LISTING: &str =
    "https://huggingface.co/api/datasets/HuggingFaceFW/fineweb/tree/main/sample/10BT";
const HF_FILE_BASE: &str = "https://huggingface.co/datasets/HuggingFaceFW/fineweb/resolve/main/";
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

// ----------------------------------------------------------------------
// HuggingFace FineWeb loading (gen mode only)
// ----------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct HfTreeEntry {
    #[serde(rename = "type")]
    kind: String,
    path: String,
}

async fn list_shard_paths() -> Result<Vec<String>> {
    let entries: Vec<HfTreeEntry> = reqwest::get(HF_API_LISTING)
        .await
        .map_err(|e| lance_core::Error::io(format!("listing HTTP: {e}")))?
        .json()
        .await
        .map_err(|e| lance_core::Error::io(format!("listing JSON: {e}")))?;
    let mut shards: Vec<String> = entries
        .into_iter()
        .filter(|e| e.kind == "file" && e.path.ends_with(".parquet"))
        .map(|e| e.path)
        .collect();
    shards.sort();
    Ok(shards)
}

async fn download_shard(rel_path: &str, dest: &std::path::Path) -> Result<()> {
    if dest.exists() {
        return Ok(());
    }
    let url = format!("{HF_FILE_BASE}{rel_path}");
    let tmp = dest.with_extension("part");
    for attempt in 1..=5u32 {
        println!("downloading {rel_path} (attempt {attempt}/5) ...");
        let r: Result<bytes::Bytes> = async {
            let resp = reqwest::get(&url)
                .await
                .map_err(|e| lance_core::Error::io(format!("HTTP: {e}")))?;
            if !resp.status().is_success() {
                return Err(lance_core::Error::io(format!("status {}", resp.status())));
            }
            resp.bytes()
                .await
                .map_err(|e| lance_core::Error::io(format!("body: {e}")))
        }
        .await;
        match r {
            Ok(bytes) => {
                std::fs::write(&tmp, &bytes)
                    .map_err(|e| lance_core::Error::io(format!("write: {e}")))?;
                std::fs::rename(&tmp, dest)
                    .map_err(|e| lance_core::Error::io(format!("rename: {e}")))?;
                return Ok(());
            }
            Err(e) if attempt < 5 => {
                eprintln!("  retry: {e}");
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

async fn read_shard_text(
    path: &std::path::Path,
    out: &mut Vec<String>,
    max_rows: usize,
) -> Result<usize> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| lance_core::Error::io(format!("open parquet: {e}")))?;
    let mut stream = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| lance_core::Error::io(format!("parquet builder: {e}")))?
        .build()
        .map_err(|e| lance_core::Error::io(format!("parquet stream: {e}")))?;
    let mut taken = 0usize;
    use futures::TryStreamExt;
    while taken < max_rows {
        let Some(rb) = stream
            .try_next()
            .await
            .map_err(|e| lance_core::Error::io(format!("parquet read: {e}")))?
        else {
            break;
        };
        let col = rb
            .column_by_name("text")
            .ok_or_else(|| lance_core::Error::io("text column missing".to_string()))?;
        let strs = col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| lance_core::Error::io("text not StringArray".to_string()))?;
        for i in 0..strs.len() {
            if taken >= max_rows {
                break;
            }
            if !strs.is_null(i) {
                out.push(strs.value(i).to_string());
                taken += 1;
            }
        }
    }
    Ok(taken)
}

async fn load_fineweb(docs: usize, cache_dir: &std::path::Path) -> Result<Vec<String>> {
    std::fs::create_dir_all(cache_dir)
        .map_err(|e| lance_core::Error::io(format!("mkdir cache: {e}")))?;
    let shards = list_shard_paths().await?;
    let mut buf: Vec<String> = Vec::with_capacity(docs);
    for rel in &shards {
        if buf.len() >= docs {
            break;
        }
        let name = rel.rsplit('/').next().unwrap_or(rel);
        let local = cache_dir.join(name);
        download_shard(rel, &local).await?;
        let want = docs - buf.len();
        let got = read_shard_text(&local, &mut buf, want).await?;
        println!("  shard {name}: {got} docs (total {})", buf.len());
    }
    if buf.len() < docs {
        return Err(lance_core::Error::io(format!(
            "fineweb yielded {} docs, need {docs}",
            buf.len()
        )));
    }
    Ok(buf)
}

// ----------------------------------------------------------------------
// gen
// ----------------------------------------------------------------------

fn one_line(s: &str) -> String {
    s.replace(['\n', '\r', '\t'], " ")
}

/// Canonical tokenizer used to produce `corpus_tok.txt` and the `tok` query
/// field. Both the Lance Run-A and Lucene Run-A indexes consume its output
/// verbatim (via a whitespace analyzer), so the choice does not bias either
/// side — it only fixes a single shared token stream.
fn canonical_tokenize(
    tokenizer: &mut dyn lance_index::scalar::inverted::tokenizer::document_tokenizer::LanceTokenizer,
    text: &str,
) -> Vec<String> {
    let mut stream = tokenizer.token_stream_for_doc(text);
    let mut out = Vec::new();
    while let Some(t) = stream.next() {
        out.push(t.text.clone());
    }
    out
}

async fn run_gen(args: &GenArgs) -> Result<()> {
    let raw = load_fineweb(args.docs, &args.cache_dir).await?;
    std::fs::create_dir_all(&args.out_dir)
        .map_err(|e| lance_core::Error::io(format!("mkdir out: {e}")))?;

    let mut tokenizer = InvertedIndexParams::default()
        .build()
        .expect("default tokenizer builds");

    // corpus.txt + corpus_tok.txt
    let corpus_path = args.out_dir.join("corpus.txt");
    let tok_path = args.out_dir.join("corpus_tok.txt");
    let mut corpus_w = BufWriter::new(
        std::fs::File::create(&corpus_path)
            .map_err(|e| lance_core::Error::io(format!("create corpus.txt: {e}")))?,
    );
    let mut tok_w = BufWriter::new(
        std::fs::File::create(&tok_path)
            .map_err(|e| lance_core::Error::io(format!("create corpus_tok.txt: {e}")))?,
    );
    let mut tok_docs: Vec<Vec<String>> = Vec::with_capacity(raw.len());
    for doc in &raw {
        writeln!(corpus_w, "{}", one_line(doc)).ok();
        let toks = canonical_tokenize(tokenizer.as_mut(), doc);
        writeln!(tok_w, "{}", toks.join(" ")).ok();
        tok_docs.push(toks);
    }
    corpus_w.flush().ok();
    tok_w.flush().ok();
    println!("wrote {} docs to corpus.txt / corpus_tok.txt", raw.len());

    // Term + phrase queries sampled from the canonical token stream.
    let queries = build_query_set(&tok_docs, args);
    let q_path = args.out_dir.join("queries.txt");
    let mut q_w = BufWriter::new(
        std::fs::File::create(&q_path)
            .map_err(|e| lance_core::Error::io(format!("create queries.txt: {e}")))?,
    );
    for q in &queries {
        // raw == tok here: queries are sampled from canonical tokens. The
        // column is kept for format stability with the Lucene bench.
        writeln!(q_w, "{}\t{}\t{}", q.kind, q.text, q.text).ok();
    }
    q_w.flush().ok();
    println!("wrote {} queries to queries.txt", queries.len());

    // Exact-BM25 ground truth over the pre-tokenized corpus (Run A).
    let truth = exact_bm25_truth(&tok_docs, &queries, args.k);
    let t_path = args.out_dir.join("truth.txt");
    let mut t_w = BufWriter::new(
        std::fs::File::create(&t_path)
            .map_err(|e| lance_core::Error::io(format!("create truth.txt: {e}")))?,
    );
    for ids in &truth {
        let line: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
        writeln!(t_w, "{}", line.join(" ")).ok();
    }
    t_w.flush().ok();
    println!(
        "wrote exact-BM25 truth for {} queries to truth.txt",
        truth.len()
    );
    Ok(())
}

struct Query {
    kind: &'static str, // "term" | "phrase" | "or"
    text: String,       // term: one token; phrase/or: space-joined tokens
}

const STOPWORDS: &[&str] = &[
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "as", "by", "is", "was",
    "are", "were", "be", "this", "that", "it", "its", "but", "not", "no", "if", "from", "at",
];

fn build_query_set(tok_docs: &[Vec<String>], args: &GenArgs) -> Vec<Query> {
    // Term queries: mid-frequency tokens (skip the most common — their
    // top-k is an unstable near-tie, as the FineWeb panel showed).
    let mut freq: HashMap<&str, u64> = HashMap::new();
    for d in tok_docs {
        for t in d {
            if t.len() >= 3 && !STOPWORDS.contains(&t.as_str()) {
                *freq.entry(t.as_str()).or_default() += 1;
            }
        }
    }
    let mut by_freq: Vec<(&str, u64)> = freq.into_iter().collect();
    by_freq.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    let skip = (by_freq.len() / 4).min(300);
    let mut queries: Vec<Query> = by_freq
        .iter()
        .skip(skip)
        .take(args.num_term_queries)
        .map(|(t, _)| Query {
            kind: "term",
            text: (*t).to_string(),
        })
        .collect();

    // Phrase queries: two adjacent non-stopword tokens, sampled on a stride.
    let stride = (tok_docs.len().max(1) / args.num_phrase_queries.max(1)).max(1);
    let mut idx = 0usize;
    while queries.iter().filter(|q| q.kind == "phrase").count() < args.num_phrase_queries
        && idx < tok_docs.len()
    {
        let d = &tok_docs[idx];
        for w in d.windows(2) {
            if w[0].len() >= 3
                && w[1].len() >= 3
                && !STOPWORDS.contains(&w[0].as_str())
                && !STOPWORDS.contains(&w[1].as_str())
            {
                queries.push(Query {
                    kind: "phrase",
                    text: format!("{} {}", w[0], w[1]),
                });
                break;
            }
        }
        idx += stride;
    }

    // OR (multi-term) queries: 3 mid-frequency tokens joined, matched as a
    // SHOULD/OR — exercises the multi-term block-max WAND. Use tokens past the
    // term-query slice so they are distinct from the single-term set.
    let mid: Vec<&str> = by_freq.iter().skip(skip).map(|(t, _)| *t).collect();
    let mut oi = args.num_term_queries;
    for _ in 0..args.num_or_queries {
        if oi + 3 > mid.len() {
            break;
        }
        queries.push(Query {
            kind: "or",
            text: format!("{} {} {}", mid[oi], mid[oi + 1], mid[oi + 2]),
        });
        oi += 3;
    }
    queries
}

/// Brute-force exact BM25 top-k over the pre-tokenized corpus.
fn exact_bm25_truth(tok_docs: &[Vec<String>], queries: &[Query], k: usize) -> Vec<Vec<usize>> {
    let n = tok_docs.len() as f64;
    let dl: Vec<f64> = tok_docs.iter().map(|d| d.len() as f64).collect();
    let avgdl = dl.iter().sum::<f64>() / n.max(1.0);
    // postings: token -> Vec<(doc, tf)>
    let mut postings: HashMap<&str, Vec<(usize, u32)>> = HashMap::new();
    for (doc, toks) in tok_docs.iter().enumerate() {
        let mut tf: HashMap<&str, u32> = HashMap::new();
        for t in toks {
            *tf.entry(t.as_str()).or_default() += 1;
        }
        for (t, c) in tf {
            postings.entry(t).or_default().push((doc, c));
        }
    }
    let term_score = |tf: f64, df: f64, d: f64| -> f64 {
        let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
        let num = tf * (BM25_K1 + 1.0);
        let den = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * (d / avgdl));
        idf * (num / den)
    };
    queries
        .iter()
        .map(|q| {
            let mut scores: HashMap<usize, f64> = HashMap::new();
            if q.kind != "phrase" {
                // term (1 token) or or (multi-token): sum each token's BM25.
                for word in q.text.split(' ') {
                    if let Some(pl) = postings.get(word) {
                        let df = pl.len() as f64;
                        for &(doc, tf) in pl {
                            *scores.entry(doc).or_default() += term_score(tf as f64, df, dl[doc]);
                        }
                    }
                }
            } else {
                let words: Vec<&str> = q.text.split(' ').collect();
                // Phrase: docs where the two tokens occur adjacently.
                for (doc, toks) in tok_docs.iter().enumerate() {
                    let adjacent = toks
                        .windows(2)
                        .any(|w| w[0] == words[0] && w[1] == words[1]);
                    if !adjacent {
                        continue;
                    }
                    let mut s = 0.0;
                    for w in &words {
                        if let Some(pl) = postings.get(*w) {
                            let df = pl.len() as f64;
                            if let Some(&(_, tf)) = pl.iter().find(|&&(d, _)| d == doc) {
                                s += term_score(tf as f64, df, dl[doc]);
                            }
                        }
                    }
                    scores.insert(doc, s);
                }
            }
            let mut ranked: Vec<(usize, f64)> = scores.into_iter().collect();
            ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            ranked.truncate(k);
            ranked.into_iter().map(|(d, _)| d).collect()
        })
        .collect()
}

// ----------------------------------------------------------------------
// bench
// ----------------------------------------------------------------------

fn read_lines(path: &std::path::Path) -> Result<Vec<String>> {
    let f = std::fs::File::open(path)
        .map_err(|e| lance_core::Error::io(format!("open {}: {e}", path.display())))?;
    std::io::BufReader::new(f)
        .lines()
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| lance_core::Error::io(format!("read {}: {e}", path.display())))
}

fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(TEXT_COL, DataType::Utf8, true),
    ]))
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn run_bench(args: &BenchArgs) -> Result<()> {
    let corpus_file = match args.run {
        'a' => "corpus_tok.txt",
        _ => "corpus.txt",
    };
    let docs = read_lines(&args.in_dir.join(corpus_file))?;
    let query_lines = read_lines(&args.in_dir.join("queries.txt"))?;
    let truth_lines = read_lines(&args.in_dir.join("truth.txt"))?;

    // Run A indexes the already-tokenized text with a whitespace tokenizer;
    // Run B indexes raw text with the default Lance tokenizer.
    let params = if args.run == 'a' {
        InvertedIndexParams::default()
            .base_tokenizer("whitespace".to_string())
            .lower_case(false)
            .stem(false)
            .remove_stop_words(false)
            .with_position(args.with_position)
    } else {
        InvertedIndexParams::default().with_position(args.with_position)
    };
    let mut index = FtsMemIndex::with_params(0, TEXT_COL.to_string(), params);
    if let Some(rows) = args.freeze_threshold {
        index = index.with_freeze_threshold_rows(rows);
    }
    let sch = schema();

    // Build: insert in batches of 1000, doc id == line number == row position.
    let build_start = Instant::now();
    let batch_size = 1000;
    let mut row = 0usize;
    while row < docs.len() {
        let end = (row + batch_size).min(docs.len());
        let ids: Vec<i64> = (row as i64..end as i64).collect();
        let texts: Vec<&str> = docs[row..end].iter().map(|s| s.as_str()).collect();
        let batch = RecordBatch::try_new(
            sch.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(texts)) as ArrayRef,
            ],
        )
        .map_err(|e| lance_core::Error::io(format!("batch: {e}")))?;
        index.insert(&batch, row as u64)?;
        row = end;
    }
    // Immutable-read mode flushes the tail into a segment (Lucene-commit
    // analogue) so an `include_tail = false` query sees every row; counted in
    // build time as Lucene counts its final commit.
    if args.immutable {
        index.flush();
    }
    let build_s = build_start.elapsed().as_secs_f64();

    let (nparts, t_terms, t_post, t_blk, t_df, t_pos, t_docs, t_tail) = index.memory_breakdown();
    let mb = |b: usize| b as f64 / 1.0e6;
    eprintln!(
        "mem_breakdown parts={nparts} term_str={:.1} postings={:.1} block_meta={:.1} \
         doc_freq={:.1} pos={:.1} docs={:.1} tail={:.1} (MB)",
        mb(t_terms),
        mb(t_post),
        mb(t_blk),
        mb(t_df),
        mb(t_pos),
        mb(t_docs),
        mb(t_tail),
    );

    // Parse queries.
    struct Q {
        kind: String,
        raw: String,
        tok: String,
    }
    let queries: Vec<Q> = query_lines
        .iter()
        .filter_map(|l| {
            let mut p = l.splitn(3, '\t');
            Some(Q {
                kind: p.next()?.to_string(),
                raw: p.next()?.to_string(),
                tok: p.next()?.to_string(),
            })
        })
        .collect();
    let truth: Vec<Vec<usize>> = truth_lines
        .iter()
        .map(|l| {
            l.split_whitespace()
                .filter_map(|s| s.parse().ok())
                .collect()
        })
        .collect();

    // Without positions, phrase search is unsupported — drop phrase queries so
    // both sides measure the same (term-only) workload apples-to-apples.
    let (queries, truth): (Vec<Q>, Vec<Vec<usize>>) = if args.with_position {
        (queries, truth)
    } else {
        queries
            .into_iter()
            .zip(truth)
            .filter(|(q, _)| q.kind != "phrase")
            .unzip()
    };

    let opts = SearchOptions::new()
        .with_limit(args.k)
        .with_include_tail(!args.immutable)
        .with_tail_skip(!args.no_tail_skip);
    let make_query = |q: &Q| -> FtsQueryExpr {
        let text = if args.run == 'a' { &q.tok } else { &q.raw };
        if q.kind == "phrase" {
            FtsQueryExpr::phrase(text.clone())
        } else {
            FtsQueryExpr::match_query(text.clone())
        }
    };

    // Warm-up.
    for q in &queries {
        let _ = index.search_with_options(&make_query(q), opts.clone());
    }

    // Single-thread latency + collect top-k.
    let mut latencies_us: Vec<f64> = Vec::with_capacity(queries.len());
    let mut topk: Vec<Vec<usize>> = Vec::with_capacity(queries.len());
    let st_start = Instant::now();
    for q in &queries {
        let t0 = Instant::now();
        let hits = index.search_with_options(&make_query(q), opts.clone());
        latencies_us.push(t0.elapsed().as_secs_f64() * 1.0e6);
        let mut ids: Vec<(usize, f32)> = hits
            .iter()
            .map(|e| (e.row_position as usize, e.score))
            .collect();
        ids.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ids.truncate(args.k);
        topk.push(ids.into_iter().map(|(d, _)| d).collect());
    }
    let st_s = st_start.elapsed().as_secs_f64();
    let qps_1t = queries.len() as f64 / st_s;

    // Multi-thread QPS.
    let reps = 4usize;
    let mt_start = Instant::now();
    let _: usize = (0..reps)
        .into_par_iter()
        .map(|_| {
            queries
                .par_iter()
                .map(|q| {
                    index
                        .search_with_options(&make_query(q), opts.clone())
                        .len()
                })
                .sum::<usize>()
        })
        .sum();
    let mt_s = mt_start.elapsed().as_secs_f64();
    let qps_nt = (queries.len() * reps) as f64 / mt_s;

    // recall@k vs exact BM25 (Run A only — truth is over the canonical corpus).
    let (mut term_recall, mut term_n) = (0.0f64, 0usize);
    let (mut phrase_recall, mut phrase_n) = (0.0f64, 0usize);
    let (mut or_recall, mut or_n) = (0.0f64, 0usize);
    if args.run == 'a' {
        for (i, q) in queries.iter().enumerate() {
            let t: std::collections::HashSet<usize> = truth
                .get(i)
                .map(|v| v.iter().copied().collect())
                .unwrap_or_default();
            if t.is_empty() {
                continue;
            }
            let hit = topk[i].iter().filter(|d| t.contains(d)).count() as f64;
            let r = hit / args.k as f64;
            match q.kind.as_str() {
                "phrase" => {
                    phrase_recall += r;
                    phrase_n += 1;
                }
                "or" => {
                    or_recall += r;
                    or_n += 1;
                }
                _ => {
                    term_recall += r;
                    term_n += 1;
                }
            }
        }
    }

    // Write this impl's top-k for the driver's mutual-overlap computation.
    let topk_path = args
        .in_dir
        .join(format!("lance_fts_run{}_topk.txt", args.run));
    let mut tw = BufWriter::new(
        std::fs::File::create(&topk_path)
            .map_err(|e| lance_core::Error::io(format!("create topk: {e}")))?,
    );
    for ids in &topk {
        let s: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
        writeln!(tw, "{}", s.join(" ")).ok();
    }
    tw.flush().ok();

    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let term_recall_v = if term_n > 0 {
        term_recall / term_n as f64
    } else {
        f64::NAN
    };
    let phrase_recall_v = if phrase_n > 0 {
        phrase_recall / phrase_n as f64
    } else {
        f64::NAN
    };
    let or_recall_v = if or_n > 0 {
        or_recall / or_n as f64
    } else {
        f64::NAN
    };

    // Per-query-type latency split via a second timing pass, bucketed by kind
    // (`latencies_us` was sorted above for the overall percentile, so it can no
    // longer be zipped with `queries` in query order).
    let mut term_lat: Vec<f64> = Vec::new();
    let mut phrase_lat: Vec<f64> = Vec::new();
    let mut or_lat: Vec<f64> = Vec::new();
    for q in &queries {
        let t0 = Instant::now();
        let _ = index.search_with_options(&make_query(q), opts.clone());
        let lat = t0.elapsed().as_secs_f64() * 1.0e6;
        match q.kind.as_str() {
            "phrase" => phrase_lat.push(lat),
            "or" => or_lat.push(lat),
            _ => term_lat.push(lat),
        }
    }
    term_lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    phrase_lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    or_lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |v: &[f64], p: f64| {
        if v.is_empty() {
            f64::NAN
        } else {
            percentile(v, p)
        }
    };
    println!(
        "result split: term_p50={:.1} term_p95={:.1} ({} q) | phrase_p50={:.1} phrase_p95={:.1} ({} q) | or_p50={:.1} or_p95={:.1} ({} q)",
        pct(&term_lat, 50.0),
        pct(&term_lat, 95.0),
        term_lat.len(),
        pct(&phrase_lat, 50.0),
        pct(&phrase_lat, 95.0),
        phrase_lat.len(),
        pct(&or_lat, 50.0),
        pct(&or_lat, 95.0),
        or_lat.len(),
    );

    println!(
        "result impl=lance_fts run={} docs={} queries={} build_s={:.3} build_docs_per_s={:.0} \
         q_p50_us={:.1} q_p95_us={:.1} qps_1t={:.0} qps_{}t={:.0} \
         term_recall={:.4} phrase_recall={:.4} mem_mb={:.1}",
        args.run,
        docs.len(),
        queries.len(),
        build_s,
        docs.len() as f64 / build_s,
        percentile(&latencies_us, 50.0),
        percentile(&latencies_us, 95.0),
        qps_1t,
        rayon::current_num_threads(),
        qps_nt,
        term_recall_v,
        phrase_recall_v,
        index.memory_usage() as f64 / 1.0e6,
    );
    println!(
        "{{\"impl\":\"lance_fts\",\"run\":\"{}\",\"docs\":{},\"queries\":{},\"k\":{},\
         \"build_s\":{:.4},\"build_docs_per_s\":{:.1},\
         \"q_p50_us\":{:.2},\"q_p95_us\":{:.2},\"qps_1t\":{:.1},\"qps_nt\":{:.1},\
         \"term_recall_at_k\":{:.4},\"phrase_recall_at_k\":{:.4},\"or_recall_at_k\":{:.4},\"mem_bytes\":{}}}",
        args.run,
        docs.len(),
        queries.len(),
        args.k,
        build_s,
        docs.len() as f64 / build_s,
        percentile(&latencies_us, 50.0),
        percentile(&latencies_us, 95.0),
        qps_1t,
        qps_nt,
        term_recall_v,
        phrase_recall_v,
        or_recall_v,
        index.memory_usage(),
    );
    Ok(())
}

// ----------------------------------------------------------------------
// CLI
// ----------------------------------------------------------------------

struct GenArgs {
    docs: usize,
    out_dir: std::path::PathBuf,
    cache_dir: std::path::PathBuf,
    num_term_queries: usize,
    num_phrase_queries: usize,
    num_or_queries: usize,
    k: usize,
}

struct BenchArgs {
    in_dir: std::path::PathBuf,
    run: char,
    k: usize,
    /// Read only immutable segments (flush the tail, `include_tail = false`) —
    /// the Lucene model. Default false keeps read-your-writes (tail included).
    immutable: bool,
    /// Override the tail freeze threshold (docs); large values keep a big
    /// mutable tail to expose tail-read cost. None = index default.
    freeze_threshold: Option<usize>,
    /// Index token positions (enables phrase). `--no-positions` turns this off
    /// for an apples-to-apples term-only comparison against Lucene
    /// `DOCS_AND_FREQS`. Default true.
    with_position: bool,
    /// Disable block-max tail pruning (`--no-tail-skip`) to A/B the tail-skip
    /// optimization: forces a full visible-tail scan on every top-k query.
    /// Default false (pruning on).
    no_tail_skip: bool,
}

fn main() -> Result<()> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let argv: Vec<&str> = argv
        .iter()
        .map(|s| s.as_str())
        .filter(|s| *s != "--bench")
        .collect();
    let mode = argv.first().copied().unwrap_or("");
    let get = |flag: &str, def: &str| -> String {
        argv.iter()
            .position(|a| *a == flag)
            .and_then(|i| argv.get(i + 1))
            .map(|s| s.to_string())
            .unwrap_or_else(|| def.to_string())
    };

    match mode {
        "gen" => {
            let args = GenArgs {
                docs: get("--docs", "1000000").parse().unwrap(),
                out_dir: get("--out-dir", "/tmp/fts_compare").into(),
                cache_dir: get("--cache-dir", "/tmp/fineweb_cache").into(),
                num_term_queries: get("--num-term-queries", "100").parse().unwrap(),
                num_phrase_queries: get("--num-phrase-queries", "50").parse().unwrap(),
                num_or_queries: get("--num-or-queries", "50").parse().unwrap(),
                k: get("--k", "10").parse().unwrap(),
            };
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| lance_core::Error::io(format!("runtime: {e}")))?;
            rt.block_on(run_gen(&args))
        }
        "bench" => {
            let threads: usize = get("--threads", "0").parse().unwrap();
            if threads > 0 {
                let _ = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build_global();
            }
            let args = BenchArgs {
                in_dir: get("--in-dir", "/tmp/fts_compare").into(),
                run: get("--run", "a").chars().next().unwrap_or('a'),
                k: get("--k", "10").parse().unwrap(),
                immutable: argv.contains(&"--immutable"),
                freeze_threshold: argv
                    .iter()
                    .position(|a| *a == "--freeze-threshold")
                    .and_then(|i| argv.get(i + 1))
                    .and_then(|s| s.parse().ok()),
                with_position: !argv.contains(&"--no-positions"),
                no_tail_skip: argv.contains(&"--no-tail-skip"),
            };
            run_bench(&args)
        }
        other => Err(lance_core::Error::invalid_input(format!(
            "usage: mem_wal_fts_bench (gen|bench) [...]; got '{other}'"
        ))),
    }
}
