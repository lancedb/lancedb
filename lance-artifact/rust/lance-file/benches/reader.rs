// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
use std::sync::{Arc, Mutex};

use arrow_array::{UInt32Array, cast::AsArray, types::Int32Type};
use arrow_schema::DataType;
use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use futures::{FutureExt, StreamExt};
use lance_core::utils::{tempfile::TempDir, tokio::get_num_compute_intensive_cpus};
use lance_datagen::ArrayGeneratorExt;
use lance_encoding::decoder::{DecoderConfig, DecoderPlugins, FilterExpression};
use lance_file::{
    reader::{FileReader, FileReaderOptions},
    testing::test_cache,
    version::ConcreteFileVersion,
    versions as file_versions,
    writer::FileWriterOptions,
};
use lance_io::{
    object_store::ObjectStore,
    scheduler::{ScanScheduler, SchedulerConfig},
    utils::CachedFileSize,
};
use object_store::path::Path;
use std::collections::HashMap;
use tokio::runtime::Runtime;

fn bench_reader(c: &mut Criterion) {
    for version in [
        ConcreteFileVersion::V2_0,
        ConcreteFileVersion::V2_1,
        ConcreteFileVersion::V2_2,
    ] {
        let mut group = c.benchmark_group(format!("reader_{}", version));
        let data = lance_datagen::gen_batch()
            .anon_col(lance_datagen::array::rand_type(&DataType::Int32))
            .into_batch_rows(lance_datagen::RowCount::from(2 * 1024 * 1024))
            .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let tmpdir = TempDir::default();
        let (object_store, base_path) = rt
            .block_on(ObjectStore::from_uri(&tmpdir.path_str()))
            .unwrap();

        let file_path = base_path.clone().join("foo.lance");
        let object_writer = rt.block_on(object_store.create(&file_path)).unwrap();

        let mut writer = file_versions::create_writer(
            version,
            object_writer,
            data.schema().as_ref().try_into().unwrap(),
            FileWriterOptions::default(),
        )
        .unwrap();
        rt.block_on(writer.write_batch(&data)).unwrap();
        rt.block_on(writer.finish()).unwrap();
        group.throughput(criterion::Throughput::Bytes(
            data.get_array_memory_size() as u64
        ));
        group.bench_function("decode", |b| {
            b.iter(|| {
                let object_store = &object_store;
                let file_path = &file_path;
                let data = &data;
                rt.block_on(async move {
                    let store_scheduler = ScanScheduler::new(
                        object_store.clone(),
                        SchedulerConfig::default_for_testing(),
                    );
                    let scheduler = store_scheduler
                        .open_file(file_path, &CachedFileSize::unknown())
                        .await
                        .unwrap();
                    let reader = FileReader::try_open(
                        scheduler.clone(),
                        None,
                        Arc::<DecoderPlugins>::default(),
                        &test_cache(),
                        FileReaderOptions::default(),
                    )
                    .await
                    .unwrap();
                    let stream = reader
                        .read_tasks(
                            lance_io::ReadBatchParams::RangeFull,
                            16 * 1024,
                            None,
                            FilterExpression::no_filter(),
                        )
                        .await
                        .unwrap();
                    let stats = Arc::new(Mutex::new((0, 0)));
                    let mut stream = stream
                        .map(|batch_task| {
                            let stats = stats.clone();
                            async move {
                                let batch = batch_task.task.await.unwrap();
                                let row_count = batch.num_rows();
                                let sum = batch
                                    .column(0)
                                    .as_primitive::<Int32Type>()
                                    .values()
                                    .iter()
                                    .map(|v| *v as i64)
                                    .sum::<i64>();
                                let mut stats = stats.lock().unwrap();
                                stats.0 += row_count;
                                stats.1 += sum;
                            }
                            .boxed()
                        })
                        .buffer_unordered(16);
                    while (stream.next().await).is_some() {}
                    let stats = stats.lock().unwrap();
                    let row_count = stats.0;
                    let sum = stats.1;
                    assert_eq!(data.num_rows(), row_count);
                    black_box(sum);
                });
            })
        });
    }
}

#[cfg(not(target_os = "linux"))]
pub fn drop_file_from_cache(_path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn drop_file_from_cache(path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let file = std::fs::File::open(path.as_ref())?;
    let fd = file.as_raw_fd();

    // POSIX_FADV_DONTNEED = 4
    // This tells the kernel to drop the file from the page cache
    let result = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };

    if result != 0 {
        return Err(std::io::Error::from_raw_os_error(result));
    }

    Ok(())
}

const MAX_PARALLELISM: usize = 64;
// Need at least 5K rows between indices to spread data across disk pages
const ROW_GAP: usize = 1024 * 5;
const TOTAL_ROWS: usize = 100_000;

struct CachedReader {
    reader: Arc<FileReader>,
    indices: UInt32Array,
    runtime: Arc<Runtime>,
}

struct CachedReaders {
    all_indices: UInt32Array,
    readers: Vec<CachedReader>,
}

type FileCache = HashMap<(String, String), Arc<CachedReaders>>;

/// Get or create a lance file for benchmarking.
///
/// This function caches the results so files are only created once per (filesystem, version) combination.
/// The version and filesystem are encoded in the filename to avoid collisions.
fn get_cached_readers(
    tmpdir: &TempDir,
    filesystem: &str,
    rt: &Runtime,
    version: ConcreteFileVersion,
) -> Arc<CachedReaders> {
    use std::sync::{LazyLock, Mutex};

    static FILE_CACHE: LazyLock<Mutex<FileCache>> = LazyLock::new(|| Mutex::new(HashMap::new()));

    let key = (filesystem.to_string(), version.to_string());

    // Check cache first
    {
        let cache = FILE_CACHE.lock().unwrap();
        if let Some(cached) = cache.get(&key) {
            return cached.clone();
        }
    }

    let num_threads = get_num_compute_intensive_cpus();

    // Create object store
    let (object_store, base_path) = if filesystem == "mem" {
        rt.block_on(ObjectStore::from_uri("memory://")).unwrap()
    } else {
        rt.block_on(ObjectStore::from_uri(&tmpdir.path_str()))
            .unwrap()
    };

    // Create filename with version to avoid collisions
    let filename = format!("bench_{}.lance", version);
    let file_path = base_path.join(filename.as_str());

    // Generate data
    let data = lance_datagen::gen_batch()
        .anon_col(lance_datagen::array::rand_type(&DataType::Int32).with_random_nulls(0.1))
        .into_batch_rows(lance_datagen::RowCount::from(500 * 1024 * 1024))
        .unwrap();

    // Write file
    let object_writer = rt.block_on(object_store.create(&file_path)).unwrap();
    let mut writer = file_versions::create_writer(
        version,
        object_writer,
        data.schema().as_ref().try_into().unwrap(),
        FileWriterOptions::default(),
    )
    .unwrap();
    rt.block_on(writer.write_batch(&data)).unwrap();
    rt.block_on(writer.finish()).unwrap();

    let indices = (0..TOTAL_ROWS as u32)
        .map(|i| i * ROW_GAP as u32)
        .collect::<Vec<_>>();
    let all_indices = UInt32Array::from(indices);

    let rows_per_thread = TOTAL_ROWS / num_threads;

    let mut readers = Vec::with_capacity(num_threads);
    for i in 0..num_threads {
        let indices = all_indices.slice(i * rows_per_thread, rows_per_thread);
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        );
        let reader = open_reader(&runtime, &object_store, &file_path);
        // Warm up reader
        read_task(
            &runtime,
            reader.clone(),
            indices.clone(),
            /*rows_at_a_time=*/ 100,
        );
        readers.push(CachedReader {
            reader,
            indices,
            runtime,
        });
    }

    let cached_readers = Arc::new(CachedReaders {
        all_indices,
        readers,
    });

    let mut cache = FILE_CACHE.lock().unwrap();
    cache.insert(key, cached_readers.clone());
    cached_readers
}

fn open_reader(rt: &Runtime, object_store: &Arc<ObjectStore>, file_path: &Path) -> Arc<FileReader> {
    rt.block_on(async {
        let store_scheduler =
            ScanScheduler::new(object_store.clone(), SchedulerConfig::default_for_testing());
        let scheduler = store_scheduler
            .open_file(file_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        Arc::new(
            FileReader::try_open(
                scheduler.clone(),
                None,
                Arc::<DecoderPlugins>::default(),
                &test_cache(),
                FileReaderOptions {
                    decoder_config: DecoderConfig {
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        )
    })
}

fn read_task(
    runtime: &Runtime,
    reader: Arc<FileReader>,
    indices: UInt32Array,
    rows_at_a_time: usize,
) {
    let num_rows = indices.len();

    let read_batch = |reader: Arc<FileReader>, indices: UInt32Array| async move {
        let stream = reader
            .read_tasks(
                lance_io::ReadBatchParams::Indices(indices),
                rows_at_a_time as u32,
                None,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap();
        let stats = Arc::new(Mutex::new((0, 0)));
        let mut stream = stream.then(|batch_task| {
            let stats = stats.clone();
            async move {
                let batch = batch_task.task.await.unwrap();
                let row_count = batch.num_rows();
                let sum = batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .values()
                    .iter()
                    .map(|v| *v as i64)
                    .sum::<i64>();
                let mut stats = stats.lock().unwrap();
                stats.0 += row_count;
                stats.1 += sum;
            }
            .boxed()
        });
        while (stream.next().await).is_some() {}
        let stats = stats.lock().unwrap();
        let row_count = stats.0;
        let sum = stats.1;
        assert_eq!(rows_at_a_time, row_count);
        black_box(sum);
    };

    runtime.block_on(async move {
        futures::stream::iter(0..num_rows / rows_at_a_time)
            .map(|i| {
                let reader = reader.clone();
                let indices = indices.clone();
                async move {
                    let reader = reader.clone();
                    let indices = indices.slice(i * rows_at_a_time, rows_at_a_time);
                    read_batch(reader, indices).await;
                }
            })
            .buffer_unordered(MAX_PARALLELISM)
            .collect::<Vec<_>>()
            .await;
    });
}

fn bench_random_access(c: &mut Criterion) {
    let filesystems = ["mem", "disk"];

    let global_runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let tmpdir = TempDir::default();

    let mut group = c.benchmark_group("take");

    let versions = [
        ConcreteFileVersion::V2_0,
        ConcreteFileVersion::V2_1,
        ConcreteFileVersion::V2_2,
    ];

    for filesystem in filesystems {
        for version in versions {
            // Get or create the file (cached)
            let cached_readers = get_cached_readers(&tmpdir, filesystem, &global_runtime, version);

            for multithreaded in [false, true] {
                for rows_at_a_time in [1, 100] {
                    for cached in [true, false] {
                        if !cached && (filesystem == "mem" || version == ConcreteFileVersion::V2_0)
                        {
                            continue;
                        }

                        let num_threads = if multithreaded {
                            get_num_compute_intensive_cpus()
                        } else {
                            1
                        };
                        let rows_per_thread = TOTAL_ROWS / num_threads;
                        group.throughput(Throughput::Elements(
                            rows_per_thread as u64 * num_threads as u64,
                        ));

                        group.bench_function(
                            format!(
                                "{}_{}_{}thread_{}_{}",
                                filesystem,
                                version,
                                num_threads,
                                rows_at_a_time,
                                if cached { "cached" } else { "nocache" },
                            ),
                            |b| {
                                b.iter_batched(
                                    || {
                                        if !cached {
                                            let filename = tmpdir
                                                .std_path()
                                                .join(format!("bench_{}.lance", version));
                                            drop_file_from_cache(tmpdir.std_path().join(&filename))
                                                .unwrap();
                                        }
                                    },
                                    |_| {
                                        let cached_readers = cached_readers.clone();
                                        global_runtime.block_on(async move {
                                            let mut handles = Vec::with_capacity(num_threads);
                                            if multithreaded {
                                                for reader in &cached_readers.readers {
                                                    let runtime = reader.runtime.clone();
                                                    let indices = reader.indices.clone();
                                                    let reader = reader.reader.clone();
                                                    handles.push(tokio::task::spawn_blocking(
                                                        move || {
                                                            read_task(
                                                                &runtime,
                                                                reader,
                                                                indices,
                                                                rows_at_a_time,
                                                            );
                                                        },
                                                    ));
                                                }
                                                for handle in handles {
                                                    handle.await.unwrap();
                                                }
                                            } else {
                                                tokio::task::spawn_blocking(move || {
                                                    read_task(
                                                        &cached_readers.readers[0].runtime,
                                                        cached_readers.readers[0].reader.clone(),
                                                        cached_readers.all_indices.clone(),
                                                        rows_at_a_time,
                                                    )
                                                })
                                                .await
                                                .unwrap();
                                            }
                                        });
                                    },
                                    // We have at least 0.1 seconds of work per iteration so don't need to worry about
                                    // overhead of BatchSize::PerIteration
                                    BatchSize::PerIteration,
                                );
                            },
                        );
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(lance_testing::pprof::PProfProfiler::new(100, lance_testing::pprof::Output::Flamegraph(None)));
    targets = bench_reader, bench_random_access);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_reader, bench_random_access);
criterion_main!(benches);
