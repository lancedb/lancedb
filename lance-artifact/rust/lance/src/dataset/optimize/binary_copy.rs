// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::Dataset;
use crate::Result;
use crate::dataset::DATA_DIR;
use crate::dataset::WriteParams;
use crate::dataset::fragment::write::generate_random_filename;
use crate::datatypes::Schema;
use lance_arrow::DataTypeExt;
use lance_core::Error;
use lance_encoding::decoder::{ColumnInfo, PageEncoding, PageInfo as DecPageInfo};
use lance_file::reader::FileReader as LFReader;
use lance_file::version::ConcreteFileVersion;
use lance_file::version::LanceFileVersion;
use lance_file::writer::FileWriterOptions;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::traits::Writer;
use lance_table::format::{DataFile, Fragment};
use prost::Message;
use prost_types::Any;
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

const ALIGN: usize = 64;

/// Apply 64-byte alignment padding for V2.1+ files.
///
/// For V2.1+, writes padding bytes to align the current position to a 64-byte boundary.
/// For V2.0 and earlier, no padding is applied as alignment is not required.
///
/// Returns the new position after padding (if any).
async fn apply_alignment_padding(
    writer: &mut dyn Writer,
    current_pos: u64,
    version: LanceFileVersion,
) -> Result<u64> {
    if version >= LanceFileVersion::V2_1 {
        static ZERO_BUFFER: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
        let zero_buf = ZERO_BUFFER.get_or_init(|| vec![0u8; ALIGN]);

        let pad = (ALIGN - (current_pos as usize % ALIGN)) % ALIGN;
        if pad != 0 {
            writer.write_all(&zero_buf[..pad]).await?;
            return Ok(current_pos + pad as u64);
        }
    }
    Ok(current_pos)
}

async fn init_writer_if_necessary(
    dataset: &Dataset,
    current_writer: &mut Option<Box<dyn Writer>>,
    current_filename: &mut Option<String>,
) -> Result<bool> {
    if current_writer.is_none() {
        let filename = format!("{}.lance", generate_random_filename());
        let path = dataset.base.clone().join(DATA_DIR).join(filename.as_str());
        let writer = dataset.object_store.create(&path).await?;
        *current_writer = Some(writer);
        *current_filename = Some(filename);
        return Ok(true);
    }
    Ok(false)
}

/// v2_0 vs v2_1+ field-to-column index mapping
///  - v2_1+ stores only leaf columns; non-leaf fields get `-1` in the mapping
///  - v2_0 includes structural headers as columns; non-leaf fields map to a concrete index
fn compute_field_column_indices(
    schema: &Schema,
    full_field_ids_len: usize,
    version: LanceFileVersion,
) -> Vec<i32> {
    let is_structural = version >= LanceFileVersion::V2_1;
    let mut field_column_indices: Vec<i32> = Vec::with_capacity(full_field_ids_len);
    let mut curr_col_idx: i32 = 0;
    for field in schema.fields_pre_order() {
        if field.is_packed_struct() || field.is_leaf() || !is_structural {
            field_column_indices.push(curr_col_idx);
            curr_col_idx += 1;
        } else {
            field_column_indices.push(-1);
        }
    }
    field_column_indices
}

/// Finalize the current output file and return it as a single [Fragment].
/// - Ensures an output writer / filename is present (creates a new file if needed).
/// - Converts the in-memory `col_pages` / `col_buffers` into `ColumnInfo` metadata, draining them.
/// - Applies v2_0 structural header rules (single page, normalized `num_rows` and `priority`).
/// - Writes the Lance footer via [flush_footer] and registers the resulting [DataFile] in a [Fragment].
///
/// PAY ATTENTION current function will:
/// - Takes (`Option::take`) the current writer and filename.
/// - Drains `col_pages` and `col_buffers` for all columns.
#[allow(clippy::too_many_arguments)]
async fn finalize_current_output_file(
    schema: &Schema,
    full_field_ids: &[i32],
    current_writer: &mut Option<Box<dyn Writer>>,
    current_filename: &mut Option<String>,
    current_page_table: &[ColumnInfo],
    col_pages: &mut [Vec<DecPageInfo>],
    col_buffers: &mut [Vec<(u64, u64)>],
    is_non_leaf_column: &[bool],
    total_rows_in_current: u64,
    version: LanceFileVersion,
) -> Result<Fragment> {
    let mut final_cols: Vec<Arc<ColumnInfo>> = Vec::with_capacity(current_page_table.len());
    for (i, column_info) in current_page_table.iter().enumerate() {
        let mut pages_vec = std::mem::take(&mut col_pages[i]);
        // For v2_0 struct headers, force a single page and set num_rows to total
        if version == LanceFileVersion::V2_0
            && is_non_leaf_column.get(i).copied().unwrap_or(false)
            && !pages_vec.is_empty()
        {
            pages_vec[0].num_rows = total_rows_in_current;
            pages_vec[0].priority = 0;
            pages_vec.truncate(1);
        }
        let pages_arc = Arc::from(pages_vec.into_boxed_slice());
        let buffers_vec = std::mem::take(&mut col_buffers[i]);
        final_cols.push(Arc::new(ColumnInfo::new(
            column_info.index,
            pages_arc,
            buffers_vec,
            column_info.encoding.clone(),
        )));
    }
    let writer = current_writer.take().unwrap();
    flush_footer(writer, schema, &final_cols, total_rows_in_current, version).await?;

    // Register the newly closed output file as a fragment data file
    let mut fragment = Fragment::new(0);
    let field_column_indices = compute_field_column_indices(schema, full_field_ids.len(), version);
    let mut data_file = DataFile::new_unstarted(
        current_filename.take().unwrap(),
        ConcreteFileVersion::from(version),
    );
    data_file.fields = full_field_ids.to_vec().into();
    data_file.column_indices = field_column_indices.into();
    fragment.files.push(data_file);
    fragment.physical_rows = Some(total_rows_in_current as usize);
    Ok(fragment)
}

/// Rewrite the files in a single task using binary copy semantics.
///
/// Flow overview (per task):
/// fragments
///   └── data files
///         └── columns
///               └── pages (batched reads) -> aligned writes -> page metadata
///         └── column buffers -> aligned writes -> buffer metadata
///   └── flush when target rows reached -> write footer -> fragment metadata
///   └── final flush for remaining rows
///
/// Behavior highlights:
/// - Assumes all input files share the same Lance file version; version drives column-count
///   calculation (v2.0 includes structural headers, v2.1+ only leaf columns).
/// - Preserves stable row ids by concatenating row-id sequences when enabled.
/// - Enforces 64-byte alignment for page and buffer writes in V2.1+ files (V2.0 does not require alignment).
/// - For v2.0, preserves single-page structural headers and normalizes their row counts/priority.
/// - Flushes an output file once `max_rows_per_file` rows are accumulated, then repeats.
///
/// Parameters:
/// - `dataset`: target dataset (for storage/config and schema).
/// - `fragments`: fragments to merge via binary copy (assumed consistent versions).
/// - `params`: write parameters (uses `max_rows_per_file`).
/// - `read_batch_bytes_opt`: optional I/O batch size when coalescing page reads.
pub async fn rewrite_files_binary_copy(
    dataset: &Dataset,
    fragments: &[Fragment],
    params: &WriteParams,
    read_batch_bytes_opt: Option<usize>,
) -> Result<Vec<Fragment>> {
    if fragments.is_empty() || fragments.iter().any(|fragment| fragment.files.is_empty()) {
        return Err(Error::invalid_input(
            "binary copy requires at least one data file",
        ));
    }

    // Binary copy algorithm overview:
    // - Reads page and buffer regions directly from source files in bounded batches
    // - Appends them to a new output file with alignment, updating offsets
    // - Recomputes page priorities by adding the cumulative row count to preserve order
    // - For v2_0, enforces single-page structural header columns when closing a file
    // - Writes a new footer (schema descriptor, column metadata, offset tables, version)
    // - Optionally carries forward stable row ids and persists them inline in fragment metadata
    // Merge small Lance files into larger ones by page-level binary copy.
    let schema = dataset.schema().clone();
    let full_field_ids = schema.field_ids();

    // The previous checks have ensured that the file versions of all files are consistent.
    let version: LanceFileVersion = ConcreteFileVersion::from_data_file_numbers(
        fragments[0].files[0].file_major_version,
        fragments[0].files[0].file_minor_version,
    )
    .unwrap()
    .into();
    // v2.0 and v2.1+ handle structural headers differently during file writing:
    // - v2_0 materializes ALL fields in pre-order traversal (leaf fields + non-leaf struct headers),
    //   which means the ColumnInfo set includes all fields in pre-order traversal.
    // - v2_1+ materializes fields that are either leaf columns OR packed structs. Non-leaf structural
    //   headers (unpacked structs with children) are not stored as columns.
    //   As a result, the ColumnInfo set contains leaf fields and packed structs.
    // To correctly align copy layout, we derive `column_count` by version:
    // - v2_0: use total number of fields in pre-order (leaf + non-leaf headers)
    // - v2_1+: use only the number of leaf fields plus packed structs
    let column_count = if version == LanceFileVersion::V2_0 {
        schema.fields_pre_order().count()
    } else {
        schema
            .fields_pre_order()
            .filter(|f| f.is_packed_struct() || f.is_leaf())
            .count()
    };

    // v2_0 compatibility: build a map to identify non-leaf structural header columns
    // - In v2_0 these headers exist as columns and must have a single page
    // - In v2_1+ these headers are not stored as columns and this map is unused
    let mut is_non_leaf_column: Vec<bool> = vec![false; column_count];
    if version == LanceFileVersion::V2_0 {
        for (col_idx, field) in schema.fields_pre_order().enumerate() {
            // Only mark non-packed Struct fields (lists remain as leaf data carriers)
            let is_non_leaf = field.data_type().is_struct() && !field.is_packed_struct();
            is_non_leaf_column[col_idx] = is_non_leaf;
        }
    }

    let mut out: Vec<Fragment> = Vec::new();
    let mut current_writer: Option<Box<dyn Writer>> = None;
    let mut current_filename: Option<String> = None;
    let mut current_pos: u64 = 0;
    let mut current_page_table: Vec<ColumnInfo> = Vec::new();
    // Baseline column encodings captured from the first source file; all subsequent
    // files must match per-column to safely concatenate column-level buffers.
    let mut baseline_col_encoding_bytes: Vec<Vec<u8>> = Vec::new();

    // Column-list<Page-List<DecPageInfo>>
    let mut col_pages: Vec<Vec<DecPageInfo>> = std::iter::repeat_with(Vec::<DecPageInfo>::new)
        .take(column_count)
        .collect();
    let mut col_buffers: Vec<Vec<(u64, u64)>> = vec![Vec::new(); column_count];
    let mut total_rows_in_current: u64 = 0;
    let max_rows_per_file = params.max_rows_per_file as u64;

    // Visit each fragment and all of its data files (a fragment may contain multiple files)
    for frag in fragments.iter() {
        for df in frag.files.iter() {
            let object_store = if let Some(base_id) = df.base_id {
                dataset.object_store(Some(base_id)).await?
            } else {
                dataset.object_store.clone()
            };
            let full_path = dataset.data_file_dir(df)?.clone().join(df.path.as_str());
            let scan_scheduler = ScanScheduler::new(
                object_store.clone(),
                SchedulerConfig::max_bandwidth(&object_store),
            );
            let file_scheduler = scan_scheduler
                .open_file_with_priority(&full_path, 0, &df.file_size_bytes)
                .await?;
            let file_meta = LFReader::read_all_metadata(&file_scheduler).await?;
            let src_column_infos = file_meta.column_infos.clone();
            // Initialize current_page_table
            if current_page_table.is_empty() {
                current_page_table = src_column_infos
                    .iter()
                    .map(|column_index| ColumnInfo {
                        index: column_index.index,
                        buffer_offsets_and_sizes: Arc::from(
                            Vec::<(u64, u64)>::new().into_boxed_slice(),
                        ),
                        page_infos: Arc::from(Vec::<DecPageInfo>::new().into_boxed_slice()),
                        encoding: column_index.encoding.clone(),
                    })
                    .collect();
                baseline_col_encoding_bytes = src_column_infos
                    .iter()
                    .map(|ci| Any::from_msg(&ci.encoding).unwrap().encode_to_vec())
                    .collect();
            }

            // Iterate through each column of the current data file of the current fragment
            for (col_idx, src_column_info) in src_column_infos.iter().enumerate() {
                // v2_0 compatibility: special handling for non-leaf structural header columns
                // - v2_0 expects structural header columns to have a SINGLE page; they carry layout
                //   metadata only and are not true data carriers.
                // - When merging multiple input files via binary copy, naively appending pages would
                //   yield multiple pages for the same structural header column, violating v2_0 rules.
                // - To preserve v2_0 invariants, we skip pages beyond the first one for these columns.
                // - During finalization we also normalize the single remaining page’s `num_rows` to the
                //   total number of rows in the output file and reset `priority` to 0.
                // - For v2_1+ this logic does not apply because non-leaf headers are not stored as columns.
                let is_non_leaf = col_idx < is_non_leaf_column.len() && is_non_leaf_column[col_idx];
                if is_non_leaf && !col_pages[col_idx].is_empty() {
                    continue;
                }

                if init_writer_if_necessary(dataset, &mut current_writer, &mut current_filename)
                    .await?
                {
                    current_pos = 0;
                }

                let read_batch_bytes: u64 = read_batch_bytes_opt.unwrap_or(16 * 1024 * 1024) as u64;

                let mut page_index = 0;

                // Iterate through each page of the current column in the current data file of the current fragment
                while page_index < src_column_info.page_infos.len() {
                    let mut batch_ranges: Vec<Range<u64>> = Vec::new();
                    let mut batch_counts: Vec<usize> = Vec::new();
                    let mut batch_bytes: u64 = 0;
                    let mut batch_pages: usize = 0;
                    // Build a single read batch by coalescing consecutive pages up to
                    // `read_batch_bytes` budget:
                    // - Accumulate total bytes (`batch_bytes`) and page count (`batch_pages`).
                    // - For each page, append its buffer ranges to `batch_ranges` and record
                    //   the number of buffers in `batch_counts` so returned bytes can be
                    //   mapped back to page boundaries.
                    // - Stop when adding the next page would exceed the byte budget, then
                    //   issue one I/O request for the collected ranges.
                    // - Advance `page_index` to reflect pages scheduled in this batch.
                    for current_page in &src_column_info.page_infos[page_index..] {
                        let page_bytes: u64 = current_page
                            .buffer_offsets_and_sizes
                            .iter()
                            .map(|(_, size)| *size)
                            .sum();
                        let would_exceed =
                            batch_pages > 0 && (batch_bytes + page_bytes > read_batch_bytes);
                        if would_exceed {
                            break;
                        }
                        batch_counts.push(current_page.buffer_offsets_and_sizes.len());
                        for (offset, size) in current_page.buffer_offsets_and_sizes.iter() {
                            if *size > 0 {
                                batch_ranges.push((*offset)..(*offset + *size));
                            }
                        }
                        batch_bytes += page_bytes;
                        batch_pages += 1;
                        page_index += 1;
                    }

                    let bytes_vec = if batch_ranges.is_empty() {
                        Vec::new()
                    } else {
                        // read many buffers at once
                        file_scheduler.submit_request(batch_ranges, 0).await?
                    };
                    let mut bytes_iter = bytes_vec.into_iter();

                    for (local_idx, buffer_count) in batch_counts.iter().enumerate() {
                        // Reconstruct the absolute page index within the source column:
                        // - `page_index` now points to the page position
                        // - `batch_pages` is how many pages we included in this batch
                        // - `local_idx` enumerates pages inside the batch [0..batch_pages)
                        // Therefore `page_index - batch_pages + local_idx` yields the exact
                        // source page we are currently materializing, allowing us to access
                        // its metadata (encoding, row count, buffers) for the new page entry.
                        let page_idx = page_index - batch_pages + local_idx;
                        let page = &src_column_info.page_infos[page_idx];
                        let mut new_offsets = Vec::with_capacity(*buffer_count);
                        for (buffer_idx, (_, size)) in
                            page.buffer_offsets_and_sizes.iter().enumerate()
                        {
                            let writer = current_writer.as_mut().unwrap().as_mut();
                            current_pos =
                                apply_alignment_padding(writer, current_pos, version).await?;
                            let start = current_pos;
                            if *size == 0 {
                                new_offsets.push((start, 0));
                            } else {
                                let bytes = bytes_iter.next().ok_or_else(|| {
                                    Error::execution(format!(
                                        "binary copy: missing page buffer bytes while rewriting data file \
                                         (column {col_idx}, page {page_idx}, buffer {buffer_idx}, expected size {size})",
                                    ))
                                })?;
                                writer.write_all(&bytes).await?;
                                current_pos += bytes.len() as u64;
                                new_offsets.push((start, bytes.len() as u64));
                            }
                        }

                        // manual clone encoding
                        let encoding = if page.encoding.is_structural() {
                            PageEncoding::Structural(page.encoding.as_structural().clone())
                        } else {
                            PageEncoding::Legacy(page.encoding.as_legacy().clone())
                        };
                        // `priority` acts as the global row offset for this page, ensuring
                        // downstream iterators maintain the correct logical order across
                        // merged inputs.
                        let new_page_info = DecPageInfo {
                            num_rows: page.num_rows,
                            priority: page.priority + total_rows_in_current,
                            encoding,
                            buffer_offsets_and_sizes: Arc::from(new_offsets.into_boxed_slice()),
                        };
                        col_pages[col_idx].push(new_page_info);
                    }
                } // finished scheduling & copying pages for this column in the current source file

                if !src_column_info.buffer_offsets_and_sizes.is_empty() {
                    // Validate column-level encoding compatibility before copying buffers
                    let src_col_encoding_bytes = Any::from_msg(&src_column_info.encoding)
                        .unwrap()
                        .encode_to_vec();
                    let baseline_bytes = &baseline_col_encoding_bytes[col_idx];
                    if src_col_encoding_bytes != *baseline_bytes {
                        return Err(Error::execution(format!(
                            "binary copy: The ColumnEncoding of column {} is incompatible with the first file, \
                            making it impossible to safely concatenate buffers",
                            col_idx
                        )));
                    }
                    let ranges: Vec<Range<u64>> = src_column_info
                        .buffer_offsets_and_sizes
                        .iter()
                        .filter(|(_, size)| *size > 0)
                        .map(|(offset, size)| (*offset)..(*offset + *size))
                        .collect();
                    let bytes_vec = if ranges.is_empty() {
                        Vec::new()
                    } else {
                        file_scheduler.submit_request(ranges, 0).await?
                    };
                    let mut bytes_iter = bytes_vec.into_iter();
                    for (buffer_idx, (_, size)) in
                        src_column_info.buffer_offsets_and_sizes.iter().enumerate()
                    {
                        let writer = current_writer.as_mut().unwrap().as_mut();
                        current_pos = apply_alignment_padding(writer, current_pos, version).await?;
                        let start = current_pos;
                        if *size == 0 {
                            col_buffers[col_idx].push((start, 0));
                        } else {
                            let bytes = bytes_iter.next().ok_or_else(|| {
                                Error::execution(format!(
                                    "binary copy: missing column buffer bytes while rewriting data file \
                                     (column {col_idx}, buffer {buffer_idx}, expected size {size})",
                                ))
                            })?;
                            writer.write_all(&bytes).await?;
                            current_pos += bytes.len() as u64;
                            col_buffers[col_idx].push((start, bytes.len() as u64));
                        }
                    }
                }
            } // finished all columns in the current source file

            // Accumulate rows for the current output file and flush when reaching the threshold
            total_rows_in_current += file_meta.num_rows;
            if total_rows_in_current >= max_rows_per_file {
                let fragment_out = finalize_current_output_file(
                    &schema,
                    &full_field_ids,
                    &mut current_writer,
                    &mut current_filename,
                    &current_page_table,
                    &mut col_pages,
                    &mut col_buffers,
                    &is_non_leaf_column,
                    total_rows_in_current,
                    version,
                )
                .await?;

                // Reset state for next output file
                current_writer = None;
                current_pos = 0;
                current_page_table.clear();
                for v in col_pages.iter_mut() {
                    v.clear();
                }
                for v in col_buffers.iter_mut() {
                    v.clear();
                }
                out.push(fragment_out);
                total_rows_in_current = 0;
            }
        }
    } // Finished writing all fragments; any remaining data in memory will be flushed below

    if total_rows_in_current > 0 {
        // Flush remaining rows as a final output file
        init_writer_if_necessary(dataset, &mut current_writer, &mut current_filename).await?;
        let frag = finalize_current_output_file(
            &schema,
            &full_field_ids,
            &mut current_writer,
            &mut current_filename,
            &current_page_table,
            &mut col_pages,
            &mut col_buffers,
            &is_non_leaf_column,
            total_rows_in_current,
            version,
        )
        .await?;
        out.push(frag);
    }
    Ok(out)
}

/// Finalizes a compacted data file by writing the Lance footer via `FileWriter`.
///
/// This function does not manually craft the footer. Instead it:
/// - Pads the current `ObjectWriter` position to a 64‑byte boundary (required for v2_1+ readers).
/// - Initializes a `lance_file::writer::FileWriter` from the collected column metadata.
/// - Calls `FileWriter::finish()` to emit column metadata, offset tables, global buffers
///   (schema descriptor), version, and to close the writer.
///
/// Preconditions:
/// - All page data and column‑level buffers referenced by `final_cols` have already been written
///   to `writer`; otherwise offsets in the footer will be invalid.
///
/// Version notes:
/// - v2_0 structural single‑page enforcement is handled when building `final_cols`; this function
///   only performs consistent finalization.
async fn flush_footer(
    mut writer: Box<dyn Writer>,
    schema: &Schema,
    final_cols: &[Arc<ColumnInfo>],
    total_rows_in_current: u64,
    version: LanceFileVersion,
) -> Result<()> {
    let pos = writer.tell().await? as u64;
    let _new_pos = apply_alignment_padding(writer.as_mut(), pos, version).await?;

    let mut file_writer = lance_file::versions::create_lazy_writer(
        ConcreteFileVersion::from(version),
        writer,
        FileWriterOptions::default(),
    )?;
    file_writer.initialize_with_external_columns(
        schema.clone(),
        final_cols,
        total_rows_in_current,
    )?;
    file_writer.finish().await?;
    Ok(())
}
