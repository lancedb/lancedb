// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::client::{IndexInfo, IndexStats, TableInfo};
use crate::sql_executor::SqlResult;
use crate::table_writer::TableWriter;
use anyhow::Result;
use arrow_array::types::ArrowDictionaryKeyType;
use arrow_array::{
    downcast_dictionary_array, Array, BinaryArray, BooleanArray, DictionaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeBinaryArray, ListArray, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_buffer::ArrowNativeType;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_json::writer::{make_encoder, Encoder, EncoderFactory, EncoderOptions, NullableEncoder};
use arrow_schema::{DataType, Field, Schema};
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;

/// Result of a query engine command
#[derive(Debug)]
pub enum QueryResult {
    /// SQL query result
    SqlResult(SqlResult),
    /// List of table names
    TableList(Vec<String>),
    /// List of tables with detailed stats
    TableListDetailed(Vec<TableInfo>),
    /// Table information
    TableInfo(TableInfo),
    /// Table information with indices
    TableInfoWithIndices(TableInfo, Vec<IndexInfo>),
    /// Index information
    IndexList(Vec<IndexInfo>),
    /// List of indices with detailed stats
    IndexListDetailed(Vec<(IndexInfo, Option<IndexStats>)>),
}

/// Output format options
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Table,
    Csv,
    Json,
    Ndjson,
}

impl FromStr for OutputFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "table" => Ok(Self::Table),
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            "ndjson" => Ok(Self::Ndjson),
            _ => Err(anyhow::anyhow!(
                "Invalid output format: {}. Must be one of: table, csv, json, ndjson",
                s
            )),
        }
    }
}

/// Formats query results for display
pub struct ResultFormatter {
    output_format: OutputFormat,
    /// Optional override for the terminal width used to choose between table
    /// and expanded rendering. `None` auto-detects from the terminal;
    /// tests pin it so output is independent of the ambient terminal size.
    terminal_width: Option<usize>,
}

impl ResultFormatter {
    /// Create a new ResultFormatter
    pub fn new(output_format: OutputFormat) -> Self {
        Self {
            output_format,
            terminal_width: None,
        }
    }

    pub fn with_terminal_width(mut self, width: usize) -> Self {
        self.terminal_width = Some(width);
        self
    }

    pub fn output_format(&self) -> OutputFormat {
        self.output_format
    }

    /// Format a QueryResult to string
    pub fn format(&self, result: &QueryResult) -> Result<String> {
        let mut output = Vec::new();
        self.format_to_writer(result, &mut output)?;
        Ok(String::from_utf8(output)?)
    }

    /// Format a QueryResult to a writer
    pub fn format_to_writer<W: Write>(&self, result: &QueryResult, writer: &mut W) -> Result<()> {
        match result {
            QueryResult::SqlResult(sql_result) => self.format_sql_result(sql_result, writer),
            QueryResult::TableList(tables) => self.format_table_list(tables, writer),
            QueryResult::TableListDetailed(tables) => {
                self.format_table_list_detailed(tables, writer)
            }
            QueryResult::TableInfo(info) => self.format_table_info(info, writer),
            QueryResult::TableInfoWithIndices(info, indices) => {
                self.format_table_info_with_indices(info, indices, writer)
            }
            QueryResult::IndexList(indices) => self.format_index_list(indices, writer),
            QueryResult::IndexListDetailed(indices) => {
                self.format_index_list_detailed(indices, writer)
            }
        }
    }

    /// Format SQL result
    fn format_sql_result<W: Write>(&self, result: &SqlResult, writer: &mut W) -> Result<()> {
        match result {
            SqlResult::Query(batches) => self.format_record_batches(batches, writer),
            SqlResult::Dml(dml) => {
                writeln!(
                    writer,
                    "{} {} row(s), version {}",
                    dml.operation, dml.rows_affected, dml.version
                )?;
                Ok(())
            }
            SqlResult::Success => {
                writeln!(writer, "Success")?;
                Ok(())
            }
        }
    }

    /// Format record batches
    pub fn format_record_batches<W: Write>(
        &self,
        batches: &[RecordBatch],
        writer: &mut W,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        match self.output_format {
            OutputFormat::Table => self.format_batches_as_table(batches, writer),
            OutputFormat::Csv => self.format_batches_as_csv(batches, writer),
            OutputFormat::Json => self.format_batches_as_json(batches, writer),
            OutputFormat::Ndjson => self.format_batches_as_ndjson(batches, writer),
        }
    }

    /// Format batches as table
    fn format_batches_as_table<W: Write>(
        &self,
        batches: &[RecordBatch],
        writer: &mut W,
    ) -> Result<()> {
        let schema = batches[0].schema();
        let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        // Check if this is an EXPLAIN query (has plan_type and plan columns)
        let is_explain =
            column_names.len() == 2 && column_names[0] == "plan_type" && column_names[1] == "plan";

        if is_explain {
            return self.format_explain_results(batches, writer);
        }

        let mut rows = Vec::new();
        let mut total_rows = 0;

        for batch in batches {
            total_rows += batch.num_rows();
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let column = batch.column(col_idx);
                    let field = schema.field(col_idx);
                    let value = format_arrow_value(column, row_idx, field);
                    row.push(value);
                }
                rows.push(row);
            }
        }

        TableWriter::print_table(writer, &column_names, &rows, self.terminal_width)?;
        writeln!(
            writer,
            "({} row{})",
            total_rows,
            if total_rows == 1 { "" } else { "s" }
        )?;
        Ok(())
    }

    /// Format EXPLAIN query results
    fn format_explain_results<W: Write>(
        &self,
        batches: &[RecordBatch],
        writer: &mut W,
    ) -> Result<()> {
        writeln!(
            writer,
            "                                            QUERY PLAN"
        )?;
        writeln!(writer, "---------------------------------------------------------------------------------------------------")?;

        let mut logical_plan = String::new();
        let mut physical_plan = String::new();
        let mut plan_with_metrics = String::new();

        // Extract the plans from the batches
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                if batch.num_columns() >= 2 {
                    let plan_type_col = batch.column(0);
                    let plan_col = batch.column(1);

                    let plan_type = plan_type_col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|arr| arr.value(row_idx))
                        .unwrap_or("");

                    let plan = plan_col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|arr| arr.value(row_idx))
                        .unwrap_or("");

                    match plan_type {
                        "logical_plan" => logical_plan = plan.to_string(),
                        "physical_plan" => physical_plan = plan.to_string(),
                        "Plan with Metrics" => plan_with_metrics = plan.to_string(),
                        _ => {}
                    }
                }
            }
        }

        // Display logical plan
        if !logical_plan.is_empty() {
            writeln!(writer, "Logical Plan:")?;
            for line in logical_plan.lines() {
                writeln!(writer, "  {}", line)?;
            }
        }

        // Display physical plan
        if !physical_plan.is_empty() {
            if !logical_plan.is_empty() {
                writeln!(writer)?;
            }
            writeln!(writer, "Physical Plan:")?;
            for line in physical_plan.lines() {
                writeln!(writer, "  {}", line)?;
            }
        }

        // Display plan with metrics (EXPLAIN ANALYZE)
        if !plan_with_metrics.is_empty() {
            if !logical_plan.is_empty() || !physical_plan.is_empty() {
                writeln!(writer)?;
            }
            for line in plan_with_metrics.lines() {
                writeln!(writer, "{}", line)?;
            }
        }

        writeln!(
            writer,
            "({} row{})",
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            if batches.iter().map(|b| b.num_rows()).sum::<usize>() == 1 {
                ""
            } else {
                "s"
            }
        )?;
        Ok(())
    }

    /// Format batches as CSV
    fn format_batches_as_csv<W: Write>(
        &self,
        batches: &[RecordBatch],
        writer: &mut W,
    ) -> Result<()> {
        let schema = batches[0].schema();

        for (index, field) in schema.fields().iter().enumerate() {
            if index > 0 {
                write!(writer, ",")?;
            }
            write_csv_value(writer, field.name())?;
        }
        writeln!(writer)?;

        let scalar_options = FormatOptions::default();
        let json_options = EncoderOptions::default()
            .with_explicit_nulls(true)
            .with_encoder_factory(Arc::new(CsvJsonEncoderFactory));
        let mut json_value = Vec::new();

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let schema = batch.schema();
            let mut formatters = schema
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(field, column)| {
                    if csv_uses_json(column.data_type()) {
                        Ok(CsvValueFormatter::Json(make_encoder(
                            field,
                            column.as_ref(),
                            &json_options,
                        )?))
                    } else {
                        Ok(CsvValueFormatter::Scalar(ArrayFormatter::try_new(
                            column.as_ref(),
                            &scalar_options,
                        )?))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let nulls: Vec<_> = batch
                .columns()
                .iter()
                .map(|column| column.logical_nulls())
                .collect();

            for row_idx in 0..batch.num_rows() {
                for (col_idx, formatter) in formatters.iter_mut().enumerate() {
                    if col_idx > 0 {
                        write!(writer, ",")?;
                    }
                    // Logical nulls include null dictionary values and Null arrays.
                    if nulls[col_idx]
                        .as_ref()
                        .is_some_and(|nulls| nulls.is_null(row_idx))
                    {
                        write!(writer, "{CSV_NULL_VALUE}")?;
                        continue;
                    }
                    match formatter {
                        CsvValueFormatter::Scalar(formatter) => {
                            write_csv_cell(writer, &formatter.value(row_idx).try_to_string()?)?;
                        }
                        CsvValueFormatter::Json(encoder) => {
                            json_value.clear();
                            encoder.encode(row_idx, &mut json_value);
                            write_csv_cell(writer, std::str::from_utf8(&json_value)?)?;
                        }
                    }
                }
                writeln!(writer)?;
            }
        }
        Ok(())
    }

    /// Format batches as JSON
    fn format_batches_as_json<W: Write>(
        &self,
        batches: &[RecordBatch],
        writer: &mut W,
    ) -> Result<()> {
        if batches.is_empty() {
            writeln!(writer, "[]")?;
            return Ok(());
        }
        // Arrow's bytes go out as written. Re-reading them through
        // `serde_json::Value` to pretty-print would round every decimal the
        // writer emitted exactly.
        let mut json = Vec::new();
        let mut json_writer = arrow_json::writer::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, arrow_json::writer::JsonArray>(&mut json);
        for batch in batches {
            json_writer.write(batch)?;
        }
        json_writer.finish()?;
        writer.write_all(&json)?;
        writeln!(writer)?;
        Ok(())
    }

    /// Format batches as NDJSON
    fn format_batches_as_ndjson<W: Write>(
        &self,
        batches: &[RecordBatch],
        writer: &mut W,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let mut json = Vec::new();
        let mut json_writer = arrow_json::writer::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, arrow_json::writer::LineDelimited>(&mut json);
        for batch in batches {
            json_writer.write(batch)?;
        }
        json_writer.finish()?;
        writer.write_all(&json)?;
        Ok(())
    }

    /// Format table list
    fn format_table_list<W: Write>(&self, tables: &[String], writer: &mut W) -> Result<()> {
        match self.output_format {
            OutputFormat::Json => {
                let json = serde_json::to_string_pretty(&tables)?;
                writeln!(writer, "{}", json)?;
            }
            _ => {
                if tables.is_empty() {
                    writeln!(writer, "Did not find any relations.")?;
                } else {
                    let headers = vec!["Tables".to_string()];
                    let data: Vec<Vec<String>> = tables.iter().map(|t| vec![t.clone()]).collect();
                    TableWriter::print_table_with_title(
                        writer,
                        "List of relations",
                        &headers,
                        &data,
                        self.terminal_width,
                    )?;
                    writeln!(
                        writer,
                        "({} row{})",
                        tables.len(),
                        if tables.len() == 1 { "" } else { "s" }
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Format detailed table list with stats
    fn format_table_list_detailed<W: Write>(
        &self,
        tables: &[TableInfo],
        writer: &mut W,
    ) -> Result<()> {
        match self.output_format {
            OutputFormat::Json => {
                let json = serde_json::json!(tables
                    .iter()
                    .map(|t| {
                        serde_json::json!({
                            "name": t.name,
                            "type": "table",
                            "rows": t.row_count,
                            "size_bytes": t.size_bytes,
                            "indices": t.num_indices,
                            "fragments": t.num_fragments,
                        })
                    })
                    .collect::<Vec<_>>());
                let json_string = serde_json::to_string_pretty(&json)?;
                writeln!(writer, "{}", json_string)?;
            }
            _ => {
                if tables.is_empty() {
                    writeln!(writer, "Did not find any relations.")?;
                } else {
                    let headers = vec![
                        "Name".to_string(),
                        "Type".to_string(),
                        "Rows".to_string(),
                        "Size".to_string(),
                        "Indices".to_string(),
                        "Fragments".to_string(),
                    ];

                    let data: Vec<Vec<String>> = tables
                        .iter()
                        .map(|t| {
                            vec![
                                t.name.clone(),
                                "table".to_string(),
                                t.row_count.map_or("".to_string(), |r| r.to_string()),
                                t.size_bytes.map_or("".to_string(), format_bytes),
                                t.num_indices
                                    .filter(|&n| n > 0)
                                    .map_or("".to_string(), |n| n.to_string()),
                                t.num_fragments
                                    .filter(|&n| n > 0)
                                    .map_or("".to_string(), |n| n.to_string()),
                            ]
                        })
                        .collect();

                    TableWriter::print_table_with_title(
                        writer,
                        "List of relations",
                        &headers,
                        &data,
                        self.terminal_width,
                    )?;
                    writeln!(
                        writer,
                        "({} row{})",
                        tables.len(),
                        if tables.len() == 1 { "" } else { "s" }
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Format table info
    fn format_table_info<W: Write>(&self, info: &TableInfo, writer: &mut W) -> Result<()> {
        match self.output_format {
            OutputFormat::Json => {
                let json = serde_json::json!({
                    "name": info.name,
                    "schema": schema_to_json(&info.schema),
                    "row_count": info.row_count,
                    "size_bytes": info.size_bytes,
                    "num_indices": info.num_indices,
                    "num_fragments": info.num_fragments,
                });
                let json_string = serde_json::to_string_pretty(&json)?;
                writeln!(writer, "{}", json_string)?;
            }
            _ => {
                // Format as table
                writeln!(writer, "                    Table \"{}\"", info.name)?;

                let headers = vec![
                    "Column".to_string(),
                    "Type".to_string(),
                    "Nullable".to_string(),
                ];
                let mut data = Vec::new();

                for field in info.schema.fields() {
                    let data_type = format_data_type(field.data_type());
                    let nullable = if field.is_nullable() { "YES" } else { "" };
                    data.push(vec![field.name().clone(), data_type, nullable.to_string()]);
                }

                TableWriter::print_table(writer, &headers, &data, self.terminal_width)?;

                // Add stats if available
                if let Some(row_count) = info.row_count {
                    writeln!(writer, "\nRows: {}", row_count)?;
                }
                if let Some(size) = info.size_bytes {
                    writeln!(writer, "Size: {}", format_bytes(size))?;
                }
            }
        }
        Ok(())
    }

    /// Format table info with indices (similar to psql \d <table>)
    fn format_table_info_with_indices<W: Write>(
        &self,
        info: &TableInfo,
        indices: &[IndexInfo],
        writer: &mut W,
    ) -> Result<()> {
        match self.output_format {
            OutputFormat::Table => {
                // Format as table (psql style)
                writeln!(writer, "                    Table \"{}\"", info.name)?;

                let headers = vec![
                    "Column".to_string(),
                    "Type".to_string(),
                    "Nullable".to_string(),
                ];
                let mut data = Vec::new();
                for field in info.schema.fields() {
                    let data_type = format_data_type(field.data_type());
                    let nullable = if field.is_nullable() { "YES" } else { "" };
                    data.push(vec![field.name().clone(), data_type, nullable.to_string()]);
                }
                TableWriter::print_table(writer, &headers, &data, self.terminal_width)?;

                // List indexes if any
                if !indices.is_empty() {
                    writeln!(writer, "Indexes:")?;
                    for index in indices {
                        let columns = index.columns.join(", ");
                        writeln!(
                            writer,
                            "    \"{}\" {} ({})",
                            index.name, index.index_type, columns
                        )?;
                    }
                }

                // Add stats if available
                if let Some(row_count) = info.row_count {
                    writeln!(writer, "\nRows: {}", row_count)?;
                }
                if let Some(size) = info.size_bytes {
                    writeln!(writer, "Size: {}", format_bytes(size))?;
                }
            }
            OutputFormat::Csv => {
                // CSV format: output schema info and indices as separate sections
                writeln!(writer, "# Table: {}", info.name)?;
                writeln!(writer, "# Schema")?;
                writeln!(writer, "Column,Type,Nullable")?;
                for field in info.schema.fields() {
                    let data_type = format_data_type(field.data_type());
                    let nullable = if field.is_nullable() { "YES" } else { "NO" };
                    writeln!(writer, "{},{},{}", field.name(), data_type, nullable)?;
                }

                if !indices.is_empty() {
                    writeln!(writer, "\n# Indices")?;
                    writeln!(writer, "Index Name,Index Type,Columns")?;
                    for index in indices {
                        let columns = index.columns.join(";");
                        writeln!(writer, "{},{},{}", index.name, index.index_type, columns)?;
                    }
                }

                if let Some(row_count) = info.row_count {
                    writeln!(writer, "\n# Row Count: {}", row_count)?;
                }
                if let Some(size) = info.size_bytes {
                    writeln!(writer, "# Size: {}", format_bytes(size))?;
                }
            }
            OutputFormat::Json => {
                let json = serde_json::json!({
                    "name": info.name,
                    "schema": schema_to_json(&info.schema),
                    "row_count": info.row_count,
                    "size_bytes": info.size_bytes,
                    "num_indices": info.num_indices,
                    "num_fragments": info.num_fragments,
                    "indices": indices,
                });
                let json_string = serde_json::to_string_pretty(&json)?;
                writeln!(writer, "{}", json_string)?;
            }
            OutputFormat::Ndjson => {
                // NDJSON: one line for table info, one line per index
                let table_json = serde_json::json!({
                    "type": "table_info",
                    "name": info.name,
                    "schema": schema_to_json(&info.schema),
                    "row_count": info.row_count,
                    "size_bytes": info.size_bytes,
                    "num_indices": info.num_indices,
                    "num_fragments": info.num_fragments,
                });
                writeln!(writer, "{}", serde_json::to_string(&table_json)?)?;

                for index in indices {
                    let index_json = serde_json::json!({
                        "type": "index_info",
                        "table": info.name,
                        "name": index.name,
                        "index_type": index.index_type,
                        "columns": index.columns,
                    });
                    writeln!(writer, "{}", serde_json::to_string(&index_json)?)?;
                }
            }
        }
        Ok(())
    }

    /// Format index list
    fn format_index_list<W: Write>(&self, indices: &[IndexInfo], writer: &mut W) -> Result<()> {
        match self.output_format {
            OutputFormat::Json => {
                let json = serde_json::to_string_pretty(&indices)?;
                writeln!(writer, "{}", json)?;
            }
            _ => {
                if indices.is_empty() {
                    writeln!(writer, "Did not find any indexes.")?;
                } else {
                    let headers = vec!["Name".to_string(), "Type".to_string(), "Table".to_string()];
                    let data: Vec<Vec<String>> = indices
                        .iter()
                        .map(|idx| {
                            vec![
                                idx.name.clone(),
                                idx.index_type.clone(),
                                idx.table_name.clone(),
                            ]
                        })
                        .collect();

                    TableWriter::print_table_with_title(
                        writer,
                        "List of indices",
                        &headers,
                        &data,
                        self.terminal_width,
                    )?;
                    writeln!(
                        writer,
                        "({} row{})",
                        indices.len(),
                        if indices.len() == 1 { "" } else { "s" }
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Format detailed index list with stats
    fn format_index_list_detailed<W: Write>(
        &self,
        indices: &[(IndexInfo, Option<IndexStats>)],
        writer: &mut W,
    ) -> Result<()> {
        match self.output_format {
            OutputFormat::Json => {
                let json = serde_json::json!(indices
                    .iter()
                    .map(|(info, stats)| {
                        let mut obj = serde_json::json!({
                            "name": info.name,
                            "type": info.index_type,
                            "table": info.table_name,
                            "columns": info.columns,
                        });
                        if let Some(stats) = stats {
                            obj["distance_type"] = serde_json::json!(stats.distance_type);
                            obj["indexed_rows"] = serde_json::json!(stats.num_indexed_rows);
                            obj["unindexed_rows"] = serde_json::json!(stats.num_unindexed_rows);
                        }
                        obj
                    })
                    .collect::<Vec<_>>());
                let json_string = serde_json::to_string_pretty(&json)?;
                writeln!(writer, "{}", json_string)?;
            }
            _ => {
                if indices.is_empty() {
                    writeln!(writer, "Did not find any indexes.")?;
                } else {
                    let headers = vec![
                        "Name".to_string(),
                        "Type".to_string(),
                        "Distance".to_string(),
                        "Indexed Rows".to_string(),
                        "Unindexed Rows".to_string(),
                        "Table".to_string(),
                    ];

                    let data: Vec<Vec<String>> = indices
                        .iter()
                        .map(|(info, stats)| {
                            let (distance, indexed, unindexed) = if let Some(s) = stats {
                                (
                                    s.distance_type.as_deref().unwrap_or(""),
                                    s.num_indexed_rows.to_string(),
                                    s.num_unindexed_rows.to_string(),
                                )
                            } else {
                                ("", "".to_string(), "".to_string())
                            };

                            vec![
                                info.name.clone(),
                                info.index_type.clone(),
                                distance.to_string(),
                                indexed,
                                unindexed,
                                info.table_name.clone(),
                            ]
                        })
                        .collect();

                    TableWriter::print_table_with_title(
                        writer,
                        "List of relations",
                        &headers,
                        &data,
                        self.terminal_width,
                    )?;
                    writeln!(
                        writer,
                        "({} row{})",
                        indices.len(),
                        if indices.len() == 1 { "" } else { "s" }
                    )?;
                }
            }
        }
        Ok(())
    }
}

// Helper functions

enum CsvValueFormatter<'a> {
    Scalar(ArrayFormatter<'a>),
    Json(NullableEncoder<'a>),
}

fn csv_uses_json(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, value) => csv_uses_json(value),
        DataType::RunEndEncoded(_, value) => csv_uses_json(value.data_type()),
        _ => data_type.is_nested(),
    }
}

const CSV_NULL_VALUE: &str = r"\N";

fn write_csv_cell<W: Write>(writer: &mut W, value: &str) -> Result<()> {
    // Escape leading backslashes so the null marker cannot collide with data.
    if value.starts_with('\\') {
        write_csv_value(writer, &format!("\\{value}"))
    } else {
        write_csv_value(writer, value)
    }
}

fn write_csv_value<W: Write>(writer: &mut W, value: &str) -> Result<()> {
    // An empty single-column record must not become a blank line.
    if value.is_empty() || value.contains([',', '"', '\n', '\r']) {
        write!(writer, "\"{}\"", value.replace('"', "\"\""))?;
    } else {
        write!(writer, "{value}")?;
    }
    Ok(())
}

#[derive(Debug)]
struct CsvJsonEncoderFactory;

impl EncoderFactory for CsvJsonEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a arrow_schema::FieldRef,
        array: &'a dyn arrow_array::Array,
        options: &'a EncoderOptions,
    ) -> std::result::Result<Option<NullableEncoder<'a>>, arrow_schema::ArrowError> {
        downcast_dictionary_array! {
            array => {
                let values = make_encoder(field, array.values().as_ref(), options)?;
                // Parent encoders must see keys pointing to null values as nulls.
                return Ok(Some(NullableEncoder::new(
                    Box::new(CsvJsonDictionaryEncoder { dictionary: array, values }),
                    array.logical_nulls(),
                )));
            }
            _ => {}
        }
        if !matches!(
            array.data_type(),
            DataType::Float16 | DataType::Float32 | DataType::Float64
        ) {
            return Ok(None);
        }
        let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())?;
        Ok(Some(NullableEncoder::new(
            Box::new(CsvJsonFloatEncoder(formatter)),
            array.nulls().cloned(),
        )))
    }
}

struct CsvJsonDictionaryEncoder<'a, K: ArrowDictionaryKeyType> {
    dictionary: &'a DictionaryArray<K>,
    values: NullableEncoder<'a>,
}

impl<K: ArrowDictionaryKeyType> Encoder for CsvJsonDictionaryEncoder<'_, K> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.values
            .encode(self.dictionary.keys().value(idx).as_usize(), out);
    }
}

struct CsvJsonFloatEncoder<'a>(ArrayFormatter<'a>);

impl Encoder for CsvJsonFloatEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = self.0.value(idx).to_string();
        // JSON has no non-finite numbers; keep their names instead of turning
        // them into nulls, as Arrow's default JSON float encoder does.
        if matches!(value.as_str(), "NaN" | "inf" | "-inf") {
            out.push(b'"');
            out.extend_from_slice(value.as_bytes());
            out.push(b'"');
        } else {
            out.extend_from_slice(value.as_bytes());
        }
    }
}

/// Longest string rendered in full when a column has not opted out of
/// truncation.
const MAX_UNTRUNCATED_STRING_LEN: usize = 50;

/// Arrow field metadata key marking a column that must be rendered in full.
const NO_TRUNCATE_METADATA_KEY: &str = "lancedb.display.no_truncate";

/// Whether this field is marked to render in full.
fn field_opts_out_of_truncation(field: &Field) -> bool {
    field
        .metadata()
        .get(NO_TRUNCATE_METADATA_KEY)
        .is_some_and(|value| value == "true")
}

/// Rows a query returned, across batches. Zero for non-query results.
pub fn row_count(result: &SqlResult) -> usize {
    match result {
        SqlResult::Query(batches) => batches.iter().map(RecordBatch::num_rows).sum(),
        _ => 0,
    }
}

/// Keep the first `limit` rows, for a listing whose reader is a human at a
/// terminal rather than a program. Slices are zero-copy, so the dropped rows
/// cost nothing beyond the memory they already occupy. Non-query results pass
/// through.
pub fn take_rows(result: SqlResult, limit: usize) -> SqlResult {
    let SqlResult::Query(batches) = result else {
        return result;
    };

    let mut remaining = limit;
    let mut kept = Vec::with_capacity(batches.len());
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let take = remaining.min(batch.num_rows());
        remaining -= take;
        kept.push(batch.slice(0, take));
    }
    SqlResult::Query(kept)
}

/// Mark `columns` so rendering keeps their values whole, for results whose
/// consumer needs to copy a value verbatim rather than read it.
pub fn mark_no_truncate(result: SqlResult, columns: &[&str]) -> SqlResult {
    match result {
        SqlResult::Query(batches) => SqlResult::Query(
            batches
                .into_iter()
                .map(|batch| mark_batch_columns(batch, columns))
                .collect(),
        ),
        other => other,
    }
}

/// Rebuild `batch` with the named fields marked, leaving its data untouched.
fn mark_batch_columns(batch: RecordBatch, columns: &[&str]) -> RecordBatch {
    let schema = batch.schema();
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if !columns.contains(&field.name().as_str()) {
                return field.as_ref().clone();
            }
            let mut metadata = field.metadata().clone();
            metadata.insert(NO_TRUNCATE_METADATA_KEY.to_string(), "true".to_string());
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect();

    let marked = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    match RecordBatch::try_new(marked, batch.columns().to_vec()) {
        Ok(batch) => batch,
        // Only the metadata changed, so this should not fail. Render the
        // original rather than the error, at the cost of abbreviating them.
        Err(err) => {
            warn!("could not mark columns to render in full, values may be abbreviated: {err}");
            batch
        }
    }
}

/// Render a string cell, abbreviating it unless the field opts out.
fn format_string_value(value: &str, field: &Field) -> String {
    if value.len() <= MAX_UNTRUNCATED_STRING_LEN || field_opts_out_of_truncation(field) {
        return value.to_string();
    }
    let end = value.floor_char_boundary(MAX_UNTRUNCATED_STRING_LEN);
    format!("{}...({} chars)", &value[..end], value.chars().count())
}

/// Format arrow value as string
fn format_arrow_value(column: &dyn arrow_array::Array, row_idx: usize, field: &Field) -> String {
    if column.is_null(row_idx) {
        return "NULL".to_string();
    }

    match field.data_type() {
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            format_string_value(array.value(row_idx), field)
        }
        DataType::LargeUtf8 => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .unwrap();
            format_string_value(array.value(row_idx), field)
        }
        DataType::Utf8View => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::StringViewArray>()
                .unwrap();
            format_string_value(array.value(row_idx), field)
        }
        DataType::Int8 => {
            let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Int16 => {
            let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt8 => {
            let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt16 => {
            let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt32 => {
            let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt64 => {
            let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Float32 => {
            let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Boolean => {
            let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::FixedSizeList(inner_field, size) => {
            // Handle fixed size vectors (e.g., embeddings)
            let list_array = column
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            let values = list_array.value(row_idx);

            // Format as [value1, value2, ...] for short vectors, with size info for long ones
            match inner_field.data_type() {
                DataType::Float32 => {
                    let float_array = values.as_any().downcast_ref::<Float32Array>().unwrap();
                    if float_array.len() > 10 {
                        let first = float_array.value(0);
                        let second = float_array.value(1);
                        format!("[{}, {}, ...{} more]", first, second, float_array.len() - 2)
                    } else {
                        // Show all values if 10 or fewer
                        let values: Vec<String> = (0..float_array.len())
                            .map(|i| float_array.value(i).to_string())
                            .collect();
                        format!("[{}]", values.join(", "))
                    }
                }
                DataType::Float64 => {
                    let float_array = values.as_any().downcast_ref::<Float64Array>().unwrap();
                    if float_array.len() > 10 {
                        let first = float_array.value(0);
                        let second = float_array.value(1);
                        format!("[{}, {}, ...{} more]", first, second, float_array.len() - 2)
                    } else {
                        // Show all values if 10 or fewer
                        let values: Vec<String> = (0..float_array.len())
                            .map(|i| float_array.value(i).to_string())
                            .collect();
                        format!("[{}]", values.join(", "))
                    }
                }
                _ => {
                    format!("[{} items]", *size)
                }
            }
        }
        DataType::List(inner_field) => {
            // Handle variable size lists
            let list_array = column.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list_array.value(row_idx);
            let size = values.len();

            // Format as [value1, value2, ...] for short vectors, truncated for long ones
            match inner_field.data_type() {
                DataType::Float32 => {
                    let float_array = values.as_any().downcast_ref::<Float32Array>().unwrap();
                    if float_array.len() > 10 {
                        let first = float_array.value(0);
                        let second = float_array.value(1);
                        format!("[{}, {}, ...{} more]", first, second, float_array.len() - 2)
                    } else {
                        // Show all values if 10 or fewer
                        let values: Vec<String> = (0..float_array.len())
                            .map(|i| float_array.value(i).to_string())
                            .collect();
                        format!("[{}]", values.join(", "))
                    }
                }
                DataType::Float64 => {
                    let float_array = values.as_any().downcast_ref::<Float64Array>().unwrap();
                    if float_array.len() > 10 {
                        let first = float_array.value(0);
                        let second = float_array.value(1);
                        format!("[{}, {}, ...{} more]", first, second, float_array.len() - 2)
                    } else {
                        // Show all values if 10 or fewer
                        let values: Vec<String> = (0..float_array.len())
                            .map(|i| float_array.value(i).to_string())
                            .collect();
                        format!("[{}]", values.join(", "))
                    }
                }
                DataType::Int32 => {
                    let int_array = values.as_any().downcast_ref::<Int32Array>().unwrap();
                    if int_array.len() > 10 {
                        let first = int_array.value(0);
                        let second = int_array.value(1);
                        format!("[{}, {}, ...{} more]", first, second, int_array.len() - 2)
                    } else {
                        let values: Vec<String> = (0..int_array.len())
                            .map(|i| int_array.value(i).to_string())
                            .collect();
                        format!("[{}]", values.join(", "))
                    }
                }
                DataType::Int64 => {
                    let int_array = values.as_any().downcast_ref::<Int64Array>().unwrap();
                    if int_array.len() > 10 {
                        let first = int_array.value(0);
                        let second = int_array.value(1);
                        format!("[{}, {}, ...{} more]", first, second, int_array.len() - 2)
                    } else {
                        let values: Vec<String> = (0..int_array.len())
                            .map(|i| int_array.value(i).to_string())
                            .collect();
                        format!("[{}]", values.join(", "))
                    }
                }
                _ => {
                    format!("[{} items]", size)
                }
            }
        }
        DataType::Binary => {
            let array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = array.value(row_idx);
            if bytes.len() > 32 {
                // Show first few bytes with ellipsis for large binary data
                let preview: Vec<String> = bytes[..8].iter().map(|b| format!("{}", b)).collect();
                format!(
                    "[{}, ...{} more bytes]",
                    preview.join(", "),
                    bytes.len() - 8
                )
            } else {
                // Show all bytes if small
                let values: Vec<String> = bytes.iter().map(|b| format!("{}", b)).collect();
                format!("[{}]", values.join(", "))
            }
        }
        DataType::LargeBinary => {
            let array = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let bytes = array.value(row_idx);
            if bytes.len() > 32 {
                // Show first few bytes with ellipsis for large binary data
                let preview: Vec<String> = bytes[..8].iter().map(|b| format!("{}", b)).collect();
                format!(
                    "[{}, ...{} more bytes]",
                    preview.join(", "),
                    bytes.len() - 8
                )
            } else {
                // Show all bytes if small
                let values: Vec<String> = bytes.iter().map(|b| format!("{}", b)).collect();
                format!("[{}]", values.join(", "))
            }
        }
        DataType::Struct(fields) => {
            let struct_array = column
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .unwrap();
            let mut field_strs = Vec::new();

            for (i, field) in fields.iter().enumerate() {
                let field_column = struct_array.column(i);
                let field_value = format_arrow_value(field_column, row_idx, field);
                field_strs.push(format!("{}: {}", field.name(), field_value));
            }

            format!("{{{}}}", field_strs.join(", "))
        }
        _ => arrow_cast::display::array_value_to_string(column, row_idx)
            .unwrap_or_else(|_| format!("{:?}", column.slice(row_idx, 1))),
    }
}

/// Format arrow value as JSON
/// Convert batches to JSON
/// Format data type for display
fn format_data_type(data_type: &arrow_schema::DataType) -> String {
    use arrow_schema::DataType;

    match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "TINYINT UNSIGNED".to_string(),
        DataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
        DataType::UInt32 => "INT UNSIGNED".to_string(),
        DataType::UInt64 => "BIGINT UNSIGNED".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 => "TEXT".to_string(),
        DataType::LargeUtf8 => "TEXT".to_string(),
        DataType::Binary => "BLOB".to_string(),
        DataType::LargeBinary => "BLOB".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_unit, tz) => {
            let tz_str = tz.as_ref().map(|s| format!(" ({})", s)).unwrap_or_default();
            format!("TIMESTAMP{}", tz_str)
        }
        DataType::Time32(_) => "TIME".to_string(),
        DataType::Time64(_) => "TIME".to_string(),
        DataType::Decimal128(precision, scale) => format!("DECIMAL({}, {})", precision, scale),
        DataType::Decimal256(precision, scale) => format!("DECIMAL({}, {})", precision, scale),
        DataType::List(field) => format!("LIST<{}>", format_data_type(field.data_type())),
        DataType::FixedSizeList(field, size) => {
            format!("{}[{}]", format_data_type(field.data_type()), size)
        }
        DataType::Struct(fields) => {
            let field_types: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name(), format_data_type(f.data_type())))
                .collect();
            format!("STRUCT<{}>", field_types.join(", "))
        }
        _ => format!("{:?}", data_type),
    }
}

/// Format bytes to human readable
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    if bytes == 0 {
        return "0 B".to_string();
    }

    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

/// Convert schema to JSON
fn schema_to_json(schema: &Schema) -> serde_json::Value {
    use serde_json::json;

    let fields: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|field| {
            json!({
                "name": field.name(),
                "type": format_data_type(field.data_type()),
                "nullable": field.is_nullable(),
            })
        })
        .collect();

    json!(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::IndexStats;
    use crate::dml_result::{DmlOperation, DmlResult};
    use arrow_array::{
        ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Int32Array, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
            Field::new("active", DataType::Boolean, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let score_array = Float32Array::from(vec![Some(95.5), None, Some(87.3)]);
        let active_array = BooleanArray::from(vec![true, false, true]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array) as ArrayRef,
                Arc::new(name_array) as ArrayRef,
                Arc::new(score_array) as ArrayRef,
                Arc::new(active_array) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn create_vector_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    1024,
                ),
                false,
            ),
        ]));

        let id_array = Int32Array::from(vec![1, 2]);

        // Create vectors for 2 rows, each with 1024 elements
        let mut vector_data = vec![];
        for row in 0..2 {
            for i in 0..1024 {
                vector_data.push((row * 1024 + i) as f32 * 0.001);
            }
        }
        let values = Float32Array::from(vector_data);
        let list_array = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            1024,
            Arc::new(values),
            None,
        );

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array) as ArrayRef,
                Arc::new(list_array) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_table_format() {
        let formatter = ResultFormatter::new(OutputFormat::Table);
        let batch = create_test_batch();
        let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));

        let output = formatter.format(&result).unwrap();

        let expected = concat!(
            " id | name    | score | active\n",
            "----+---------+-------+--------\n",
            "  1 | Alice   |  95.5 | true  \n",
            "  2 | Bob     |  NULL | false \n",
            "  3 | Charlie |  87.3 | true  \n",
            "(3 rows)\n",
        );

        assert_eq!(output, expected);
    }

    #[test]
    fn test_csv_format() {
        let formatter = ResultFormatter::new(OutputFormat::Csv);
        let batch = create_test_batch();
        let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));

        let output = formatter.format(&result).unwrap();

        // Check CSV header
        assert!(output.starts_with("id,name,score,active\n"));

        // Check data rows
        assert!(output.contains("1,Alice,95.5,true"));
        assert!(output.contains("2,Bob,\\N,false"));
        assert!(output.contains("3,Charlie,87.3,true"));
    }

    #[test]
    fn test_json_format() {
        let formatter = ResultFormatter::new(OutputFormat::Json);
        let batch = create_test_batch();
        let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));

        let output = formatter.format(&result).unwrap();
        let json: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert!(json.is_array());
        let array = json.as_array().unwrap();
        assert_eq!(array.len(), 3);

        // Check first row
        assert_eq!(array[0]["id"], 1);
        assert_eq!(array[0]["name"], "Alice");
        assert_eq!(array[0]["score"], 95.5);
        assert_eq!(array[0]["active"], true);

        // Check NULL handling
        assert_eq!(array[1]["score"], serde_json::Value::Null);
    }

    /// A timestamp renders as its value, not the `Debug` dump of a one-row array.
    #[test]
    fn test_table_renders_timestamp_value() {
        let ts = arrow_array::TimestampMillisecondArray::from(vec![1_758_217_603_403])
            .with_timezone("UTC");
        let batch =
            RecordBatch::try_from_iter(vec![("last_refresh", Arc::new(ts) as ArrayRef)]).unwrap();

        let output = ResultFormatter::new(OutputFormat::Table)
            .format(&QueryResult::SqlResult(SqlResult::Query(vec![batch])))
            .unwrap();

        assert_eq!(
            output,
            " last_refresh            \n--------------------------\n 2025-09-18T17:46:43.403Z\n(1 row)\n"
        );
    }

    /// Arrow writes the exact decimal token; re-reading it through a
    /// `serde_json::Value` would round it to the nearest double.
    #[test]
    fn test_json_preserves_exact_decimal_tokens() {
        let amounts =
            arrow_array::Decimal128Array::from(vec![123_456_789_012_345_678_901_234_567_890_i128])
                .with_precision_and_scale(30, 10)
                .unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "amount",
            Arc::new(amounts) as arrow_array::ArrayRef,
        )])
        .unwrap();

        let output = ResultFormatter::new(OutputFormat::Json)
            .format(&QueryResult::SqlResult(SqlResult::Query(vec![batch])))
            .unwrap();

        assert!(
            output.contains("12345678901234567890.1234567890"),
            "decimal token was rewritten: {output}"
        );
    }

    /// The display path elides a long value and previews a list; a machine
    /// format has to carry both whole.
    #[test]
    fn test_json_is_lossless_for_long_and_nested_values() {
        let long = "x".repeat(80);
        let listed =
            arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
                Some(vec![Some(1), Some(2), Some(3)]),
            ]);
        let batch = RecordBatch::try_from_iter(vec![
            (
                "note",
                Arc::new(arrow_array::LargeStringArray::from(vec![long.as_str()]))
                    as arrow_array::ArrayRef,
            ),
            ("scores", Arc::new(listed) as arrow_array::ArrayRef),
        ])
        .unwrap();

        let output = ResultFormatter::new(OutputFormat::Json)
            .format(&QueryResult::SqlResult(SqlResult::Query(vec![batch])))
            .unwrap();
        let json: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert_eq!(json[0]["note"], long);
        assert_eq!(json[0]["scores"], serde_json::json!([1, 2, 3]));
    }

    /// A narrow integer used to fall through to a debug dump of the sliced
    /// array; `SHOW ERRORS` returns `row_offset` as one.
    #[test]
    fn test_json_renders_narrow_integers_as_numbers() {
        let batch = arrow_array::record_batch!(
            ("row_offset", UInt32, [1_u32, 2]),
            ("attempt", UInt8, [3_u8, 4])
        )
        .unwrap();
        let output = ResultFormatter::new(OutputFormat::Json)
            .format(&QueryResult::SqlResult(SqlResult::Query(vec![batch])))
            .unwrap();
        let json: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert_eq!(json[0]["row_offset"], 1);
        assert_eq!(json[0]["attempt"], 3);
        assert_eq!(json[1]["row_offset"], 2);
    }

    #[test]
    fn test_ndjson_format() {
        let formatter = ResultFormatter::new(OutputFormat::Ndjson);
        let batch = create_test_batch();
        let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));

        let output = formatter.format(&result).unwrap();
        let lines: Vec<&str> = output.trim().split('\n').collect();

        assert_eq!(lines.len(), 3);

        // Parse each line as JSON
        for line in lines.iter() {
            let json: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(json.is_object());
            assert!(json["id"].is_number());
            assert!(json["name"].is_string());
        }
    }

    #[test]
    fn test_vector_formatting() {
        let formatter = ResultFormatter::new(OutputFormat::Table);
        let batch = create_vector_batch();
        let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));

        let output = formatter.format(&result).unwrap();

        // Check that vectors are abbreviated
        assert!(output.contains("[0, 0.001, ...1022 more]"));
        assert!(!output.contains("0.999")); // Should not show all values
    }

    #[test]
    fn test_empty_result() {
        let formatter = ResultFormatter::new(OutputFormat::Table);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::new_empty(schema);
        let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));

        let output = formatter.format(&result).unwrap();

        assert!(output.contains("(0 rows)"));
    }

    #[test]
    fn test_table_list_formatting() {
        let formatter = ResultFormatter::new(OutputFormat::Table);
        let tables = vec![
            "users".to_string(),
            "products".to_string(),
            "orders".to_string(),
        ];
        let result = QueryResult::TableList(tables);

        let output = formatter.format(&result).unwrap();

        let expected = "List of relations
 Tables  
----------
 users   
 products
 orders  
(3 rows)
";

        assert_eq!(output, expected);
    }

    #[test]
    fn test_table_list_detailed_formatting() {
        let formatter = ResultFormatter::new(OutputFormat::Table).with_terminal_width(200);
        let tables = vec![
            TableInfo {
                name: "users".to_string(),
                schema: Arc::new(Schema::empty()),
                row_count: Some(1000),
                size_bytes: Some(1024 * 1024), // 1 MB
                num_indices: Some(2),
                num_fragments: Some(1),
            },
            TableInfo {
                name: "products".to_string(),
                schema: Arc::new(Schema::empty()),
                row_count: Some(500),
                size_bytes: Some(512 * 1024), // 512 KB
                num_indices: None,
                num_fragments: None,
            },
        ];
        let result = QueryResult::TableListDetailed(tables);

        let output = formatter.format(&result).unwrap();

        let expected = "                    List of relations
 Name     | Type  | Rows | Size     | Indices | Fragments
----------+-------+------+----------+---------+-----------
 users    | table | 1000 | 1.0 MB   |       2 |         1
 products | table |  500 | 512.0 KB |         |          
(2 rows)
";

        assert_eq!(output, expected);
    }

    #[test]
    fn test_index_list_formatting() {
        let formatter = ResultFormatter::new(OutputFormat::Table);
        let indices = vec![
            IndexInfo {
                name: "idx_users_email".to_string(),
                table_name: "users".to_string(),
                index_type: "IVF_PQ".to_string(),
                columns: vec!["email".to_string()],
            },
            IndexInfo {
                name: "idx_products_name".to_string(),
                table_name: "products".to_string(),
                index_type: "BTREE".to_string(),
                columns: vec!["name".to_string()],
            },
        ];
        let result = QueryResult::IndexList(indices);

        let output = formatter.format(&result).unwrap();

        let expected = "            List of indices
 Name              | Type   | Table   
-------------------+--------+----------
 idx_users_email   | IVF_PQ | users   
 idx_products_name | BTREE  | products
(2 rows)
";

        assert_eq!(output, expected);
    }

    #[test]
    fn test_csv_escaping() {
        let formatter = ResultFormatter::new(OutputFormat::Csv);
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));

        let array = StringArray::from(vec!["simple", "with,comma", "with\"quote", "with\nnewline"]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).unwrap();

        let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));
        let output = formatter.format(&result).unwrap();

        assert!(output.contains("simple"));
        assert!(output.contains("\"with,comma\""));
        assert!(output.contains("\"with\"\"quote\""));
        assert!(output.contains("\"with\nnewline\""));
    }

    #[test]
    fn test_csv_preserves_scalar_values() {
        let long = "µ".repeat(300);
        let binary = vec![0xab; 100];
        let batch = RecordBatch::try_from_iter(vec![
            (
                "long",
                Arc::new(StringArray::from(vec![long.as_str()])) as ArrayRef,
            ),
            (
                "large",
                Arc::new(arrow_array::LargeStringArray::from(vec![long.as_str()])),
            ),
            (
                "view",
                Arc::new(arrow_array::StringViewArray::from(vec![long.as_str()])),
            ),
            ("small", Arc::new(Float64Array::from(vec![0.000123]))),
            ("f32", Arc::new(Float32Array::from(vec![0.00693]))),
            (
                "f64",
                Arc::new(Float64Array::from(vec![1.2345678901234567])),
            ),
            ("literal_null", Arc::new(StringArray::from(vec!["NULL"]))),
            ("null", Arc::new(StringArray::from(vec![None::<&str>]))),
            ("empty", Arc::new(StringArray::from(vec![""]))),
            (
                "binary",
                Arc::new(BinaryArray::from(vec![binary.as_slice()])),
            ),
            (
                "decimal",
                Arc::new(
                    arrow_array::Decimal128Array::from(vec![123456789012345678901234567890_i128])
                        .with_precision_and_scale(30, 10)
                        .unwrap(),
                ),
            ),
        ])
        .unwrap();
        let mut output = Vec::new();
        ResultFormatter::new(OutputFormat::Csv)
            .format_record_batches(&[batch], &mut output)
            .unwrap();

        let row = csv::Reader::from_reader(output.as_slice())
            .records()
            .next()
            .unwrap()
            .unwrap();
        for index in 0..3 {
            assert_eq!(&row[index], long);
        }
        assert_eq!(row[3].parse::<f64>().unwrap(), 0.000123);
        assert_eq!(
            row[4].parse::<f32>().unwrap().to_bits(),
            0.00693_f32.to_bits()
        );
        assert_eq!(
            row[5].parse::<f64>().unwrap().to_bits(),
            1.2345678901234567_f64.to_bits()
        );
        assert_eq!(&row[6], "NULL");
        assert_eq!(&row[7], CSV_NULL_VALUE);
        assert_eq!(&row[8], "");
        assert!(String::from_utf8(output)
            .unwrap()
            .contains(",NULL,\\N,\"\","));
        assert_eq!(&row[9], "ab".repeat(100));
        assert_eq!(&row[10], "12345678901234567890.1234567890");
    }

    #[test]
    fn test_csv_preserves_nested_values() {
        use arrow_array::builder::{ListBuilder, StringBuilder};
        use arrow_array::types::{Float16Type, Float32Type, UInt8Type};
        use arrow_array::Array;

        let mut tags = ListBuilder::new(StringBuilder::new());
        for value in [
            Some("a,b"),
            Some("\"quoted\""),
            Some("NULL"),
            Some(""),
            None,
        ] {
            tags.values().append_option(value);
        }
        tags.append(true);
        let tags = tags.finish();
        let vector = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            [Some(
                (0..12)
                    .map(|i| Some(i as f32 + 0.000123))
                    .collect::<Vec<_>>(),
            )],
            12,
        );
        let small = ListArray::from_iter_primitive::<Float16Type, _, _>([Some(vec![
            Some(half::f16::from_f32(0.00693)),
            None,
        ])]);
        let bytes = ListArray::from_iter_primitive::<UInt8Type, _, _>([Some(vec![
            Some(0),
            Some(255),
            None,
        ])]);
        let multivector = ListArray::new(
            Arc::new(Field::new("item", vector.data_type().clone(), true)),
            arrow_buffer::OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(vector.clone()),
            None,
        );
        let special = ListArray::from_iter_primitive::<Float32Type, _, _>([Some(vec![
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            None,
        ])]);
        let nested = arrow_array::StructArray::from(vec![(
            Arc::new(Field::new("tags", tags.data_type().clone(), true)),
            {
                let mut values = ListBuilder::new(StringBuilder::new());
                values.values().append_value("NULL");
                values.values().append_null();
                values.append(true);
                Arc::new(values.finish()) as ArrayRef
            },
        )]);
        let batch = RecordBatch::try_from_iter(vec![
            ("tags", Arc::new(tags) as ArrayRef),
            ("vector", Arc::new(vector)),
            ("f16", Arc::new(small)),
            ("bytes", Arc::new(bytes)),
            ("multivector", Arc::new(multivector)),
            ("struct", Arc::new(nested)),
            ("special", Arc::new(special)),
        ])
        .unwrap();
        let mut output = Vec::new();
        ResultFormatter::new(OutputFormat::Csv)
            .format_record_batches(&[batch], &mut output)
            .unwrap();
        let row = csv::Reader::from_reader(output.as_slice())
            .records()
            .next()
            .unwrap()
            .unwrap();
        let values: Vec<serde_json::Value> = row
            .iter()
            .map(|cell| serde_json::from_str(cell).unwrap())
            .collect();
        assert_eq!(
            values[0],
            serde_json::json!(["a,b", "\"quoted\"", "NULL", "", null])
        );
        assert_eq!(values[1].as_array().unwrap().len(), 12);
        for (index, value) in values[1].as_array().unwrap().iter().enumerate() {
            assert_eq!(value.as_f64().unwrap() as f32, index as f32 + 0.000123);
        }
        assert_eq!(
            values[2][0].as_f64().unwrap() as f32,
            half::f16::from_f32(0.00693).to_f32()
        );
        assert!(values[2][1].is_null());
        assert_eq!(values[3], serde_json::json!([0, 255, null]));
        assert_eq!(values[4], serde_json::json!([values[1]]));
        assert_eq!(values[5], serde_json::json!({"tags": ["NULL", null]}));
        assert_eq!(values[6], serde_json::json!(["NaN", "inf", "-inf", null]));
    }

    #[test]
    fn test_csv_distinguishes_nulls_from_empty_values() {
        use arrow_array::types::{Float64Type, Int8Type};
        let dictionary = arrow_array::DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None]),
            Arc::new(StringArray::from(vec![None, Some("")])),
        )
        .unwrap();
        let lists = ListArray::from_iter_primitive::<Float64Type, _, _>([
            None,
            Some(vec![]),
            Some(vec![None]),
        ]);
        let batch = RecordBatch::try_from_iter(vec![
            ("null", Arc::new(arrow_array::NullArray::new(3)) as ArrayRef),
            ("dictionary", Arc::new(dictionary)),
            ("list", Arc::new(lists)),
        ])
        .unwrap();
        let mut output = Vec::new();
        ResultFormatter::new(OutputFormat::Csv)
            .format_record_batches(&[batch], &mut output)
            .unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "null,dictionary,list\n\\N,\\N,\\N\n\\N,\"\",[]\n\\N,\\N,[null]\n"
        );
    }

    #[test]
    fn test_csv_preserves_single_column_null_rows() {
        let original = vec![
            Some("before"),
            None,
            Some(""),
            Some("NULL"),
            Some(r"\N"),
            Some(r"\\N"),
            Some(r"\path"),
            Some("after"),
        ];
        let batch = RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(StringArray::from(original.clone())) as ArrayRef,
        )])
        .unwrap();
        let mut output = Vec::new();
        ResultFormatter::new(OutputFormat::Csv)
            .format_record_batches(&[batch], &mut output)
            .unwrap();
        let rows = csv::Reader::from_reader(output.as_slice())
            .records()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 8);
        let values: Vec<_> = rows.iter().map(|row| &row[0]).collect();
        assert_eq!(
            values,
            ["before", r"\N", "", "NULL", r"\\N", r"\\\N", r"\\path", "after"]
        );
        let decoded: Vec<_> = values
            .iter()
            .map(|&value| {
                if value == CSV_NULL_VALUE {
                    None
                } else if value.starts_with(r"\\") {
                    Some(&value[1..])
                } else {
                    Some(value)
                }
            })
            .collect();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_csv_preserves_nested_dictionary_nulls() {
        use arrow_array::types::Int8Type;
        use arrow_array::{Array, DictionaryArray, StructArray};

        let dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None]),
            Arc::new(StringArray::from(vec![None, Some("")])),
        )
        .unwrap();
        let field = Arc::new(Field::new("item", dictionary.data_type().clone(), true));
        let list = ListArray::new(
            field.clone(),
            arrow_buffer::OffsetBuffer::new(vec![0, 3].into()),
            Arc::new(dictionary.clone()),
            None,
        );
        let structs = StructArray::from(vec![(field, Arc::new(dictionary) as ArrayRef)]);
        let struct_list = ListArray::new(
            Arc::new(Field::new("item", structs.data_type().clone(), true)),
            arrow_buffer::OffsetBuffer::new(vec![0, 3].into()),
            Arc::new(structs),
            None,
        );
        let encoded_list =
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), Arc::new(list.clone()))
                .unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            ("list", Arc::new(list) as ArrayRef),
            ("structs", Arc::new(struct_list)),
            ("encoded_list", Arc::new(encoded_list)),
        ])
        .unwrap();
        let mut output = Vec::new();
        ResultFormatter::new(OutputFormat::Csv)
            .format_record_batches(&[batch], &mut output)
            .unwrap();
        let row = csv::Reader::from_reader(output.as_slice())
            .records()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(&row[0], r#"[null,"",null]"#);
        assert_eq!(&row[1], r#"[{"item":null},{"item":""},{"item":null}]"#);
        assert_eq!(&row[2], r#"[null,"",null]"#);
    }

    #[test]
    fn test_csv_escapes_headers_and_carriage_returns_across_batches() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "with,comma",
                Arc::new(StringArray::from(vec!["with\rcarriage return"])) as ArrayRef,
            ),
            (
                "with\"quote",
                Arc::new(StringArray::from(vec!["with\"quote"])),
            ),
            (
                "with\nnewline",
                Arc::new(StringArray::from(vec!["with\nnewline"])),
            ),
        ])
        .unwrap();
        let mut output = Vec::new();
        ResultFormatter::new(OutputFormat::Csv)
            .format_record_batches(&[batch.slice(0, 0), batch.clone(), batch], &mut output)
            .unwrap();
        let mut reader = csv::Reader::from_reader(output.as_slice());
        assert_eq!(
            reader.headers().unwrap(),
            &csv::StringRecord::from(vec!["with,comma", "with\"quote", "with\nnewline"])
        );
        let rows = reader
            .records()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 2);
        for row in rows {
            assert_eq!(
                row,
                csv::StringRecord::from(vec![
                    "with\rcarriage return",
                    "with\"quote",
                    "with\nnewline"
                ])
            );
        }
    }

    #[test]
    fn test_table_preserves_small_float_precision() {
        let batch = arrow_array::record_batch!(("small", Float64, [0.000123, 0.00693])).unwrap();
        let mut output = Vec::new();
        ResultFormatter::new(OutputFormat::Table)
            .with_terminal_width(200)
            .format_record_batches(&[batch], &mut output)
            .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("0.000123"));
        assert!(output.contains("0.00693"));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
    }

    #[test]
    fn test_format_data_type() {
        assert_eq!(format_data_type(&DataType::Boolean), "BOOLEAN");
        assert_eq!(format_data_type(&DataType::Int32), "INT");
        assert_eq!(format_data_type(&DataType::Float64), "DOUBLE");
        assert_eq!(format_data_type(&DataType::Utf8), "TEXT");
        assert_eq!(
            format_data_type(&DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                128
            )),
            "FLOAT[128]"
        );
    }

    #[test]
    fn test_sql_result_dml_insert() {
        let formatter = ResultFormatter::new(OutputFormat::Table);
        let dml = DmlResult::new("foo", DmlOperation::Insert, 1, 3);
        let result = QueryResult::SqlResult(SqlResult::Dml(dml));

        let output = formatter.format(&result).unwrap();
        assert_eq!(output, "INSERT 1 row(s), version 3\n");
    }

    #[test]
    fn test_sql_result_dml_delete() {
        let formatter = ResultFormatter::new(OutputFormat::Table);
        let dml = DmlResult::new("bar", DmlOperation::Delete, 0, 4);
        let result = QueryResult::SqlResult(SqlResult::Dml(dml));

        let output = formatter.format(&result).unwrap();
        assert_eq!(output, "DELETE 0 row(s), version 4\n");
    }

    #[test]
    fn test_sql_result_success() {
        let formatter = ResultFormatter::new(OutputFormat::Table);
        let result = QueryResult::SqlResult(SqlResult::Success);

        let output = formatter.format(&result).unwrap();
        assert_eq!(output, "Success\n");
    }

    #[test]
    fn test_table_info_formatting() {
        let formatter = ResultFormatter::new(OutputFormat::Table);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("email", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("score", DataType::Float64, true),
        ]));

        let table_info = TableInfo {
            name: "users".to_string(),
            schema,
            row_count: Some(1500),
            size_bytes: Some(2 * 1024 * 1024), // 2 MB
            num_indices: Some(3),
            num_fragments: Some(2),
        };

        let result = QueryResult::TableInfo(table_info);
        let output = formatter.format(&result).unwrap();

        let expected = "                    Table \"users\"
 Column     | Type      | Nullable
------------+-----------+----------
 id         | INT       |         
 email      | TEXT      |         
 created_at | TIMESTAMP |         
 score      | DOUBLE    | YES     

Rows: 1500
Size: 2.0 MB
";

        assert_eq!(output, expected);
    }

    #[test]
    fn test_data_type_formatting() {
        let formatter = ResultFormatter::new(OutputFormat::Table);

        struct TestCase {
            name: &'static str,
            field: Field,
            data: ArrayRef,
            expected_values: Vec<&'static str>,
        }

        let test_cases = vec![
            TestCase {
                name: "timestamp",
                field: Field::new(
                    "ts",
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                    false,
                ),
                data: Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
                    1609459200000000, // 2021-01-01 00:00:00 UTC
                    1625097600000000, // 2021-07-01 00:00:00 UTC
                ])) as ArrayRef,
                expected_values: vec!["2021-01-01", "2021-07-01"],
            },
            TestCase {
                name: "date",
                field: Field::new("date", DataType::Date64, false),
                data: Arc::new(arrow_array::Date64Array::from(vec![
                    1622505600000, // 2021-06-01
                    1622592000000, // 2021-06-02
                ])) as ArrayRef,
                expected_values: vec!["2021-06-01", "2021-06-02"],
            },
            TestCase {
                name: "largeutf8",
                field: Field::new("text", DataType::LargeUtf8, false),
                data: Arc::new(arrow_array::LargeStringArray::from(vec![
                    "Hello, World!",
                    "Large UTF-8 String",
                ])) as ArrayRef,
                expected_values: vec!["Hello, World!", "Large UTF-8 String"],
            },
            TestCase {
                name: "utf8view",
                field: Field::new("view", DataType::Utf8View, false),
                data: Arc::new(arrow_array::StringViewArray::from(vec![
                    "View String 1",
                    "View String 2",
                ])) as ArrayRef,
                expected_values: vec!["View String 1", "View String 2"],
            },
            TestCase {
                name: "struct",
                field: Field::new(
                    "data",
                    DataType::Struct(
                        vec![
                            Field::new("name", DataType::Utf8, false),
                            Field::new("age", DataType::Int32, false),
                        ]
                        .into(),
                    ),
                    false,
                ),
                data: Arc::new(arrow_array::StructArray::from(vec![
                    (
                        Arc::new(Field::new("name", DataType::Utf8, false)),
                        Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("age", DataType::Int32, false)),
                        Arc::new(Int32Array::from(vec![30, 25])) as ArrayRef,
                    ),
                ])) as ArrayRef,
                expected_values: vec!["{name: Alice, age: 30}", "{name: Bob, age: 25}"],
            },
        ];

        for test_case in test_cases {
            let schema = Arc::new(Schema::new(vec![test_case.field]));
            let batch = RecordBatch::try_new(schema, vec![test_case.data]).unwrap();
            let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));
            let output = formatter.format(&result).unwrap();

            for expected in test_case.expected_values {
                assert!(
                    output.contains(expected),
                    "Test case '{}' failed: expected output to contain '{}', but got:\n{}",
                    test_case.name,
                    expected,
                    output
                );
            }
        }
    }

    #[test]
    fn marked_columns_render_in_full() {
        let formatter = ResultFormatter::new(OutputFormat::Table).with_terminal_width(500);
        let long_string = format!("j1_{}", "a".repeat(300));

        let schema = Arc::new(Schema::new(vec![
            Field::new("job_id", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![long_string.as_str()])) as ArrayRef,
                Arc::new(StringArray::from(vec![long_string.as_str()])) as ArrayRef,
            ],
        )
        .unwrap();

        let marked = mark_no_truncate(SqlResult::Query(vec![batch]), &["job_id"]);
        let output = formatter.format(&QueryResult::SqlResult(marked)).unwrap();

        assert!(
            output.contains(&long_string),
            "a marked column must render in full: {output}"
        );
        assert!(
            output.contains("...(303 chars)"),
            "an unmarked column must still be abbreviated: {output}"
        );
    }

    fn job_ids(result: &SqlResult) -> Vec<i32> {
        let SqlResult::Query(batches) = result else {
            panic!("expected a query result");
        };
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[test]
    fn taking_rows_cuts_across_batch_boundaries() {
        let first = arrow_array::record_batch!(("job_id", Int32, [1, 2, 3])).unwrap();
        let second = arrow_array::record_batch!(("job_id", Int32, [4, 5, 6])).unwrap();
        let batches = vec![first, second];

        assert_eq!(row_count(&SqlResult::Query(batches.clone())), 6);

        // A cut inside the second batch keeps the first whole and slices the
        // second; a limit at or past the total is a no-op.
        let capped = take_rows(SqlResult::Query(batches.clone()), 4);
        assert_eq!(job_ids(&capped), vec![1, 2, 3, 4]);
        assert_eq!(
            job_ids(&take_rows(SqlResult::Query(batches.clone()), 6)),
            vec![1, 2, 3, 4, 5, 6]
        );
        assert_eq!(
            job_ids(&take_rows(SqlResult::Query(batches), 99)),
            vec![1, 2, 3, 4, 5, 6]
        );
    }

    #[test]
    fn taking_rows_leaves_non_query_results_alone() {
        assert!(matches!(
            take_rows(SqlResult::Success, 0),
            SqlResult::Success
        ));
        assert_eq!(row_count(&SqlResult::Success), 0);
    }

    #[test]
    fn test_string_elision() {
        let formatter = ResultFormatter::new(OutputFormat::Table);

        // Create a long string that should be elided
        let long_string = "a".repeat(100);
        let short_string = "short string";

        let schema = Arc::new(Schema::new(vec![
            Field::new("short", DataType::Utf8, false),
            Field::new("long", DataType::Utf8, false),
            Field::new("long_large", DataType::LargeUtf8, false),
        ]));

        let short_array = StringArray::from(vec![short_string]);
        let long_array = StringArray::from(vec![long_string.as_str()]);
        let long_large_array = arrow_array::LargeStringArray::from(vec![long_string.as_str()]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(short_array) as ArrayRef,
                Arc::new(long_array) as ArrayRef,
                Arc::new(long_large_array) as ArrayRef,
            ],
        )
        .unwrap();

        let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));
        let output = formatter.format(&result).unwrap();

        // Short string should appear in full
        assert!(output.contains(short_string));

        // Long strings should be truncated with char count
        assert!(output.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...(100 chars)"));
        assert!(!output.contains(&long_string)); // Full string should not appear
    }

    #[test]
    fn test_string_truncation_with_multibyte_utf8() {
        let formatter = ResultFormatter::new(OutputFormat::Table);

        // The en-dash '–' is 3 bytes in UTF-8. Place it so byte index 50 falls
        // inside the character, which would panic with naive &value[..50].
        // 48 ASCII bytes + '–' (3 bytes) + padding = well over 50 bytes
        let multibyte_string = format!("{}{}", "x".repeat(48), "–".repeat(20));

        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("text_large", DataType::LargeUtf8, false),
            Field::new("text_view", DataType::Utf8View, false),
        ]));

        let utf8_array = StringArray::from(vec![multibyte_string.as_str()]);
        let large_array = arrow_array::LargeStringArray::from(vec![multibyte_string.as_str()]);
        let view_array = arrow_array::StringViewArray::from(vec![multibyte_string.as_str()]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(utf8_array) as ArrayRef,
                Arc::new(large_array) as ArrayRef,
                Arc::new(view_array) as ArrayRef,
            ],
        )
        .unwrap();

        let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));
        let output = formatter.format(&result).unwrap();

        // Should not panic, and should contain truncation indicator
        // 48 ASCII chars + 20 en-dashes = 68 chars total
        assert!(output.contains("...(68 chars)"));
        // The truncated prefix should end at a char boundary (48 x's, not mid en-dash)
        assert!(output.contains(&"x".repeat(48)));
    }

    #[test]
    fn test_struct_formatting() {
        let formatter = ResultFormatter::new(OutputFormat::Table).with_terminal_width(200);

        // Create a struct with multiple fields
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "user_info",
                DataType::Struct(
                    vec![
                        Field::new("name", DataType::Utf8, false),
                        Field::new("age", DataType::Int32, false),
                        Field::new("email", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            ),
        ]));

        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["Alice", "Bob"]);
        let age_array = Int32Array::from(vec![30, 25]);
        let email_array = StringArray::from(vec![Some("alice@example.com"), None]);

        let struct_array = arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(name_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, false)),
                Arc::new(age_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("email", DataType::Utf8, true)),
                Arc::new(email_array) as ArrayRef,
            ),
        ]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array) as ArrayRef,
                Arc::new(struct_array) as ArrayRef,
            ],
        )
        .unwrap();

        let result = QueryResult::SqlResult(SqlResult::Query(vec![batch]));
        let output = formatter.format(&result).unwrap();

        // Check the exact literal output for proper struct formatting
        let expected = " id | user_info                                       
----+--------------------------------------------------
  1 | {name: Alice, age: 30, email: alice@example.com}
  2 | {name: Bob, age: 25, email: NULL}               
(2 rows)
";

        assert_eq!(output, expected);
    }

    #[test]
    fn test_index_list_detailed_formatting() {
        // Pin a wide terminal so the table renders columnar ("hotdog") rather
        // than auto-expanding ("hamburger") in a narrow terminal — keeps the
        // golden output independent of the ambient terminal width.
        let formatter = ResultFormatter::new(OutputFormat::Table).with_terminal_width(200);
        let indices = vec![
            (
                IndexInfo {
                    name: "idx_embeddings".to_string(),
                    table_name: "documents".to_string(),
                    index_type: "IVF_PQ".to_string(),
                    columns: vec!["embedding".to_string()],
                },
                Some(IndexStats {
                    num_indexed_rows: 10000,
                    num_unindexed_rows: 500,
                    distance_type: Some("L2".to_string()),
                }),
            ),
            (
                IndexInfo {
                    name: "idx_created_at".to_string(),
                    table_name: "documents".to_string(),
                    index_type: "BTREE".to_string(),
                    columns: vec!["created_at".to_string()],
                },
                None,
            ),
        ];
        let result = QueryResult::IndexListDetailed(indices);

        let output = formatter.format(&result).unwrap();

        let expected = "                               List of relations
 Name           | Type   | Distance | Indexed Rows | Unindexed Rows | Table    
----------------+--------+----------+--------------+----------------+-----------
 idx_embeddings | IVF_PQ | L2       |        10000 |            500 | documents
 idx_created_at | BTREE  |          |              |                | documents
(2 rows)
";

        assert_eq!(output, expected);
    }
}
