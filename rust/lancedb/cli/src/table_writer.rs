// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/*
MIT License

Copyright (c) Foxglove Technologies Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

// Ported from
// https://github.com/foxglove/foxglove-cli/blob/main/foxglove/util/tablewriter/tablewriter.go

use std::io::{self, Write};
use std::process::Command;
use unicode_width::UnicodeWidthStr;

pub struct TableWriter;

impl TableWriter {
    pub fn print_table<W: Write>(
        writer: &mut W,
        headers: &[String],
        data: &[Vec<String>],
        term_width: Option<usize>,
    ) -> io::Result<()> {
        let term_width = term_width.unwrap_or_else(get_terminal_width);
        let table_width = compute_hotdog_table_width(headers, data);

        if term_width < table_width {
            Self::print_hamburger(writer, term_width, headers, data)
        } else {
            Self::print_hotdog(writer, headers, data)
        }
    }

    pub fn print_table_with_title<W: Write>(
        writer: &mut W,
        title: &str,
        headers: &[String],
        data: &[Vec<String>],
        term_width: Option<usize>,
    ) -> io::Result<()> {
        let term_width = term_width.unwrap_or_else(get_terminal_width);
        let table_width = compute_hotdog_table_width(headers, data);

        if term_width < table_width {
            // For hamburger mode, just print the title normally
            writeln!(writer, "{}", title)?;
            Self::print_hamburger(writer, term_width, headers, data)
        } else {
            // For hotdog mode, center the title over the table
            let title_padding = if table_width > title.width() {
                (table_width - title.width()) / 2
            } else {
                0
            };
            writeln!(writer, "{}{}", " ".repeat(title_padding), title)?;
            Self::print_hotdog(writer, headers, data)
        }
    }

    /// Prints table in horizontal "hotdog" format (PostgreSQL style)
    fn print_hotdog<W: Write>(
        writer: &mut W,
        headers: &[String],
        data: &[Vec<String>],
    ) -> io::Result<()> {
        let cell_widths = compute_hotdog_cell_widths(headers, data);
        let column_alignments = compute_column_alignments(data);

        // Write headers
        write!(writer, " ")?;
        for (i, header) in headers.iter().enumerate() {
            if i > 0 {
                write!(writer, " | ")?;
            }
            let padding = cell_widths[i] - header.width();
            write!(writer, "{}{}", header, " ".repeat(padding))?;
        }
        writeln!(writer)?;

        // Write separator with + at junctions
        write!(writer, "-")?; // Leading edge
        for (i, &width) in cell_widths.iter().enumerate() {
            write!(writer, "{}", "-".repeat(width))?;
            if i < cell_widths.len() - 1 {
                write!(writer, "-+-")?; // Junction between columns
            }
        }
        writeln!(writer, "-")?; // Trailing edge

        // Write data rows
        for row in data {
            write!(writer, " ")?;
            for (i, col) in row.iter().enumerate() {
                if i > 0 {
                    write!(writer, " | ")?;
                }
                let padding = cell_widths[i] - col.width();

                // Right-align numeric columns, left-align others
                if i < column_alignments.len() && column_alignments[i] {
                    write!(writer, "{}{}", " ".repeat(padding), col)?;
                } else {
                    write!(writer, "{}{}", col, " ".repeat(padding))?;
                }
            }
            writeln!(writer)?;
        }

        Ok(())
    }

    /// Prints table in vertical "hamburger" format (psql expanded display style)
    fn print_hamburger<W: Write>(
        writer: &mut W,
        term_width: usize,
        headers: &[String],
        data: &[Vec<String>],
    ) -> io::Result<()> {
        let mut max_header_width = 0;
        let mut max_record_width = 0;

        // Compute max header width
        for header in headers {
            if header.width() > max_header_width {
                max_header_width = header.width();
            }
        }

        // Compute max record width (cap at reasonable limit)
        const MAX_VALUE_DISPLAY_WIDTH: usize = 80; // Reasonable max for value display
        for row in data {
            for col in row {
                let col_width = col.width().min(MAX_VALUE_DISPLAY_WIDTH);
                if col_width > max_record_width {
                    max_record_width = col_width;
                }
            }
        }

        // Ensure sufficient room for record headers
        let longest_record_header = format!("-[ RECORD {} ]", data.len());
        if longest_record_header.len() > max_header_width {
            max_header_width = longest_record_header.len();
        }

        // Calculate right dash extent - aim for a reasonable width
        let desired_dash_extent = 60; // Reasonable default width for dashes
        let mut dashes_right_extent = desired_dash_extent.min(max_record_width + 15);
        let max_allowed_extent = term_width.saturating_sub(max_header_width + 3); // +3 for "+-" and space
        if dashes_right_extent > max_allowed_extent {
            dashes_right_extent = max_allowed_extent;
        }
        let right_dashes = "-".repeat(dashes_right_extent);

        // Print records
        for (i, row) in data.iter().enumerate() {
            let record_header = format!("-[ RECORD {} ]", i + 1);
            let header_line = format!(
                "{}{}+{}",
                record_header,
                "-".repeat(max_header_width - record_header.len()),
                right_dashes
            );
            writeln!(writer, "{}", header_line)?;

            for (j, col) in row.iter().enumerate() {
                writeln!(
                    writer,
                    "{}{}| {}",
                    headers[j],
                    " ".repeat(max_header_width - headers[j].width()),
                    col
                )?;
            }
        }

        Ok(())
    }
}

fn compute_hotdog_cell_widths(headers: &[String], data: &[Vec<String>]) -> Vec<usize> {
    let mut cell_widths = vec![0; headers.len()];

    // Initialize with header widths
    for (i, header) in headers.iter().enumerate() {
        cell_widths[i] = header.width();
    }

    // Check data column widths
    for row in data {
        for (i, col) in row.iter().enumerate() {
            if i < cell_widths.len() && cell_widths[i] < col.width() {
                cell_widths[i] = col.width();
            }
        }
    }

    cell_widths
}

fn compute_hotdog_table_width(headers: &[String], data: &[Vec<String>]) -> usize {
    let cell_widths = compute_hotdog_cell_widths(headers, data);
    // Account for " | " separators between columns (3 chars each) + leading space + trailing space
    let separator_width = if headers.len() > 1 {
        (headers.len() - 1) * 3
    } else {
        0
    };
    2 + cell_widths.iter().sum::<usize>() + separator_width
}

fn compute_column_alignments(data: &[Vec<String>]) -> Vec<bool> {
    if data.is_empty() {
        return vec![];
    }

    let num_cols = data[0].len();
    let mut is_numeric = vec![true; num_cols];

    // Check each column to see if all non-empty values are numeric
    for col_idx in 0..num_cols {
        for row in data {
            if col_idx < row.len() && !row[col_idx].is_empty() {
                let value = &row[col_idx];
                // Check if the value is numeric (integer or float)
                // Also handle NULL values
                if value != "NULL" && !is_numeric_value(value) {
                    is_numeric[col_idx] = false;
                    break;
                }
            }
        }
    }

    is_numeric
}

fn is_numeric_value(s: &str) -> bool {
    // Try to parse as integer or float
    s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok()
}

fn get_terminal_width() -> usize {
    if let Ok(output) = Command::new("stty").arg("size").output() {
        if let Ok(output_str) = String::from_utf8(output.stdout) {
            let parts: Vec<&str> = output_str.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(cols) = parts[1].parse::<usize>() {
                    return cols;
                }
            }
        }
    }

    // Fallback: try termsize crate
    if let Some(size) = termsize::get() {
        return size.cols as usize;
    }

    // Final fallback
    80
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_print_hotdog() {
        let mut output = Cursor::new(Vec::new());
        let headers = vec!["ID".to_string(), "Name".to_string(), "Age".to_string()];
        let data = vec![
            vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
            vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
        ];

        TableWriter::print_hotdog(&mut output, &headers, &data).unwrap();
        let result = String::from_utf8(output.into_inner()).unwrap();
        assert_eq!(result, expected_output());
    }

    #[test]
    fn test_unicode_table_padding_uses_terminal_columns() {
        let headers = vec!["µ".to_string(), "Type".to_string()];
        let data = vec![
            vec!["µ".to_string(), "TEXT".to_string()],
            vec!["漢".to_string(), "TEXT".to_string()],
            vec!["e\u{301}".to_string(), "TEXT".to_string()],
        ];
        let mut output = Vec::new();
        TableWriter::print_table(&mut output, &headers, &data, Some(11)).unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            " µ  | Type\n----+------\n µ  | TEXT\n 漢 | TEXT\n e\u{301}  | TEXT\n"
        );
    }

    fn expected_output() -> String {
        let mut output = String::new();
        output.push_str(" ID | Name    | Age\n");
        output.push_str("----+---------+-----\n");
        output.push_str("  1 | Alice   |  30\n");
        output.push_str("  2 | Bob     |  25\n");
        output.push_str("  3 | Charlie |  35\n");
        output
    }

    #[test]
    fn test_print_hamburger() {
        let mut output = Cursor::new(Vec::new());
        let headers = vec!["ID".to_string(), "Name".to_string(), "Age".to_string()];
        let data = vec![
            vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
            vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
        ];

        // Test with a narrow terminal width
        TableWriter::print_hamburger(&mut output, 80, &headers, &data).unwrap();
        let result = String::from_utf8(output.into_inner()).unwrap();
        assert_eq!(result, expected_hamburger_output());
    }

    fn expected_hamburger_output() -> String {
        let mut output = String::new();
        output.push_str("-[ RECORD 1 ]+----------------------\n");
        output.push_str("ID           | 1\n");
        output.push_str("Name         | Alice\n");
        output.push_str("Age          | 30\n");
        output.push_str("-[ RECORD 2 ]+----------------------\n");
        output.push_str("ID           | 2\n");
        output.push_str("Name         | Bob\n");
        output.push_str("Age          | 25\n");
        output.push_str("-[ RECORD 3 ]+----------------------\n");
        output.push_str("ID           | 3\n");
        output.push_str("Name         | Charlie\n");
        output.push_str("Age          | 35\n");
        output
    }

    #[test]
    fn test_print_table_with_title() {
        let mut output = Cursor::new(Vec::new());
        let headers = vec!["ID".to_string(), "Name".to_string()];
        let data = vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "Bob".to_string()],
        ];

        TableWriter::print_table_with_title(&mut output, "User List", &headers, &data, Some(200))
            .unwrap();
        let result = String::from_utf8(output.into_inner()).unwrap();

        // Should center the title and print hotdog format
        let expected = " User List\n ID | Name \n----+-------\n  1 | Alice\n  2 | Bob  \n";
        assert_eq!(result, expected);
    }
}
