// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Configuration for compression parameters
//!
//! This module provides types for configuring compression strategies
//! on a per-column or per-type basis using a parameter-driven approach.

use std::collections::HashMap;

use arrow_schema::DataType;

/// Byte stream split encoding mode
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BssMode {
    /// Never use BSS
    Off,
    /// Always use BSS for floating point data
    On,
    /// Automatically decide based on data characteristics
    Auto,
}

impl BssMode {
    /// Convert to internal sensitivity value
    pub fn to_sensitivity(&self) -> f32 {
        match self {
            Self::Off => 0.0,
            Self::On => 1.0,
            Self::Auto => 0.5, // Default sensitivity for auto mode
        }
    }

    /// Parse from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "off" => Some(Self::Off),
            "on" => Some(Self::On),
            "auto" => Some(Self::Auto),
            _ => None,
        }
    }
}

/// Compression parameter configuration
#[derive(Debug, Clone, PartialEq)]
pub struct CompressionParams {
    /// Column-level parameters: column name/pattern -> parameters
    pub columns: HashMap<String, CompressionFieldParams>,

    /// Type-level parameters: data type name -> parameters
    pub types: HashMap<String, CompressionFieldParams>,
}

/// Field-level compression parameters
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CompressionFieldParams {
    /// RLE threshold (0.0-1.0)
    /// When run_count < num_values * threshold, RLE will be used
    pub rle_threshold: Option<f64>,

    /// General compression scheme: "lz4", "zstd", "none"
    pub compression: Option<String>,

    /// Compression level (only for schemes that support it, e.g., zstd)
    pub compression_level: Option<i32>,

    /// Byte stream split mode for floating point data
    pub bss: Option<BssMode>,

    /// Minichunk size threshold for encoding
    pub minichunk_size: Option<i64>,
}

impl CompressionParams {
    /// Create empty compression parameters
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
            types: HashMap::new(),
        }
    }

    /// Get effective parameters for a field (merging type params and column params)
    pub fn get_field_params(
        &self,
        field_name: &str,
        data_type: &DataType,
    ) -> CompressionFieldParams {
        let mut params = CompressionFieldParams::default();

        // Apply type-level parameters
        let type_name = data_type.to_string();
        if let Some(type_params) = self.types.get(&type_name) {
            params.merge(type_params);
        }

        // Apply column-level parameters (highest priority)
        // First check exact match
        if let Some(col_params) = self.columns.get(field_name) {
            params.merge(col_params);
        } else {
            // Check pattern matching
            for (pattern, col_params) in &self.columns {
                if matches_pattern(field_name, pattern) {
                    params.merge(col_params);
                    break; // Use first matching pattern
                }
            }
        }

        params
    }
}

impl Default for CompressionParams {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionFieldParams {
    /// Merge another CompressionFieldParams, non-None values will override
    pub fn merge(&mut self, other: &Self) {
        if other.rle_threshold.is_some() {
            self.rle_threshold = other.rle_threshold;
        }
        if other.compression.is_some() {
            self.compression = other.compression.clone();
        }
        if other.compression_level.is_some() {
            self.compression_level = other.compression_level;
        }
        if other.bss.is_some() {
            self.bss = other.bss;
        }
        if other.minichunk_size.is_some() {
            self.minichunk_size = other.minichunk_size;
        }
    }
}

/// Check if a name matches a pattern (supports wildcards)
fn matches_pattern(name: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if let Some(prefix) = pattern.strip_suffix('*') {
        return name.starts_with(prefix);
    }

    if let Some(suffix) = pattern.strip_prefix('*') {
        return name.ends_with(suffix);
    }

    if pattern.contains('*') {
        // Simple glob pattern matching (only supports single * in middle)
        if let Some(pos) = pattern.find('*') {
            let prefix = &pattern[..pos];
            let suffix = &pattern[pos + 1..];
            return name.starts_with(prefix)
                && name.ends_with(suffix)
                && name.len() >= pattern.len() - 1;
        }
    }

    name == pattern
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matching() {
        assert!(matches_pattern("user_id", "*_id"));
        assert!(matches_pattern("product_id", "*_id"));
        assert!(!matches_pattern("identity", "*_id"));

        assert!(matches_pattern("log_message", "log_*"));
        assert!(matches_pattern("log_level", "log_*"));
        assert!(!matches_pattern("message_log", "log_*"));

        assert!(matches_pattern("test_field_name", "test_*_name"));
        assert!(matches_pattern("test_column_name", "test_*_name"));
        assert!(!matches_pattern("test_name", "test_*_name"));

        assert!(matches_pattern("anything", "*"));
        assert!(matches_pattern("exact_match", "exact_match"));
    }

    #[test]
    fn test_field_params_merge() {
        let mut params = CompressionFieldParams::default();
        assert_eq!(params.rle_threshold, None);
        assert_eq!(params.compression, None);
        assert_eq!(params.compression_level, None);
        assert_eq!(params.bss, None);

        let other = CompressionFieldParams {
            rle_threshold: Some(0.3),
            compression: Some("lz4".to_string()),
            compression_level: None,
            bss: Some(BssMode::On),
            minichunk_size: None,
        };

        params.merge(&other);
        assert_eq!(params.rle_threshold, Some(0.3));
        assert_eq!(params.compression, Some("lz4".to_string()));
        assert_eq!(params.compression_level, None);
        assert_eq!(params.bss, Some(BssMode::On));

        let another = CompressionFieldParams {
            rle_threshold: None,
            compression: Some("zstd".to_string()),
            compression_level: Some(3),
            bss: Some(BssMode::Auto),
            minichunk_size: None,
        };

        params.merge(&another);
        assert_eq!(params.rle_threshold, Some(0.3)); // Not overridden
        assert_eq!(params.compression, Some("zstd".to_string())); // Overridden
        assert_eq!(params.compression_level, Some(3)); // New value
        assert_eq!(params.bss, Some(BssMode::Auto)); // Overridden
    }

    #[test]
    fn test_get_field_params() {
        let mut params = CompressionParams::new();

        // Set type-level params
        params.types.insert(
            "Int32".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.5),
                compression: Some("lz4".to_string()),
                ..Default::default()
            },
        );

        // Set column-level params
        params.columns.insert(
            "*_id".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.3),
                compression: Some("zstd".to_string()),
                compression_level: Some(3),
                bss: None,
                minichunk_size: None,
            },
        );

        // Test no match (should get default)
        let field_params = params.get_field_params("some_field", &DataType::Float32);
        assert_eq!(field_params.compression, None);
        assert_eq!(field_params.rle_threshold, None);

        // Test type match only
        let field_params = params.get_field_params("some_field", &DataType::Int32);
        assert_eq!(field_params.compression, Some("lz4".to_string())); // From type
        assert_eq!(field_params.rle_threshold, Some(0.5)); // From type

        // Test column override (pattern match)
        let field_params = params.get_field_params("user_id", &DataType::Int32);
        assert_eq!(field_params.compression, Some("zstd".to_string())); // From column
        assert_eq!(field_params.compression_level, Some(3)); // From column
        assert_eq!(field_params.rle_threshold, Some(0.3)); // From column (overrides type)
    }

    #[test]
    fn test_exact_match_priority() {
        let mut params = CompressionParams::new();

        // Add pattern
        params.columns.insert(
            "*_id".to_string(),
            CompressionFieldParams {
                compression: Some("lz4".to_string()),
                ..Default::default()
            },
        );

        // Add exact match
        params.columns.insert(
            "user_id".to_string(),
            CompressionFieldParams {
                compression: Some("zstd".to_string()),
                ..Default::default()
            },
        );

        // Exact match should win
        let field_params = params.get_field_params("user_id", &DataType::Int32);
        assert_eq!(field_params.compression, Some("zstd".to_string()));
    }
}
