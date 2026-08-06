// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use object_store::path::Path;

/// Format a blob sidecar path for a data file.
///
/// Layout: `<base>/<data_file_key>/<obfuscated_blob_id>.blob`
/// - `base` is typically the dataset's data directory.
/// - `data_file_key` is the stem of the data file (without extension).
/// - `blob_id` is transformed via `reverse_bits()` before binary formatting.
pub fn blob_path(base: &Path, data_file_key: &str, blob_id: u32) -> Path {
    let file_name = format!("{:032b}.blob", blob_id.reverse_bits());
    base.clone().join(data_file_key).join(file_name.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_path_formatting() {
        let base = Path::from("base");
        let path = blob_path(&base, "deadbeef", 2);
        assert_eq!(
            path.to_string(),
            "base/deadbeef/01000000000000000000000000000000.blob"
        );
    }

    #[test]
    fn test_blob_path_scattered_prefixes_for_sequential_ids() {
        let base = Path::from("base");
        let p1 = blob_path(&base, "deadbeef", 1);
        let p2 = blob_path(&base, "deadbeef", 2);
        assert_ne!(p1.to_string(), p2.to_string());
        assert_eq!(
            p1.to_string(),
            "base/deadbeef/10000000000000000000000000000000.blob"
        );
        assert_eq!(
            p2.to_string(),
            "base/deadbeef/01000000000000000000000000000000.blob"
        );
    }
}
