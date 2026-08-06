// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Data overlay files.
//!
//! An overlay file supplies new values for a subset of `(physical offset, field)`
//! cells within a fragment, without rewriting the fragment's base data files. See
//! the Data Overlay Files specification for the full rules; the invariants this
//! module relies on are:
//!
//! - **Physical-offset coverage.** Coverage bitmaps index *physical* row offsets
//!   (positions in the base data files, counting deleted rows), so they are stable
//!   across deletions, like deletion vectors.
//! - **Rank-based values.** The overlay's `data_file` stores one value column per
//!   field, with no row-offset key column. Within a value column, a covered
//!   offset's value sits at its **rank** — the 0-based count of set bits below it
//!   in that field's coverage bitmap.
//! - **Dense vs. sparse coverage.** A dense overlay shares one bitmap across every
//!   field ([`OverlayCoverage::Shared`]); a sparse overlay carries one bitmap per
//!   field ([`OverlayCoverage::PerField`]).
//! - **Parse once.** Bitmaps are parsed from their 32-bit Roaring encoding a single
//!   time when the fragment loads and held behind an `Arc`, so cloning a fragment
//!   is cheap.
//! - **Newest-last ordering.** A fragment's overlays are stored newest-last and
//!   stable-sorted by `committed_version` on load (see [`sort_overlays_newest_last`]),
//!   with list position breaking ties for equal versions. When two overlays cover
//!   the same `(offset, field)`, the higher `committed_version` wins.
//! - **Field tombstones.** When new base values are written for a field (a
//!   DataReplacement, or an in-place column rewrite), any overlay value for that
//!   field is stale and must stop shadowing the fresh base. The field is marked
//!   obsolete in the overlay's `data_file.fields` with [`TOMBSTONE_FIELD_ID`]
//!   (the same sentinel used for obsolete base columns) rather than physically
//!   removed, so the overlay's other fields — and its coverage positions — stay
//!   intact (see [`tombstone_overlay_fields`]).

use std::sync::Arc;

use lance_core::Error;
use lance_core::deepsize::DeepSizeOf;
use lance_core::error::Result;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use object_store::path::Path;

use super::DataFile;
use crate::format::pb;

/// Field-id sentinel marking a tombstoned (obsolete) field within an overlay's
/// `data_file.fields`. Matches the tombstone convention for obsolete columns in
/// base data files; a tombstoned field's values are ignored on read.
pub const TOMBSTONE_FIELD_ID: i32 = -2;

/// Which `(physical offset, field)` cells a [`DataOverlayFile`] provides values
/// for.
///
/// Bitmaps are parsed from their 32-bit Roaring encoding once when the fragment
/// is loaded and held behind an `Arc` so cloning a fragment is cheap; use
/// [`DataOverlayFile::coverage_for_field`] to obtain the one that applies to a
/// given field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "OverlayCoverageBytes", try_from = "OverlayCoverageBytes")]
pub enum OverlayCoverage {
    /// A single bitmap that applies to every field in the overlay's
    /// `data_file.fields` (a dense / rectangular overlay): every covered offset
    /// has a value for every field.
    Shared(Arc<RoaringBitmap>),
    /// One bitmap per field, in the same order as the overlay's
    /// `data_file.fields` (a sparse overlay): different fields may cover
    /// different offset sets.
    PerField(Vec<Arc<RoaringBitmap>>),
}

/// Serialized form of [`OverlayCoverage`] — each bitmap as its 32-bit Roaring
/// byte encoding. The in-memory form parses these once at load.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum OverlayCoverageBytes {
    Shared(Vec<u8>),
    PerField(Vec<Vec<u8>>),
}

// The bytes come from a persisted overlay (the protobuf manifest or a
// serialized fragment), so a decode failure is on-disk corruption, not caller
// input. `path` locates the overlay's data file when known (empty on the serde
// path, which deserializes coverage in isolation).
fn deserialize_roaring(bytes: &[u8], path: &Path) -> Result<RoaringBitmap> {
    RoaringBitmap::deserialize_from(bytes).map_err(|e| {
        Error::corrupt_file(
            path.clone(),
            format!("failed to deserialize overlay coverage bitmap: {e}"),
        )
    })
}

fn serialize_roaring(bitmap: &RoaringBitmap) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(bitmap.serialized_size());
    // Writing to a Vec is infallible.
    bitmap.serialize_into(&mut bytes).unwrap();
    bytes
}

impl From<OverlayCoverage> for OverlayCoverageBytes {
    fn from(coverage: OverlayCoverage) -> Self {
        match coverage {
            OverlayCoverage::Shared(bitmap) => Self::Shared(serialize_roaring(&bitmap)),
            OverlayCoverage::PerField(bitmaps) => {
                Self::PerField(bitmaps.iter().map(|b| serialize_roaring(b)).collect())
            }
        }
    }
}

impl TryFrom<OverlayCoverageBytes> for OverlayCoverage {
    type Error = Error;

    fn try_from(bytes: OverlayCoverageBytes) -> Result<Self> {
        // Serde deserializes the coverage in isolation, so the owning data
        // file's path is not available here.
        let path = Path::default();
        Ok(match bytes {
            OverlayCoverageBytes::Shared(b) => {
                Self::Shared(Arc::new(deserialize_roaring(&b, &path)?))
            }
            OverlayCoverageBytes::PerField(bs) => Self::PerField(
                bs.iter()
                    .map(|b| deserialize_roaring(b, &path).map(Arc::new))
                    .collect::<Result<_>>()?,
            ),
        })
    }
}

impl DeepSizeOf for OverlayCoverage {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        // The same `Arc<RoaringBitmap>` is shared across every clone of a
        // fragment, so mark each Arc's pointer and count its heap only the first
        // time it is seen — otherwise walking many fragments double-counts the
        // shared bitmaps. RoaringBitmap does not expose its allocation size; its
        // serialized size is a cheap, close proxy for the heap it holds.
        let bitmap_heap = |bitmap: &Arc<RoaringBitmap>,
                           context: &mut lance_core::deepsize::Context| {
            if context.mark_seen(Arc::as_ptr(bitmap) as usize) {
                std::mem::size_of::<RoaringBitmap>() + bitmap.serialized_size()
            } else {
                0
            }
        };
        match self {
            Self::Shared(bitmap) => bitmap_heap(bitmap, context),
            Self::PerField(bitmaps) => {
                bitmaps.capacity() * std::mem::size_of::<Arc<RoaringBitmap>>()
                    + bitmaps
                        .iter()
                        .map(|b| bitmap_heap(b, context))
                        .sum::<usize>()
            }
        }
    }
}

impl OverlayCoverage {
    /// Build a dense coverage from a single bitmap shared across every field.
    pub fn dense(bitmap: RoaringBitmap) -> Self {
        Self::Shared(Arc::new(bitmap))
    }

    /// Build a sparse coverage from one bitmap per field.
    pub fn sparse(bitmaps: Vec<RoaringBitmap>) -> Self {
        Self::PerField(bitmaps.into_iter().map(Arc::new).collect())
    }
}

/// An overlay file supplies new values for a subset of `(physical offset, field)`
/// cells within a fragment, without rewriting the fragment's base data files. See
/// the [module documentation](self) for the coverage, rank, and versioning rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct DataOverlayFile {
    /// The data file storing the overlay's new cell values.
    pub data_file: DataFile,
    /// Which cells this overlay provides values for.
    pub coverage: OverlayCoverage,
    /// The dataset version at which this overlay became effective (the version of
    /// the commit that introduced it, stamped at commit time and re-stamped on
    /// retry). Higher wins when two overlays cover the same `(offset, field)`.
    pub committed_version: u64,
}

impl DataOverlayFile {
    /// The parsed coverage bitmap that applies to the field stored at
    /// `field_pos` within `data_file.fields`.
    ///
    /// For a dense overlay the same shared bitmap is returned for every field;
    /// for a sparse overlay the per-field bitmap at `field_pos` is returned. The
    /// bitmap is already parsed, so this is a cheap `Arc` clone.
    pub fn coverage_for_field(&self, field_pos: usize) -> Result<Arc<RoaringBitmap>> {
        match &self.coverage {
            OverlayCoverage::Shared(bitmap) => Ok(bitmap.clone()),
            OverlayCoverage::PerField(bitmaps) => {
                bitmaps.get(field_pos).cloned().ok_or_else(|| {
                    Error::invalid_input(format!(
                        "overlay per-field coverage has {} bitmaps but field position {} was requested",
                        bitmaps.len(),
                        field_pos
                    ))
                })
            }
        }
    }
}

/// Stable-sort a fragment's overlays newest-last by `committed_version`. The
/// stable sort preserves list position as the tiebreak for equal versions, so
/// resolution can rely on the ordering without re-checking. See the [module
/// documentation](self) for the ordering invariant.
pub fn sort_overlays_newest_last(overlays: &mut [DataOverlayFile]) {
    overlays.sort_by_key(|overlay| overlay.committed_version);
}

/// Verify a fragment's overlays are stored newest-last (non-decreasing
/// `committed_version`), the ordering invariant readers rely on for
/// resolution. Returns an error identifying the first out-of-order pair.
///
/// [`sort_overlays_newest_last`] normalizes on load; this is the write-side
/// guard that rejects any commit path that assembled overlays out of order. See
/// the [module documentation](self) for the ordering invariant.
pub fn verify_overlays_newest_last(overlays: &[DataOverlayFile]) -> Result<()> {
    for pair in overlays.windows(2) {
        if pair[0].committed_version > pair[1].committed_version {
            return Err(Error::invalid_input(format!(
                "overlay files must be stored newest-last, but committed_version {} precedes {}",
                pair[0].committed_version, pair[1].committed_version
            )));
        }
    }
    Ok(())
}

/// Tombstone `fields` across a fragment's `overlays`, dropping any overlay left
/// with no live fields.
///
/// Called when new base values are written for those fields (a DataReplacement,
/// or an in-place column rewrite): the stale overlay values must stop shadowing
/// the fresh base. Each matching field id is replaced with [`TOMBSTONE_FIELD_ID`]
/// in place, preserving the overlay's remaining fields and its coverage positions
/// (a per-field coverage bitmap stays aligned with `data_file.fields`). An overlay
/// whose fields are now all tombstoned is removed entirely. See the [module
/// documentation](self) for the tombstone invariant.
pub fn tombstone_overlay_fields(overlays: &mut Vec<DataOverlayFile>, fields: &[u32]) {
    for overlay in overlays.iter_mut() {
        let tombstoned: Vec<i32> = overlay
            .data_file
            .fields
            .iter()
            .map(|&field| {
                if field >= 0 && fields.contains(&(field as u32)) {
                    TOMBSTONE_FIELD_ID
                } else {
                    field
                }
            })
            .collect();
        overlay.data_file.fields = tombstoned.into();
    }
    overlays.retain(|overlay| {
        overlay
            .data_file
            .fields
            .iter()
            .any(|&field| field != TOMBSTONE_FIELD_ID)
    });
}

impl From<&DataOverlayFile> for pb::DataOverlayFile {
    fn from(overlay: &DataOverlayFile) -> Self {
        let coverage = match &overlay.coverage {
            OverlayCoverage::Shared(bitmap) => {
                pb::data_overlay_file::Coverage::SharedOffsetBitmap(serialize_roaring(bitmap))
            }
            OverlayCoverage::PerField(bitmaps) => {
                pb::data_overlay_file::Coverage::FieldCoverage(pb::FieldCoverage {
                    offset_bitmaps: bitmaps.iter().map(|b| serialize_roaring(b)).collect(),
                })
            }
        };
        Self {
            data_file: Some(pb::DataFile::from(&overlay.data_file)),
            coverage: Some(coverage),
            committed_version: overlay.committed_version,
        }
    }
}

impl TryFrom<pb::DataOverlayFile> for DataOverlayFile {
    type Error = Error;

    fn try_from(proto: pb::DataOverlayFile) -> Result<Self> {
        let data_file = proto
            .data_file
            .ok_or_else(|| Error::invalid_input("DataOverlayFile is missing its data_file"))?;
        let path = Path::from(data_file.path.as_str());
        let coverage = match proto.coverage {
            Some(pb::data_overlay_file::Coverage::SharedOffsetBitmap(bytes)) => {
                OverlayCoverage::Shared(Arc::new(deserialize_roaring(&bytes, &path)?))
            }
            Some(pb::data_overlay_file::Coverage::FieldCoverage(fc)) => OverlayCoverage::PerField(
                fc.offset_bitmaps
                    .iter()
                    .map(|b| deserialize_roaring(b, &path).map(Arc::new))
                    .collect::<Result<_>>()?,
            ),
            None => {
                return Err(Error::invalid_input(
                    "DataOverlayFile is missing its coverage",
                ));
            }
        };
        Ok(Self {
            data_file: DataFile::try_from(data_file)?,
            coverage,
            committed_version: proto.committed_version,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_overlay_missing_fields_error() {
        // A DataOverlayFile proto missing its coverage or data_file is rejected.
        let no_coverage = pb::DataOverlayFile {
            data_file: Some(pb::DataFile::from(&DataFile::new_legacy_from_fields(
                "overlay.lance",
                vec![3],
                None,
            ))),
            coverage: None,
            committed_version: 1,
        };
        let err = DataOverlayFile::try_from(no_coverage).unwrap_err();
        assert!(err.to_string().contains("missing its coverage"), "{err}");

        let no_data_file = pb::DataOverlayFile {
            data_file: None,
            coverage: Some(pb::data_overlay_file::Coverage::SharedOffsetBitmap(
                serialize_roaring(&RoaringBitmap::from_iter([0u32])),
            )),
            committed_version: 1,
        };
        let err = DataOverlayFile::try_from(no_data_file).unwrap_err();
        assert!(err.to_string().contains("missing its data_file"), "{err}");
    }

    #[test]
    fn test_overlay_coverage_serde_json_roundtrip() {
        // The custom serde impl round-trips through JSON for dense/sparse,
        // including empty bitmaps and a zero-bitmap sparse coverage.
        for coverage in [
            OverlayCoverage::dense(RoaringBitmap::from_iter([1u32, 5, 100])),
            OverlayCoverage::dense(RoaringBitmap::new()),
            OverlayCoverage::sparse(vec![
                RoaringBitmap::from_iter([2u32, 3]),
                RoaringBitmap::new(),
            ]),
            OverlayCoverage::sparse(vec![]),
        ] {
            let json = serde_json::to_string(&coverage).unwrap();
            let back: OverlayCoverage = serde_json::from_str(&json).unwrap();
            assert_eq!(back, coverage);
        }
    }

    #[test]
    fn test_tombstone_overlay_fields() {
        // An overlay covering fields [3, 5]: replacing field 5 tombstones just
        // field 5's slot and keeps field 3. An overlay covering only field 5 is
        // dropped entirely. An overlay touching no replaced field is untouched.
        let mut overlays = vec![
            DataOverlayFile {
                data_file: DataFile::new_legacy_from_fields("a.lance", vec![3, 5], None),
                coverage: OverlayCoverage::sparse(vec![
                    RoaringBitmap::from_iter([0u32]),
                    RoaringBitmap::from_iter([1u32]),
                ]),
                committed_version: 1,
            },
            DataOverlayFile {
                data_file: DataFile::new_legacy_from_fields("b.lance", vec![5], None),
                coverage: OverlayCoverage::dense(RoaringBitmap::from_iter([0u32])),
                committed_version: 1,
            },
            DataOverlayFile {
                data_file: DataFile::new_legacy_from_fields("c.lance", vec![7], None),
                coverage: OverlayCoverage::dense(RoaringBitmap::from_iter([0u32])),
                committed_version: 1,
            },
        ];

        tombstone_overlay_fields(&mut overlays, &[5]);

        // The single-field overlay on field 5 is gone; the others remain.
        assert_eq!(overlays.len(), 2);
        // Field 3 preserved, field 5 tombstoned in place (coverage stays aligned).
        assert_eq!(
            overlays[0].data_file.fields.as_ref(),
            &[3, TOMBSTONE_FIELD_ID]
        );
        // The untouched overlay keeps its field.
        assert_eq!(overlays[1].data_file.fields.as_ref(), &[7]);
    }

    #[test]
    fn test_verify_overlays_newest_last() {
        let mk = |version: u64| DataOverlayFile {
            data_file: DataFile::new_legacy_from_fields("o.lance", vec![3], None),
            coverage: OverlayCoverage::dense(RoaringBitmap::from_iter([0u32])),
            committed_version: version,
        };
        // Non-decreasing (including equal versions) is accepted.
        assert!(verify_overlays_newest_last(&[]).is_ok());
        assert!(verify_overlays_newest_last(&[mk(1), mk(2), mk(2), mk(5)]).is_ok());
        // A newer version before an older one is rejected.
        let err = verify_overlays_newest_last(&[mk(2), mk(1)]).unwrap_err();
        assert!(err.to_string().contains("newest-last"), "{err}");
    }

    #[test]
    fn test_coverage_for_field_out_of_bounds() {
        let overlay = DataOverlayFile {
            data_file: DataFile::new_legacy_from_fields("o.lance", vec![2, 4], None),
            coverage: OverlayCoverage::sparse(vec![
                RoaringBitmap::from_iter([1u32]),
                RoaringBitmap::from_iter([2u32]),
            ]),
            committed_version: 1,
        };
        assert!(overlay.coverage_for_field(0).is_ok());
        assert!(overlay.coverage_for_field(1).is_ok());
        let err = overlay.coverage_for_field(5).unwrap_err();
        assert!(err.to_string().contains("field position"), "{err}");
    }
}
