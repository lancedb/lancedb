# Changelog

## Unreleased

- `BaseTableAdapter` now defaults to `BlobSchema::Read`, exposing the Blob v2
  descriptors returned by scans. SQL INSERT into Blob v2 tables requires opting
  into `BlobSchema::Write` with `with_blob_schema`. Write-mode queries support
  direct scalar projections, simple nested scalar projections, scalar filters,
  and `COUNT(*)`; selecting blob fields fails during planning. Computed scalar
  and window queries beside nested blobs can also require `BlobSchema::Read`,
  depending on the physical plan. Register a separate read adapter for these queries.
