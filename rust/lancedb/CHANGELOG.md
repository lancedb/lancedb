# Changelog

## Unreleased

- `BaseTableAdapter` now defaults to `BlobSchema::Read`, exposing the Blob v2
  descriptors returned by scans. SQL INSERT into Blob v2 tables requires opting
  into `BlobSchema::Write` with `with_blob_schema`. Write-mode queries support
  scalar projections that exclude blobs, scalar filters, and `COUNT(*)`.
  Scans must exactly match the projected provider schema, including metadata
  and nullability. Blob v2 descriptor scans fail during planning; legacy v1
  blobs expose `LargeBinary` write inputs and reject descriptor scans with a
  schema mismatch. A scalar sibling inside a blob-containing struct also needs
  a separate read adapter because DataFusion projects the whole parent struct.
  Tables without blob columns behave the same in both modes. Generated `_rowid`,
  `_rowaddr`, and `_score` columns are exposed only in read mode.
