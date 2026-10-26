# Changelog

## Unreleased

- `BaseTableAdapter` now defaults to `BlobSchema::Read`, exposing the Blob v2
  descriptors returned by scans. SQL INSERT into Blob v2 tables requires opting
  into `BlobSchema::Write` with `with_blob_schema`. Write-mode queries support
  scalar columns, nested scalar siblings, and `COUNT(*)`; selecting blob fields
  fails during planning. Register separate adapters when both contracts are needed.
