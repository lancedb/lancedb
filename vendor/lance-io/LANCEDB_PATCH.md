# LanceDB patch provenance

This directory vendors `lance-io` 11.0.0-beta.2 from Lance commit
`35da5d920159b49d1b53032652f7615ab699c160`.

`Cargo.toml` uses the equivalent standalone dependency metadata from the published crate. Upstream
benchmark and integration-test targets are omitted because this copy is compiled only as a patched
dependency; the library sources are otherwise retained.

The local patch makes AWS credential-family merging atomic before backend selection and teaches
the built-in OpenDAL S3 path to refresh credential-only storage options. Keeping the change inside
`AwsStoreProvider` leaves arbitrary registry providers and their complete `ObjectStore` results
untouched. Remove this patch when the same behavior is available in the pinned Lance release.
