# LanceDB patch provenance

This artifact vendors the Lance 11.0.0-beta.3 Rust workspace from Lance commit
`f7d475539cefbd140cc46a828f3d843e68cd10f1`. The complete workspace keeps all mutually coupled
Lance crates on one Cargo source identity when `lancedb` consumes the pinned artifact commit.

The local patch makes AWS credential-family merging atomic before backend selection and teaches
the built-in OpenDAL S3 signer to resolve credential-only storage options at request time. This
keeps long-lived multipart uploads refreshable without rebuilding a store or changing its outer
metadata. Keeping the change inside `AwsStoreProvider` leaves arbitrary registry providers and
their complete `ObjectStore` results untouched. Remove this patch when the same behavior is
available in the pinned Lance release.
