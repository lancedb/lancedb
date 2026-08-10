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

The LanceDB workspace pins every coupled Lance crate to the immutable repository commit containing
this artifact. That durable source survives transitive Git consumption instead of relying on a
root `[patch]`, which Cargo ignores when LanceDB itself is used as a dependency.

The artifact also contains Apache OpenDAL 0.58.1's `opendal` and `opendal-service-s3` crates. The
only OpenDAL change adds a direct custom-provider hook alongside its existing credential-chain
hook. Lance uses the direct hook so a selected dynamic authority can propagate refresh and
validation errors; static or ambient credentials are considered only when that authority returns
`Ok(None)`. Remove these copies when upstream OpenDAL exposes an equivalent hook.
