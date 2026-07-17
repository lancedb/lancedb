# Rust Development

All the other language bindings are wrappers around the Rust library. This means
that most of the development happens in the Rust codebase.

## Working with Lance Changes

LanceDB tables are wrapper around Lance datasets. So many of the features are
developed at the Lance level. You can concurrently work on LanceDB and Lance
together:

1. Clone both Lance and LanceDB, under the same parent directory:
   ```bash
   git clone https://github.com/lancedb/lance.git
   git clone https://github.com/lancedb/lancedb.git
   cd lancedb
   ```
2. Get LanceDB to reference the local Lance repo:
   ```bash
   python ci/set_lance_version.py local
   ```

This let's you test your Lance changes in LanceDB during development.

Once you are ready to merge your changes, you can do:

1. Make a PR in Lance, and wait for it to be merged.
2. Once the PR is merged, request a preview release of Lance.
3. Once a preview release is available, change LanceDB to use the preview release:
   ```bash
   python ci/set_lance_version.py preview
   ```
4. Create a PR in LanceDB with your completed changes.
