# LanceDB prose rules

`Dashes`, `PlainLanguage`, `ProductNames`, `VagueHeadings` and `VaguePhrases`
come from the `LanceDB.zip` asset of `lancedb/docs` release
`vale-lancedb-v0.1.0`, whose archive SHA-256 is
`0730a43e919774121068811d5687c61dfc7a0421cff78e1393f5e565b622cb0f`.
`ProductNames` no longer swaps in "LanceDB OSS".

The product-terminology rules were added here: `EmbeddedName`,
`RemoteExecution`, `OSSProduct`, `DistributedEnterprise`, `InternalNames`
(warnings) and `LegacyCloud` (suggestion). They enforce the names in the
docs repository's `STYLE.md#names`. Run every rule's tests from
`.github/vale` with `vale test --coverage styles`.

Keeping the rules here lets contributors and CI run Vale without access to the
source repository. The root `.vale.ini` preserves the release's MDX settings
and built-in repeated-word check. No `vale sync` is needed.

The rules check product names, dashes, plain language, vague headings and vague
phrases. Findings are suggestions and do not fail the CI check.

To update, a maintainer downloads the next published package, verifies its
checksum and replaces `styles/LanceDB` with the release's files. Check the
package's `.vale.ini` for configuration changes, and update the version and
checksum here in the same PR.
