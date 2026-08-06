# Protobuf Guidelines

Also see [root AGENTS.md](../AGENTS.md) for cross-language standards.

## Compatibility

- Protobuf schemas that are part of a stable file format or any other stable persisted contract must remain backwards compatible. Never reuse or change their existing field numbers.
- Protobuf schemas used exclusively by an unstable file format follow the root file-format stability contract: do not preserve compatibility with prior unstable revisions. Before making a breaking protobuf change, verify that the schema is not shared with a stable format or another persisted contract.

## Schema Design

- Use `optional` when you need to distinguish "not set" from "zero value" — `optional` enables presence tracking (`has_*` methods) and maps to `Option<T>` in Rust. Bare proto3 fields have no presence semantics: they always hold a value (defaulting to zero), so you cannot tell if the sender explicitly set them.
- Use structured message types (e.g., `BasePath`) instead of plain scalars, and scope fields to operation-specific messages (e.g., `InsertTransaction`) rather than generic top-level ones.
- Don't duplicate data across messages — store each fact once and derive relationships. Prefer parallel sequences over maps when keys already exist in another field.

## Documentation

- Document the semantic meaning of both present and absent states for `optional` fields — explain when each case applies.
- Use precise domain terminology in field descriptions — avoid ambiguous abbreviations or terms that collide with domain concepts.
