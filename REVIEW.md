# Code review guidelines

Repo-specific guidance for automated PR reviews.

## Cross-SDK parity

LanceDB exposes the same core (`rust/lancedb`) through Python, TypeScript (`nodejs`),
and Java bindings. Behavioral drift between SDKs is a recurring problem, so watch for
parity gaps when reviewing — but only flag real ones:

* If the change adds or modifies user-facing API or behavior in the shared core
  (`rust/lancedb`), check whether each binding that should expose it (`python`,
  `nodejs`) does. A core change with no corresponding binding update is worth a note.
* If the change adds or modifies a public API in one SDK but not the other, open the
  sibling SDK's corresponding module and state whether an equivalent exists. If not,
  note it as a possible parity gap and suggest a follow-up issue.
* For bug fixes, first read the sibling SDK's analogous code path to check whether the
  same bug exists there. Only raise parity if it actually does. Do not ask to "port" a
  fix for a bug that only ever existed in one binding.
* Stay silent on internal-only refactors, tests, docs, and changes with no cross-SDK
  surface.
* Parity expectations apply to the Python and TypeScript (`nodejs`) SDKs. Java currently
  implements only Enterprise tables, not the Embedded backend, so it is expected to
  be partial — do not flag Java for missing Embedded-only functionality.
* Keep parity feedback to a short, clearly-labeled note (e.g. "Possible SDK parity
  gap: …"). It is advisory, not a merge blocker.

## User-facing text and API compatibility

Check new user-facing text and public API changes against the rules in `AGENTS.md`:

* Docs, docstrings, error messages, and examples say "Embedded" and "Enterprise", not
  "local" or "remote" tables.
* Released public APIs are deprecated rather than removed, and released names are not
  reused for something with a different meaning.
* User guides (`docs/web`) document only the current API, with no migration or
  deprecation notices.
* Breaking changes have `!` in the PR title and a `BREAKING CHANGE:` paragraph at the
  top of the PR body.
