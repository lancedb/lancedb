# Contributing to LanceDB

LanceDB is an open-source project and we welcome contributions from the community.
This document outlines the process for contributing to LanceDB.

## Reporting Issues

If you encounter a bug or have a feature request, please open an issue on the
[GitHub issue tracker](https://github.com/lancedb/lancedb).

## Picking an issue

We track issues on the GitHub issue tracker. If you are looking for something to
work on, check the [good first issue](https://github.com/lancedb/lancedb/contribute) label. These issues are typically the best described and have the smallest scope.

If there's an issue you are interested in working on, please leave a comment on the issue. This will help us avoid duplicate work. Additionally, if you have questions about the issue, please ask them in the issue comments. We are happy to provide guidance on how to approach the issue.

## Configuring Git

First, fork the repository on GitHub, then clone your fork:

```bash
git clone https://github.com/<username>/lancedb.git
cd lancedb
```

Then add the main repository as a remote:

```bash
git remote add upstream https://github.com/lancedb/lancedb.git
git fetch upstream
```

## Setting up your development environment

We have development environments for Python, Typescript, and Java. Each environment has its own setup instructions.

* [Python](python/CONTRIBUTING.md)
* [Typescript](nodejs/CONTRIBUTING.md)
<!-- TODO: add Java contributing guide -->
* [Documentation](docs/README.md)


## Best practices for pull requests

For the best chance of having your pull request accepted, please follow these guidelines:

1. Unit test all bug fixes and new features. Your code will not be merged if it
   doesn't have tests.
1. If you change the public API, update the documentation in the `docs` directory.
1. Aim to minimize the number of changes in each pull request. Keep to solving
   one problem at a time, when possible.
1. Before marking a pull request ready-for-review, do a self review of your code.
   Is it clear why you are making the changes? Are the changes easy to understand?
1. Use [conventional commit messages](https://www.conventionalcommits.org/en/) as pull request titles. Examples:
    * New feature: `feat: adding foo API`
    * Bug fix: `fix: issue with foo API`
    * Documentation change: `docs: adding foo API documentation`
1. If your pull request is a work in progress, leave the pull request as a draft.
   We will assume the pull request is ready for review when it is opened.
1. When writing tests, test the error cases. Make sure they have understandable
   error messages.

## Project structure

The core library is written in Rust. The Python, Typescript, and Java libraries
are wrappers around the Rust library.

* `src/lancedb`: Rust library source code
* `python`: Python package source code
* `nodejs`: Typescript package source code
* `node`: **Deprecated** Typescript package source code
* `java`: Java package source code
* `docs/src`: SDK reference, built with mkdocs
* `docs/web`: website guides for LanceDB Embedded and the APIs used by both Embedded and Distributed
* `docs/web-tests`: the tests the documentation's code examples are extracted from

## Documentation

Update the docs in the same pull request as the behavior they describe. You can
merge docs before a feature launches; keep the unfinished material hidden until
it is ready for readers.

### Choose the right place

- `docs/web/` holds website guides for LanceDB Embedded and the APIs used by both
  Embedded and Distributed.
- `docs/src/` holds the SDK reference. See [docs/README.md](docs/README.md) for
  its build and API-generation instructions.
- `docs/web-tests/` and `rust/lancedb/tests/` hold tests used to generate the
  website's code examples.

Document SDK APIs here, and explain which operations work in Embedded and Distributed.
Put Enterprise-specific deployment and operating instructions in the Enterprise
documentation.

Some pages are placeholders that the website build replaces with an Enterprise
page. Changes to a placeholder won't appear on the combined site. Put
instructions that both products need on a separate page and link to it.

### Preview and check your change

From the repository root:

```bash
npm i -g mint@4.2.888
(cd docs/web && mint dev)
```

This previews this checkout, including your uncommitted changes. It doesn't
include the other documentation sources or render the availability labels.
Use the [combined preview](https://github.com/lancedb/docs#development) when
checking Enterprise replacements or links to pages from another source.
Staging shows saved release editions, so it won't show a new source change
just because that change merged.

Follow the [style guide](https://github.com/lancedb/docs/blob/main/STYLE.md).
[Vale](https://vale.sh) checks wording, names and punctuation using the
[bundled LanceDB rules](.github/vale/README.md). Pull requests show suggestions
on the lines they change. With Vale 3.24.0 installed, check a page locally with:

```bash
vale docs/web/indexing/vector-index.mdx
```

Keep existing `{#anchor}` values when changing headings, so links to those
sections keep working. Give new headings stable anchors. For links in rendered
components, write the path directly, such as `href="/quickstart"`, and spell out
attributes rather than using property spreads. That lets versioned docs keep
links inside the selected edition. Code examples aren't subject to this rule.

### Code examples

Edit the test that produces an example, then regenerate its snippet. The
sources are `docs/web-tests/{py,ts,rs}`, `docs/web-tests/blob-v2/{py,ts,rs}` and
`rust/lancedb/tests`. Run this from the repository root:

```bash
uv run docs/web-tests/mdx_snippets_gen.py \
  -s docs/web-tests/py -s docs/web-tests/ts -s docs/web-tests/rs \
  -s docs/web-tests/blob-v2/py -s docs/web-tests/blob-v2/ts -s docs/web-tests/blob-v2/rs \
  -s rust/lancedb/tests -o docs/web/snippets
```

Run the tests for the examples you changed; generating a snippet doesn't run
its example. Review the generated diff and commit only the relevant snippets
with their tests. Don't assume the PR checks execute every documentation
example.

### Say where a feature works

Use `availability` in a page's frontmatter to describe the support covered by
that page. Each product (`oss` for LanceDB Embedded, `enterprise` for LanceDB
Enterprise) is either `available` or `unavailable`; use the summary
and body for setup requirements, SDK differences and unsupported operations.
For example, a page about signing in to Enterprise could declare:

```yaml
availability:
  oss: unavailable
  enterprise: available
  summary: >-
    These sign-in instructions apply to LanceDB Enterprise. LanceDB
    Embedded has no server to sign in to.
```

Check these claims with the feature owner. Don't use `varies`, treat an API
stub as working support, or mark a supported feature unavailable just because
it needs configuration. If one label can't describe the whole page accurately,
make the page's scope narrower and explain the differences.

Assembly turns this declaration into a page label and, for a feature supported
by only one product, a sidebar tag. Don't add those badges by hand. Availability
labels do not hide unreleased content.

### Write docs before launch

You don't need to hold a documentation PR until launch. Use these controls:

- For a whole page, add its path to `docs/web/.mintignore`. Leave it out of
  `docs/web/docs.json`, redirects and links from published pages. Removing a
  sidebar entry alone doesn't hide a page.
- For a section, wrap it in an MDX comment, `{/* ... */}`, with a note such as
  `Draft for the Feature X release.` Put draft table rows and list items in a
  comment below the table or list, not between entries.

Drafts remain in the source. Use them to avoid advertising an unfinished
feature, not to keep information confidential. See
[Writing docs before release](https://github.com/lancedb/docs#writing-docs-before-release)
for examples and how to preview a draft.

Before the intended release, remove the draft controls, restore the navigation
and links, and check the result. Those changes must be in the source commit the
edition uses. A later change to `main` doesn't update an edition already saved.
For a mistake in an existing edition, fix the source and request a
[recorded correction](https://github.com/lancedb/docs#correcting-a-saved-edition).

## Release process

For information on the release process, see: [release_process.md](release_process.md)
