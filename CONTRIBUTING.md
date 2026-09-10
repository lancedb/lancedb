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
* `docs/web`: the open-source pages of [docs.lancedb.com](https://docs.lancedb.com)
* `docs/web-tests`: the tests the documentation's code examples are extracted from

## Documentation

The open-source pages of [docs.lancedb.com](https://docs.lancedb.com) live in
`docs/web`, so a change to behaviour and the change to the page describing it
belong in the same pull request.

Preview them locally — this serves `docs/web` on its own, with no other checkout:

```bash
npm i -g mint      # https://mintlify.com/docs/installation
cd docs/web && mint dev
```

Enterprise pages live in a separate private repository and are merged in when
the published site is assembled, so links to `/enterprise/...` and the SQL and
Geneva sections will not resolve locally. Every open-source page does.

Two things to know before editing:

**Code examples are not written into the pages.** They live in real tests under
`docs/web-tests/{py,ts,rs}`, are extracted into `docs/web/snippets/`, and are
imported by the pages. Edit the test, then regenerate:

```bash
uv run docs/web-tests/mdx_snippets_gen.py -s docs/web-tests/py
```

An example that stops compiling fails the pull request that broke it.

**Every heading carries an explicit `{#anchor}`.** Those anchors are how the
Enterprise pages attach their additions to the right section, so they are
assigned once and never regenerated. Leave an existing anchor alone even when
you reword the heading above it; only new headings need a new one.

## Release process

For information on the release process, see: [release_process.md](release_process.md)
