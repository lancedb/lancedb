# Contributing to LanceDB

LanceDB is an open-source project and we welcome contributions from the community.
This document outlines the process for contributing to LanceDB.

## Reporting Issues

If you encounter a bug or have a feature request, please open an issue on the
[GitHub issue tracker](https://github.com/lancedb/lancedb).

## Opening a pull request

For the best chance of having your pull request accepted, please follow these guidelines:

1. Unit test all bug fixes and new features. Your code will not be merged if it
   doesn't have tests.
1. Aim to minimize the number of changes in each pull request. Keep to solving
   one problem at a time, when possible.
1. Before marking a pull request ready-for-review, do a self review of your code.
   Is it clear why you are making the changes? Are the changes easy to understand?
1. Use [conventional commit messages]() as pull request titles. Examples:
    * New feature: `feat: adding foo API`
    * Bug fix: `fix: issue with foo API`
    * Documentation change: `docs: adding foo API documentation`
1. If your pull request is a work in progress, leave the pull request as a draft.
   We will assume the pull request is ready for review when it is opened.

## Project structure

The core library is written in Rust. The Python, Typescript, and Java libraries
are wrappers around the Rust library.

* `src/lancedb`: Rust library source code
* `python`: Python package source code
* `nodejs`: Typescript package source code
* `node`: **Deprecated** Typescript package source code
* `java`: Java package source code
* `docs`: Documentation source code

## Development environments

For Python development, see: [python/CONTRIBUTING.md](python/CONTRIBUTING.md)

## Release process

For information on the release process, see: [release_process.md](release_process.md)
