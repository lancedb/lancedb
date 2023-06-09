# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.5] - 2023-06-00

### Added

- Support for macOS X86

## [0.1.4] - 2023-06-03

### Added

- Select / Project query API

### Changed

-  Deprecated created_index in favor of createIndex

## [0.1.3] - 2023-06-01

### Added

- Support S3 and Google Cloud Storage
- Embedding functions support
- OpenAI embedding function

## [0.1.2] - 2023-05-27

### Added

- Append records API
- Extra query params to to nodejs client
- Create_index API
 
### Fixed

- bugfix: string columns should be converted to Utf8Array (#94)

## [0.1.1] - 2023-05-16

### Added

- create_table API
- limit parameter for queries
- Typescript / JavaScript examples
- Linux support

## [0.1.0] - 2023-05-16

### Added

- Initial  JavaScript / Node.js library for LanceDB
- Read-only api to query LanceDB datasets
- Supports macOS arm only

## [pre-0.1.0]

- Various prototypes / test builds

