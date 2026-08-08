# lance-arrow-scalar

A scalar type backed by Apache Arrow arrays with `Ord`, `Hash`, and `Eq` support.

## Overview

`ArrowScalar` wraps a single-element Arrow array and provides comparison and hashing operations by leveraging Apache Arrow's `OwnedRow` representation. This ensures:

- **Correct total ordering** for all Arrow types
- **Proper NaN handling** for floating-point values
- **Consistent null ordering**
- **O(1) comparisons** via cached row bytes

## Features

- `Eq`, `Ord`, and `Hash` traits for Arrow scalar values
- Support for all Arrow data types
- Serde serialization/deserialization support
- Zero-copy conversion from Arrow arrays

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
lance-arrow-scalar = "57.0.0"
```

Then use in your code:

```rust
use lance_arrow_scalar::ArrowScalar;

// Create from primitive types
let a = ArrowScalar::from(42i32);
let b = ArrowScalar::from(100i32);
assert!(a < b);

// Create from strings
let s1 = ArrowScalar::from("hello");
let s2 = ArrowScalar::from("world");
assert!(s1 < s2);

// Use in collections
use std::collections::HashMap;
let mut map = HashMap::new();
map.insert(ArrowScalar::from("key"), ArrowScalar::from(123));
```

## Cross-Type Comparison

Comparing scalars of different data types produces an arbitrary but consistent ordering based on the underlying row bytes. This allows scalars to be used as keys in sorted collections regardless of type, though the ordering across types is not semantically meaningful.

## Implementation Details

Comparisons and hashing are delegated to [`arrow_row::OwnedRow`], which provides efficient byte-level operations. The row representation is cached at construction time, making all comparison and hashing operations O(1).
