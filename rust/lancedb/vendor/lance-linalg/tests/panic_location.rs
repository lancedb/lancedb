// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Where a length-contract panic reports itself.
//!
//! This lives in its own integration binary because it replaces the global
//! panic hook. Any other test that panics while the hook is installed lands in
//! this one's sink instead, and the lib test binary runs its tests on threads of
//! one process.

use std::ffi::OsStr;
use std::path::Path;

use lance_linalg::distance::{dot_u8::dot_u8, l2::l2_distance_batch_into};

/// Runs `f`, which must panic, and returns where the panic was reported and
/// what it said.
fn panic_details(f: impl FnOnce() + std::panic::UnwindSafe) -> (String, u32, String) {
    let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
    let sink = std::sync::Arc::clone(&captured);
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        // Both panics here come from `assert_eq!`, which always formats, so the
        // payload is a `String`. A payload of any other type leaves this empty
        // and the message assertions below fail rather than pass silently.
        let message = info
            .payload()
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_default();
        *sink.lock().unwrap() = info
            .location()
            .map(|loc| (loc.file().to_owned(), loc.line(), message));
    }));
    let outcome = std::panic::catch_unwind(f);
    std::panic::set_hook(previous);
    assert!(outcome.is_err(), "expected a panic");
    captured.lock().unwrap().take().expect("no panic location")
}

/// Without `#[track_caller]` on the two helpers in `distance.rs`, both of these
/// panics report `distance.rs` and the reader cannot tell which metric fired.
/// The message is asserted too, so an unrelated panic in the same file does not
/// satisfy the test.
#[test]
fn length_contract_panics_name_the_distance_function() {
    let (file, line, message) = panic_details(|| {
        dot_u8(&[1, 2], &[1]);
    });
    assert_eq!(
        Path::new(&file).file_name(),
        Some(OsStr::new("dot_u8.rs")),
        "expected dot_u8.rs, got {file}:{line}"
    );
    assert!(
        message.contains("equal lengths"),
        "{file}:{line}: {message}"
    );

    let (file, line, message) = panic_details(|| {
        l2_distance_batch_into(&[1.0f32, 2.0], &[1.0f32, 2.0, 3.0], 2, &mut [0.0]);
    });
    assert_eq!(
        Path::new(&file).file_name(),
        Some(OsStr::new("l2.rs")),
        "expected l2.rs, got {file}:{line}"
    );
    assert!(
        message.contains("divisible by dimension"),
        "{file}:{line}: {message}"
    );
}
