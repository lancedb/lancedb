// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Protobuf definitions for Lance Format
pub mod pb {
    #![allow(clippy::all)]
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]
    #![allow(improper_ctypes)]
    #![allow(clippy::upper_case_acronyms)]
    #![allow(clippy::use_self)]
    include!(concat!(env!("OUT_DIR"), "/lance.file.rs"));
}

/// Protobuf definitions for Lance Format v2
pub mod pbfile {
    #![allow(clippy::all)]
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]
    #![allow(improper_ctypes)]
    #![allow(clippy::upper_case_acronyms)]
    #![allow(clippy::use_self)]
    include!(concat!(env!("OUT_DIR"), "/lance.file.v2.rs"));
}

/// These version/magic values are written at the end of Lance files (e.g. versions/1.version)
pub const MAJOR_VERSION: i16 = 0;
pub const MINOR_VERSION: i16 = 2;
pub const MAGIC: &[u8; 4] = b"LANC";
