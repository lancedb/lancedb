// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use lancedb::table::Reference;
use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi(object)]
pub struct JsBranchReference {
    pub branch: String,
    pub version: Option<i64>,
}

pub type JsReference = Either3<i64, String, JsBranchReference>;

pub(crate) fn parse_version(value: i64, field_name: &str) -> napi::Result<u64> {
    u64::try_from(value)
        .map_err(|_| napi::Error::from_reason(format!("{field_name} must be non-negative")))
}

pub fn parse_reference(reference: JsReference) -> napi::Result<Reference> {
    match reference {
        Either3::A(version) => Ok(Reference::VersionNumber(parse_version(version, "version")?)),
        Either3::B(tag) => Ok(Reference::Tag(tag)),
        Either3::C(branch) => Ok(Reference::Version(
            Some(branch.branch),
            branch
                .version
                .map(|version| parse_version(version, "branch version"))
                .transpose()?,
        )),
    }
}

pub fn parse_optional_reference(reference: Option<JsReference>) -> napi::Result<Option<Reference>> {
    reference.map(parse_reference).transpose()
}
