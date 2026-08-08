// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use bytes::Buf;
use quick_xml::de;

use crate::core::parse_error;
use crate::core::*;
use opendal_core::EntryMode;
use opendal_core::Error;
use opendal_core::Metadata;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::oio::PageContext;
use opendal_core::raw::*;

pub type S3Listers = ThreeWays<
    oio::PageLister<S3ListerV1>,
    oio::PageLister<S3ListerV2>,
    oio::PageLister<S3ObjectVersionsLister>,
>;

/// S3ListerV1 implements ListObjectV1 for s3 backend.
pub struct S3ListerV1 {
    core: Arc<S3Core>,
    ctx: OperationContext,

    path: String,
    args: OpList,

    delimiter: &'static str,
    /// marker can also be used as `start-after` for list objects v1.
    /// We will use it as `start-after` for the first page and then ignore
    /// it in the following pages.
    first_marker: String,
}

impl S3ListerV1 {
    pub fn new(core: Arc<S3Core>, ctx: OperationContext, path: &str, args: OpList) -> Self {
        let delimiter = if args.recursive() { "" } else { "/" };
        let first_marker = args
            .start_after()
            .map(|start_after| build_abs_path(&core.root, start_after))
            .unwrap_or_default();

        Self {
            core,
            ctx,

            path: path.to_string(),
            args,
            delimiter,
            first_marker,
        }
    }
}

impl oio::PageList for S3ListerV1 {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .s3_list_objects_v1(
                &self.ctx,
                &self.path,
                // `marker` is used as `start-after` for the first page.
                if !ctx.token.is_empty() {
                    &ctx.token
                } else {
                    &self.first_marker
                },
                self.delimiter,
                self.args.limit(),
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp));
        }
        let bs = resp.into_body();

        let output: ListObjectsOutputV1 = de::from_reader(bs.reader())
            .map_err(new_xml_deserialize_error)
            // Allow S3 list to retry on XML deserialization errors.
            //
            // This is because the S3 list API may return incomplete XML data under high load.
            // We are confident that our XML decoding logic is correct. When this error occurs,
            // we allow retries to obtain the correct data.
            .map_err(Error::set_temporary)?;

        // Try our best to check whether this list is done.
        //
        // - Check `is_truncated`
        // - Check the length of `common_prefixes` and `contents` (very rare case)
        ctx.done = if let Some(is_truncated) = output.is_truncated {
            !is_truncated
        } else {
            output.common_prefixes.is_empty() && output.contents.is_empty()
        };
        // Try out best to find the next marker.
        //
        // - Check `next-marker`
        // - Check the last object key
        // - Check the last common prefix
        ctx.token = if let Some(next_marker) = &output.next_marker {
            next_marker.clone()
        } else if let Some(content) = output.contents.last() {
            content.key.clone()
        } else if let Some(prefix) = output.common_prefixes.last() {
            prefix.prefix.clone()
        } else {
            "".to_string()
        };

        for prefix in output.common_prefixes {
            let de = oio::Entry::new(
                &build_rel_path(&self.core.root, &prefix.prefix),
                Metadata::new(EntryMode::DIR),
            );

            ctx.entries.push_back(de);
        }

        for object in output.contents {
            let mut path = build_rel_path(&self.core.root, &object.key);
            if path.is_empty() {
                path = "/".to_string();
            }

            let mut meta = Metadata::new(EntryMode::from_path(&path));
            meta.set_is_current(true);
            if let Some(etag) = &object.etag {
                meta.set_etag(etag);
            }
            meta.set_content_length(object.size);

            // object.last_modified provides more precise time that contains
            // nanosecond, let's trim them.
            meta.set_last_modified(object.last_modified.parse::<Timestamp>()?);

            let de = oio::Entry::with(path, meta);
            ctx.entries.push_back(de);
        }

        Ok(())
    }
}

/// S3ListerV2 implements ListObjectV2 for s3 backend.
pub struct S3ListerV2 {
    core: Arc<S3Core>,
    ctx: OperationContext,

    path: String,
    args: OpList,

    delimiter: &'static str,
    abs_start_after: Option<String>,
}

impl S3ListerV2 {
    pub fn new(core: Arc<S3Core>, ctx: OperationContext, path: &str, args: OpList) -> Self {
        let delimiter = if args.recursive() { "" } else { "/" };
        let abs_start_after = args
            .start_after()
            .map(|start_after| build_abs_path(&core.root, start_after));

        Self {
            core,
            ctx,

            path: path.to_string(),
            args,
            delimiter,
            abs_start_after,
        }
    }
}

impl oio::PageList for S3ListerV2 {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .s3_list_objects_v2(
                &self.ctx,
                &self.path,
                &ctx.token,
                self.delimiter,
                self.args.limit(),
                // start after should only be set for the first page.
                if ctx.token.is_empty() {
                    self.abs_start_after.clone()
                } else {
                    None
                },
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp));
        }
        let bs = resp.into_body();

        let output: ListObjectsOutputV2 = de::from_reader(bs.reader())
            .map_err(new_xml_deserialize_error)
            // Allow S3 list to retry on XML deserialization errors.
            //
            // This is because the S3 list API may return incomplete XML data under high load.
            // We are confident that our XML decoding logic is correct. When this error occurs,
            // we allow retries to obtain the correct data.
            .map_err(Error::set_temporary)?;

        // Try our best to check whether this list is done.
        //
        // - Check `is_truncated`
        // - Check `next_continuation_token`
        // - Check the length of `common_prefixes` and `contents` (very rare case)
        ctx.done = if let Some(is_truncated) = output.is_truncated {
            !is_truncated
        } else if let Some(next_continuation_token) = output.next_continuation_token.as_ref() {
            next_continuation_token.is_empty()
        } else {
            output.common_prefixes.is_empty() && output.contents.is_empty()
        };
        ctx.token = output.next_continuation_token.clone().unwrap_or_default();

        for prefix in output.common_prefixes {
            let de = oio::Entry::new(
                &build_rel_path(&self.core.root, &prefix.prefix),
                Metadata::new(EntryMode::DIR),
            );

            ctx.entries.push_back(de);
        }

        for object in output.contents {
            let mut path = build_rel_path(&self.core.root, &object.key);
            if path.is_empty() {
                path = "/".to_string();
            }

            let mut meta = Metadata::new(EntryMode::from_path(&path));
            meta.set_is_current(true);
            if let Some(etag) = &object.etag {
                meta.set_etag(etag);
            }
            meta.set_content_length(object.size);

            // object.last_modified provides more precise time that contains
            // nanosecond, let's trim them.
            meta.set_last_modified(object.last_modified.parse::<Timestamp>()?);

            let de = oio::Entry::with(path, meta);
            ctx.entries.push_back(de);
        }

        Ok(())
    }
}

/// refer: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
pub struct S3ObjectVersionsLister {
    core: Arc<S3Core>,
    ctx: OperationContext,

    prefix: String,
    args: OpList,

    delimiter: &'static str,
    abs_start_after: Option<String>,
}

impl S3ObjectVersionsLister {
    pub fn new(core: Arc<S3Core>, ctx: OperationContext, path: &str, args: OpList) -> Self {
        let delimiter = if args.recursive() { "" } else { "/" };
        let abs_start_after = args
            .start_after()
            .map(|start_after| build_abs_path(&core.root, start_after));

        Self {
            core,
            ctx,
            prefix: path.to_string(),
            args,
            delimiter,
            abs_start_after,
        }
    }
}

impl oio::PageList for S3ObjectVersionsLister {
    async fn next_page(&self, ctx: &mut PageContext) -> Result<()> {
        let markers = ctx.token.rsplit_once(" ");
        let (key_marker, version_id_marker) = if let Some(data) = markers {
            data
        } else if let Some(start_after) = &self.abs_start_after {
            (start_after.as_str(), "")
        } else {
            ("", "")
        };

        let resp = self
            .core
            .s3_list_object_versions(
                &self.ctx,
                &self.prefix,
                self.delimiter,
                self.args.limit(),
                key_marker,
                version_id_marker,
            )
            .await?;
        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp));
        }

        let body = resp.into_body();
        let output: ListObjectVersionsOutput = de::from_reader(body.reader())
            .map_err(new_xml_deserialize_error)
            // Allow S3 list to retry on XML deserialization errors.
            //
            // This is because the S3 list API may return incomplete XML data under high load.
            // We are confident that our XML decoding logic is correct. When this error occurs,
            // we allow retries to obtain the correct data.
            .map_err(Error::set_temporary)?;

        ctx.done = if let Some(is_truncated) = output.is_truncated {
            !is_truncated
        } else {
            false
        };
        ctx.token = format!(
            "{} {}",
            output.next_key_marker.unwrap_or_default(),
            output.next_version_id_marker.unwrap_or_default()
        );

        for prefix in output.common_prefixes {
            let de = oio::Entry::new(
                &build_rel_path(&self.core.root, &prefix.prefix),
                Metadata::new(EntryMode::DIR),
            );
            ctx.entries.push_back(de);
        }

        for version_object in output.version {
            // `list` must be additive, so we need to include the latest version object
            // even if `versions` is not enabled.
            //
            // Here we skip all non-latest version objects if `versions` is not enabled.
            if !(self.args.versions() || version_object.is_latest) {
                continue;
            }

            let mut path = build_rel_path(&self.core.root, &version_object.key);
            if path.is_empty() {
                path = "/".to_owned();
            }

            let mut meta = Metadata::new(EntryMode::from_path(&path));
            meta.set_version(&version_object.version_id);
            meta.set_is_current(version_object.is_latest);
            meta.set_content_length(version_object.size);
            meta.set_last_modified(version_object.last_modified.parse::<Timestamp>()?);

            if let Some(etag) = version_object.etag {
                meta.set_etag(&etag);
            }

            let entry = oio::Entry::new(&path, meta);
            ctx.entries.push_back(entry);
        }

        if self.args.deleted() {
            for delete_marker in output.delete_marker {
                let mut path = build_rel_path(&self.core.root, &delete_marker.key);
                if path.is_empty() {
                    path = "/".to_owned();
                }

                let mut meta = Metadata::new(EntryMode::from_path(&path));
                meta.set_version(&delete_marker.version_id);
                meta.set_is_deleted(true);
                meta.set_is_current(delete_marker.is_latest);
                meta.set_last_modified(delete_marker.last_modified.parse::<Timestamp>()?);

                let entry = oio::Entry::new(&path, meta);
                ctx.entries.push_back(entry);
            }
        }

        Ok(())
    }
}
