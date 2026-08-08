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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Write;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use constants::X_AMZ_META_PREFIX;
use crc_fast::CrcAlgorithm;
use crc_fast::Digest as CrcDigest;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::header::CACHE_CONTROL;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_ENCODING;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::HOST;
use http::header::HeaderName;
use http::header::IF_MATCH;
use http::header::IF_MODIFIED_SINCE;
use http::header::IF_NONE_MATCH;
use http::header::IF_UNMODIFIED_SINCE;
use reqsign_aws_v4::Credential;
use reqsign_core::{Context, Signer};
use serde::Deserialize;
use serde::Serialize;

use opendal_core::raw::*;
use opendal_core::*;

pub mod constants {
    pub const X_AMZ_COPY_SOURCE: &str = "x-amz-copy-source";
    pub const X_AMZ_COPY_SOURCE_RANGE: &str = "x-amz-copy-source-range";

    pub const X_AMZ_SERVER_SIDE_ENCRYPTION: &str = "x-amz-server-side-encryption";
    pub const X_AMZ_SERVER_REQUEST_PAYER: (&str, &str) = ("x-amz-request-payer", "requester");
    pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: &str =
        "x-amz-server-side-encryption-customer-algorithm";
    pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: &str =
        "x-amz-server-side-encryption-customer-key";
    pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: &str =
        "x-amz-server-side-encryption-customer-key-md5";
    pub const X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID: &str =
        "x-amz-server-side-encryption-aws-kms-key-id";
    pub const X_AMZ_STORAGE_CLASS: &str = "x-amz-storage-class";

    pub const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: &str =
        "x-amz-copy-source-server-side-encryption-customer-algorithm";
    pub const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: &str =
        "x-amz-copy-source-server-side-encryption-customer-key";
    pub const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: &str =
        "x-amz-copy-source-server-side-encryption-customer-key-md5";

    pub const X_AMZ_WRITE_OFFSET_BYTES: &str = "x-amz-write-offset-bytes";

    pub const X_AMZ_META_PREFIX: &str = "x-amz-meta-";

    pub const X_AMZ_VERSION_ID: &str = "x-amz-version-id";
    pub const X_AMZ_OBJECT_SIZE: &str = "x-amz-object-size";

    pub const X_AMZ_ACL: &str = "x-amz-acl";

    pub const RESPONSE_CONTENT_DISPOSITION: &str = "response-content-disposition";
    pub const RESPONSE_CONTENT_TYPE: &str = "response-content-type";
    pub const RESPONSE_CACHE_CONTROL: &str = "response-cache-control";

    pub const S3_QUERY_VERSION_ID: &str = "versionId";
}

pub struct S3Core {
    pub info: ServiceInfo,
    pub capability: Capability,

    pub bucket: String,
    pub endpoint: String,
    pub root: String,
    pub server_side_encryption: Option<HeaderValue>,
    pub server_side_encryption_aws_kms_key_id: Option<HeaderValue>,
    pub server_side_encryption_customer_algorithm: Option<HeaderValue>,
    pub server_side_encryption_customer_key: Option<HeaderValue>,
    pub server_side_encryption_customer_key_md5: Option<HeaderValue>,
    pub default_storage_class: Option<HeaderValue>,
    pub skip_signature: bool,
    pub disable_list_objects_v2: bool,
    pub enable_request_payer: bool,
    pub default_acl: Option<String>,

    pub signer: Signer<Credential>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
}

pub(crate) struct S3UploadPartCopyRequest<'a> {
    pub(crate) from: &'a str,
    pub(crate) to: &'a str,
    pub(crate) source_version: Option<&'a str>,
    pub(crate) upload_id: &'a str,
    pub(crate) part_number: usize,
    pub(crate) range: BytesRange,
}

fn format_crc32c_iter(body: Buffer) -> String {
    let mut digest = CrcDigest::new(CrcAlgorithm::Crc32Iscsi);
    body.for_each(|b| digest.update(&b));

    let crc = digest.finalize() as u32;
    BASE64_STANDARD.encode(crc.to_be_bytes())
}

impl Debug for S3Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Core")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl S3Core {
    fn signer(&self, ctx: &OperationContext) -> Signer<Credential> {
        self.signer.clone().with_context(
            Context::new()
                .with_file_read(reqsign_file_read_tokio::TokioFileRead)
                .with_http_send(ctx.http_transport().clone())
                .with_env(reqsign_core::OsEnv),
        )
    }

    pub async fn sign_query<T>(
        &self,
        ctx: &OperationContext,
        req: Request<T>,
        duration: Duration,
    ) -> Result<Request<T>> {
        if self.skip_signature {
            return Ok(req);
        }

        // Sign the request with presigned URL
        let (mut parts, body) = req.into_parts();

        self.signer(ctx)
            .sign(&mut parts, Some(duration))
            .await
            .map_err(|e| new_request_sign_error(e.into()))?;

        // Always remove host header, let users' client to set it based on HTTP
        // version.
        //
        // As discussed in <https://github.com/seanmonstar/reqwest/issues/1809>,
        // google server could send RST_STREAM of PROTOCOL_ERROR if our request
        // contains host header.
        parts.headers.remove(HOST);

        Ok(Request::from_parts(parts, body))
    }

    pub async fn send(
        &self,
        ctx: &OperationContext,
        req: Request<Buffer>,
    ) -> Result<Response<Buffer>> {
        if self.skip_signature {
            return ctx.http_transport().send(req).await;
        }

        let (mut parts, body) = req.into_parts();

        self.signer(ctx)
            .sign(&mut parts, None)
            .await
            .map_err(|e| new_request_sign_error(e.into()))?;

        // Always remove host header, let users' client to set it based on HTTP
        // version.
        //
        // As discussed in <https://github.com/seanmonstar/reqwest/issues/1809>,
        // google server could send RST_STREAM of PROTOCOL_ERROR if our request
        // contains host header.
        parts.headers.remove(HOST);

        ctx.http_transport()
            .send(Request::from_parts(parts, body))
            .await
    }

    pub async fn fetch(
        &self,
        ctx: &OperationContext,
        req: Request<Buffer>,
    ) -> Result<Response<HttpBody>> {
        if self.skip_signature {
            return ctx.http_transport().fetch(req).await;
        }

        let (mut parts, body) = req.into_parts();

        self.signer(ctx)
            .sign(&mut parts, None)
            .await
            .map_err(|e| new_request_sign_error(e.into()))?;

        // Always remove host header, let users' client to set it based on HTTP
        // version.
        //
        // As discussed in <https://github.com/seanmonstar/reqwest/issues/1809>,
        // google server could send RST_STREAM of PROTOCOL_ERROR if our request
        // contains host header.
        parts.headers.remove(HOST);

        ctx.http_transport()
            .fetch(Request::from_parts(parts, body))
            .await
    }

    /// # Note
    ///
    /// header like X_AMZ_SERVER_SIDE_ENCRYPTION doesn't need to set while
    /// get or stat.
    pub fn insert_sse_headers(
        &self,
        mut req: http::request::Builder,
        is_write: bool,
    ) -> http::request::Builder {
        if is_write {
            if let Some(v) = &self.server_side_encryption {
                let mut v = v.clone();
                v.set_sensitive(true);

                req = req.header(
                    HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION),
                    v,
                )
            }
            if let Some(v) = &self.server_side_encryption_aws_kms_key_id {
                let mut v = v.clone();
                v.set_sensitive(true);

                req = req.header(
                    HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID),
                    v,
                )
            }
        }

        if let Some(v) = &self.server_side_encryption_customer_algorithm {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM),
                v,
            )
        }
        if let Some(v) = &self.server_side_encryption_customer_key {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY),
                v,
            )
        }
        if let Some(v) = &self.server_side_encryption_customer_key_md5 {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5),
                v,
            )
        }

        req
    }
    pub fn calculate_checksum(&self, body: &Buffer) -> Option<String> {
        match self.checksum_algorithm {
            None => None,
            Some(ChecksumAlgorithm::Crc32c) => Some(format_crc32c_iter(body.clone())),
            Some(ChecksumAlgorithm::Md5) => Some(format_content_md5_iter(body.clone())),
        }
    }
    pub fn insert_checksum_header(
        &self,
        mut req: http::request::Builder,
        checksum: &str,
    ) -> http::request::Builder {
        if let Some(checksum_algorithm) = self.checksum_algorithm.as_ref() {
            req = req.header(checksum_algorithm.to_header_name(), checksum);
        }
        req
    }

    pub fn insert_checksum_type_header(
        &self,
        mut req: http::request::Builder,
    ) -> http::request::Builder {
        if let Some(checksum_algorithm) = self.checksum_algorithm.as_ref() {
            req = req.header("x-amz-checksum-algorithm", checksum_algorithm.to_string());
        }
        req
    }

    pub fn insert_metadata_headers(
        &self,
        mut req: http::request::Builder,
        size: Option<u64>,
        args: &OpWrite,
    ) -> http::request::Builder {
        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size.to_string())
        }

        if let Some(mime) = args.content_type() {
            req = req.header(CONTENT_TYPE, mime)
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, pos)
        }

        if let Some(encoding) = args.content_encoding() {
            req = req.header(CONTENT_ENCODING, encoding);
        }

        if let Some(cache_control) = args.cache_control() {
            req = req.header(CACHE_CONTROL, cache_control)
        }

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        if args.if_not_exists() {
            req = req.header(IF_NONE_MATCH, "*");
        }

        // Set storage class header
        if let Some(v) = &self.default_storage_class {
            req = req.header(HeaderName::from_static(constants::X_AMZ_STORAGE_CLASS), v);
        }

        // Set user metadata headers.
        if let Some(user_metadata) = args.user_metadata() {
            for (key, value) in user_metadata {
                req = req.header(format!("{X_AMZ_META_PREFIX}{key}"), value)
            }
        }

        // Set ACL header.
        if let Some(acl) = &self.default_acl {
            req = req.header(constants::X_AMZ_ACL, acl);
        }
        req
    }

    pub fn insert_request_payer_header(
        &self,
        mut req: http::request::Builder,
    ) -> http::request::Builder {
        if self.enable_request_payer {
            req = req.header(
                HeaderName::from_static(constants::X_AMZ_SERVER_REQUEST_PAYER.0),
                HeaderValue::from_static(constants::X_AMZ_SERVER_REQUEST_PAYER.1),
            );
        }
        req
    }
}

impl S3Core {
    pub fn s3_head_object_request(&self, path: &str, args: OpStat) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        // Add query arguments to the URL based on response overrides
        let mut query_args = Vec::new();
        if let Some(override_content_disposition) = args.override_content_disposition() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CONTENT_DISPOSITION,
                percent_encode_path(override_content_disposition)
            ))
        }
        if let Some(override_content_type) = args.override_content_type() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CONTENT_TYPE,
                percent_encode_path(override_content_type)
            ))
        }
        if let Some(override_cache_control) = args.override_cache_control() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CACHE_CONTROL,
                percent_encode_path(override_cache_control)
            ))
        }
        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::S3_QUERY_VERSION_ID,
                percent_encode_path(version)
            ))
        }
        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::head(&url);

        req = self.insert_sse_headers(req, false);

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }
        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        if let Some(if_modified_since) = args.if_modified_since() {
            req = req.header(IF_MODIFIED_SINCE, if_modified_since.format_http_date());
        }
        if let Some(if_unmodified_since) = args.if_unmodified_since() {
            req = req.header(IF_UNMODIFIED_SINCE, if_unmodified_since.format_http_date());
        }

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        // Inject operation to the request.
        req = req
            .extension(Operation::Stat)
            .extension(ServiceOperation("HeadObject"));

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn s3_get_object_request(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        // Construct headers to add to the request
        let mut url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        // Add query arguments to the URL based on response overrides
        let mut query_args = Vec::new();
        if let Some(override_content_disposition) = args.override_content_disposition() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CONTENT_DISPOSITION,
                percent_encode_path(override_content_disposition)
            ))
        }
        if let Some(override_content_type) = args.override_content_type() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CONTENT_TYPE,
                percent_encode_path(override_content_type)
            ))
        }
        if let Some(override_cache_control) = args.override_cache_control() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CACHE_CONTROL,
                percent_encode_path(override_cache_control)
            ))
        }
        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::S3_QUERY_VERSION_ID,
                percent_encode_path(version)
            ))
        }
        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        if let Some(if_modified_since) = args.if_modified_since() {
            req = req.header(IF_MODIFIED_SINCE, if_modified_since.format_http_date());
        }

        if let Some(if_unmodified_since) = args.if_unmodified_since() {
            req = req.header(IF_UNMODIFIED_SINCE, if_unmodified_since.format_http_date());
        }

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        // Set SSE headers.
        // TODO: how will this work with presign?
        req = self.insert_sse_headers(req, false);

        // Inject operation to the request.
        req = req
            .extension(Operation::Read)
            .extension(ServiceOperation("GetObject"));

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn s3_get_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let req = self.s3_get_object_request(path, range, args)?;
        self.fetch(ctx, req).await
    }

    pub fn s3_put_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::put(&url);

        req = self.insert_metadata_headers(req, size, args);

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        // Calculate Checksum.
        if let Some(checksum) = self.calculate_checksum(&body) {
            // Set Checksum header.
            req = self.insert_checksum_header(req, &checksum);
        }

        // Inject operation to the request.
        req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("PutObject"));

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn s3_append_object_request(
        &self,
        path: &str,
        position: u64,
        size: u64,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));
        let mut req = Request::put(&url);

        // Only include full metadata headers when creating a new object via append (position == 0)
        // For existing objects or subsequent appends, only include content-length
        if position == 0 {
            req = self.insert_metadata_headers(req, Some(size), args);
        } else {
            req = req.header(CONTENT_LENGTH, size.to_string());
        }

        req = req.header(constants::X_AMZ_WRITE_OFFSET_BYTES, position.to_string());

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        // Calculate Checksum.
        if let Some(checksum) = self.calculate_checksum(&body) {
            // Set Checksum header.
            req = self.insert_checksum_header(req, &checksum);
        }

        // Inject operation to the request.
        req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("PutObject"));

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn s3_head_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> Result<Response<Buffer>> {
        let req = self.s3_head_object_request(path, args)?;
        self.send(ctx, req).await
    }

    pub fn s3_delete_object_request(&self, path: &str, args: &OpDelete) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut query_args = Vec::new();

        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::S3_QUERY_VERSION_ID,
                percent_encode_path(version)
            ))
        }

        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::delete(&url);

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        let req = req
            // Inject operation to the request.
            .extension(Operation::Delete)
            .extension(ServiceOperation("DeleteObject"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn s3_delete_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: &OpDelete,
    ) -> Result<Response<Buffer>> {
        let req = self.s3_delete_object_request(path, args)?;
        self.send(ctx, req).await
    }

    pub async fn s3_copy_object(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: &OpCopy,
    ) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let source = format!("{}/{}", self.bucket, percent_encode_path(&from));
        let source = if let Some(version) = args.source_version() {
            QueryPairsWriter::new(&source)
                .push(
                    constants::S3_QUERY_VERSION_ID,
                    &percent_encode_path(version),
                )
                .finish()
        } else {
            source
        };
        let target = format!("{}/{}", self.endpoint, percent_encode_path(&to));

        let mut req = Request::put(&target);

        // Set conditional copy headers.
        if args.if_not_exists() {
            req = req.header(IF_NONE_MATCH, "*");
        }
        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        if let Some(v) = &self.server_side_encryption_customer_algorithm {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(
                    constants::X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
                ),
                v,
            )
        }

        if let Some(v) = &self.server_side_encryption_customer_key {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(
                    constants::X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
                ),
                v,
            )
        }

        if let Some(v) = &self.server_side_encryption_customer_key_md5 {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(
                    constants::X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
                ),
                v,
            )
        }

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        let req = req
            // Inject operation to the request.
            .extension(Operation::Copy)
            .extension(ServiceOperation("CopyObject"))
            .header(constants::X_AMZ_COPY_SOURCE, &source)
            // AWS S3 accepts CopyObject without Content-Length, but some S3-compatible
            // providers, such as NetApp, require `Content-Length: 0` for its empty body.
            .header(CONTENT_LENGTH, 0)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn s3_list_objects_v1(
        &self,
        ctx: &OperationContext,
        path: &str,
        marker: &str,
        delimiter: &str,
        limit: Option<usize>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = QueryPairsWriter::new(&self.endpoint);

        if !p.is_empty() {
            url = url.push("prefix", &percent_encode_path(&p));
        }
        if !delimiter.is_empty() {
            url = url.push("delimiter", delimiter);
        }
        if let Some(limit) = limit {
            url = url.push("max-keys", &limit.to_string());
        }
        if !marker.is_empty() {
            url = url.push("marker", &percent_encode_path(marker));
        }

        let mut req = Request::get(url.finish());

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        let req = req
            // Inject operation to the request.
            .extension(Operation::List)
            .extension(ServiceOperation("ListObjects"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn s3_list_objects_v2(
        &self,
        ctx: &OperationContext,
        path: &str,
        continuation_token: &str,
        delimiter: &str,
        limit: Option<usize>,
        start_after: Option<String>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = QueryPairsWriter::new(&self.endpoint);
        url = url.push("list-type", "2");

        if !p.is_empty() {
            url = url.push("prefix", &percent_encode_path(&p));
        }
        if !delimiter.is_empty() {
            url = url.push("delimiter", delimiter);
        }
        if let Some(limit) = limit {
            url = url.push("max-keys", &limit.to_string());
        }
        if let Some(start_after) = start_after {
            url = url.push("start-after", &percent_encode_path(&start_after));
        }
        if !continuation_token.is_empty() {
            // AWS S3 could return continuation-token that contains `=`
            // which could lead `reqsign` parse query wrongly.
            // URL encode continuation-token before starting signing so that
            // our signer will not be confused.
            url = url.push(
                "continuation-token",
                &percent_encode_path(continuation_token),
            );
        }

        let mut req = Request::get(url.finish());

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        let req = req
            // Inject operation to the request.
            .extension(Operation::List)
            .extension(ServiceOperation("ListObjectsV2"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn s3_initiate_multipart_upload(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}?uploads", self.endpoint, percent_encode_path(&p));

        let mut req = Request::post(&url);

        if let Some(mime) = args.content_type() {
            req = req.header(CONTENT_TYPE, mime)
        }

        if let Some(content_disposition) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, content_disposition)
        }

        if let Some(cache_control) = args.cache_control() {
            req = req.header(CACHE_CONTROL, cache_control)
        }

        if let Some(content_encoding) = args.content_encoding() {
            req = req.header(CONTENT_ENCODING, content_encoding)
        }

        // Set storage class header
        if let Some(v) = &self.default_storage_class {
            req = req.header(HeaderName::from_static(constants::X_AMZ_STORAGE_CLASS), v);
        }

        // Set user metadata headers.
        if let Some(user_metadata) = args.user_metadata() {
            for (key, value) in user_metadata {
                req = req.header(format!("{X_AMZ_META_PREFIX}{key}"), value)
            }
        }

        // also set acl header if default_acl is set.
        if let Some(acl) = &self.default_acl {
            req = req.header(constants::X_AMZ_ACL, acl);
        }

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        // Set checksum type headers.
        // For multipart upload creation, only CRC32 | CRC32C | SHA1 | SHA256 | CRC64NVME are accepted.
        // Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
        if matches!(self.checksum_algorithm, Some(ChecksumAlgorithm::Md5)) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "checksum_algorithm \"md5\" is not supported for multipart uploads. \
                 S3 CreateMultipartUpload only accepts: CRC32, CRC32C, SHA1, SHA256.",
            ));
        }
        req = self.insert_checksum_type_header(req);

        // Inject operation to the request.
        req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("CreateMultipartUpload"));

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn s3_initiate_multipart_copy(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}?uploads", self.endpoint, percent_encode_path(&p));

        let mut req = Request::post(&url);

        // Set storage class header.
        if let Some(v) = &self.default_storage_class {
            req = req.header(HeaderName::from_static(constants::X_AMZ_STORAGE_CLASS), v);
        }

        // Also set acl header if default_acl is set.
        if let Some(acl) = &self.default_acl {
            req = req.header(constants::X_AMZ_ACL, acl);
        }

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        let req = req
            .extension(Operation::Copy)
            .extension(ServiceOperation("CreateMultipartUpload"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub(crate) fn s3_upload_part_copy_request(
        &self,
        input: S3UploadPartCopyRequest<'_>,
    ) -> Result<Request<Buffer>> {
        let from = build_abs_path(&self.root, input.from);
        let to = build_abs_path(&self.root, input.to);

        let source = format!("{}/{}", self.bucket, percent_encode_path(&from));
        let source = if let Some(version) = input.source_version {
            QueryPairsWriter::new(&source)
                .push(
                    constants::S3_QUERY_VERSION_ID,
                    &percent_encode_path(version),
                )
                .finish()
        } else {
            source
        };

        let url = format!(
            "{}/{}?partNumber={}&uploadId={}",
            self.endpoint,
            percent_encode_path(&to),
            input.part_number,
            percent_encode_path(input.upload_id)
        );

        let mut req = Request::put(&url);

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        if let Some(v) = &self.server_side_encryption_customer_algorithm {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(
                    constants::X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
                ),
                v,
            )
        }

        if let Some(v) = &self.server_side_encryption_customer_key {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(
                    constants::X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
                ),
                v,
            )
        }

        if let Some(v) = &self.server_side_encryption_customer_key_md5 {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(
                    constants::X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
                ),
                v,
            )
        }

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        let req = req
            .extension(Operation::Copy)
            .extension(ServiceOperation("UploadPartCopy"))
            .header(constants::X_AMZ_COPY_SOURCE, source)
            .header(constants::X_AMZ_COPY_SOURCE_RANGE, input.range.to_header())
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn s3_upload_part_request(
        &self,
        path: &str,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
        checksum: Option<String>,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?partNumber={}&uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            part_number,
            percent_encode_path(upload_id)
        );

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, size);

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        if let Some(checksum) = checksum {
            // Set Checksum header.
            req = self.insert_checksum_header(req, &checksum);
        }

        // Inject operation to the request.
        req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("UploadPart"));

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn s3_complete_multipart_upload(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
        parts: Vec<CompleteMultipartUploadRequestPart>,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            percent_encode_path(upload_id)
        );

        let mut req = Request::post(&url);

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        let content = quick_xml::se::to_string(&CompleteMultipartUploadRequest { part: parts })
            .map_err(new_xml_serialize_error)?;
        // Make sure content length has been set to avoid post with chunked encoding.
        req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        req = req.header(CONTENT_TYPE, "application/xml");

        // Set conditional write headers.
        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }
        if args.if_not_exists() {
            req = req.header(IF_NONE_MATCH, "*");
        }

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        // Inject operation to the request.
        req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("CompleteMultipartUpload"));

        let req = req
            .body(Buffer::from(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn s3_complete_multipart_copy(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
        parts: Vec<CompleteMultipartUploadRequestPart>,
        args: &OpCopy,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            percent_encode_path(upload_id)
        );

        let mut req = Request::post(&url);

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        let content = quick_xml::se::to_string(&CompleteMultipartUploadRequest { part: parts })
            .map_err(new_xml_serialize_error)?;
        // Make sure content length has been set to avoid post with chunked encoding.
        req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        req = req.header(CONTENT_TYPE, "application/xml");

        if args.if_not_exists() {
            req = req.header(IF_NONE_MATCH, "*");
        }
        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        let req = req
            .extension(Operation::Copy)
            .extension(ServiceOperation("CompleteMultipartUpload"))
            .body(Buffer::from(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    /// Abort an on-going multipart upload.
    pub async fn s3_abort_multipart_upload(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            percent_encode_path(upload_id)
        );

        let mut req = Request::delete(&url);

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        let req = req
            // Inject operation to the request.
            .extension(Operation::Write)
            .extension(ServiceOperation("AbortMultipartUpload"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    /// Abort an on-going multipart copy.
    pub async fn s3_abort_multipart_copy(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            percent_encode_path(upload_id)
        );

        let mut req = Request::delete(&url);

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        let req = req
            .extension(Operation::Copy)
            .extension(ServiceOperation("AbortMultipartUpload"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn s3_delete_objects(
        &self,
        ctx: &OperationContext,
        paths: &[(String, OpDelete)],
    ) -> Result<Response<Buffer>> {
        let url = format!("{}/?delete", self.endpoint);

        let mut req = Request::post(&url);

        let content = quick_xml::se::to_string(&DeleteObjectsRequest {
            quiet: true,
            object: paths
                .iter()
                .map(|(path, op)| DeleteObjectsRequestObject {
                    key: build_abs_path(&self.root, path),
                    version_id: op.version().map(|v| v.to_owned()),
                })
                .collect(),
        })
        .map_err(new_xml_serialize_error)?;

        // Make sure content length has been set to avoid post with chunked encoding.
        req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        req = req.header(CONTENT_TYPE, "application/xml");
        // Set content-md5 as required by API.
        req = req.header("CONTENT-MD5", format_content_md5(content.as_bytes()));

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        // Inject operation to the request.
        req = req
            .extension(Operation::Delete)
            .extension(ServiceOperation("DeleteObjects"));

        let req = req
            .body(Buffer::from(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn s3_list_object_versions(
        &self,
        ctx: &OperationContext,
        prefix: &str,
        delimiter: &str,
        limit: Option<usize>,
        key_marker: &str,
        version_id_marker: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, prefix);

        let mut url = format!("{}?versions", self.endpoint);
        if !p.is_empty() {
            write!(url, "&prefix={}", percent_encode_path(p.as_str()))
                .expect("write into string must succeed");
        }
        if !delimiter.is_empty() {
            write!(url, "&delimiter={delimiter}").expect("write into string must succeed");
        }

        if let Some(limit) = limit {
            write!(url, "&max-keys={limit}").expect("write into string must succeed");
        }
        if !key_marker.is_empty() {
            write!(url, "&key-marker={}", percent_encode_path(key_marker))
                .expect("write into string must succeed");
        }
        if !version_id_marker.is_empty() {
            write!(
                url,
                "&version-id-marker={}",
                percent_encode_path(version_id_marker)
            )
            .expect("write into string must succeed");
        }

        let mut req = Request::get(&url);

        // Set request payer header if enabled.
        req = self.insert_request_payer_header(req);

        let req = req
            // Inject operation to the request.
            .extension(Operation::List)
            .extension(ServiceOperation("ListObjectVersions"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }
}

/// Result of CreateMultipartUpload
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    pub upload_id: String,
}

/// Request of CompleteMultipartUploadRequest
#[derive(Default, Debug, Serialize)]
#[serde(default, rename = "CompleteMultipartUpload", rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequest {
    pub part: Vec<CompleteMultipartUploadRequestPart>,
}

#[derive(Clone, Default, Debug, Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequestPart {
    #[serde(rename = "PartNumber")]
    pub part_number: usize,
    /// # TODO
    ///
    /// quick-xml will do escape on `"` which leads to our serialized output is
    /// not the same as aws s3's example.
    ///
    /// Ideally, we could use `serialize_with` to address this (buf failed)
    ///
    /// ```ignore
    /// #[derive(Default, Debug, Serialize)]
    /// #[serde(default, rename_all = "PascalCase")]
    /// struct CompleteMultipartUploadRequestPart {
    ///     #[serde(rename = "PartNumber")]
    ///     part_number: usize,
    ///     #[serde(rename = "ETag", serialize_with = "partial_escape")]
    ///     etag: String,
    /// }
    ///
    /// fn partial_escape<S>(s: &str, ser: S) -> Result<S::Ok, S::Error>
    /// where
    ///     S: serde::Serializer,
    /// {
    ///     ser.serialize_str(&String::from_utf8_lossy(
    ///         &quick_xml::escape::partial_escape(s.as_bytes()),
    ///     ))
    /// }
    /// ```
    ///
    /// ref: <https://github.com/tafia/quick-xml/issues/362>
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "ChecksumCRC32C", skip_serializing_if = "Option::is_none")]
    pub checksum_crc32c: Option<String>,
}

/// Output of `CompleteMultipartUpload` operation
#[derive(Debug, Default, Deserialize)]
#[serde[default, rename_all = "PascalCase"]]
pub struct CompleteMultipartUploadResult {
    pub bucket: String,
    pub key: String,
    pub location: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    pub code: String,
    pub message: String,
    pub request_id: String,
}

/// Body of a `CopyObject` or `UploadPartCopy` response.
///
/// ref: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html#API_CopyObject_ResponseSyntax>
/// ref: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html#API_UploadPartCopy_ResponseSyntax>
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CopyObjectResult {
    #[serde(rename = "ETag")]
    pub etag: String,
    pub last_modified: String,
    pub code: String,
    pub message: String,
    pub request_id: String,
}

/// Request of DeleteObjects.
#[derive(Default, Debug, Serialize)]
#[serde(default, rename = "Delete", rename_all = "PascalCase")]
pub struct DeleteObjectsRequest {
    pub quiet: bool,
    pub object: Vec<DeleteObjectsRequestObject>,
}

#[derive(Default, Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteObjectsRequestObject {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// Result of DeleteObjects.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename = "DeleteResult", rename_all = "PascalCase")]
pub struct DeleteObjectsResult {
    pub error: Vec<DeleteObjectsResultError>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct DeleteObjectsResultError {
    pub code: String,
    pub key: String,
    pub message: String,
    pub version_id: Option<String>,
}

/// Output of ListBucket/ListObjects (a.k.a ListObjectsV1).
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectsOutputV1 {
    pub is_truncated: Option<bool>,
    /// ## Notes
    ///
    /// `next_marker` is returned only if we have the delimiter request parameter
    /// specified. If the response does not include the NextMarker element and it
    /// is truncated, we should use the value of the last Key element in the
    /// response as the marker parameter in the subsequent request to get the
    /// next set of object keys.
    ///
    /// If the contents is empty, we should find common_prefixes instead.
    pub next_marker: Option<String>,
    pub common_prefixes: Vec<OutputCommonPrefix>,
    pub contents: Vec<ListObjectsOutputContent>,
}

/// Output of ListBucketV2/ListObjectsV2.
///
/// ## Note
///
/// Use `Option` in `is_truncated` and `next_continuation_token` to make
/// the behavior more clear so that we can be compatible to more s3 services.
///
/// And enable `serde(default)` so that we can keep going even when some field
/// is not exist.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectsOutputV2 {
    pub is_truncated: Option<bool>,
    pub next_continuation_token: Option<String>,
    pub common_prefixes: Vec<OutputCommonPrefix>,
    pub contents: Vec<ListObjectsOutputContent>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectsOutputContent {
    pub key: String,
    pub size: u64,
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: Option<String>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OutputCommonPrefix {
    pub prefix: String,
}

/// Output of ListObjectVersions
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectVersionsOutput {
    pub is_truncated: Option<bool>,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
    pub common_prefixes: Vec<OutputCommonPrefix>,
    pub version: Vec<ListObjectVersionsOutputVersion>,
    pub delete_marker: Vec<ListObjectVersionsOutputDeleteMarker>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectVersionsOutputVersion {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub size: u64,
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: Option<String>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectVersionsOutputDeleteMarker {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
}

pub enum ChecksumAlgorithm {
    Crc32c,
    /// Mapping to the `Content-MD5` header from S3.
    Md5,
}
impl ChecksumAlgorithm {
    pub fn to_header_name(&self) -> HeaderName {
        match self {
            Self::Crc32c => HeaderName::from_static("x-amz-checksum-crc32c"),
            Self::Md5 => HeaderName::from_static("content-md5"),
        }
    }
}
impl Display for ChecksumAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Crc32c => "CRC32C",
                Self::Md5 => "MD5",
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use bytes::Bytes;

    use super::*;

    /// This example is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html#API_CreateMultipartUpload_Examples
    #[test]
    fn test_deserialize_initiate_multipart_upload_result() {
        let bs = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Bucket>example-bucket</Bucket>
              <Key>example-object</Key>
              <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
            </InitiateMultipartUploadResult>"#,
        );

        let out: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!(
            out.upload_id,
            "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"
        )
    }

    /// This example is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html#API_CompleteMultipartUpload_Examples
    #[test]
    fn test_serialize_complete_multipart_upload_request() {
        let req = CompleteMultipartUploadRequest {
            part: vec![
                CompleteMultipartUploadRequestPart {
                    part_number: 1,
                    etag: "\"a54357aff0632cce46d942af68356b38\"".to_string(),
                    ..Default::default()
                },
                CompleteMultipartUploadRequestPart {
                    part_number: 2,
                    etag: "\"0c78aef83f66abc1fa1e8477f296d394\"".to_string(),
                    ..Default::default()
                },
                CompleteMultipartUploadRequestPart {
                    part_number: 3,
                    etag: "\"acbd18db4cc2f85cedef654fccc4a4d8\"".to_string(),
                    ..Default::default()
                },
            ],
        };

        let actual = quick_xml::se::to_string(&req).expect("must succeed");

        pretty_assertions::assert_eq!(
            actual,
            r#"<CompleteMultipartUpload>
             <Part>
                <PartNumber>1</PartNumber>
               <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
             </Part>
             <Part>
                <PartNumber>2</PartNumber>
               <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
             </Part>
             <Part>
               <PartNumber>3</PartNumber>
               <ETag>"acbd18db4cc2f85cedef654fccc4a4d8"</ETag>
             </Part>
            </CompleteMultipartUpload>"#
                // Cleanup space and new line
                .replace([' ', '\n'], "")
        )
    }

    /// this example is from: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    #[test]
    fn test_deserialize_complete_multipart_upload_result() {
        let bs = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
             <Location>http://Example-Bucket.s3.region.amazonaws.com/Example-Object</Location>
             <Bucket>Example-Bucket</Bucket>
             <Key>Example-Object</Key>
             <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
            </CompleteMultipartUploadResult>"#,
        );

        let out: CompleteMultipartUploadResult =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!(out.bucket, "Example-Bucket");
        assert_eq!(out.key, "Example-Object");
        assert_eq!(
            out.location,
            "http://Example-Bucket.s3.region.amazonaws.com/Example-Object"
        );
        assert_eq!(out.etag, "\"3858f62230ac3c915f300c664312c11f-9\"");
    }

    #[test]
    fn test_deserialize_complete_multipart_upload_result_when_return_error() {
        let bs = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>

                <Error>
                <Code>InternalError</Code>
                <Message>We encountered an internal error. Please try again.</Message>
                <RequestId>656c76696e6727732072657175657374</RequestId>
                <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
                </Error>"#,
        );

        let out: CompleteMultipartUploadResult =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!(out.code, "InternalError");
        assert_eq!(
            out.message,
            "We encountered an internal error. Please try again."
        );
        assert_eq!(out.request_id, "656c76696e6727732072657175657374");
    }

    /// This example is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html#API_DeleteObjects_Examples
    #[test]
    fn test_serialize_delete_objects_request() {
        let req = DeleteObjectsRequest {
            quiet: true,
            object: vec![
                DeleteObjectsRequestObject {
                    key: "sample1.txt".to_string(),
                    version_id: None,
                },
                DeleteObjectsRequestObject {
                    key: "sample2.txt".to_string(),
                    version_id: Some("11111".to_owned()),
                },
            ],
        };

        let actual = quick_xml::se::to_string(&req).expect("must succeed");

        pretty_assertions::assert_eq!(
            actual,
            r#"<Delete>
             <Quiet>true</Quiet>
             <Object>
             <Key>sample1.txt</Key>
             </Object>
             <Object>
               <Key>sample2.txt</Key>
               <VersionId>11111</VersionId>
             </Object>
             </Delete>"#
                // Cleanup space and new line
                .replace([' ', '\n'], "")
        )
    }

    /// This example is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html#API_DeleteObjects_Examples
    #[test]
    fn test_deserialize_delete_objects_result() {
        let bs = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
             <Error>
              <Key>sample2.txt</Key>
              <Code>AccessDenied</Code>
              <Message>Access Denied</Message>
             </Error>
            </DeleteResult>"#,
        );

        let out: DeleteObjectsResult =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!(out.error.len(), 1);
        assert_eq!(out.error[0].key, "sample2.txt");
        assert_eq!(out.error[0].code, "AccessDenied");
        assert_eq!(out.error[0].message, "Access Denied");
    }

    /// This example is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html#API_ListObjects_Examples
    #[test]
    fn test_parse_list_output_v1() {
        let bs = bytes::Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>bucket</Name>
                <Prefix/>
                <Marker/>
                <MaxKeys>1000</MaxKeys>
                <IsTruncated>false</IsTruncated>
                <Contents>
                    <Key>my-image.jpg</Key>
                    <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                    <ETag>"fba9dede5f27731c9771645a39863328"</ETag>
                    <Size>434234</Size>
                    <StorageClass>STANDARD</StorageClass>
                    <Owner>
                        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                        <DisplayName>mtd@amazon.com</DisplayName>
                    </Owner>
                </Contents>
                <Contents>
                   <Key>my-third-image.jpg</Key>
                     <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                     <ETag>"1b2cf535f27731c974343645a3985328"</ETag>
                     <Size>64994</Size>
                     <StorageClass>STANDARD_IA</StorageClass>
                     <Owner>
                        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                        <DisplayName>mtd@amazon.com</DisplayName>
                    </Owner>
                </Contents>
            </ListBucketResult>"#,
        );

        let out: ListObjectsOutputV1 =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert!(!out.is_truncated.unwrap());
        assert!(out.next_marker.is_none());
        assert!(out.common_prefixes.is_empty());
        assert_eq!(
            out.contents,
            vec![
                ListObjectsOutputContent {
                    key: "my-image.jpg".to_string(),
                    size: 434234,
                    etag: Some("\"fba9dede5f27731c9771645a39863328\"".to_string()),
                    last_modified: "2009-10-12T17:50:30.000Z".to_string(),
                },
                ListObjectsOutputContent {
                    key: "my-third-image.jpg".to_string(),
                    size: 64994,
                    last_modified: "2009-10-12T17:50:30.000Z".to_string(),
                    etag: Some("\"1b2cf535f27731c974343645a3985328\"".to_string()),
                },
            ]
        )
    }

    #[test]
    fn test_parse_list_output_v2() {
        let bs = bytes::Bytes::from(
            r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>example-bucket</Name>
  <Prefix>photos/2006/</Prefix>
  <KeyCount>3</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>photos/2006</Key>
    <LastModified>2016-04-30T23:51:29.000Z</LastModified>
    <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
    <Size>56</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>photos/2007</Key>
    <LastModified>2016-04-30T23:51:29.000Z</LastModified>
    <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
    <Size>100</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>photos/2008</Key>
    <LastModified>2016-05-30T23:51:29.000Z</LastModified>
    <Size>42</Size>
  </Contents>

  <CommonPrefixes>
    <Prefix>photos/2006/February/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>photos/2006/January/</Prefix>
  </CommonPrefixes>
</ListBucketResult>"#,
        );

        let out: ListObjectsOutputV2 =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert!(!out.is_truncated.unwrap());
        assert!(out.next_continuation_token.is_none());
        assert_eq!(
            out.common_prefixes
                .iter()
                .map(|v| v.prefix.clone())
                .collect::<Vec<String>>(),
            vec!["photos/2006/February/", "photos/2006/January/"]
        );
        assert_eq!(
            out.contents,
            vec![
                ListObjectsOutputContent {
                    key: "photos/2006".to_string(),
                    size: 56,
                    etag: Some("\"d41d8cd98f00b204e9800998ecf8427e\"".to_string()),
                    last_modified: "2016-04-30T23:51:29.000Z".to_string(),
                },
                ListObjectsOutputContent {
                    key: "photos/2007".to_string(),
                    size: 100,
                    last_modified: "2016-04-30T23:51:29.000Z".to_string(),
                    etag: Some("\"d41d8cd98f00b204e9800998ecf8427e\"".to_string()),
                },
                ListObjectsOutputContent {
                    key: "photos/2008".to_string(),
                    size: 42,
                    last_modified: "2016-05-30T23:51:29.000Z".to_string(),
                    etag: None,
                },
            ]
        )
    }

    #[test]
    fn test_parse_list_object_versions() {
        let bs = bytes::Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
                <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>mtp-versioning-fresh</Name>
                <Prefix/>
                <KeyMarker>key3</KeyMarker>
                <VersionIdMarker>null</VersionIdMarker>
                <NextKeyMarker>key3</NextKeyMarker>
                <NextVersionIdMarker>d-d309mfjFrUmoQ0DBsVqmcMV15OI.</NextVersionIdMarker>
                <MaxKeys>3</MaxKeys>
                <IsTruncated>true</IsTruncated>
                <Version>
                    <Key>key3</Key>
                    <VersionId>8XECiENpj8pydEDJdd-_VRrvaGKAHOaGMNW7tg6UViI.</VersionId>
                    <IsLatest>true</IsLatest>
                    <LastModified>2009-12-09T00:18:23.000Z</LastModified>
                    <ETag>"396fefef536d5ce46c7537ecf978a360"</ETag>
                    <Size>217</Size>
                    <Owner>
                        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    </Owner>
                    <StorageClass>STANDARD</StorageClass>
                </Version>
                <Version>
                    <Key>key3</Key>
                    <VersionId>d-d309mfjFri40QYukDozqBt3UmoQ0DBsVqmcMV15OI.</VersionId>
                    <IsLatest>false</IsLatest>
                    <LastModified>2009-12-09T00:18:08.000Z</LastModified>
                    <ETag>"396fefef536d5ce46c7537ecf978a360"</ETag>
                    <Size>217</Size>
                    <Owner>
                        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    </Owner>
                    <StorageClass>STANDARD</StorageClass>
                </Version>
                <CommonPrefixes>
                    <Prefix>photos/</Prefix>
                </CommonPrefixes>
                <CommonPrefixes>
                    <Prefix>videos/</Prefix>
                </CommonPrefixes>
                 <DeleteMarker>
                    <Key>my-third-image.jpg</Key>
                    <VersionId>03jpff543dhffds434rfdsFDN943fdsFkdmqnh892</VersionId>
                    <IsLatest>true</IsLatest>
                    <LastModified>2009-10-15T17:50:30.000Z</LastModified>
                    <Owner>
                        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                        <DisplayName>mtd@amazon.com</DisplayName>
                    </Owner>
                </DeleteMarker>
                </ListVersionsResult>"#,
        );

        let output: ListObjectVersionsOutput =
            quick_xml::de::from_reader(bs.reader()).expect("must succeed");

        assert!(output.is_truncated.unwrap());
        assert_eq!(output.next_key_marker, Some("key3".to_owned()));
        assert_eq!(
            output.next_version_id_marker,
            Some("d-d309mfjFrUmoQ0DBsVqmcMV15OI.".to_owned())
        );
        assert_eq!(
            output.common_prefixes,
            vec![
                OutputCommonPrefix {
                    prefix: "photos/".to_owned()
                },
                OutputCommonPrefix {
                    prefix: "videos/".to_owned()
                }
            ]
        );

        assert_eq!(
            output.version,
            vec![
                ListObjectVersionsOutputVersion {
                    key: "key3".to_owned(),
                    version_id: "8XECiENpj8pydEDJdd-_VRrvaGKAHOaGMNW7tg6UViI.".to_owned(),
                    is_latest: true,
                    size: 217,
                    last_modified: "2009-12-09T00:18:23.000Z".to_owned(),
                    etag: Some("\"396fefef536d5ce46c7537ecf978a360\"".to_owned()),
                },
                ListObjectVersionsOutputVersion {
                    key: "key3".to_owned(),
                    version_id: "d-d309mfjFri40QYukDozqBt3UmoQ0DBsVqmcMV15OI.".to_owned(),
                    is_latest: false,
                    size: 217,
                    last_modified: "2009-12-09T00:18:08.000Z".to_owned(),
                    etag: Some("\"396fefef536d5ce46c7537ecf978a360\"".to_owned()),
                }
            ]
        );

        assert_eq!(
            output.delete_marker,
            vec![ListObjectVersionsOutputDeleteMarker {
                key: "my-third-image.jpg".to_owned(),
                version_id: "03jpff543dhffds434rfdsFDN943fdsFkdmqnh892".to_owned(),
                is_latest: true,
                last_modified: "2009-10-15T17:50:30.000Z".to_owned(),
            },]
        );
    }
}

mod error {
    use bytes::Buf;
    use http::Response;
    use http::response::Parts;
    use quick_xml::de;
    use serde::Deserialize;

    use opendal_core::raw::*;
    use opendal_core::*;

    /// S3Error is the error returned by s3 service.
    #[derive(Default, Debug, Deserialize, PartialEq, Eq)]
    #[serde(default, rename_all = "PascalCase")]
    pub(crate) struct S3Error {
        pub code: String,
        pub message: String,
        pub resource: String,
        pub request_id: String,
    }

    /// Parse error response into Error.
    pub(crate) fn parse_error(resp: Response<Buffer>) -> Error {
        let (parts, body) = resp.into_parts();
        let bs = body.to_bytes();

        let (mut kind, mut retryable) = match parts.status.as_u16() {
            403 => (ErrorKind::PermissionDenied, false),
            404 => (ErrorKind::NotFound, false),
            304 | 412 => (ErrorKind::ConditionNotMatch, false),
            // 409 Conflict can be returned e.g. when PutObject with conditions.
            // In this case the AWS docs say to retry.
            409 => (ErrorKind::ConditionNotMatch, true),
            // Service like R2 could return 499 error with a message like:
            // Client Disconnect, we should retry it.
            499 => (ErrorKind::Unexpected, true),
            500 | 502 | 503 | 504 => (ErrorKind::Unexpected, true),
            429 => (ErrorKind::RateLimited, true),
            _ => (ErrorKind::Unexpected, false),
        };

        let body_content = bs.chunk();
        let (message, s3_err) = de::from_reader::<_, S3Error>(body_content.reader())
            .map(|s3_err| (format!("{s3_err:?}"), Some(s3_err)))
            .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

        if let Some(s3_err) = s3_err {
            (kind, retryable) =
                parse_s3_error_code(s3_err.code.as_str()).unwrap_or((kind, retryable));
        }

        let mut err = Error::new(kind, message);

        err = with_error_response_context(err, parts);

        if retryable {
            err = err.set_temporary();
        }

        err
    }

    /// Util function to build [`Error`] from a [`S3Error`] object.
    pub(crate) fn from_s3_error(s3_error: S3Error, parts: Parts) -> Error {
        let (kind, retryable) =
            parse_s3_error_code(s3_error.code.as_str()).unwrap_or((ErrorKind::Unexpected, false));
        let mut err = Error::new(kind, format!("{s3_error:?}"));

        err = with_error_response_context(err, parts);

        if retryable {
            err = err.set_temporary();
        }

        err
    }

    /// Returns the `Error kind` of this code and whether the error is retryable.
    /// All possible error code: <https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList>
    pub fn parse_s3_error_code(code: &str) -> Option<(ErrorKind, bool)> {
        match code {
            // > The specified bucket does not exist.
            //
            // Although the status code is 404, NoSuchBucket is
            // a config invalid error, and it's not retryable from OpenDAL.
            "NoSuchBucket" => Some((ErrorKind::ConfigInvalid, false)),
            // > Your socket connection to the server was not read from
            // > or written to within the timeout period."
            //
            // It's Ok for us to retry it again.
            "RequestTimeout" => Some((ErrorKind::Unexpected, true)),
            // > An internal error occurred. Try again.
            "InternalError" => Some((ErrorKind::Unexpected, true)),
            // > A conflicting conditional operation is currently in progress
            // > against this resource. Try again.
            "OperationAborted" => Some((ErrorKind::Unexpected, true)),
            // > Please reduce your request rate.
            //
            // It's Ok to retry since later on the request rate may get reduced.
            "SlowDown" => Some((ErrorKind::RateLimited, true)),
            // > Service is unable to handle request.
            //
            // ServiceUnavailable is considered a retryable error because it typically
            // indicates a temporary issue with the service or server, such as high load,
            // maintenance, or an internal problem.
            "ServiceUnavailable" => Some((ErrorKind::Unexpected, true)),
            // > Too Many Requests - rate limit exceeded.
            //
            // It's Ok to retry since later on the request rate may get reduced.
            "TooManyRequests" => Some((ErrorKind::RateLimited, true)),
            // > Compatibility with Volcengine TOS
            //
            // TOS returns following error codes along with 429 status code, while both
            // of them indicate rate limit exceeded.
            // See https://www.volcengine.com/docs/6349/74874 for more details.
            "ExceedAccountQPSLimit"
            | "ExceedAccountRateLimit"
            | "ExceedBucketQPSLimit"
            | "ExceedBucketRateLimit" => Some((ErrorKind::RateLimited, true)),
            "InvalidRange" => Some((ErrorKind::RangeNotSatisfied, false)),
            _ => None,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Error response example is from https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        #[test]
        fn test_parse_error() {
            let bs = bytes::Bytes::from(
                r#"
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The resource you requested does not exist</Message>
  <Resource>/mybucket/myfoto.jpg</Resource>
  <RequestId>4442587FB7D0A2F9</RequestId>
</Error>
"#,
            );

            let out: S3Error = de::from_reader(bs.reader()).expect("must success");
            println!("{out:?}");

            assert_eq!(out.code, "NoSuchKey");
            assert_eq!(out.message, "The resource you requested does not exist");
            assert_eq!(out.resource, "/mybucket/myfoto.jpg");
            assert_eq!(out.request_id, "4442587FB7D0A2F9");
        }

        #[test]
        fn test_parse_error_from_unrelated_input() {
            let bs = bytes::Bytes::from(
                r#"
<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://Example-Bucket.s3.ap-southeast-1.amazonaws.com/Example-Object</Location>
  <Bucket>Example-Bucket</Bucket>
  <Key>Example-Object</Key>
  <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
</CompleteMultipartUploadResult>
"#,
            );

            let out: S3Error = de::from_reader(bs.reader()).expect("must success");
            assert_eq!(out, S3Error::default());
        }

        #[test]
        fn test_parse_s3_error_code_invalid_range() {
            assert_eq!(
                parse_s3_error_code("InvalidRange"),
                Some((ErrorKind::RangeNotSatisfied, false))
            );
        }
    }
}

pub(super) use error::*;
