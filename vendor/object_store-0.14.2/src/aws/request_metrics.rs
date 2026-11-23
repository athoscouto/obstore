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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use http::Method;

use crate::client::{HttpClient, HttpError, HttpRequest, HttpResponse, HttpService};

#[derive(Clone, Copy)]
enum Api {
    PutObject,
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    GetObject,
    HeadObject,
    CopyObject,
    DeleteObject,
    DeleteObjects,
    ListObjectsV2,
    GetObjectTagging,
    PutObjectTagging,
    Other,
}

const APIS: [Api; 14] = [
    Api::PutObject,
    Api::CreateMultipartUpload,
    Api::UploadPart,
    Api::CompleteMultipartUpload,
    Api::AbortMultipartUpload,
    Api::GetObject,
    Api::HeadObject,
    Api::CopyObject,
    Api::DeleteObject,
    Api::DeleteObjects,
    Api::ListObjectsV2,
    Api::GetObjectTagging,
    Api::PutObjectTagging,
    Api::Other,
];

impl Api {
    fn name(self) -> &'static str {
        match self {
            Self::PutObject => "PutObject",
            Self::CreateMultipartUpload => "CreateMultipartUpload",
            Self::UploadPart => "UploadPart",
            Self::CompleteMultipartUpload => "CompleteMultipartUpload",
            Self::AbortMultipartUpload => "AbortMultipartUpload",
            Self::GetObject => "GetObject",
            Self::HeadObject => "HeadObject",
            Self::CopyObject => "CopyObject",
            Self::DeleteObject => "DeleteObject",
            Self::DeleteObjects => "DeleteObjects",
            Self::ListObjectsV2 => "ListObjectsV2",
            Self::GetObjectTagging => "GetObjectTagging",
            Self::PutObjectTagging => "PutObjectTagging",
            Self::Other => "Other",
        }
    }
}

/// Opt-in, cumulative counts of S3 HTTP transport attempts.
///
/// Handles may be shared across stores. Retries and transport failures count as
/// attempts; values are not successful-operation or billable-request counts.
/// Credential-provider traffic and local presigning are not counted. Snapshots
/// contain only fixed API names and numbers, never object keys or request URLs.
#[derive(Debug, Default)]
pub struct S3RequestMetrics {
    counts: [AtomicU64; APIS.len()],
}

impl S3RequestMetrics {
    /// Return cumulative per-API counts, including zero values.
    ///
    /// Concurrent requests may increment different counters during a snapshot;
    /// taking a snapshot does not reset any counter.
    pub fn snapshot(&self) -> HashMap<&'static str, u64> {
        APIS.iter()
            .enumerate()
            .map(|(i, api)| (api.name(), self.counts[i].load(Ordering::Relaxed)))
            .collect()
    }

    pub(crate) fn wrap(self: Arc<Self>, client: HttpClient) -> HttpClient {
        HttpClient::new(MeteredS3Client {
            client,
            metrics: self,
        })
    }
}

#[derive(Debug)]
struct MeteredS3Client {
    client: HttpClient,
    metrics: Arc<S3RequestMetrics>,
}

#[async_trait]
impl HttpService for MeteredS3Client {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.metrics.counts[classify(&request) as usize].fetch_add(1, Ordering::Relaxed);
        self.client.execute(request).await
    }
}

fn classify(request: &HttpRequest) -> Api {
    let mut uploads = false;
    let mut upload_id = false;
    let mut part_number = false;
    let mut tagging = false;
    let mut delete = false;
    let mut list_v2 = false;
    for (key, value) in
        url::form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes())
    {
        match key.as_ref() {
            "uploads" => uploads = true,
            "uploadId" => upload_id = true,
            "partNumber" => part_number = true,
            "tagging" => tagging = true,
            "delete" => delete = true,
            "list-type" if value == "2" => list_v2 = true,
            _ => (),
        }
    }
    let copy = request.headers().contains_key("x-amz-copy-source");
    match *request.method() {
        Method::PUT if upload_id && part_number && !copy => Api::UploadPart,
        Method::POST if uploads => Api::CreateMultipartUpload,
        Method::POST if upload_id => Api::CompleteMultipartUpload,
        Method::DELETE if upload_id => Api::AbortMultipartUpload,
        Method::PUT if tagging => Api::PutObjectTagging,
        Method::PUT if copy && !upload_id => Api::CopyObject,
        Method::PUT if !upload_id && !part_number => Api::PutObject,
        Method::GET if tagging => Api::GetObjectTagging,
        Method::GET if list_v2 => Api::ListObjectsV2,
        Method::GET if !uploads && !upload_id => Api::GetObject,
        Method::HEAD => Api::HeadObject,
        Method::DELETE => Api::DeleteObject,
        Method::POST if delete => Api::DeleteObjects,
        _ => Api::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::HttpRequestBody;

    #[test]
    fn bounded_api_classification() {
        let cases = [
            (Method::PUT, "", false, "PutObject"),
            (Method::POST, "?uploads", false, "CreateMultipartUpload"),
            (
                Method::PUT,
                "?partNumber=1&uploadId=private",
                false,
                "UploadPart",
            ),
            (
                Method::POST,
                "?uploadId=private",
                false,
                "CompleteMultipartUpload",
            ),
            (
                Method::DELETE,
                "?uploadId=private",
                false,
                "AbortMultipartUpload",
            ),
            (Method::GET, "?versionId=private", false, "GetObject"),
            (Method::HEAD, "", false, "HeadObject"),
            (Method::PUT, "", true, "CopyObject"),
            (Method::DELETE, "", false, "DeleteObject"),
            (Method::POST, "?delete", false, "DeleteObjects"),
            (
                Method::GET,
                "?list-type=2&prefix=private",
                false,
                "ListObjectsV2",
            ),
            (Method::GET, "?tagging", false, "GetObjectTagging"),
            (Method::PUT, "?tagging", false, "PutObjectTagging"),
            (Method::GET, "?uploadId=private", false, "Other"),
            (Method::PUT, "?uploadId=private&partNumber=1", true, "Other"),
            (Method::PATCH, "", false, "Other"),
        ];
        for (method, query, copy, expected) in cases {
            let mut request = http::Request::builder()
                .method(method)
                .uri(format!("https://example.test/private-key{query}"));
            if copy {
                request = request.header("x-amz-copy-source", "private-source");
            }
            let request = request.body(HttpRequestBody::empty()).unwrap();
            assert_eq!(classify(&request).name(), expected);
        }
    }
}
