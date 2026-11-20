// Copyright 2026 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! An Azure store bound to one already-authorized source root and SAS lease.
//! OpenDAL takes decoded object keys. Keep URI identity until scope validation,
//! then decode exactly once instead of feeding escaped URI paths to OpenDAL.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage};
use iceberg::{Error, ErrorKind, Result};
use opendal::{Operator, services::Azdls};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::source_http::SourceHttpClient;
use crate::source_lease::SourceLeaseCache;
use crate::source_read_context::{OwnedSourceContext, SourceFailure};

fn invalid() -> Error {
    Error::new(ErrorKind::Unexpected, "Invalid Azure source resource")
        .with_source(SourceFailure::InvalidResponse)
}

fn denied() -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Azure source resource is not authorized",
    )
    .with_source(SourceFailure::Denied)
}

fn read_only() -> Error {
    Error::new(ErrorKind::FeatureUnsupported, "Source storage is read-only")
}

// OpenDAL errors can embed a signed request URI. Preserve their machine-readable
// category and temporary flag, never the raw error/body/URI or SAS.
#[derive(Debug)]
pub(crate) struct SourceStorageFailure {
    pub(crate) kind: opendal::ErrorKind,
    pub(crate) temporary: bool,
    // OpenDAL marks every error leaving RetryLayer as Persistent, including
    // terminal errors that were never retried. Preserve the original flag;
    // it does not by itself prove that retries occurred or were exhausted.
    pub(crate) persistent: bool,
}

impl fmt::Display for SourceStorageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Azure source I/O failed ({:?}, temporary={}, persistent={})",
            self.kind, self.temporary, self.persistent
        )
    }
}
impl std::error::Error for SourceStorageFailure {}

fn io_error(error: opendal::Error) -> Error {
    // Authorization errors from the per-attempt HTTP adapter must survive
    // OpenDAL's error wrappers and retries as the original typed source cause.
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = cause {
        if let Some(failure) = current.downcast_ref::<SourceFailure>() {
            return Error::new(ErrorKind::Unexpected, "Source authorization failed")
                .with_source(*failure);
        }
        cause = current.source();
    }
    Error::new(ErrorKind::Unexpected, "Azure source I/O failed").with_source(SourceStorageFailure {
        kind: error.kind(),
        temporary: error.is_temporary(),
        persistent: error.is_persistent(),
    })
}

struct BoundState {
    operator: Operator,
    authority: String,
    root_path: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SourceAzureStorage {
    // Neither SAS nor an operator carrying it can be reconstructed by serde.
    #[serde(skip)]
    state: Option<Arc<BoundState>>,
}

impl fmt::Debug for SourceAzureStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceAzureStorage")
            .field("has_runtime_state", &self.state.is_some())
            .finish()
    }
}

impl SourceAzureStorage {
    pub(crate) fn with_source_context(
        canonical_root: &str,
        sas: &str,
        context: OwnedSourceContext,
        leases: Arc<SourceLeaseCache>,
    ) -> Result<Self> {
        let client =
            SourceHttpClient::new(context, leases, canonical_root, sas).map_err(io_error)?;
        Self::new(canonical_root, sas)?.with_http_client(opendal::raw::HttpClient::with(client))
    }

    pub(crate) fn with_http_client(mut self, client: opendal::raw::HttpClient) -> Result<Self> {
        let state = self
            .state
            .as_mut()
            .and_then(Arc::get_mut)
            .ok_or_else(invalid)?;
        state.operator = state
            .operator
            .clone()
            .layer(opendal::layers::HttpClientLayer::new(client));
        Ok(self)
    }

    pub(crate) fn new(canonical_root: &str, sas: &str) -> Result<Self> {
        let root = Url::parse(canonical_root).map_err(|_| invalid())?;
        let (account, suffix) = root
            .host_str()
            .and_then(|host| host.split_once(".blob."))
            .ok_or_else(invalid)?;
        let (container, root_path) = root
            .path()
            .strip_prefix('/')
            .and_then(|p| p.split_once('/'))
            .ok_or_else(invalid)?;
        if root.scheme() != "https"
            || root.query().is_some()
            || root.fragment().is_some()
            || !root.username().is_empty()
            || root.password().is_some()
            || root.port().is_some()
            || account.is_empty()
            || container.is_empty()
            || root_path.is_empty()
            || sas.is_empty()
        {
            return Err(invalid());
        }
        // Only the native-validated root and issued SAS configure this store.
        // No arbitrary options, environment keys or endpoint overrides enter it.
        let authority = format!("{container}@{account}.dfs.{suffix}");
        let operator = Operator::new(
            Azdls::default()
                .filesystem(container)
                .endpoint(&format!("https://{account}.dfs.{suffix}"))
                .account_name(account)
                .sas_token(sas),
        )
        .map_err(io_error)?
        .layer(opendal::layers::RetryLayer::new())
        .finish();
        Ok(Self {
            state: Some(Arc::new(BoundState {
                operator,
                authority,
                root_path: root_path.trim_end_matches('/').to_string(),
            })),
        })
    }

    fn route(&self, path: &str) -> Result<(&Operator, String)> {
        let state = self.state.as_ref().ok_or_else(denied)?;
        // Require the canonical full ABFSS authority from the outer native
        // scope check; do not normalize an attacker-controlled authority here.
        let prefix = format!("abfss://{}/", state.authority);
        let key = path.strip_prefix(&prefix).ok_or_else(denied)?;
        if key != state.root_path
            && !key
                .strip_prefix(&state.root_path)
                .is_some_and(|p| p.starts_with('/'))
        {
            return Err(denied());
        }
        let decoded = percent_decode_str(key)
            .decode_utf8()
            .map_err(|_| invalid())?;
        // The native parser already rejects these. Retain the invariant at this
        // last URI-to-object-key boundary as well, including encoded separators.
        if key.contains('?')
            || key.contains('#')
            || decoded.contains('\\')
            || decoded.contains('\0')
            || decoded.split('/').any(|p| p == "." || p == "..")
            || decoded.matches('/').count() != key.matches('/').count()
        {
            return Err(denied());
        }
        Ok((&state.operator, decoded.into_owned()))
    }
}

#[typetag::serde(name = "SourceAzureStorage")]
#[async_trait]
impl Storage for SourceAzureStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, key) = self.route(path)?;
        op.exists(&key).await.map_err(io_error)
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, key) = self.route(path)?;
        Ok(FileMetadata {
            size: op.stat(&key).await.map_err(io_error)?.content_length(),
        })
    }
    async fn read(&self, path: &str) -> Result<bytes::Bytes> {
        let (op, key) = self.route(path)?;
        Ok(op.read(&key).await.map_err(io_error)?.to_bytes())
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, key) = self.route(path)?;
        Ok(Box::new(SourceAzureReader(
            op.reader(&key).await.map_err(io_error)?,
        )))
    }
    async fn write(&self, _: &str, _: bytes::Bytes) -> Result<()> {
        Err(read_only())
    }
    async fn writer(&self, _: &str) -> Result<Box<dyn FileWrite>> {
        Err(read_only())
    }
    async fn delete(&self, _: &str) -> Result<()> {
        Err(read_only())
    }
    async fn delete_prefix(&self, _: &str) -> Result<()> {
        Err(read_only())
    }
    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.route(path)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }
    fn new_output(&self, _: &str) -> Result<OutputFile> {
        Err(read_only())
    }
}

struct SourceAzureReader(opendal::Reader);
#[async_trait]
impl FileRead for SourceAzureReader {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<bytes::Bytes> {
        Ok(self.0.read(range).await.map_err(io_error)?.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::Buffer;
    use opendal::raw::{HttpBody, HttpClient, HttpFetch};
    use std::sync::Mutex;

    const ROOT: &str = "https://acct.blob.core.windows.net/tables/team/table";
    const PREFIX: &str = "abfss://tables@acct.dfs.core.windows.net/team/table/";
    const SAS: &str = "sv=2024-11-04&sp=rl&sig=local-test-only";

    #[derive(Clone)]
    struct Capture {
        requests: Arc<Mutex<Vec<(String, String, String)>>>,
    }
    impl HttpFetch for Capture {
        async fn fetch(
            &self,
            req: http::Request<Buffer>,
        ) -> opendal::Result<http::Response<HttpBody>> {
            self.requests.lock().unwrap().push((
                req.method().to_string(),
                req.uri().host().unwrap().to_string(),
                req.uri().path().to_string(),
            ));
            // Assert only; never record a query carrying a credential.
            assert_eq!(
                url::form_urlencoded::parse(req.uri().query().unwrap().as_bytes())
                    .find(|(k, _)| k == "sig")
                    .unwrap()
                    .1,
                "local-test-only"
            );
            let range = req.headers().get("range");
            let mut response = http::Response::builder()
                .status(if range.is_some() { 206 } else { 200 })
                .header("content-length", "4")
                .header("x-ms-resource-type", "file")
                .header("etag", "\"test\"")
                .header("last-modified", "Thu, 19 Nov 2026 00:00:00 GMT");
            if range.is_some() {
                response = response.header("content-range", "bytes 0-3/4");
            }
            let data = if req.method() == http::Method::HEAD {
                Vec::new()
            } else {
                vec![Ok(Buffer::from("data"))]
            };
            Ok(response
                .body(HttpBody::new(futures::stream::iter(data), None))
                .unwrap())
        }
    }

    fn captured_store() -> (SourceAzureStorage, Capture) {
        let capture = Capture {
            requests: Arc::new(Mutex::new(Vec::new())),
        };
        let mut storage = SourceAzureStorage::new(ROOT, SAS).unwrap();
        let state = Arc::get_mut(storage.state.as_mut().unwrap()).unwrap();
        state.operator = state
            .operator
            .clone()
            .layer(opendal::layers::HttpClientLayer::new(HttpClient::with(
                capture.clone(),
            )));
        (storage, capture)
    }

    #[tokio::test]
    async fn actual_signed_requests_encode_keys_once_for_metadata_and_ranges() {
        let (storage, capture) = captured_store();
        for name in [
            "a%20b.parquet",
            "a%25b.parquet",
            "%E4%B8%AD.parquet",
            "a+b.parquet",
            "a%23b%3Fc.parquet",
            "%252e%252e.parquet",
        ] {
            let uri = format!("{PREFIX}{name}");
            assert_eq!(storage.metadata(&uri).await.unwrap().size, 4);
            let reader = storage.reader(&uri).await.unwrap();
            assert_eq!(reader.read(0..4).await.unwrap().as_ref(), b"data");
            let requests = capture.requests.lock().unwrap();
            for (_, host, path) in requests.iter().rev().take(2) {
                assert_eq!(host, "acct.dfs.core.windows.net");
                // '+' may be escaped by the transport; compare decoded object
                // names as Azure does, without form-urlencoded '+' semantics.
                let expected = format!("/tables/team/table/{name}");
                assert_eq!(
                    percent_decode_str(path).decode_utf8().unwrap(),
                    percent_decode_str(&expected).decode_utf8().unwrap()
                );
            }
        }
        assert_eq!(capture.requests.lock().unwrap().len(), 12);
    }

    #[tokio::test]
    async fn invalid_scope_never_reaches_transport() {
        let (storage, capture) = captured_store();
        for uri in [
            "abfss://tables@other.dfs.core.windows.net/team/table/data",
            "abfss://other@acct.dfs.core.windows.net/team/table/data",
            "abfss://tables@acct.dfs.core.windows.net/team/table-other/data",
            "abfss://tables@acct.dfs.core.windows.net/team/table/%2e%2e/data",
            "abfss://tables@acct.dfs.core.windows.net/team/table/a%2fb",
            "abfss://tables@acct.dfs.core.windows.net/team/table/data?redirect=1",
        ] {
            assert!(storage.metadata(uri).await.is_err());
        }
        assert!(capture.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn serde_does_not_restore_operator_or_credential() {
        let storage = SourceAzureStorage::new(ROOT, SAS).unwrap();
        let serialized = serde_json::to_string(&storage).unwrap();
        assert!(!serialized.contains(SAS));
        let copy: SourceAzureStorage = serde_json::from_str(&serialized).unwrap();
        assert!(copy.metadata(&format!("{PREFIX}data")).await.is_err());
        assert!(!format!("{storage:?}").contains(SAS));
    }

    #[test]
    fn error_projection_preserves_category_without_signed_uri_or_body() {
        for kind in [
            opendal::ErrorKind::PermissionDenied,
            opendal::ErrorKind::NotFound,
            opendal::ErrorKind::RateLimited,
        ] {
            let input = opendal::Error::new(kind, SAS)
                .with_context("uri", format!("{ROOT}?{SAS}"))
                .set_temporary();
            let error = io_error(input);
            let cause = std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<SourceStorageFailure>()
                .unwrap();
            assert_eq!(cause.kind, kind);
            assert!(cause.temporary);
            assert!(!cause.persistent);
            assert!(!format!("{error:?}").contains("local-test-only"));
        }
    }

    #[derive(Clone)]
    struct FailureResponse {
        status: u16,
        requests: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl HttpFetch for FailureResponse {
        async fn fetch(
            &self,
            _req: http::Request<Buffer>,
        ) -> opendal::Result<http::Response<HttpBody>> {
            self.requests
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(http::Response::builder()
                .status(self.status)
                .body(HttpBody::new(
                    futures::stream::iter([Ok(Buffer::from("local-test-only"))]),
                    None,
                ))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn actual_http_failures_preserve_denial_not_found_and_retry_exhaustion() {
        for (status, expected_kind, exhausted) in [
            (403, opendal::ErrorKind::PermissionDenied, false),
            (404, opendal::ErrorKind::NotFound, false),
            // Pinned Azdls 0.56 does not classify HTTP 429 as retryable.
            // Preserve that observed behavior here; fixing it is separate.
            (429, opendal::ErrorKind::Unexpected, false),
            (503, opendal::ErrorKind::Unexpected, true),
        ] {
            let transport = FailureResponse {
                status,
                requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            };
            let mut storage = SourceAzureStorage::new(ROOT, SAS).unwrap();
            let state = Arc::get_mut(storage.state.as_mut().unwrap()).unwrap();
            state.operator = state
                .operator
                .clone()
                .layer(opendal::layers::HttpClientLayer::new(HttpClient::with(
                    transport.clone(),
                )));
            let error = storage
                .metadata(&format!("{PREFIX}data"))
                .await
                .err()
                .unwrap();
            let cause = std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<SourceStorageFailure>()
                .unwrap();
            assert_eq!(cause.kind, expected_kind, "HTTP {status}");
            assert!(!cause.temporary, "retrying layer has already finished");
            assert!(
                cause.persistent,
                "OpenDAL marks every final RetryLayer error persistent"
            );
            let requests = transport.requests.load(std::sync::atomic::Ordering::SeqCst);
            if exhausted {
                assert!(requests > 1, "transient HTTP {status} was not retried");
            } else {
                assert_eq!(requests, 1, "terminal HTTP {status} was retried");
            }
            assert!(!format!("{error:?}").contains("local-test-only"));
            use crate::source_context_ffi::SourcePlanFailure as Failure;
            let projected = crate::source_plan_result::source_plan_result(Err(error.into()));
            assert_eq!(
                projected.failure,
                match status {
                    403 => Failure::IoPermissionDenied,
                    404 => Failure::IoNotFound,
                    _ => Failure::IoUnexpected,
                }
            );
            assert!(!projected.io_temporary);
            assert!(projected.io_persistent);
            assert!(projected.files.is_empty());
        }
    }
}
