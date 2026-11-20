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

//! Reauthorize each OpenDAL HTTP attempt, including attempts retried internally.
//! No signed URI or response body leaves this boundary in an error or retry log.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use opendal::{Buffer, Error, ErrorKind, Result};
use tokio::time::Instant;

use crate::source_lease::SourceLeaseCache;
use crate::source_read_context::{OwnedSourceContext, SourceFailure, SourceOperation};

fn authorization(error: SourceFailure) -> Error {
    // Credential acquisition already performs its own bounded retries. Do not
    // multiply them through OpenDAL's independent storage retry policy.
    let kind = if error == SourceFailure::Denied {
        ErrorKind::PermissionDenied
    } else {
        ErrorKind::Unexpected
    };
    Error::new(kind, "Source authorization failed").set_source(error)
}

fn sanitized(error: Error) -> Error {
    // HttpClient errors can contain the signed URI. Keep only the safe kind and
    // retry flags before RetryLayer gets a chance to log the error.
    let mut safe = Error::new(error.kind(), "Azure source transport failed")
        .with_temporary(error.is_temporary());
    if error.is_persistent() {
        safe = safe.set_persistent();
    }
    safe
}

fn timeout() -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Azure source request deadline exceeded",
    )
    .set_temporary()
}

async fn guarded<T>(
    context: &OwnedSourceContext,
    deadline: Instant,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::pin!(future);
    loop {
        context.check_state().map_err(authorization)?;
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(timeout()),
            value = &mut future => {
                context.check_state().map_err(authorization)?;
                return value;
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
}

pub(crate) struct SourceHttpClient {
    context: OwnedSourceContext,
    leases: Arc<SourceLeaseCache>,
    list_leases: Option<SourceLeaseCache>,
    root: String,
    initial_sas_keys: HashSet<String>,
    transport: HttpClient,
}

impl SourceHttpClient {
    pub(crate) fn new(
        context: OwnedSourceContext,
        leases: Arc<SourceLeaseCache>,
        root: &str,
        initial_sas: &str,
    ) -> Result<Self> {
        // A redirect would bypass our per-attempt scope check. Azure account
        // endpoints are addressed directly, so do not follow redirects.
        let client = reqwest13::Client::builder()
            .redirect(reqwest13::redirect::Policy::none())
            .build()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Cannot create source HTTP client"))?;
        Self::with_transport(context, leases, root, initial_sas, HttpClient::with(client))
    }

    pub(crate) fn with_transport(
        context: OwnedSourceContext,
        leases: Arc<SourceLeaseCache>,
        root: &str,
        initial_sas: &str,
        transport: HttpClient,
    ) -> Result<Self> {
        let root = context.resolve(root).map_err(authorization)?.scope_root_uri;
        let initial_sas_keys =
            url::form_urlencoded::parse(initial_sas.trim_start_matches('?').as_bytes())
                .map(|(key, _)| key.into_owned())
                .collect();
        Ok(Self {
            context,
            leases,
            list_leases: None,
            root,
            initial_sas_keys,
            transport,
        })
    }

    /// Listing is opt-in. READ and LIST never share credential cache entries,
    /// even if a broker happens to issue a SAS containing both permissions.
    pub(crate) fn with_listing(mut self) -> Result<Self> {
        self.list_leases = Some(
            SourceLeaseCache::new(self.context.clone(), &self.root, SourceOperation::List)
                .map_err(authorization)?,
        );
        Ok(self)
    }

    fn request_leases(&self, request: &http::Request<Buffer>) -> Result<&SourceLeaseCache> {
        let uri = request.uri().to_string();
        let unsigned = uri.split('?').next().unwrap();
        let query = request.uri().query().unwrap_or("");
        let pairs: Vec<_> = url::form_urlencoded::parse(query.as_bytes()).collect();
        // Azdls lists at the container URL; the directory is carried in the
        // query. Validating only the HTTP path would authorize the wrong scope.
        let listing = pairs.iter().any(|(key, _)| {
            matches!(
                key.to_ascii_lowercase().as_str(),
                "resource"
                    | "directory"
                    | "recursive"
                    | "maxresults"
                    | "continuation"
                    | "comp"
                    | "restype"
                    | "prefix"
                    | "marker"
                    | "delimiter"
                    | "include"
            )
        });
        let (resolved, leases) = if listing {
            let leases = self
                .list_leases
                .as_ref()
                .ok_or_else(|| authorization(SourceFailure::Denied))?;
            // Strip only the SDK's initial SAS fields. Native classification
            // validates operation parameters (including duplicates), decodes
            // directory exactly once, and checks the actual requested root.
            let operation_query = query
                .split('&')
                .filter(|pair| {
                    let key = pair.split('=').next().unwrap_or("");
                    !self.initial_sas_keys.contains(key)
                })
                .collect::<Vec<_>>()
                .join("&");
            let resolved = self
                .context
                .resolve_http(
                    request.method().as_str(),
                    &format!("{unsigned}?{operation_query}"),
                )
                .map_err(authorization)?;
            if resolved.operation != SourceOperation::List as u32 {
                return Err(authorization(SourceFailure::Denied));
            }
            (resolved, leases)
        } else {
            (
                self.context.resolve(unsigned).map_err(authorization)?,
                self.leases.as_ref(),
            )
        };
        if resolved.scope_root_uri != self.root {
            return Err(authorization(SourceFailure::Denied));
        }
        Ok(leases)
    }
}

impl HttpFetch for SourceHttpClient {
    async fn fetch(&self, mut request: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        self.context.check_state().map_err(authorization)?;
        if request.uri().scheme_str() != Some("https")
            || !matches!(*request.method(), http::Method::GET | http::Method::HEAD)
        {
            return Err(authorization(SourceFailure::Denied));
        }
        let uri = request.uri().to_string();
        let unsigned = uri.split('?').next().unwrap();
        // Validate scope before URI normalization or credential acquisition.
        let leases = self.request_leases(&request)?;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(self.context.request_timeout_ms()))
            .ok_or_else(|| authorization(SourceFailure::InvalidResponse))?;
        let lease = guarded(&self.context, deadline, async {
            leases.acquire().await.map_err(authorization)
        })
        .await?;
        let replacement: Vec<_> =
            url::form_urlencoded::parse(lease.token.trim_start_matches('?').as_bytes())
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect();
        let signed_uri = {
            let mut query = url::form_urlencoded::Serializer::new(String::new());
            for (key, value) in
                url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
            {
                if !self.initial_sas_keys.contains(key.as_ref())
                    && !replacement.iter().any(|(new_key, _)| new_key == &key)
                {
                    query.append_pair(&key, &value);
                }
            }
            for (key, value) in replacement {
                query.append_pair(&key, &value);
            }
            format!("{unsigned}?{}", query.finish())
        };
        *request.uri_mut() = signed_uri
            .parse()
            .map_err(|_| authorization(SourceFailure::InvalidResponse))?;
        request.headers_mut().remove(http::header::AUTHORIZATION);
        let response = guarded(&self.context, deadline, async {
            self.transport.fetch(request).await.map_err(sanitized)
        })
        .await?;
        let status = response.status();
        if !status.is_success() {
            // Return errors before Azdls parses/logs a potentially sensitive
            // response body or embeds the request URI. Preserve storage kinds.
            let (kind, temporary) = match status.as_u16() {
                401 | 403 => (ErrorKind::PermissionDenied, false),
                404 => (ErrorKind::NotFound, false),
                409 | 412 => (ErrorKind::ConditionNotMatch, false),
                416 => (ErrorKind::RangeNotSatisfied, false),
                429 => (ErrorKind::RateLimited, true),
                408 | 500 | 502 | 503 | 504 => (ErrorKind::Unexpected, true),
                _ => (ErrorKind::Unexpected, false),
            };
            return Err(Error::new(kind, "Azure source HTTP request failed")
                .with_context("status", status.as_u16().to_string())
                .with_temporary(temporary));
        }
        let (parts, body) = response.into_parts();
        let context = self.context.clone();
        let body = body.map_inner(move |stream| {
            Box::new(Box::pin(futures::stream::try_unfold(
                (stream, context),
                move |(mut stream, context)| async move {
                    let next = guarded(&context, deadline, async {
                        stream.next().await.transpose().map_err(sanitized)
                    })
                    .await?;
                    Ok(next.map(|buffer| (buffer, (stream, context))))
                },
            )))
        });
        Ok(http::Response::from_parts(parts, body))
    }
}
