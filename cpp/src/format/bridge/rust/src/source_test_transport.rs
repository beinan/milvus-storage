// Copyright 2026 Zilliz
// SPDX-License-Identifier: Apache-2.0

//! Final HTTP transport injection for native integration tests only.
//! SourceHttpClient still validates and signs every request before this layer.

use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use opendal::{Buffer, Error, ErrorKind};

#[cxx::bridge(namespace = "milvus_storage::source_context::test")]
mod ffi {
    struct HttpHeader {
        name: String,
        value: String,
    }
    struct HttpResponse {
        status: u16,
        headers: Vec<HttpHeader>,
        body: Vec<u8>,
    }
    unsafe extern "C++" {
        include!("source_http_test_support.h");
        fn has_http_transport(identity: u64) -> bool;
        fn send_http_request(
            identity: u64,
            method: &str,
            uri: &str,
            headers: Vec<HttpHeader>,
        ) -> Result<HttpResponse>;
    }
}

struct NativeTransport(u64);

fn failure() -> Error {
    // Neither the signed request nor callback exception may escape this layer.
    Error::new(ErrorKind::Unexpected, "Native test transport failed")
}

impl HttpFetch for NativeTransport {
    async fn fetch(
        &self,
        request: http::Request<Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        let identity = self.0;
        let method = request.method().to_string();
        let uri = request.uri().to_string();
        let headers = request
            .headers()
            .iter()
            .map(|(name, value)| {
                Ok(ffi::HttpHeader {
                    name: name.to_string(),
                    value: value.to_str().map_err(|_| failure())?.to_owned(),
                })
            })
            .collect::<opendal::Result<Vec<_>>>()?;
        let response = tokio::task::spawn_blocking(move || {
            ffi::send_http_request(identity, &method, &uri, headers).map_err(|_| failure())
        })
        .await
        .map_err(|_| failure())??;
        let mut builder = http::Response::builder().status(response.status);
        for header in response.headers {
            builder = builder.header(header.name, header.value);
        }
        let chunks = if response.body.is_empty() {
            Vec::new()
        } else {
            vec![Ok(Buffer::from(response.body))]
        };
        builder
            .body(HttpBody::new(futures::stream::iter(chunks), None))
            .map_err(|_| failure())
    }
}

pub(crate) fn for_context(identity: u64) -> Option<HttpClient> {
    ffi::has_http_transport(identity).then(|| HttpClient::with(NativeTransport(identity)))
}
