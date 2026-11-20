// Copyright 2026 Zilliz
// SPDX-License-Identifier: Apache-2.0

//! Real generated Lance datasets through the production scoped Rust store.
//! Only the final HTTP transport and native issuer clock/outcome are injected.

use super::{source_lance_store::*, source_read_context::*, test_ffi};
use arrow_array58::{Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema58::{DataType, Field, Schema};
use lance::{
    Dataset,
    dataset::{ReadParams, builder::DatasetBuilder},
    session::Session,
};
use opendal::{
    Buffer,
    raw::{HttpBody, HttpClient, HttpFetch},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

const ROOT: &str = "https://acct.blob.core.windows.net/tables/team/table";
const NOW: i64 = 1893456000000;
const CHUNK: usize = 65536;

#[derive(Clone, Default)]
struct FixtureTransport {
    context: Option<OwnedSourceContext>,
    injected: Arc<AtomicUsize>,
    files: Arc<BTreeMap<String, Vec<u8>>>,
    requests: Arc<Mutex<Vec<(String, String, String)>>>,
}

impl HttpFetch for FixtureTransport {
    async fn fetch(
        &self,
        request: http::Request<Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        let query: BTreeMap<_, _> =
            url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
        let signature = query
            .get("sig")
            .expect("production adapter must sign every request");
        assert_ne!(signature, "source-runtime-unissued");
        let expiry = chrono::DateTime::parse_from_rfc3339(query.get("se").expect("SAS expiry"))
            .unwrap()
            .timestamp_millis();
        assert!(
            expiry > self.context.as_ref().unwrap().now_unix_ms(),
            "expired SAS reached storage"
        );
        self.requests.lock().unwrap().push((
            request.method().to_string(),
            request.uri().path().into(),
            signature.clone(),
        ));
        if self.context.as_ref().unwrap().now_unix_ms() >= NOW + 130000 {
            let injected = self
                .injected
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    (n < 2).then_some(n + 1)
                });
            if let Ok(n) = injected {
                let body = br#"{"error":{"code":"ServerBusy","message":"injected storage fault"}}"#
                    .to_vec();
                return Ok(http::Response::builder()
                    .status(if n == 0 { 503 } else { 429 })
                    .header("content-length", body.len())
                    .header("x-ms-error-code", "ServerBusy")
                    .body(HttpBody::new(
                        futures::stream::iter(vec![Ok(Buffer::from(body))]),
                        None,
                    ))
                    .unwrap());
            }
        }
        let mut reply = http::Response::builder()
            .header("last-modified", "Thu, 19 Nov 2026 00:00:00 GMT")
            .header("etag", "\"fixture\"");
        let body: Vec<u8>;
        if query.get("resource").map(String::as_str) == Some("filesystem") {
            assert_eq!(query.get("sp").map(String::as_str), Some("l"));
            let directory = query
                .get("directory")
                .expect("scoped listing has a directory");
            let prefix = format!("{}/", directory.trim_end_matches('/'));
            let paths: Vec<_> = self.files.iter().filter(|(name, _)| name.starts_with(&prefix)).map(|(name, bytes)| {
                serde_json::json!({"name":name,"contentLength":bytes.len().to_string(),"etag":"fixture","lastModified":"Thu, 19 Nov 2026 00:00:00 GMT"})
            }).collect();
            body = serde_json::to_vec(&serde_json::json!({"paths": paths})).unwrap();
            reply = reply
                .status(200)
                .header("content-length", body.len())
                .header("content-type", "application/json");
        } else {
            assert_eq!(query.get("sp").map(String::as_str), Some("r"));
            let key = percent_encoding::percent_decode_str(
                request.uri().path().strip_prefix("/tables/").unwrap(),
            )
            .decode_utf8()
            .unwrap();
            if let Some(bytes) = self.files.get(key.as_ref()) {
                reply = reply.header("x-ms-resource-type", "file");
                if request.method() == http::Method::HEAD {
                    reply = reply.status(200).header("content-length", bytes.len());
                    body = Vec::new();
                } else if let Some(range) = request.headers().get("range") {
                    let (start, end) = range
                        .to_str()
                        .unwrap()
                        .strip_prefix("bytes=")
                        .unwrap()
                        .split_once('-')
                        .unwrap();
                    let (start, end) = if start.is_empty() {
                        (
                            bytes.len().saturating_sub(end.parse::<usize>().unwrap()),
                            bytes.len() - 1,
                        )
                    } else {
                        (
                            start.parse::<usize>().unwrap(),
                            if end.is_empty() {
                                bytes.len() - 1
                            } else {
                                end.parse::<usize>().unwrap().min(bytes.len() - 1)
                            },
                        )
                    };
                    body = bytes[start..=end].to_vec();
                    reply = reply
                        .status(206)
                        .header("content-length", body.len())
                        .header(
                            "content-range",
                            format!("bytes {start}-{end}/{}", bytes.len()),
                        );
                } else {
                    body = bytes.clone();
                    reply = reply.status(200).header("content-length", body.len());
                }
            } else {
                body = br#"{"error":{"code":"PathNotFound","message":"fixture path absent"}}"#
                    .to_vec();
                reply = reply
                    .status(404)
                    .header("x-ms-error-code", "PathNotFound")
                    .header("content-length", body.len());
            }
        }
        let chunks = if body.is_empty() {
            vec![]
        } else {
            vec![Ok(Buffer::from(body))]
        };
        Ok(reply
            .body(HttpBody::new(futures::stream::iter(chunks), None))
            .unwrap())
    }
}

async fn fixture() -> (tempfile::TempDir, FixtureTransport, Vec<i64>) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("fixture.lance");
    // Incompressible values exceed the footer and block caches.
    let mut random = 0x123456789abcdef_u64;
    let values: Vec<i64> = (0..4 * CHUNK)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            (random & i64::MAX as u64) as i64
        })
        .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(values.clone()))],
    )
    .unwrap();
    Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        path.to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    fn collect(
        dir: &std::path::Path,
        base: &std::path::Path,
        files: &mut BTreeMap<String, Vec<u8>>,
    ) {
        for item in std::fs::read_dir(dir).unwrap() {
            let path = item.unwrap().path();
            if path.is_dir() {
                collect(&path, base, files);
            } else {
                files.insert(
                    format!(
                        "team/table/{}",
                        path.strip_prefix(base).unwrap().to_str().unwrap()
                    ),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    collect(&path, &path, &mut files);
    assert!(files.values().map(Vec::len).sum::<usize>() > 1024 * 1024);
    (
        temp,
        FixtureTransport {
            files: Arc::new(files),
            ..Default::default()
        },
        values,
    )
}

async fn open(
    h: &test_ffi::Harness,
    transport: FixtureTransport,
    version: u64,
) -> lance::Result<Dataset> {
    let context = OwnedSourceContext::new(h.context()).unwrap();
    let mut provider = SourceLanceProvider::new(context.clone(), ROOT, 4)?;
    let transport = FixtureTransport {
        context: Some(context.clone()),
        ..transport
    };
    provider.store = Arc::new(
        SourceLanceStore::new(context, ROOT)
            .unwrap()
            .with_transport(HttpClient::with(transport)),
    );
    let registry = Arc::new(provider).registry();
    let session = Arc::new(Session::new(0, 0, registry));
    let mut builder = DatasetBuilder::from_uri("az://tables/team/table")
        .with_read_params(ReadParams {
            index_cache_size_bytes: 0,
            metadata_cache_size_bytes: 0,
            ..Default::default()
        })
        .with_session(session);
    if version != 0 {
        builder = builder.with_version(version);
    }
    builder.load().await
}

async fn read(dataset: &Dataset, round: usize) -> lance::Result<RecordBatch> {
    let indices: Vec<_> = (round * CHUNK..(round + 1) * CHUNK)
        .map(|n| n as u64)
        .collect();
    dataset
        .take(&indices, dataset.schema().project(&["value"])?)
        .await
}

fn failure(error: &(dyn std::error::Error + 'static)) -> Option<SourceFailure> {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if let Some(value) = error.downcast_ref::<SourceFailure>() {
            return Some(*value);
        }
        cause = error.source();
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_dataset_renews_recovers_and_isolates_users() {
    let (_temp, transport, expected) = fixture().await;
    for version in [1, 0] {
        transport.injected.store(0, Ordering::SeqCst);
        let h = test_ffi::make_harness();
        let dataset = open(&h, transport.clone(), version).await.unwrap();
        for round in 0..3 {
            let previous_calls = h.calls();
            h.set_time(NOW + round as i64 * 130000);
            let batch = read(&dataset, round).await.unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values.len(), CHUNK);
            for i in 0..CHUNK {
                assert_eq!(values.value(i), expected[round * CHUNK + i]);
            }
            if round > 0 {
                assert!(
                    h.calls() > previous_calls,
                    "a cache hit must not count as renewal"
                );
            }
        }
        assert_eq!(transport.injected.load(Ordering::SeqCst), 2);
        h.set_time(NOW + 390000);
        h.set_status(3);
        let before_failure = h.calls();
        let error = read(&dataset, 3).await.unwrap_err();
        eprintln!(
            "Lance version={version}: failed read used {} provider attempts",
            h.calls() - before_failure
        );
        assert_eq!(failure(&error), Some(SourceFailure::Transient), "{error}");
        // Lance's outer get retry currently repeats the four-attempt native
        // acquisition batch. Keep the whole single-file read bounded too.
        assert!((1..=16).contains(&(h.calls() - before_failure)));
        h.set_status(0);
        let recovered = read(&dataset, 3).await.unwrap();
        assert_eq!(recovered.num_rows(), CHUNK);
        let values = recovered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..CHUNK {
            assert_eq!(values.value(i), expected[3 * CHUNK + i]);
        }
        let bob = test_ffi::make_harness();
        bob.set_status(1);
        let denied = open(&bob, transport.clone(), version).await.err().unwrap();
        assert_eq!(failure(&denied), Some(SourceFailure::Denied), "{denied}");
        assert_eq!(bob.calls(), 1);
        assert_eq!(read(&dataset, 0).await.unwrap().num_rows(), CHUNK);
        h.cancel();
        let cancelled = read(&dataset, 1).await.unwrap_err();
        assert_eq!(
            failure(&cancelled),
            Some(SourceFailure::Cancelled),
            "{cancelled}"
        );
    }
}
