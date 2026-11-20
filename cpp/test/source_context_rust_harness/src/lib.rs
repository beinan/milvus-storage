#[path = "../../../src/format/bridge/rust/src/source_azure_storage.rs"]
mod source_azure_storage;
include!(concat!(env!("CARGO_MANIFEST_DIR"), "/bridge.rs"));
#[path = "../../../src/format/bridge/rust/src/source_read_context.rs"]
mod source_read_context;

#[cxx::bridge(namespace = "milvus_storage::source_context::test")]
mod test_ffi {
    unsafe extern "C++" {
        include!("test_support.h");
        #[namespace = "milvus_storage::source_context::ffi"]
        type SourceContextBridge = crate::source_context_ffi::SourceContextBridge;
        type Harness;
        fn make_harness() -> SharedPtr<Harness>;
        fn context(self: &Harness) -> SharedPtr<SourceContextBridge>;
        fn drop_owner(self: &Harness);
        fn cancel(self: &Harness);
        fn set_time(self: &Harness, millis: i64);
        fn set_status(self: &Harness, status: u32);
        fn block(self: &Harness, value: bool);
        fn calls(self: &Harness) -> u64;
        fn list_calls(self: &Harness) -> u64;
        fn destroyed(self: &Harness) -> u64;
    }
}

#[cfg(test)]
mod tests {
    use super::{test_ffi, source_read_context::*};
    const FILE: &str = "https://acct.blob.core.windows.net/tables/team/table/data";
    const NOW: i64 = 1893456000000;

    #[tokio::test(flavor = "current_thread")]
    async fn retained_rust_owner_survives_caller_release_and_fetches_new_credentials() {
        let h = test_ffi::make_harness();
        let native = h.context();
        let owned = OwnedSourceContext::new(native.retain().unwrap()).unwrap();
        let id = owned.identity();
        drop(native);
        h.drop_owner();
        assert_eq!(h.destroyed(), 0);
        let first = owned.fetch(FILE, SourceOperation::Read).await.unwrap();
        assert_eq!(first.expires_at_unix_ms, NOW + 120000);
        h.set_time(NOW + 130000);
        let second = owned.fetch(FILE, SourceOperation::Read).await.unwrap();
        assert_eq!(second.expires_at_unix_ms, NOW + 250000);
        assert_ne!(first.sas_token, second.sas_token);
        assert_eq!(owned.identity(), id);
        assert_eq!(h.calls(), 2);
        drop(owned);
        assert_eq!(h.destroyed(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn confinement_and_denial_cross_the_real_cpp_rust_boundary() {
        let h = test_ffi::make_harness();
        let owned = OwnedSourceContext::new(h.context()).unwrap();
        assert!(matches!(owned.fetch("https://acct.blob.core.windows.net/tables/team/private/data", SourceOperation::Read).await, Err(SourceFailure::Denied)));
        assert_eq!(h.calls(), 0);
        h.set_status(3);
        assert!(matches!(owned.fetch(FILE, SourceOperation::Read).await, Err(SourceFailure::Transient)));
        h.set_status(0);
        assert!(owned.fetch(FILE, SourceOperation::List).await.is_ok());
        h.set_status(1);
        assert!(matches!(owned.fetch(FILE, SourceOperation::Read).await, Err(SourceFailure::Denied)));
        h.set_status(0);
        assert_eq!(owned.check_state(), Err(SourceFailure::Denied));
        assert!(matches!(owned.fetch(FILE, SourceOperation::Read).await, Err(SourceFailure::Denied)));
        assert_eq!(h.calls(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_callback_does_not_block_executor_and_cancellation_rejects_result() {
        let h = test_ffi::make_harness();
        let owned = OwnedSourceContext::new(h.context()).unwrap();
        h.block(true);
        let task = tokio::spawn(async move { owned.fetch(FILE, SourceOperation::Read).await });
        // If fetch ran on the async worker, this timer could never fire.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while h.calls() == 0 { tokio::time::sleep(std::time::Duration::from_millis(1)).await; }
        }).await.unwrap();
        h.cancel();
        h.drop_owner();
        assert_eq!(h.destroyed(), 0);
        h.block(false);
        assert!(matches!(task.await.unwrap(), Err(SourceFailure::Cancelled)));
        assert_eq!(h.destroyed(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_fetch_future_retains_userdata_until_blocking_callback_finishes() {
        let h = test_ffi::make_harness();
        let owned = OwnedSourceContext::new(h.context()).unwrap();
        h.block(true);
        let task = tokio::spawn(async move { owned.fetch(FILE, SourceOperation::Read).await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while h.calls() == 0 { tokio::time::sleep(std::time::Duration::from_millis(1)).await; }
        }).await.unwrap();
        h.drop_owner();
        task.abort();
        assert!(task.await.is_err());
        assert_eq!(h.destroyed(), 0);
        h.block(false);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while h.destroyed() == 0 { tokio::time::sleep(std::time::Duration::from_millis(1)).await; }
        }).await.unwrap();
        assert_eq!(h.destroyed(), 1);
    }
}

#[path = "../../../src/format/bridge/rust/src/source_iceberg_storage.rs"]
mod source_iceberg_storage;

#[cfg(test)]
mod iceberg_scope_tests {
    use super::{test_ffi, source_read_context::*, source_iceberg_storage::*};
    use iceberg::io::{StorageFactory, Storage, StorageConfig, FileRead};
    use std::{collections::HashMap, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
    const FILE: &str = "https://acct.blob.core.windows.net/tables/team/table/data";
    fn configured(h: &test_ffi::Harness) -> SourceScopedIcebergFactory {
        SourceScopedIcebergFactory::new(OwnedSourceContext::new(h.context()).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn metadata_and_input_paths_reject_other_roots_before_calling_issuer() {
        let h = test_ffi::make_harness();
        let factory = configured(&h);
        let storage = factory.build(&StorageConfig::from_props(HashMap::new())).unwrap();
        assert!(storage.new_input(FILE).is_ok());
        assert!(storage.new_input("abfss://tables@acct.dfs.core.windows.net/team/table/data").is_ok());
        h.set_status(1); // Even a broken guard must not reach Azure with a fake SAS.
        for path in ["https://acct.blob.core.windows.net/tables/team/private/data",
                     "https://other.blob.core.windows.net/tables/team/table/data",
                     "https://acct.blob.core.windows.net.attacker.example/tables/team/table/data",
                     "https://acct.blob.core.windows.net/tables/team/table-other/data",
                     "abfss://tables/team/table/data", "file:///tmp/other"] {
            assert!(storage.new_input(path).is_err());
            assert!(storage.exists(path).await.is_err());
            assert!(storage.metadata(path).await.is_err());
            assert!(storage.read(path).await.is_err());
            assert!(storage.reader(path).await.is_err());
        }
        assert_eq!(h.calls(), 0);
    }

    #[tokio::test]
    async fn native_denial_blocks_authorized_path_without_static_credential_fallback() {
        let h = test_ffi::make_harness();
        h.set_status(1);
        let factory = configured(&h);
        let storage = factory.build(&StorageConfig::from_props(HashMap::from([
            ("adls.account-name".to_string(), "other".to_string()),
            ("adls.account-key".to_string(), "must-not-use".to_string()),
            ("adls.sas-token".to_string(), "sig=must-not-use".to_string()),
        ]))).unwrap();
        assert!(storage.metadata(FILE).await.is_err());
        assert_eq!(h.calls(), 1);
        assert!(storage.reader(FILE).await.is_err());
        assert_eq!(h.calls(), 1);
    }

    #[tokio::test]
    async fn read_only_storage_rejects_all_output_entry_points() {
        let h = test_ffi::make_harness();
        let storage = configured(&h).build(&StorageConfig::from_props(HashMap::new())).unwrap();
        assert!(storage.new_output(FILE).is_err());
        assert!(storage.write(FILE, bytes::Bytes::from_static(b"x")).await.is_err());
        assert!(storage.writer(FILE).await.is_err());
        assert!(storage.delete(FILE).await.is_err());
        assert!(storage.delete_prefix(FILE).await.is_err());
        assert_eq!(h.calls(), 0);
    }

    #[tokio::test]
    async fn serialization_cannot_restore_a_runtime_grant() {
        let h = test_ffi::make_harness();
        let factory = configured(&h);
        let serialized = serde_json::to_string(&factory).unwrap();
        let restored: SourceScopedIcebergFactory = serde_json::from_str(&serialized).unwrap();
        assert!(restored.build(&StorageConfig::from_props(HashMap::new())).is_err());
        let storage = factory.build(&StorageConfig::from_props(HashMap::new())).unwrap();
        let serialized = serde_json::to_string(storage.as_ref()).unwrap();
        let restored: Box<dyn Storage> = serde_json::from_str(&serialized).unwrap();
        assert!(restored.new_input(FILE).is_err());
        assert_eq!(h.calls(), 0);
    }

    struct RangeReader {
        calls: Arc<AtomicUsize>,
        entered: Option<Arc<tokio::sync::Notify>>,
        released: Option<Arc<tokio::sync::Notify>>,
    }
    #[async_trait::async_trait]
    impl FileRead for RangeReader {
        async fn read(&self, _: std::ops::Range<u64>) -> iceberg::Result<bytes::Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(entered) = &self.entered { entered.notify_one(); }
            if let Some(released) = &self.released { released.notified().await; }
            Ok(bytes::Bytes::from_static(b"data"))
        }
    }

    #[tokio::test]
    async fn retained_range_reader_checks_native_context_on_each_read() {
        let h = test_ffi::make_harness();
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = SourceScopedIcebergReader::new(OwnedSourceContext::new(h.context()).unwrap(), FILE,
            Box::new(RangeReader{calls: calls.clone(), entered: None, released: None})).unwrap();
        h.drop_owner();
        assert_eq!(reader.read(0..4).await.unwrap().as_ref(), b"data");
        h.cancel();
        assert!(reader.read(0..4).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.destroyed(), 0);
        drop(reader);
        assert_eq!(h.destroyed(), 1);
    }

    #[tokio::test]
    async fn range_completion_after_cancellation_does_not_return_data() {
        let h = test_ffi::make_harness();
        let entered = Arc::new(tokio::sync::Notify::new());
        let released = Arc::new(tokio::sync::Notify::new());
        let reader = SourceScopedIcebergReader::new(OwnedSourceContext::new(h.context()).unwrap(), FILE,
            Box::new(RangeReader{calls: Arc::new(AtomicUsize::new(0)), entered: Some(entered.clone()), released: Some(released.clone())})).unwrap();
        let task = tokio::spawn(async move {reader.read(0..4).await});
        entered.notified().await;
        h.cancel();
        released.notify_one();
        assert!(task.await.unwrap().is_err());
    }
}

#[cfg(test)]
mod iceberg_file_io_tests {
    use super::{test_ffi, source_read_context::*, source_iceberg_storage::*};
    use iceberg::io::FileIOBuilder;
    use std::sync::Arc;

    #[tokio::test]
    async fn actual_iceberg_file_io_keeps_context_after_owner_release_and_denies_issuer_failure() {
        let h = test_ffi::make_harness();
        let factory = SourceScopedIcebergFactory::new(OwnedSourceContext::new(h.context()).unwrap()).unwrap();
        let io = FileIOBuilder::new(Arc::new(factory)).build();
        let input = io.new_input("abfss://tables@acct.dfs.core.windows.net/team/table/metadata.json").unwrap();
        h.drop_owner();
        h.set_status(1);
        assert!(input.metadata().await.is_err());
        assert_eq!(h.calls(), 1);
        assert!(input.reader().await.is_err());
        assert_eq!(h.calls(), 1);
        assert_eq!(h.destroyed(), 0);
        drop(input);
        drop(io);
        assert_eq!(h.destroyed(), 1);
    }

    #[tokio::test]
    async fn native_issuer_failure_type_survives_actual_file_io_error_chain() {
        for status in 1..=5 {
            let h = test_ffi::make_harness();
            h.set_status(status);
            let factory = SourceScopedIcebergFactory::new(OwnedSourceContext::new(h.context()).unwrap()).unwrap();
            let io = FileIOBuilder::new(Arc::new(factory)).build();
            let input = io.new_input("abfss://tables@acct.dfs.core.windows.net/team/table/metadata.json").unwrap();
            let error: anyhow::Error = input.metadata().await.err().unwrap().into();
            let source = error.chain().find_map(|e| e.downcast_ref::<SourceFailure>()).copied();
            assert_eq!(source, SourceFailure::from_status(status).err());
            assert_eq!(h.calls(), if status == 3 {4} else {1});
            use crate::source_context_ffi::SourcePlanFailure as Failure;
            let projected = crate::source_plan_result::source_plan_result(Err(error));
            let expected = match status {
                1 => Failure::Denied,
                2 => Failure::Expired,
                3 => Failure::CredentialTransient,
                4 => Failure::Cancelled,
                _ => Failure::InvalidCredentialResponse,
            };
            assert_eq!(projected.failure, expected);
            assert!(projected.files.is_empty());
            assert!(!projected.io_temporary);
            assert!(!projected.io_persistent);
        }
    }

    #[tokio::test]
    async fn actual_iceberg_file_io_has_no_relative_or_local_fallback() {
        let h = test_ffi::make_harness();
        let factory = SourceScopedIcebergFactory::new(OwnedSourceContext::new(h.context()).unwrap()).unwrap();
        let io = FileIOBuilder::new(Arc::new(factory)).build();
        for path in ["/tmp/metadata.json", "file:///tmp/metadata.json", "metadata.json", "https://acct.blob.core.windows.net/tables/team/private/data"] {
            assert!(io.new_input(path).is_err());
        }
        assert_eq!(h.calls(), 0);
    }
}

#[cfg(test)]
mod upstream_azure_path_probe {
    use iceberg::io::{StorageConfig, StorageFactory};
    use iceberg_storage_opendal::OpenDalStorageFactory;
    use std::collections::HashMap;

    // Run in a separate process with an explicitly configured local HTTP proxy.
    // ABFS uses the same path conversion as ABFSS and keeps this dummy-credential
    // request observable without changing any TLS trust settings.
    #[tokio::test]
    #[ignore]
    async fn observe_azure_request_path() {
        let factory: OpenDalStorageFactory = serde_json::from_str(
            r#"{"Azdls":{"configured_scheme":"Abfs"}}"#).unwrap();
        let storage = factory.build(&StorageConfig::from_props(HashMap::from([
            (iceberg::io::ADLS_ACCOUNT_NAME.to_string(), "acct".to_string()),
            (iceberg::io::ADLS_SAS_TOKEN.to_string(), "sv=2024-11-04&sp=r&sig=local-test-only".to_string()),
        ]))).unwrap();
        let uri = std::env::var("SOURCE_PATH_PROBE_URI").unwrap();
        assert_eq!(storage.metadata(&uri).await.unwrap().size, 4);
    }
}

#[path = "../../../src/format/bridge/rust/src/source_plan_result.rs"]
mod source_plan_result;

#[path = "../../../src/format/bridge/rust/src/source_lease.rs"]
mod source_lease;

#[cfg(test)]
mod native_lease_tests {
    use super::{source_lease::SourceLeaseCache, source_read_context::*, test_ffi};
    use std::{sync::Arc, time::Duration};
    const FILE: &str = "https://acct.blob.core.windows.net/tables/team/table/data";
    const NOW: i64 = 1893456000000;

    fn cache(h: &test_ffi::Harness) -> SourceLeaseCache {
        SourceLeaseCache::new(OwnedSourceContext::new(h.context()).unwrap(), FILE, SourceOperation::Read).unwrap()
    }

    #[tokio::test]
    async fn real_native_issuer_rotates_across_idle_expiry_and_isolates_contexts() {
        let first = test_ffi::make_harness();
        let second = test_ffi::make_harness();
        let a = cache(&first);
        let b = cache(&second);
        first.drop_owner();
        let one = a.acquire().await.unwrap();
        assert_eq!(one.expires_at, NOW + 120000);
        assert_eq!(a.acquire().await.unwrap().generation, one.generation);
        assert_eq!(first.calls(), 1);
        second.set_status(1);
        assert_eq!(b.acquire().await.err(), Some(SourceFailure::Denied));
        assert!(a.acquire().await.is_ok());
        first.set_time(NOW + 4 * 60 * 60 * 1000);
        let two = a.acquire().await.unwrap();
        assert_ne!(one.token, two.token);
        assert!(two.generation > one.generation);
        assert_eq!(two.expires_at, NOW + 4 * 60 * 60 * 1000 + 120000);
        assert_eq!(first.calls(), 2);
        first.cancel();
        assert_eq!(a.acquire().await.err(), Some(SourceFailure::Cancelled));
        assert_eq!(first.calls(), 2);
        assert_eq!(first.destroyed(), 0);
        drop(a);
        assert_eq!(first.destroyed(), 1);
    }

    #[tokio::test]
    async fn real_native_transient_renewal_falls_back_only_before_expiry_and_recovers() {
        let h = test_ffi::make_harness();
        let cache = cache(&h);
        let first = cache.acquire().await.unwrap();
        h.set_time(NOW + 60000);
        h.set_status(3);
        assert_eq!(cache.acquire().await.unwrap().token, first.token);
        assert_eq!(h.calls(), 2);
        h.set_time(NOW + 120000);
        assert_eq!(cache.acquire().await.err(), Some(SourceFailure::Transient));
        assert_eq!(h.calls(), 6);
        h.set_status(0);
        let renewed = cache.acquire().await.unwrap();
        assert_ne!(first.token, renewed.token);
        assert_eq!(h.calls(), 7);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_waiter_releases_before_native_callback_and_callback_keeps_owner() {
        let h = test_ffi::make_harness();
        let cache = Arc::new(cache(&h));
        h.block(true);
        let task = tokio::spawn({
            let cache = cache.clone();
            async move { cache.acquire().await }
        });
        let started = tokio::time::timeout(Duration::from_secs(2), async {
            while h.calls() == 0 { tokio::time::sleep(Duration::from_millis(1)).await; }
        }).await;
        h.cancel();
        h.drop_owner();
        let result = tokio::time::timeout(Duration::from_millis(200), task).await;
        drop(cache);
        let retained = h.destroyed();
        h.block(false);
        assert!(started.is_ok());
        assert_eq!(retained, 0);
        assert_eq!(result.unwrap().unwrap().err(), Some(SourceFailure::Cancelled));
        tokio::time::timeout(Duration::from_secs(2), async {
            while h.destroyed() == 0 { tokio::time::sleep(Duration::from_millis(1)).await; }
        }).await.unwrap();
        assert_eq!(h.destroyed(), 1);
    }
}

#[path = "../../../src/format/bridge/rust/src/source_http.rs"]
mod source_http;

#[cfg(test)]
mod source_http_tests {
    use super::{test_ffi, source_read_context::*, source_lease::SourceLeaseCache, source_http::SourceHttpClient, source_azure_storage::{SourceAzureStorage, SourceStorageFailure}};
    use std::{sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}}, time::Duration};
    use opendal::{Buffer, Error, ErrorKind};
    use opendal::raw::{HttpBody, HttpClient, HttpFetch};
    use iceberg::io::Storage;
    const ROOT: &str = "https://acct.blob.core.windows.net/tables/team/table";
    const PATH: &str = "abfss://tables@acct.dfs.core.windows.net/team/table/data";
    const NOW: i64 = 1893456000000;

    async fn store(h: &test_ffi::Harness, transport: impl HttpFetch) -> SourceAzureStorage {
        let context = OwnedSourceContext::new(h.context()).unwrap();
        let leases = Arc::new(SourceLeaseCache::new(context.clone(), ROOT, SourceOperation::Read).unwrap());
        let initial = leases.acquire().await.unwrap();
        let client = SourceHttpClient::with_transport(context, leases, ROOT, &initial.token, HttpClient::with(transport)).unwrap();
        SourceAzureStorage::new(ROOT, &initial.token).unwrap().with_http_client(HttpClient::with(client)).unwrap()
    }

    #[derive(Clone)]
    struct Sequence {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        signatures: Arc<Mutex<Vec<String>>>,
        first_status: u16,
    }
    impl Sequence {
        fn new(first_status: u16) -> Self {
            Self { entered: Arc::new(tokio::sync::Notify::new()), release: Arc::new(tokio::sync::Notify::new()), signatures: Arc::new(Mutex::new(Vec::new())), first_status }
        }
    }
    impl HttpFetch for Sequence {
        async fn fetch(&self, req: http::Request<Buffer>) -> opendal::Result<http::Response<HttpBody>> {
            assert_eq!(req.uri().host(), Some("acct.dfs.core.windows.net"));
            assert_eq!(req.uri().path(), "/tables/team/table/data");
            let signatures: Vec<_> = url::form_urlencoded::parse(req.uri().query().unwrap().as_bytes()).filter(|(k,_)| k=="sig").map(|(_,v)| v.into_owned()).collect();
            assert_eq!(signatures.len(), 1);
            let count = { let mut values = self.signatures.lock().unwrap(); values.push(signatures[0].clone()); values.len() };
            if count == 1 {
                self.entered.notify_one();
                self.release.notified().await;
                return Ok(http::Response::builder().status(self.first_status).body(HttpBody::new(futures::stream::iter([Ok(Buffer::from("private-response-must-not-leak"))]),None)).unwrap());
            }
            let ranged = req.headers().contains_key("range");
            let mut response = http::Response::builder().status(if ranged {206} else {200})
                .header("content-length", "4").header("x-ms-resource-type","file")
                .header("etag","\"test\"").header("last-modified","Thu, 19 Nov 2026 00:00:00 GMT");
            if ranged { response = response.header("content-range","bytes 0-3/4"); }
            let data = if req.method()==http::Method::HEAD {vec![]} else {vec![Ok(Buffer::from("data"))]};
            Ok(response.body(HttpBody::new(futures::stream::iter(data),None)).unwrap())
        }
    }

    #[tokio::test]
    async fn actual_opendal_retry_reauthorizes_metadata_and_retained_ranges_after_expiry() {
        for status in [503,429] {
            for range in [false,true] {
                let h = test_ffi::make_harness();
                let transport = Sequence::new(status);
                let store = store(&h, transport.clone()).await;
                let reader = store.reader(PATH).await.unwrap();
                let control = async {
                    transport.entered.notified().await;
                    h.set_time(NOW + 130000);
                    transport.release.notify_one();
                };
                let operation = async {
                    if range { assert_eq!(reader.read(0..4).await.unwrap().as_ref(), b"data"); }
                    else { assert_eq!(store.metadata(PATH).await.unwrap().size, 4); }
                };
                tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(operation,control); }).await.unwrap();
                assert_eq!(*transport.signatures.lock().unwrap(), vec!["lease1","lease2"]);
                assert_eq!(h.calls(), 2);
            }
        }
    }

    #[tokio::test]
    async fn native_denial_after_first_storage_failure_stops_retry_before_network() {
        let h = test_ffi::make_harness();
        let transport = Sequence::new(503);
        let store = store(&h, transport.clone()).await;
        let control = async {
            transport.entered.notified().await;
            h.set_time(NOW + 130000);
            h.set_status(1);
            transport.release.notify_one();
        };
        let (result,_) = tokio::join!(store.metadata(PATH), control);
        let error: anyhow::Error = result.err().unwrap().into();
        assert_eq!(error.chain().find_map(|e| e.downcast_ref::<SourceFailure>()).copied(),Some(SourceFailure::Denied));
        assert_eq!(*transport.signatures.lock().unwrap(), vec!["lease1"]);
        assert_eq!(h.calls(), 2);
        assert!(!format!("{error:?}").contains("private-response"));
    }

    #[derive(Clone)]
    struct Stalled {
        body: bool,
        entered: Arc<tokio::sync::Notify>,
        requests: Arc<AtomicUsize>,
    }
    impl HttpFetch for Stalled {
        async fn fetch(&self, _request: http::Request<Buffer>) -> opendal::Result<http::Response<HttpBody>> {
            self.requests.fetch_add(1,Ordering::SeqCst);
            if !self.body {
                self.entered.notify_one();
                return futures::future::pending().await;
            }
            let entered = self.entered.clone();
            let stream = futures::stream::once(async move {
                entered.notify_one();
                futures::future::pending::<opendal::Result<Buffer>>().await
            });
            Ok(http::Response::builder().status(200).header("content-length","4")
                .body(HttpBody::new(Box::pin(stream),Some(4))).unwrap())
        }
    }

    #[tokio::test]
    async fn cancellation_reaches_stalled_headers_and_streaming_body_without_retry() {
        for body in [false,true] {
            let h = test_ffi::make_harness();
            let transport = Stalled { body, entered: Arc::new(tokio::sync::Notify::new()), requests: Arc::new(AtomicUsize::new(0)) };
            let store = store(&h, transport.clone()).await;
            let control = async { transport.entered.notified().await; h.cancel(); };
            let (result,_) = tokio::time::timeout(Duration::from_millis(500), async { tokio::join!(store.read(PATH),control) }).await.unwrap();
            let error: anyhow::Error = result.err().unwrap().into();
            assert_eq!(error.chain().find_map(|e| e.downcast_ref::<SourceFailure>()).copied(),Some(SourceFailure::Cancelled));
            assert_eq!(transport.requests.load(Ordering::SeqCst),1);
        }
    }

    #[tokio::test]
    async fn deadline_bounds_stalled_headers_and_streaming_body() {
        for body in [false,true] {
            let h = test_ffi::make_harness();
            let transport = Stalled { body, entered: Arc::new(tokio::sync::Notify::new()), requests: Arc::new(AtomicUsize::new(0)) };
            let store = store(&h, transport.clone()).await;
            tokio::time::pause();
            let error = store.read(PATH).await.err().unwrap();
            tokio::time::resume();
            let cause = std::error::Error::source(&error).unwrap().downcast_ref::<SourceStorageFailure>().unwrap();
            assert_eq!(cause.kind,ErrorKind::Unexpected);
            assert!(cause.persistent);
            assert!(transport.requests.load(Ordering::SeqCst)>1);
            assert!(transport.requests.load(Ordering::SeqCst)<=10);
        }
    }

    struct LeakyError;
    impl HttpFetch for LeakyError {
        async fn fetch(&self, _request: http::Request<Buffer>) -> opendal::Result<http::Response<HttpBody>> {
            Err(Error::new(ErrorKind::PermissionDenied,"sig=private-signature").with_context("url","https://acct.dfs.core.windows.net/tables/team/table/data?sig=private-signature"))
        }
    }
    #[tokio::test]
    async fn raw_transport_error_is_redacted_before_returning_to_opendal() {
        let h = test_ffi::make_harness();
        let store = store(&h,LeakyError).await;
        let error = store.read(PATH).await.err().unwrap();
        assert!(!format!("{error:?}").contains("private-signature"));
        let cause = std::error::Error::source(&error).unwrap().downcast_ref::<SourceStorageFailure>().unwrap();
        assert_eq!(cause.kind,ErrorKind::PermissionDenied);
    }
}

#[cfg(test)]
mod source_http_scope_tests {
    use super::{test_ffi, source_read_context::*, source_lease::SourceLeaseCache, source_http::SourceHttpClient};
    use std::sync::{Arc,atomic::{AtomicUsize,Ordering}};
    use opendal::{Buffer,raw::{HttpBody,HttpClient,HttpFetch}};
    const ROOT: &str = "https://acct.blob.core.windows.net/tables/team/table";
    struct Capture(Arc<AtomicUsize>);
    impl HttpFetch for Capture {
        async fn fetch(&self, req: http::Request<Buffer>) -> opendal::Result<http::Response<HttpBody>> {
            self.0.fetch_add(1,Ordering::SeqCst);
            let query: Vec<_> = url::form_urlencoded::parse(req.uri().query().unwrap().as_bytes()).map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
            assert_eq!(query.iter().filter(|(k,_)| k=="sig").map(|(_,v)|v.as_str()).collect::<Vec<_>>(),vec!["lease1"]);
            assert!(!query.iter().any(|(k,_)|k=="st"));
            assert!(query.contains(&("timeout".into(),"5".into())));
            assert!(query.contains(&("action".into(),"getStatus".into())));
            assert!(!req.headers().contains_key(http::header::AUTHORIZATION));
            Ok(http::Response::builder().status(200).body(HttpBody::new(futures::stream::empty(),None)).unwrap())
        }
    }
    #[tokio::test]
    async fn raw_requests_are_confined_before_transport_and_sas_replacement_preserves_operation() {
        let h=test_ffi::make_harness();
        let context=OwnedSourceContext::new(h.context()).unwrap();
        let leases=Arc::new(SourceLeaseCache::new(context.clone(),ROOT,SourceOperation::Read).unwrap());
        let calls=Arc::new(AtomicUsize::new(0));
        let client=SourceHttpClient::with_transport(context,leases,ROOT,"st=old-start&sig=old",HttpClient::with(Capture(calls.clone()))).unwrap();
        for (method,uri) in [
            ("GET","https://other.dfs.core.windows.net/tables/team/table/data"),
            ("GET","https://acct.dfs.core.windows.net/tables/team/private/data"),
            ("GET","https://acct.dfs.core.windows.net/tables/team/table/../private/data"),
            ("GET","https://acct.dfs.core.windows.net/tables/team/table/%2e%2e/private/data"),
            ("GET","http://acct.dfs.core.windows.net/tables/team/table/data"),
            ("PUT","https://acct.dfs.core.windows.net/tables/team/table/data"),
        ] {
            let request=http::Request::builder().method(method).uri(uri).body(Buffer::new()).unwrap();
            assert!(client.fetch(request).await.is_err());
        }
        assert_eq!(calls.load(Ordering::SeqCst),0);
        assert_eq!(h.calls(),0);
        let request=http::Request::builder().method("GET")
            .uri("https://acct.dfs.core.windows.net/tables/team/table/data?timeout=5&action=getStatus&st=old-start&sig=old")
            .header(http::header::AUTHORIZATION,"must-be-removed").body(Buffer::new()).unwrap();
        assert!(client.fetch(request).await.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst),1);
        assert_eq!(h.calls(),1);
    }
}


#[cfg(test)]
mod source_listing_tests {
    use super::{test_ffi, source_http::SourceHttpClient, source_lease::SourceLeaseCache, source_read_context::*};
    use opendal::{Buffer, Operator, services::Azdls, raw::{HttpBody,HttpClient,HttpFetch}};
    use std::sync::{Arc,Mutex};
    use futures::TryStreamExt;
    const ROOT:&str="https://acct.blob.core.windows.net/tables/team/table";
    const NOW:i64=1893456000000;
    #[derive(Clone,Default)]
    struct Capture(Arc<Mutex<Vec<(String,String)>>>);
    impl HttpFetch for Capture {
        async fn fetch(&self,request:http::Request<Buffer>)->opendal::Result<http::Response<HttpBody>> {
            let q:std::collections::HashMap<_,_>=url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes()).map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
            self.0.lock().unwrap().push((q["sp"].clone(),q["sig"].clone()));
            Ok(http::Response::builder().status(200).body(HttpBody::new(futures::stream::empty(),None)).unwrap())
        }
    }
    fn client(h:&test_ffi::Harness,t:impl HttpFetch,listing:bool)->SourceHttpClient {
        let c=OwnedSourceContext::new(h.context()).unwrap();
        let leases=Arc::new(SourceLeaseCache::new(c.clone(),ROOT,SourceOperation::Read).unwrap());
        let client=SourceHttpClient::with_transport(c,leases,ROOT,"sig=initial",HttpClient::with(t)).unwrap();
        if listing {client.with_listing().unwrap()} else {client}
    }
    async fn get(c:&SourceHttpClient,path:&str)->opendal::Result<http::Response<HttpBody>> {
        c.fetch(http::Request::builder().uri(path).body(Buffer::new()).unwrap()).await
    }
    #[tokio::test]
    async fn listing_requires_explicit_enablement_and_native_scope_validation() {
        let h=test_ffi::make_harness();let t=Capture::default();
        let off=client(&h,t.clone(),false);
        assert!(get(&off,"https://acct.dfs.core.windows.net/tables?resource=filesystem&recursive=false&directory=team/table&sig=initial").await.is_err());
        let on=client(&h,t.clone(),true);
        for uri in [
          "https://acct.dfs.core.windows.net/tables?resource=filesystem&recursive=false",
          "https://acct.dfs.core.windows.net/tables?resource=filesystem&recursive=false&directory=team",
          "https://acct.dfs.core.windows.net/tables?resource=filesystem&recursive=false&directory=team/table-other",
          "https://acct.dfs.core.windows.net/tables?resource=filesystem&recursive=false&directory=team/table/%2e%2e/private",
          "https://acct.dfs.core.windows.net/tables?resource=filesystem&recursive=false&directory=team/table&directory=team/private",
          "https://acct.dfs.core.windows.net/tables?resource=filesystem&recursive=false&directory=team/table&%64irectory=team/private",
          "https://acct.dfs.core.windows.net/tables?resource=filesystem&recursive=false&directory=team/table&action=setAccessControl",
          "https://other.dfs.core.windows.net/tables?resource=filesystem&recursive=false&directory=team/table",
          "https://acct.dfs.core.windows.net/other?resource=filesystem&recursive=false&directory=team/table",
          "https://acct.blob.core.windows.net/tables?restype=container&comp=list&prefix=team/table",
          "https://acct.blob.core.windows.net/tables/team/table/data?comp=list",
        ] {assert!(get(&on,uri).await.is_err(),"{uri}");}
        assert_eq!(h.calls(),0);assert!(t.0.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn read_and_list_use_distinct_native_operation_credentials() {
        let h=test_ffi::make_harness();let t=Capture::default();let c=client(&h,t.clone(),true);
        let read="https://acct.dfs.core.windows.net/tables/team/table/data?sig=initial";
        let list="https://acct.dfs.core.windows.net/tables?resource=filesystem&recursive=false&directory=team%2Ftable&sig=initial";
        get(&c,read).await.unwrap();get(&c,list).await.unwrap();get(&c,read).await.unwrap();get(&c,list).await.unwrap();
        assert_eq!(*t.0.lock().unwrap(),vec![("r".into(),"lease1".into()),("l".into(),"lease2".into()),("r".into(),"lease1".into()),("l".into(),"lease2".into())]);
        assert_eq!(h.calls(),2);assert_eq!(h.list_calls(),1);
    }
    #[derive(Clone,Default)]
    struct Pages { signatures:Arc<Mutex<Vec<String>>>, entered:Arc<tokio::sync::Notify>, release:Arc<tokio::sync::Notify> }
    impl HttpFetch for Pages {
      async fn fetch(&self,r:http::Request<Buffer>)->opendal::Result<http::Response<HttpBody>> {
        let q:std::collections::HashMap<_,_>=url::form_urlencoded::parse(r.uri().query().unwrap().as_bytes()).map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
        assert_eq!(q["sp"],"l");assert_eq!(q["directory"],"team/table");assert_eq!(q["resource"],"filesystem");
        let n={let mut s=self.signatures.lock().unwrap();s.push(q["sig"].clone());s.len()};
        if n==1 {self.entered.notify_one();self.release.notified().await;return Ok(http::Response::builder().status(503).body(HttpBody::new(futures::stream::empty(),None)).unwrap());}
        let name=if n==2 {"a"} else {assert_eq!(q["continuation"],"page/2");"b"};
        let body=serde_json::json!({"paths":[{"name":format!("team/table/{name}"),"contentLength":"8","etag":"test","lastModified":"Thu, 19 Nov 2026 00:00:00 GMT"}]}).to_string();
        let mut response=http::Response::builder().status(200);
        if n==2 {response=response.header("x-ms-continuation","page/2");}
        Ok(response.body(HttpBody::new(futures::stream::iter([Ok(Buffer::from(body))]),None)).unwrap())
      }
    }
    #[tokio::test]
    async fn actual_azdls_listing_renews_on_retry_and_next_page_after_expiry() {
      let h=test_ffi::make_harness();let t=Pages::default();let c=client(&h,t.clone(),true);
      let op=Operator::new(Azdls::default().filesystem("tables").endpoint("https://acct.dfs.core.windows.net").account_name("acct").sas_token("sig=initial")).unwrap()
        .layer(opendal::layers::RetryLayer::new()).layer(opendal::layers::HttpClientLayer::new(HttpClient::with(c))).finish();
      let mut lister=op.lister("team/table/").await.unwrap();
      let control=async {t.entered.notified().await;h.set_time(NOW+130000);t.release.notify_one();};
      let (first,())=tokio::join!(lister.try_next(),control);
      assert_eq!(first.unwrap().unwrap().path(),"team/table/");
      h.set_time(NOW+260000);
      let entries=lister.try_collect::<Vec<_>>().await.unwrap();
      assert_eq!(entries.iter().map(|e|e.path()).collect::<Vec<_>>(),vec!["team/table/a","team/table/b"]);
      assert_eq!(*t.signatures.lock().unwrap(),vec!["lease1","lease2","lease3"]);
      assert_eq!(h.calls(),3);assert_eq!(h.list_calls(),3);
    }
}
