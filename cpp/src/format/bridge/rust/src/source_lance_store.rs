// Copyright 2026 Zilliz
// SPDX-License-Identifier: Apache-2.0

//! A read-only Lance store retaining the native source authorization context.
//! No credential or context identity is reconstructed from serialized options.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use lance_core::{Error as LanceError, Result as LanceResult};
use lance_io::object_store::{
    DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_DOWNLOAD_RETRY_COUNT, ObjectStore as LanceStore,
    ObjectStoreParams, ObjectStoreProvider, ObjectStoreRegistry,
};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use object_store_opendal::OpendalStore;
use opendal::{Operator, services::Azdls};
use url::Url;

use crate::source_http::SourceHttpClient;
use crate::source_lease::SourceLeaseCache;
use crate::source_read_context::{OwnedSourceContext, SourceFailure, SourceOperation};

fn auth(error: SourceFailure) -> object_store::Error {
    object_store::Error::Generic {
        store: "source authorization",
        source: Box::new(error),
    }
}
fn readonly() -> object_store::Error {
    object_store::Error::NotSupported {
        source: Box::new(SourceFailure::Denied),
    }
}
fn io(error: opendal::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "source Azure",
        source: Box::new(error),
    }
}
fn lance(error: impl std::error::Error + Send + Sync + 'static) -> LanceError {
    LanceError::IO {
        source: Box::new(error),
        location: snafu::location!(),
    }
}

#[derive(Clone)]
pub(crate) struct SourceLanceStore {
    context: OwnedSourceContext,
    // Native-validated container URI retains the explicit account/endpoint.
    container_uri: String,
    account: String,
    container: String,
    endpoint: String,
    roots: Arc<Mutex<HashMap<String, Arc<dyn ObjectStore>>>>,
    #[cfg(test)]
    transport: Option<opendal::raw::HttpClient>,
}
impl fmt::Debug for SourceLanceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceLanceStore")
            .field("context_identity", &self.context.identity())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for SourceLanceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SourceLanceStore")
    }
}
impl SourceLanceStore {
    pub(crate) fn new(context: OwnedSourceContext, source_uri: &str) -> Result<Self> {
        let source = context.resolve(source_uri).map_err(auth)?;
        let uri =
            Url::parse(&source.resource_uri).map_err(|_| auth(SourceFailure::InvalidResponse))?;
        let host = uri
            .host_str()
            .ok_or_else(|| auth(SourceFailure::InvalidResponse))?;
        let (account, suffix) = host
            .split_once(".blob.")
            .ok_or_else(|| auth(SourceFailure::InvalidResponse))?;
        let container = uri
            .path()
            .trim_start_matches('/')
            .split('/')
            .next()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| auth(SourceFailure::InvalidResponse))?;
        Ok(Self {
            context,
            container_uri: format!("https://{host}/{container}"),
            account: account.to_owned(),
            container: container.to_owned(),
            endpoint: format!("https://{account}.dfs.{suffix}"),
            roots: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            transport: None,
        })
    }
    #[cfg(test)]
    pub(crate) fn with_transport(mut self, transport: opendal::raw::HttpClient) -> Self {
        self.transport = Some(transport);
        self
    }
    fn resolve_path(&self, path: &Path) -> Result<String> {
        self.context.check_state().map_err(auth)?;
        // OpendalStore interprets Path as escaped and decodes once. Resolve the
        // same escaped bytes natively before that decode or URL normalization.
        let target = self
            .context
            .resolve(&format!("{}/{}", self.container_uri, path.as_ref()))
            .map_err(auth)?;
        Ok(target.scope_root_uri)
    }
    fn store(&self, path: &Path) -> Result<Arc<dyn ObjectStore>> {
        let root = self.resolve_path(path)?;
        let mut roots = self
            .roots
            .lock()
            .map_err(|_| auth(SourceFailure::InvalidResponse))?;
        if let Some(store) = roots.get(&root) {
            return Ok(store.clone());
        }
        let leases = Arc::new(
            SourceLeaseCache::new(self.context.clone(), &root, SourceOperation::Read)
                .map_err(auth)?,
        );
        // This is only an unsigned placeholder for OpenDAL's SAS-only signer.
        // It cannot authenticate to Azure. SourceHttpClient always replaces it
        // with a native-validated READ/LIST lease before invoking its transport.
        // Avoid bootstrapping with READ when the first operation is LIST.
        const UNSIGNED: &str = "sig=source-runtime-unissued";
        #[cfg(test)]
        let configured = if let Some(transport) = &self.transport {
            SourceHttpClient::with_transport(
                self.context.clone(),
                leases,
                &root,
                UNSIGNED,
                transport.clone(),
            )
        } else {
            SourceHttpClient::new(self.context.clone(), leases, &root, UNSIGNED)
        };
        #[cfg(not(test))]
        let configured = SourceHttpClient::new(self.context.clone(), leases, &root, UNSIGNED);
        let client = configured.map_err(io)?.with_listing().map_err(io)?;
        let operator = Operator::new(
            Azdls::default()
                .filesystem(&self.container)
                .endpoint(&self.endpoint)
                .account_name(&self.account)
                .sas_token(UNSIGNED),
        )
        .map_err(io)?
        .layer(opendal::layers::RetryLayer::new())
        .layer(opendal::layers::HttpClientLayer::new(
            opendal::raw::HttpClient::with(client),
        ))
        .finish();
        let store = Arc::new(OpendalStore::new(operator)) as Arc<dyn ObjectStore>;
        roots.insert(root, store.clone());
        Ok(store)
    }
    fn validate_meta(&self, meta: &ObjectMeta) -> Result<()> {
        self.resolve_path(&meta.location).map(|_| ())
    }
    pub(crate) fn resolve_raw_uri(&self, raw: &str) -> LanceResult<Path> {
        self.context.check_state().map_err(lance)?;
        let expanded;
        let full = if let Some(key) = raw.strip_prefix(&format!("az://{}/", self.container)) {
            expanded = format!("{}/{}", self.container_uri, key);
            expanded.as_str()
        } else {
            raw
        };
        let resolved = self.context.resolve(full).map_err(lance)?;
        let relative = resolved
            .resource_uri
            .strip_prefix(&(self.container_uri.clone() + "/"))
            .ok_or_else(|| lance(SourceFailure::Denied))?;
        Path::parse(relative).map_err(|_| lance(SourceFailure::Denied))
    }
    pub(crate) fn resolve_url(&self, url: &Url) -> LanceResult<Path> {
        self.resolve_raw_uri(url.as_str())
    }
}

#[async_trait::async_trait]
impl ObjectStore for SourceLanceStore {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        Err(readonly())
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(readonly())
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(readonly())
    }
    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        stream::once(async { Err(readonly()) }).boxed()
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        let store = self.store(path)?;
        let mut result = store.get_opts(path, options).await?;
        self.context.check_state().map_err(auth)?;
        self.validate_meta(&result.meta)?;
        let context = self.context.clone();
        result.payload = match result.payload {
            GetResultPayload::Stream(stream) => GetResultPayload::Stream(
                stream::try_unfold((stream, context), |(mut stream, context)| async move {
                    context.check_state().map_err(auth)?;
                    let next = stream.try_next().await?;
                    context.check_state().map_err(auth)?;
                    Ok(next.map(|bytes| (bytes, (stream, context))))
                })
                .boxed(),
            ),
            _ => return Err(auth(SourceFailure::InvalidResponse)),
        };
        Ok(result)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let this = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move {
            let prefix = prefix.ok_or_else(|| auth(SourceFailure::Denied))?;
            let store = this.store(&prefix)?;
            let stream = store.list(Some(&prefix));
            Ok::<_, object_store::Error>(
                stream::try_unfold((stream, this), |(mut stream, this)| async move {
                    this.context.check_state().map_err(auth)?;
                    let next = stream.try_next().await?;
                    this.context.check_state().map_err(auth)?;
                    if let Some(meta) = &next {
                        this.validate_meta(meta)?;
                    }
                    Ok(next.map(|meta| (meta, (stream, this))))
                })
                .boxed(),
            )
        })
        .try_flatten()
        .boxed()
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = offset.clone();
        self.list(prefix)
            .try_filter(move |meta| futures::future::ready(meta.location > offset))
            .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.ok_or_else(|| auth(SourceFailure::Denied))?;
        let result = self
            .store(prefix)?
            .list_with_delimiter(Some(prefix))
            .await?;
        self.context.check_state().map_err(auth)?;
        for meta in &result.objects {
            self.validate_meta(meta)?;
        }
        for path in &result.common_prefixes {
            self.resolve_path(path)?;
        }
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct SourceLanceProvider {
    pub(crate) store: Arc<SourceLanceStore>,
    parallelism: usize,
}
impl SourceLanceProvider {
    pub(crate) fn new(
        context: OwnedSourceContext,
        source_uri: &str,
        parallelism: usize,
    ) -> LanceResult<Self> {
        if parallelism > 256 {
            return Err(lance(SourceFailure::InvalidResponse));
        }
        Ok(Self {
            store: Arc::new(SourceLanceStore::new(context, source_uri).map_err(lance)?),
            parallelism: if parallelism == 0 {
                DEFAULT_CLOUD_IO_PARALLELISM
            } else {
                parallelism
            },
        })
    }
    pub(crate) fn registry(self: Arc<Self>) -> Arc<ObjectStoreRegistry> {
        let registry = ObjectStoreRegistry::empty();
        registry.insert("az", self.clone());
        registry.insert("https", self.clone());
        registry.insert("abfss", self);
        Arc::new(registry)
    }
}
#[async_trait::async_trait]
impl ObjectStoreProvider for SourceLanceProvider {
    async fn new_store(&self, url: Url, params: &ObjectStoreParams) -> LanceResult<LanceStore> {
        self.store.resolve_url(&url)?;
        if params.object_store_wrapper.is_some() {
            return Err(lance(SourceFailure::Denied));
        }
        // LanceStore::new consults the default registry for its internal prefix,
        // even when a custom provider created the store. Supply the bound account
        // explicitly so az://container URIs never fall back to environment state.
        let identity = self.calculate_object_store_prefix(&url, None)?;
        let options = HashMap::from([(
            "azure_storage_account_name".to_owned(),
            self.store.account.clone(),
        )]);
        let mut store = LanceStore::new(
            self.store.clone(),
            url,
            params.block_size,
            None,
            false,
            false,
            self.parallelism,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            Some(&options),
        );
        // All scheduler/cache users must distinguish retained authorization
        // contexts, including those using the ObjectStore's own prefix.
        store.store_prefix = identity;
        Ok(store)
    }
    fn extract_path(&self, url: &Url) -> LanceResult<Path> {
        self.store.resolve_url(url)
    }
    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _: Option<&HashMap<String, String>>,
    ) -> LanceResult<String> {
        self.store.resolve_url(url)?;
        Ok(format!(
            "source${}${}",
            self.store.context.identity(),
            self.store.container_uri
        ))
    }
}
