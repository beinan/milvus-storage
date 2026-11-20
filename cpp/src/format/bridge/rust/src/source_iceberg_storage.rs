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

//! Read-only Iceberg routing for an explicitly retained runtime context.
//! Keep the full canonical authority until selecting the authenticated store.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};

use crate::source_azure_storage::SourceAzureStorage;
use crate::source_lease::SourceLeaseCache;
use crate::source_read_context::{OwnedSourceContext, SourceFailure, SourceOperation};

fn source_error(error: SourceFailure) -> Error {
    Error::new(ErrorKind::Unexpected, error.to_string()).with_source(error)
}

fn read_only() -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        "Source-read credentials cannot authorize writes",
    )
}

// Derive the ADLS request from the canonical URI returned by native scope
// resolution. Never use an independently supplied endpoint or account here.
fn adls_location(canonical_uri: &str) -> Result<(String, String)> {
    let url =
        url::Url::parse(canonical_uri).map_err(|_| source_error(SourceFailure::InvalidResponse))?;
    if url.scheme() != "https"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
    {
        return Err(source_error(SourceFailure::InvalidResponse));
    }
    let (account, suffix) = url
        .host_str()
        .and_then(|host| host.split_once(".blob."))
        .ok_or_else(|| source_error(SourceFailure::InvalidResponse))?;
    let (container, key) = url
        .path()
        .strip_prefix('/')
        .and_then(|path| path.split_once('/'))
        .ok_or_else(|| source_error(SourceFailure::InvalidResponse))?;
    if account.is_empty() || suffix.is_empty() || container.is_empty() || key.is_empty() {
        return Err(source_error(SourceFailure::InvalidResponse));
    }
    Ok((
        account.to_string(),
        format!("abfss://{container}@{account}.dfs.{suffix}/{key}"),
    ))
}

// Own one lease cache and connection pool per authorized root. Retained
// readers consult it before every range read, including after hours of idle.
struct SourceRootStorage {
    root: String,
    leases: Arc<SourceLeaseCache>,
    context: OwnedSourceContext,
    storage: tokio::sync::Mutex<Option<(u64, Arc<SourceAzureStorage>)>>,
}

impl SourceRootStorage {
    fn for_source_context(
        context: OwnedSourceContext,
        root: &str,
        _account: String,
    ) -> Result<Self> {
        let root = context.resolve(root).map_err(source_error)?.scope_root_uri;
        let leases = SourceLeaseCache::new(context.clone(), &root, SourceOperation::Read)
            .map_err(source_error)?;
        Ok(Self {
            root,
            leases: Arc::new(leases),
            context,
            storage: tokio::sync::Mutex::new(None),
        })
    }

    async fn current_storage(&self) -> Result<(u64, Arc<SourceAzureStorage>)> {
        let lease = self.leases.acquire().await.map_err(source_error)?;
        let mut cached = self.storage.lock().await;
        if let Some((generation, storage)) = &*cached {
            if *generation >= lease.generation {
                return Ok((*generation, storage.clone()));
            }
        }
        let storage = Arc::new(SourceAzureStorage::with_source_context(
            &self.root,
            &lease.token,
            self.context.clone(),
            self.leases.clone(),
        )?);
        *cached = Some((lease.generation, storage.clone()));
        Ok((lease.generation, storage))
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        self.current_storage().await?.1.exists(path).await
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.current_storage().await?.1.metadata(path).await
    }
    async fn read(&self, path: &str) -> Result<bytes::Bytes> {
        self.current_storage().await?.1.read(path).await
    }
    async fn reader(self: &Arc<Self>, path: &str) -> Result<Box<dyn FileRead>> {
        let (generation, storage) = self.current_storage().await?;
        let reader = Arc::from(storage.reader(path).await?);
        Ok(Box::new(RenewingSourceReader {
            storage: self.clone(),
            path: path.to_owned(),
            cached: tokio::sync::Mutex::new((generation, reader)),
        }))
    }
}

struct RenewingSourceReader {
    storage: Arc<SourceRootStorage>,
    path: String,
    cached: tokio::sync::Mutex<(u64, Arc<dyn FileRead>)>,
}

#[async_trait]
impl FileRead for RenewingSourceReader {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<bytes::Bytes> {
        let (generation, storage) = self.storage.current_storage().await?;
        let reader = {
            let mut cached = self.cached.lock().await;
            if generation > cached.0 {
                *cached = (generation, Arc::from(storage.reader(&self.path).await?));
            }
            cached.1.clone()
        };
        reader.read(range).await
    }
}

struct SourceStorageState {
    context: OwnedSourceContext,
    // A native context identity owns this entire map. Roots cannot share SAS
    // providers, and a different context cannot reach these cached stores.
    stores: Mutex<HashMap<u64, Arc<SourceRootStorage>>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SourceScopedIcebergFactory {
    #[serde(skip)]
    state: Option<Arc<SourceStorageState>>,
}

impl SourceScopedIcebergFactory {
    pub(crate) fn new(context: OwnedSourceContext) -> Result<Self> {
        context.check_state().map_err(source_error)?;
        Ok(Self {
            state: Some(Arc::new(SourceStorageState {
                context,
                stores: Mutex::new(HashMap::new()),
            })),
        })
    }
}

impl fmt::Debug for SourceScopedIcebergFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceScopedIcebergFactory")
            .field("has_runtime_context", &self.state.is_some())
            .finish()
    }
}

#[typetag::serde(name = "SourceScopedIcebergFactory")]
impl StorageFactory for SourceScopedIcebergFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let state = self
            .state
            .clone()
            .ok_or_else(|| source_error(SourceFailure::Denied))?;
        state.context.check_state().map_err(source_error)?;
        Ok(Arc::new(SourceScopedIcebergStorage { state: Some(state) }))
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct SourceScopedIcebergStorage {
    #[serde(skip)]
    state: Option<Arc<SourceStorageState>>,
}

impl fmt::Debug for SourceScopedIcebergStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceScopedIcebergStorage")
            .field("has_runtime_context", &self.state.is_some())
            .finish()
    }
}

impl SourceScopedIcebergStorage {
    fn context(&self) -> Result<&OwnedSourceContext> {
        self.state
            .as_ref()
            .map(|state| &state.context)
            .ok_or_else(|| source_error(SourceFailure::Denied))
    }

    fn route(&self, path: &str) -> Result<(Arc<SourceRootStorage>, String)> {
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| source_error(SourceFailure::Denied))?;
        let resolved = state.context.resolve(path).map_err(source_error)?;
        let (account, adls_uri) = adls_location(&resolved.resource_uri)?;
        let mut stores = state
            .stores
            .lock()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Source storage cache unavailable"))?;
        let storage = match stores.get(&resolved.root_index) {
            Some(storage) => storage.clone(),
            None => {
                let storage = Arc::new(SourceRootStorage::for_source_context(
                    state.context.clone(),
                    &resolved.scope_root_uri,
                    account,
                )?);
                stores.insert(resolved.root_index, storage.clone());
                storage
            }
        };
        Ok((storage, adls_uri))
    }
}

#[typetag::serde(name = "SourceScopedIcebergStorage")]
#[async_trait]
impl Storage for SourceScopedIcebergStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (storage, uri) = self.route(path)?;
        let result = storage.exists(&uri).await;
        self.context()?.check_state().map_err(source_error)?;
        result
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (storage, uri) = self.route(path)?;
        let result = storage.metadata(&uri).await;
        self.context()?.check_state().map_err(source_error)?;
        result
    }
    async fn read(&self, path: &str) -> Result<bytes::Bytes> {
        let (storage, uri) = self.route(path)?;
        let result = storage.read(&uri).await;
        self.context()?.check_state().map_err(source_error)?;
        result
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let context = self.context()?.clone();
        let canonical_uri = context.resolve(path).map_err(source_error)?.resource_uri;
        let (storage, uri) = self.route(&canonical_uri)?;
        let inner = storage.reader(&uri).await?;
        context.check_state().map_err(source_error)?;
        Ok(Box::new(SourceScopedIcebergReader::new(
            context,
            &canonical_uri,
            inner,
        )?))
    }
    async fn write(&self, _path: &str, _bytes: bytes::Bytes) -> Result<()> {
        Err(read_only())
    }
    async fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
        Err(read_only())
    }
    async fn delete(&self, _path: &str) -> Result<()> {
        Err(read_only())
    }
    async fn delete_prefix(&self, _path: &str) -> Result<()> {
        Err(read_only())
    }
    fn new_input(&self, path: &str) -> Result<InputFile> {
        let resolved = self.context()?.resolve(path).map_err(source_error)?;
        Ok(InputFile::new(
            Arc::new(self.clone()),
            resolved.resource_uri,
        ))
    }
    fn new_output(&self, _path: &str) -> Result<OutputFile> {
        Err(read_only())
    }
}

pub(crate) struct SourceScopedIcebergReader {
    context: OwnedSourceContext,
    canonical_uri: String,
    inner: Box<dyn FileRead>,
}

impl SourceScopedIcebergReader {
    pub(crate) fn new(
        context: OwnedSourceContext,
        resource_uri: &str,
        inner: Box<dyn FileRead>,
    ) -> Result<Self> {
        let canonical_uri = context
            .resolve(resource_uri)
            .map_err(source_error)?
            .resource_uri;
        Ok(Self {
            context,
            canonical_uri,
            inner,
        })
    }
}

#[async_trait]
impl FileRead for SourceScopedIcebergReader {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<bytes::Bytes> {
        self.context
            .resolve(&self.canonical_uri)
            .map_err(source_error)?;
        let result = self.inner.read(range).await;
        self.context.check_state().map_err(source_error)?;
        result
    }
}
