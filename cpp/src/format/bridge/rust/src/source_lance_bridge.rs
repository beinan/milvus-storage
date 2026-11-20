// Copyright 2026 Zilliz
// SPDX-License-Identifier: Apache-2.0

use crate::source_context_ffi::SourceContextBridge;
use crate::source_lance_store::SourceLanceProvider;
use crate::source_read_context::{OwnedSourceContext, SourceFailure};
use crate::{
    TOKIO_RT,
    lance_bridgeimpl::{
        self as legacy, BlockingDataset, BlockingFragmentReader, BlockingScanner,
        FragmentFilesMetadata,
    },
};
use arrow_array58::{RecordBatch, RecordBatchReader};
use arrow_schema58::{ArrowError, SchemaRef};
use arrow58::ffi_stream::FFI_ArrowArrayStream;
use futures::StreamExt;
use lance::dataset::{ReadParams, builder::DatasetBuilder};
use lance::session::Session;
use lance_table::utils::stream::ReadBatchFutStream;
use std::{fmt, sync::Arc};

// A finite source-only envelope crosses CXX/Arrow error-string boundaries.
// Unknown Lance errors are deliberately sanitized, never parsed for categories.
#[derive(Debug)]
pub(crate) struct SourceLanceError(u32);
impl From<SourceFailure> for SourceLanceError {
    fn from(e: SourceFailure) -> Self {
        Self(match e {
            SourceFailure::Denied => 117,
            SourceFailure::Expired => 118,
            SourceFailure::Transient => 119,
            SourceFailure::Cancelled => 120,
            SourceFailure::InvalidResponse => 121,
        })
    }
}
impl SourceLanceError {
    fn from_error(error: &(dyn std::error::Error + 'static)) -> Self {
        let mut current = Some(error);
        while let Some(cause) = current {
            if let Some(e) = cause.downcast_ref::<SourceFailure>() {
                return (*e).into();
            }
            if let Some(e) = cause.downcast_ref::<Self>() {
                return Self(e.0);
            }
            current = cause.source();
        }
        Self(0)
    }
}
impl From<lance::Error> for SourceLanceError {
    fn from(e: lance::Error) -> Self {
        Self::from_error(&e)
    }
}
impl fmt::Display for SourceLanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "__MILVUS_SOURCE_LANCE__={};", self.0)
    }
}
impl std::error::Error for SourceLanceError {}
type Result<T> = std::result::Result<T, SourceLanceError>;
fn context(dataset: &BlockingDataset) -> Result<&OwnedSourceContext> {
    dataset
        .source_context
        .as_ref()
        .ok_or_else(|| SourceFailure::Denied.into())
}
fn reader_context(reader: &BlockingFragmentReader) -> Result<&OwnedSourceContext> {
    reader
        .source_context
        .as_ref()
        .ok_or_else(|| SourceFailure::Denied.into())
}
fn checked<T>(context: &OwnedSourceContext, call: impl FnOnce() -> lance::Result<T>) -> Result<T> {
    context.check_state()?;
    let result = call().map_err(SourceLanceError::from);
    context.check_state()?;
    result
}

pub(crate) fn open_source_dataset(
    native: cxx::SharedPtr<SourceContextBridge>,
    source_uri: &str,
    lance_uri: &str,
    parallelism: u32,
    version: u64,
) -> Result<Box<BlockingDataset>> {
    let context = OwnedSourceContext::new(native)?;
    // Check the raw input before Url or DatasetBuilder can normalize it.
    context.resolve(source_uri)?;
    let provider = Arc::new(SourceLanceProvider::new(
        context.clone(),
        source_uri,
        parallelism as usize,
    )?);
    let selected =
        url::Url::parse(lance_uri).map_err(|_| SourceLanceError::from(SourceFailure::Denied))?;
    let path = provider.store.resolve_url(&selected)?;
    // The explicitly retained full URI and Lance's short URI must be identical.
    let full = context.resolve(source_uri)?.resource_uri;
    let tail = full
        .strip_prefix("https://")
        .and_then(|s| s.split_once('/'))
        .map(|(_, s)| s)
        .ok_or_else(|| SourceLanceError::from(SourceFailure::Denied))?;
    if format!("az://{tail}") != lance_uri || path.as_ref().is_empty() {
        return Err(SourceFailure::Denied.into());
    }
    let registry = provider.registry();
    let session = Arc::new(Session::new(0, 0, registry.clone()));
    let mut builder = DatasetBuilder::from_uri(lance_uri)
        .with_read_params(ReadParams {
            index_cache_size_bytes: 0,
            metadata_cache_size_bytes: 0,
            ..Default::default()
        })
        .with_session(session);
    if version != 0 {
        builder = builder.with_version(version);
    }
    let inner = TOKIO_RT.block_on(builder.load())?;
    // Validate raw manifest BasePaths before Lance converts them to Url. An
    // empty registry prevents any fallback provider while loading later refs.
    for base in inner.manifest().base_paths.values() {
        let bound = SourceLanceProvider::new(context.clone(), source_uri, parallelism as usize)?;
        bound.store.resolve_raw_uri(base.path.as_str())?;
    }
    context.check_state()?;
    let mut dataset = BlockingDataset::new(inner)?;
    dataset.source_context = Some(context);
    Ok(Box::new(dataset))
}

pub(crate) fn source_fragment_rows(
    dataset: &BlockingDataset,
    fragment: u64,
    physical: bool,
) -> Result<u64> {
    checked(context(dataset)?, || {
        if physical {
            legacy::get_fragment_physical_row_count(dataset, fragment)
        } else {
            legacy::get_fragment_row_count(dataset, fragment)
        }
    })
}
pub(crate) fn source_fragment_deletions(
    dataset: &BlockingDataset,
    fragment: u64,
) -> Result<Vec<u64>> {
    checked(context(dataset)?, || {
        legacy::get_fragment_deletion_positions(dataset, fragment)
    })
}
pub(crate) unsafe fn source_fragment_schema(
    dataset: &BlockingDataset,
    fragment: u64,
    schema: *mut u8,
) -> Result<()> {
    checked(context(dataset)?, || unsafe {
        legacy::get_fragment_schema(dataset, fragment, schema)
    })
}
pub(crate) unsafe fn source_open_fragment(
    dataset: &BlockingDataset,
    fragment: u64,
    schema: *mut u8,
    metadata: *const FragmentFilesMetadata,
) -> Result<Box<BlockingFragmentReader>> {
    checked(context(dataset)?, || unsafe {
        legacy::open_fragment_reader(dataset, fragment, schema, metadata)
    })
}
pub(crate) fn source_fragment_estimates(
    reader: &BlockingFragmentReader,
) -> Result<Vec<crate::lance_ffi::LanceColumnMemoryEstimate>> {
    checked(reader_context(reader)?, || {
        reader.estimate_fragment_column_memory()
    })
}
pub(crate) unsafe fn source_take_batch(
    reader: &BlockingFragmentReader,
    indices: &[u32],
    output: *mut u8,
) -> Result<()> {
    let context = reader_context(reader)?;
    let mut array = arrow58::ffi::FFI_ArrowArray::empty();
    checked(context, || {
        reader.take_as_single_batch(
            indices,
            (&mut array as *mut arrow58::ffi::FFI_ArrowArray).cast(),
        )
    })?;
    unsafe { std::ptr::write(output.cast(), array) };
    Ok(())
}

struct SourceBatchReader {
    stream: futures::stream::Buffered<ReadBatchFutStream>,
    schema: SchemaRef,
    context: OwnedSourceContext,
}
impl Iterator for SourceBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Err(e) = self.context.check_state() {
            return Some(Err(ArrowError::ExternalError(Box::new(
                SourceLanceError::from(e),
            ))));
        }
        let result = TOKIO_RT.block_on(self.stream.next());
        if let Err(e) = self.context.check_state() {
            return Some(Err(ArrowError::ExternalError(Box::new(
                SourceLanceError::from(e),
            ))));
        }
        result
            .map(|r| r.map_err(|e| ArrowError::ExternalError(Box::new(SourceLanceError::from(e)))))
    }
}
impl RecordBatchReader for SourceBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
pub(crate) unsafe fn source_fragment_stream(
    reader: &BlockingFragmentReader,
    mode: u32,
    indices: &[u32],
    start: u32,
    end: u32,
    batch_size: u32,
    output: *mut u8,
) -> Result<()> {
    let context = reader_context(reader)?.clone();
    context.check_state()?;
    let stream = match mode {
        0 => TOKIO_RT.block_on(reader.inner.read_all(batch_size))?,
        1 => TOKIO_RT.block_on(reader.inner.read_range(start..end, batch_size))?,
        2 => TOKIO_RT.block_on(reader.inner.take(
            &reader.map_logical_indices(indices),
            batch_size,
            None,
        ))?,
        _ => return Err(SourceFailure::InvalidResponse.into()),
    };
    context.check_state()?;
    let out = FFI_ArrowArrayStream::new(Box::new(SourceBatchReader {
        stream: stream.buffered(1),
        schema: Arc::new(reader.projection.clone()),
        context,
    }));
    unsafe { std::ptr::write(output as *mut FFI_ArrowArrayStream, out) };
    Ok(())
}

// Scan/Take currently materialize batches in the legacy implementation. Retain
// authorization on the exported stream as well, so cancellation/deadline is
// still checked when the caller consumes previously materialized data.
struct GuardedReader {
    reader: arrow58::ffi_stream::ArrowArrayStreamReader,
    context: OwnedSourceContext,
}
impl Iterator for GuardedReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        let error = |e| ArrowError::ExternalError(Box::new(SourceLanceError::from(e)));
        if let Err(e) = self.context.check_state() {
            return Some(Err(error(e)));
        }
        let result = self.reader.next();
        if let Err(e) = self.context.check_state() {
            return Some(Err(error(e)));
        }
        result.map(|r| {
            r.map_err(|e| ArrowError::ExternalError(Box::new(SourceLanceError::from_error(&e))))
        })
    }
}
impl RecordBatchReader for GuardedReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}
unsafe fn checked_export(
    output: *mut u8,
    context: &OwnedSourceContext,
    call: impl FnOnce(*mut u8) -> lance::Result<()>,
) -> Result<()> {
    // Keep ownership local until the post-operation authorization check passes.
    // If cancellation wins after materialization, Drop releases the Arrow stream.
    let mut stream = FFI_ArrowArrayStream::empty();
    checked(context, || {
        call((&mut stream as *mut FFI_ArrowArrayStream).cast())
    })?;
    let reader = arrow58::ffi_stream::ArrowArrayStreamReader::try_new(stream)
        .map_err(|e| SourceLanceError::from_error(&e))?;
    let exported = FFI_ArrowArrayStream::new(Box::new(GuardedReader {
        reader,
        context: context.clone(),
    }));
    unsafe { std::ptr::write(output.cast(), exported) };
    Ok(())
}
pub(crate) unsafe fn source_create_scanner(
    dataset: &BlockingDataset,
    schema: *mut u8,
    batch_size: u32,
) -> Result<Box<BlockingScanner>> {
    checked(context(dataset)?, || unsafe {
        legacy::create_scanner(dataset, schema, batch_size)
    })
}
pub(crate) fn source_count_rows(scanner: &BlockingScanner) -> Result<u64> {
    let context = scanner
        .source_context
        .as_ref()
        .ok_or_else(|| SourceLanceError::from(SourceFailure::Denied))?;
    checked(context, || scanner.count_rows())
}
pub(crate) unsafe fn source_scanner_stream(
    scanner: &BlockingScanner,
    output: *mut u8,
) -> Result<()> {
    let context = scanner
        .source_context
        .as_ref()
        .ok_or_else(|| SourceLanceError::from(SourceFailure::Denied))?;
    unsafe { checked_export(output, context, |out| scanner.open_stream(out)) }
}
pub(crate) unsafe fn source_dataset_take(
    dataset: &BlockingDataset,
    indices: &[u64],
    schema: *mut u8,
    output: *mut u8,
) -> Result<()> {
    unsafe {
        checked_export(output, context(dataset)?, |out| {
            legacy::dataset_take(dataset, indices, schema, out)
        })
    }
}
