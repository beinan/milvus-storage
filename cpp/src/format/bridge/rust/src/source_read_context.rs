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

//! Runtime-only source authorization. Never encode a native handle or this
//! identity into serialized properties; callers must pass the owned context.

use std::fmt;

use cxx::SharedPtr;

use crate::source_context_ffi::{ResolvedSource, SourceContextBridge, SourceCredential};

// SAFETY: the C++ wrapper has immutable shared ownership. SourceReadContext
// synchronizes terminal state and its provider contract requires thread-safe
// callbacks. No borrowed engine buffers or caller handles are retained here.
unsafe impl Send for SourceContextBridge {}
unsafe impl Sync for SourceContextBridge {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceFailure {
    Denied,
    Expired,
    Transient,
    Cancelled,
    InvalidResponse,
}

impl SourceFailure {
    pub(crate) fn from_status(status: u32) -> Result<(), Self> {
        match status {
            0 => Ok(()),
            1 => Err(Self::Denied),
            2 => Err(Self::Expired),
            3 => Err(Self::Transient),
            4 => Err(Self::Cancelled),
            _ => Err(Self::InvalidResponse),
        }
    }
}

impl fmt::Display for SourceFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Denied => "source authorization denied",
            Self::Expired => "source authorization expired",
            Self::Transient => "source credential provider temporarily unavailable",
            Self::Cancelled => "source operation cancelled",
            Self::InvalidResponse => "invalid source credential response",
        })
    }
}

impl std::error::Error for SourceFailure {}

#[derive(Clone, Copy)]
pub(crate) enum SourceOperation {
    Read = 0,
    List = 1,
}

#[derive(Clone)]
pub(crate) struct OwnedSourceContext {
    native: SharedPtr<SourceContextBridge>,
}

impl OwnedSourceContext {
    pub(crate) fn new(native: SharedPtr<SourceContextBridge>) -> Result<Self, SourceFailure> {
        if native.is_null() || native.identity() == 0 {
            return Err(SourceFailure::Denied);
        }
        let context = Self { native };
        context.check_state()?;
        Ok(context)
    }

    pub(crate) fn now_unix_ms(&self) -> i64 {
        self.native.now_unix_ms()
    }

    pub(crate) fn identity(&self) -> u64 {
        self.native.identity()
    }

    pub(crate) fn request_timeout_ms(&self) -> u64 {
        // Native Create requires a positive finite request timeout.
        self.native.request_timeout_ms() as u64
    }

    /// Check before every cached read as well as after potentially blocking work.
    pub(crate) fn check_state(&self) -> Result<(), SourceFailure> {
        SourceFailure::from_status(self.native.check_state())
    }

    pub(crate) fn resolve(&self, resource_uri: &str) -> Result<ResolvedSource, SourceFailure> {
        self.check_state()?;
        let resolved = self
            .native
            .resolve(resource_uri)
            .map_err(|_| SourceFailure::InvalidResponse)?;
        SourceFailure::from_status(resolved.status)?;
        Ok(resolved)
    }

    pub(crate) fn resolve_http(
        &self,
        method: &str,
        uri: &str,
    ) -> Result<ResolvedSource, SourceFailure> {
        self.check_state()?;
        let resolved = self
            .native
            .resolve_http(method, uri)
            .map_err(|_| SourceFailure::InvalidResponse)?;
        SourceFailure::from_status(resolved.status)?;
        Ok(resolved)
    }

    /// Engine callbacks can perform blocking I/O. Run them on Tokio's blocking
    /// pool, retaining native ownership until completion even if this future is
    /// dropped. The engine callback must still honor its finite request deadline.
    pub(crate) async fn fetch(
        &self,
        resource_uri: &str,
        operation: SourceOperation,
    ) -> Result<SourceCredential, SourceFailure> {
        let resolved = self.resolve(resource_uri)?;
        let retained = self.clone();
        let credential = tokio::task::spawn_blocking(move || {
            retained.check_state()?;
            let response = retained
                .native
                .fetch(&resolved.resource_uri, operation as u32)
                .map_err(|_| SourceFailure::InvalidResponse)?;
            retained.check_state()?;
            SourceFailure::from_status(response.status)?;
            Ok(response)
        })
        .await
        .map_err(|_| SourceFailure::InvalidResponse)??;
        self.check_state()?;
        Ok(credential)
    }
}
