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

use crate::source_azure_storage::SourceStorageFailure;
use crate::source_context_ffi::{
    IcebergSourcePlanResult, SourceIcebergFileInfo, SourcePlanFailure,
};
use crate::source_read_context::SourceFailure;

/// Preserve the actual typed causes from native authorization and Azure I/O.
/// Unrecognized planner errors remain explicitly unclassified; neither their
/// text nor any nested signed URI is copied into the CXX result.
pub(crate) fn source_plan_result(
    result: anyhow::Result<Vec<SourceIcebergFileInfo>>,
) -> IcebergSourcePlanResult {
    let error = match result {
        Ok(files) => {
            return IcebergSourcePlanResult {
                files,
                failure: SourcePlanFailure::None,
                io_temporary: false,
                io_persistent: false,
            };
        }
        Err(error) => error,
    };
    let mut result = IcebergSourcePlanResult {
        files: Vec::new(),
        failure: SourcePlanFailure::OtherPlannerFailure,
        io_temporary: false,
        io_persistent: false,
    };
    for cause in error.chain() {
        if let Some(failure) = cause.downcast_ref::<SourceFailure>() {
            result.failure = match failure {
                SourceFailure::Denied => SourcePlanFailure::Denied,
                SourceFailure::Expired => SourcePlanFailure::Expired,
                SourceFailure::Transient => SourcePlanFailure::CredentialTransient,
                SourceFailure::Cancelled => SourcePlanFailure::Cancelled,
                SourceFailure::InvalidResponse => SourcePlanFailure::InvalidCredentialResponse,
            };
            return result;
        }
        if let Some(failure) = cause.downcast_ref::<SourceStorageFailure>() {
            use opendal::ErrorKind as Io;
            result.failure = match failure.kind {
                Io::Unexpected => SourcePlanFailure::IoUnexpected,
                Io::Unsupported => SourcePlanFailure::IoUnsupported,
                Io::ConfigInvalid => SourcePlanFailure::IoConfigInvalid,
                Io::NotFound => SourcePlanFailure::IoNotFound,
                Io::PermissionDenied => SourcePlanFailure::IoPermissionDenied,
                Io::IsADirectory => SourcePlanFailure::IoIsADirectory,
                Io::NotADirectory => SourcePlanFailure::IoNotADirectory,
                Io::AlreadyExists => SourcePlanFailure::IoAlreadyExists,
                Io::RateLimited => SourcePlanFailure::IoRateLimited,
                Io::IsSameFile => SourcePlanFailure::IoIsSameFile,
                Io::ConditionNotMatch => SourcePlanFailure::IoConditionNotMatch,
                Io::RangeNotSatisfied => SourcePlanFailure::IoRangeNotSatisfied,
                _ => SourcePlanFailure::IoUnknown,
            };
            result.io_temporary = failure.temporary;
            result.io_persistent = failure.persistent;
            return result;
        }
    }
    result
}
