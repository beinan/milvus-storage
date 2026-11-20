// Copyright 2026 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <memory>

#include "rust/cxx.h"
#include "milvus-storage/source_read_context.h"

namespace milvus_storage::source_context::ffi {

struct ResolvedSource;
struct SourceCredential;

// Owns the runtime context independently of an engine/C ABI handle. No handle,
// credential or context identity is reconstructed from storage properties.
class SourceContextBridge final {
  public:
  explicit SourceContextBridge(SourceReadContextPtr context) : context_(std::move(context)) {}
  uint64_t identity() const;
  int64_t now_unix_ms() const;
  uint32_t check_state() const;
  int64_t request_timeout_ms() const;
  std::shared_ptr<SourceContextBridge> retain() const;
  ResolvedSource resolve(rust::Str resource_uri) const;
  ResolvedSource resolve_http(rust::Str method, rust::Str uri) const;
  SourceCredential fetch(rust::Str resource_uri, uint32_t operation) const;

  private:
  const SourceReadContextPtr context_;
};

}  // namespace milvus_storage::source_context::ffi
