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

#include "source_context_bridge.h"
#include "rust-bridge/lib.h"

#include <string_view>

namespace milvus_storage::source_context::ffi {

int64_t SourceContextBridge::now_unix_ms() const { return context_ ? context_->NowUnixMillis() : 0; }

uint64_t SourceContextBridge::identity() const { return context_ ? context_->identity() : 0; }
int64_t SourceContextBridge::request_timeout_ms() const { return context_ ? context_->request_timeout_ms() : 0; }

uint32_t SourceContextBridge::check_state() const {
  return context_ ? context_->CheckState() : LOON_SOURCE_CREDENTIAL_DENIED;
}

std::shared_ptr<SourceContextBridge> SourceContextBridge::retain() const {
  return std::make_shared<SourceContextBridge>(context_);
}

ResolvedSource SourceContextBridge::resolve(rust::Str resource_uri) const {
  if (!context_)
    return {LOON_SOURCE_CREDENTIAL_DENIED, {}, {}, 0, LOON_SOURCE_READ};
  const auto target = context_->Resolve(std::string_view(resource_uri.data(), resource_uri.size()));
  return {static_cast<uint32_t>(target.status), rust::String(target.resource_uri), rust::String(target.scope_root_uri),
          target.root_index, static_cast<uint32_t>(target.operation)};
}

ResolvedSource SourceContextBridge::resolve_http(rust::Str method, rust::Str uri) const {
  if (!context_)
    return {LOON_SOURCE_CREDENTIAL_DENIED, {}, {}, 0, LOON_SOURCE_READ};
  const auto target = context_->ResolveHttpRequest(std::string_view(method.data(), method.size()),
                                                   std::string_view(uri.data(), uri.size()));
  return {static_cast<uint32_t>(target.status), rust::String(target.resource_uri), rust::String(target.scope_root_uri),
          target.root_index, static_cast<uint32_t>(target.operation)};
}

SourceCredential SourceContextBridge::fetch(rust::Str resource_uri, uint32_t operation) const {
  if (!context_ || (operation != LOON_SOURCE_READ && operation != LOON_SOURCE_LIST)) {
    return {LOON_SOURCE_CREDENTIAL_DENIED, {}, 0};
  }
  auto credential = context_->Fetch(std::string_view(resource_uri.data(), resource_uri.size()),
                                    static_cast<LoonSourceReadOperation>(operation));
  return {static_cast<uint32_t>(credential.status), rust::String(credential.sas_token), credential.expires_at_unix_ms};
}

}  // namespace milvus_storage::source_context::ffi
