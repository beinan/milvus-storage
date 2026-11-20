// Copyright 2026 Zilliz
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <memory>
#include "rust/cxx.h"

namespace Azure::Core::Http {
class HttpTransport;
}

namespace milvus_storage::source_context::test {
struct HttpHeader;
struct HttpResponse;

// Implemented only in WITH_UT builds. Unique context identity prevents a test
// transport from intercepting another reader's traffic.
class ScopedHttpTransport final {
  public:
  ScopedHttpTransport(uint64_t identity, std::shared_ptr<Azure::Core::Http::HttpTransport> transport);
  ~ScopedHttpTransport();
  ScopedHttpTransport(const ScopedHttpTransport&) = delete;
  ScopedHttpTransport& operator=(const ScopedHttpTransport&) = delete;

  private:
  uint64_t identity_;
};

bool has_http_transport(uint64_t identity);
HttpResponse send_http_request(uint64_t identity, rust::Str method, rust::Str uri, rust::Vec<HttpHeader> headers);
}  // namespace milvus_storage::source_context::test
