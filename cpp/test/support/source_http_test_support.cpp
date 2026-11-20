// Copyright 2026 Zilliz
// SPDX-License-Identifier: Apache-2.0

#ifdef BUILD_GTEST
#include "source_http_test_support.h"
#include "rust-bridge/source_test_transport.h"

#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <azure/core/http/http.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>

namespace milvus_storage::source_context::test {
namespace {
std::mutex mutex;
std::unordered_map<uint64_t, std::shared_ptr<Azure::Core::Http::HttpTransport>> transports;
}  // namespace

ScopedHttpTransport::ScopedHttpTransport(uint64_t identity, std::shared_ptr<Azure::Core::Http::HttpTransport> transport)
    : identity_(identity) {
  std::lock_guard lock(mutex);
  if (!identity || !transport || !transports.emplace(identity, std::move(transport)).second)
    throw std::logic_error("Invalid or duplicate native test transport");
}

ScopedHttpTransport::~ScopedHttpTransport() {
  std::lock_guard lock(mutex);
  transports.erase(identity_);
}

bool has_http_transport(uint64_t identity) {
  std::lock_guard lock(mutex);
  return transports.contains(identity);
}

HttpResponse send_http_request(uint64_t identity, rust::Str method, rust::Str uri, rust::Vec<HttpHeader> headers) {
  std::shared_ptr<Azure::Core::Http::HttpTransport> transport;
  {
    std::lock_guard lock(mutex);
    auto it = transports.find(identity);
    if (it == transports.end())
      throw std::runtime_error("Native test transport was released");
    transport = it->second;
  }
  Azure::Core::Http::Request request{Azure::Core::Http::HttpMethod(std::string(method)),
                                     Azure::Core::Url(std::string(uri))};
  for (const auto& header : headers) request.SetHeader(std::string(header.name), std::string(header.value));
  auto response = transport->Send(request, Azure::Core::Context{});
  HttpResponse result{static_cast<uint16_t>(response->GetStatusCode()), {}, {}};
  for (const auto& [name, value] : response->GetHeaders())
    result.headers.push_back({rust::String(name), rust::String(value)});
  auto body = response->GetBody();
  if (auto stream = response->ExtractBodyStream())
    body = stream->ReadToEnd();
  for (auto byte : body) result.body.push_back(byte);
  return result;
}
}  // namespace milvus_storage::source_context::test
#endif
