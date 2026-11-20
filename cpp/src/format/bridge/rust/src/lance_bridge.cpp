// Copyright 2023 Zilliz
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

#include "lance_bridge.h"
#include "bridge_util.h"

#include <memory>
#include <charconv>
#include "source_context_bridge.h"
#include "milvus-storage/filesystem/azure/source_sas_token_policy.h"

namespace milvus_storage::lance {

namespace {
void CheckSource(const SourceReadContextPtr& context) {
  if (context) {
    auto status = fs::SourceCredentialArrowStatus(context->CheckState());
    if (!status.ok())
      throw LanceException(std::move(status));
  }
}
class SourceBatchReader final : public arrow::RecordBatchReader {
  public:
  explicit SourceBatchReader(std::shared_ptr<arrow::RecordBatchReader> reader) : reader_(std::move(reader)) {}
  std::shared_ptr<arrow::Schema> schema() const override { return reader_->schema(); }
  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    auto status = reader_->ReadNext(batch);
    return status.ok() ? status : SourceLanceBridgeStatus(status.message());
  }
  arrow::Status Close() override {
    auto status = reader_->Close();
    return status.ok() ? status : SourceLanceBridgeStatus(status.message());
  }

  private:
  std::shared_ptr<arrow::RecordBatchReader> reader_;
};
}  // namespace
arrow::Status SourceLanceBridgeStatus(std::string_view message) {
  constexpr std::string_view marker = "__MILVUS_SOURCE_LANCE__=";
  const auto pos = message.find(marker);
  if (pos != std::string_view::npos) {
    auto code = message.substr(pos + marker.size());
    const auto end = code.find(';');
    if (end != std::string_view::npos) {
      code = code.substr(0, end);
      if (code == "117")
        return fs::SourceCredentialArrowStatus(LOON_SOURCE_CREDENTIAL_DENIED);
      if (code == "118")
        return fs::SourceCredentialArrowStatus(LOON_SOURCE_CREDENTIAL_EXPIRED);
      if (code == "119")
        return fs::SourceCredentialArrowStatus(LOON_SOURCE_CREDENTIAL_TRANSIENT);
      if (code == "120")
        return fs::SourceCredentialArrowStatus(LOON_SOURCE_CREDENTIAL_CANCELLED);
      if (code == "121")
        return fs::SourceCredentialArrowStatus(LOON_SOURCE_CREDENTIAL_INVALID_RESPONSE);
    }
  }
  return arrow::Status::UnknownError("Source Lance operation failed");
}
std::shared_ptr<arrow::RecordBatchReader> WrapSourceLanceBatchReader(std::shared_ptr<arrow::RecordBatchReader> reader) {
  return std::make_shared<SourceBatchReader>(std::move(reader));
}

std::shared_ptr<BlockingDataset> BlockingDataset::OpenWithContext(const std::string& source_uri,
                                                                  const std::string& lance_uri,
                                                                  const StorageOptions& options,
                                                                  SourceReadContextPtr context,
                                                                  uint64_t version) {
  if (!context)
    throw LanceException(fs::SourceCredentialArrowStatus(LOON_SOURCE_CREDENTIAL_DENIED));
  CheckSource(context);
  uint32_t parallelism = 0;
  if (auto it = options.find("lance_io_parallelism"); it != options.end()) {
    auto result = std::from_chars(it->second.data(), it->second.data() + it->second.size(), parallelism);
    if (result.ec != std::errc() || result.ptr != it->second.data() + it->second.size() || parallelism > 256)
      throw LanceException(arrow::Status::Invalid("Invalid source Lance parallelism"));
  }
  try {
    auto bridge = std::make_shared<source_context::ffi::SourceContextBridge>(context);
    auto result = ffi::open_source_dataset(std::move(bridge), source_uri, lance_uri, parallelism, version);
    CheckSource(context);
    return std::make_shared<BlockingDataset>(std::move(result), std::move(context));
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(SourceLanceBridgeStatus(e.what()));
  }
}

void ReplaceLanceRuntime(uint32_t num_threads) {}

using milvus_storage::ConvertStorageOptions;

std::shared_ptr<BlockingDataset> BlockingDataset::Open(const std::string& uri,
                                                       const StorageOptions& storage_options,
                                                       uint64_t version) {
  try {
    rust::Vec<rust::String> keys, values;
    ConvertStorageOptions(storage_options, keys, values);
    return std::make_shared<BlockingDataset>(
        ffi::open_dataset(rust::Str(uri.data(), uri.length()), std::move(keys), std::move(values), version));
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(e.what());
  }
}

uint64_t BlockingDataset::ResolveLatestVersion(const std::string& uri, const StorageOptions& storage_options) {
  try {
    rust::Vec<rust::String> keys, values;
    ConvertStorageOptions(storage_options, keys, values);
    return ffi::resolve_latest_dataset_version(rust::Str(uri.data(), uri.length()), std::move(keys), std::move(values));
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(e.what());
  }
}

uint64_t BlockingDataset::Version() const {
  CheckSource(source_context_);
  return impl_->version();
}

std::unique_ptr<BlockingDataset> BlockingDataset::OpenUnique(const std::string& uri,
                                                             const StorageOptions& storage_options) {
  try {
    rust::Vec<rust::String> keys, values;
    ConvertStorageOptions(storage_options, keys, values);
    return std::make_unique<BlockingDataset>(
        ffi::open_dataset(rust::Str(uri.data(), uri.length()), std::move(keys), std::move(values), 0));
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(e.what());
  }
}

std::unique_ptr<BlockingDataset> BlockingDataset::WriteDataset(const std::string& uri,
                                                               struct ArrowArrayStream* stream,
                                                               const StorageOptions& storage_options,
                                                               LanceDataStorageFormat format,
                                                               bool enable_stable_row_ids) {
  try {
    rust::Vec<rust::String> keys, values;
    ConvertStorageOptions(storage_options, keys, values);
    auto ffi_format = static_cast<ffi::LanceDataStorageFormat>(format);
    return std::make_unique<BlockingDataset>(ffi::write_dataset(rust::Str(uri.data(), uri.length()),
                                                                reinterpret_cast<uint8_t*>(stream), std::move(keys),
                                                                std::move(values), ffi_format, enable_stable_row_ids));
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(e.what());
  }
}

void BlockingDataset::DeleteRows(const std::string& predicate) {
  if (source_context_)
    throw LanceException(fs::SourceCredentialArrowStatus(LOON_SOURCE_CREDENTIAL_DENIED));
  try {
    ffi::dataset_delete_rows(*impl_, rust::Str(predicate.data(), predicate.length()));
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

std::vector<uint64_t> BlockingDataset::GetAllFragmentIds() const {
  try {
    CheckSource(source_context_);
    auto fragment_ids = impl_->get_all_fragment_ids();
    return {fragment_ids.begin(), fragment_ids.end()};
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

std::vector<uint64_t> BlockingDataset::GetFragmentDeletionPositions(uint64_t fragment_id) const {
  try {
    auto positions = source_context_ ? ffi::source_fragment_deletions(*impl_, fragment_id)
                                     : ffi::get_fragment_deletion_positions(*impl_, fragment_id);
    return {positions.begin(), positions.end()};
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

uint64_t BlockingDataset::GetFragmentPhysicalRowCount(uint64_t fragment_id) const {
  try {
    return source_context_ ? ffi::source_fragment_rows(*impl_, fragment_id, true)
                           : ffi::get_fragment_physical_row_count(*impl_, fragment_id);
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

uint64_t BlockingDataset::GetFragmentRowCount(uint64_t fragment_id) const {
  try {
    return source_context_ ? ffi::source_fragment_rows(*impl_, fragment_id, false)
                           : ffi::get_fragment_row_count(*impl_, fragment_id);
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

void BlockingDataset::GetFragmentSchema(uint64_t fragment_id, ArrowSchema& out_schema) const {
  try {
    if (source_context_)
      ffi::source_fragment_schema(*impl_, fragment_id, reinterpret_cast<uint8_t*>(&out_schema));
    else
      ffi::get_fragment_schema(*impl_, fragment_id, reinterpret_cast<uint8_t*>(&out_schema));
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

void BlockingDataset::WriteArrowArrayStream(struct ArrowArrayStream* stream) {
  if (source_context_)
    throw LanceException(fs::SourceCredentialArrowStatus(LOON_SOURCE_CREDENTIAL_DENIED));
  try {
    impl_->write_stream(reinterpret_cast<uint8_t*>(stream));
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

std::unique_ptr<BlockingFragmentReader> BlockingFragmentReader::Open(const BlockingDataset& dataset,
                                                                     uint64_t fragment_id,
                                                                     ArrowSchema& schema,
                                                                     const FragmentFilesMetadata* metadata) {
  try {
    auto impl = dataset.SourceContext()
                    ? ffi::source_open_fragment(dataset.Impl(), fragment_id, reinterpret_cast<uint8_t*>(&schema),
                                                metadata ? &metadata->Impl() : nullptr)
                    : ffi::open_fragment_reader(dataset.Impl(), fragment_id, reinterpret_cast<uint8_t*>(&schema),
                                                metadata ? &metadata->Impl() : nullptr);
    return std::make_unique<BlockingFragmentReader>(std::move(impl), dataset.SourceContext());
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(dataset.SourceContext() ? SourceLanceBridgeStatus(e.what())
                                                 : arrow::Status::IOError(e.what()));
  }
}

uint64_t BlockingFragmentReader::RowCount() const {
  try {
    CheckSource(source_context_);
    return impl_->number_of_rows();
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

arrow::Result<std::vector<uint64_t>> BlockingFragmentReader::EstimateFragmentColumnMemory() const {
  try {
    auto estimates =
        source_context_ ? ffi::source_fragment_estimates(*impl_) : impl_->estimate_fragment_column_memory();
    std::vector<uint64_t> memory_sizes;
    memory_sizes.reserve(estimates.size());
    for (const auto& estimate : estimates) {
      memory_sizes.push_back(estimate.memory_size);
    }
    return memory_sizes;
  } catch (const rust::cxxbridge1::Error& e) {
    if (source_context_)
      return SourceLanceBridgeStatus(e.what());
    return arrow::Status::NotImplemented("Lance column memory size estimation is not available: ", e.what());
  }
}

arrow::Result<uint64_t> BlockingFragmentReader::EstimateFragmentMemory() const {
  try {
    if (source_context_) {
      ARROW_ASSIGN_OR_RAISE(auto sizes, EstimateFragmentColumnMemory());
      uint64_t total = 0;
      for (auto size : sizes) total = size > UINT64_MAX - total ? UINT64_MAX : total + size;
      return total;
    }
    return impl_->estimate_fragment_memory();
  } catch (const rust::cxxbridge1::Error& e) {
    if (source_context_)
      return SourceLanceBridgeStatus(e.what());
    return arrow::Status::NotImplemented("Lance memory size estimation is not available: ", e.what());
  }
}

std::shared_ptr<const FragmentFilesMetadata> BlockingFragmentReader::GetFilesMetadata() const {
  CheckSource(source_context_);
  return std::make_shared<FragmentFilesMetadata>(impl_->get_files_metadata());
}

void BlockingFragmentReader::TakeAsSingleBatch(const std::vector<int64_t>& indices, ArrowArray& out_array) {
  try {
    std::vector<uint32_t> uint32_indices(indices.begin(), indices.end());
    rust::Slice<const uint32_t> indices_slice(uint32_indices.data(), uint32_indices.size());
    if (source_context_)
      ffi::source_take_batch(*impl_, indices_slice, reinterpret_cast<uint8_t*>(&out_array));
    else
      impl_->take_as_single_batch(indices_slice, reinterpret_cast<uint8_t*>(&out_array));
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

ArrowArrayStream BlockingFragmentReader::TakeAsStream(const std::vector<int64_t>& indices, uint32_t batch_size) {
  try {
    ArrowArrayStream stream;
    std::vector<uint32_t> uint32_indices(indices.begin(), indices.end());
    rust::Slice<const uint32_t> indices_slice(uint32_indices.data(), uint32_indices.size());
    if (source_context_)
      ffi::source_fragment_stream(*impl_, 2, indices_slice, 0, 0, batch_size, reinterpret_cast<uint8_t*>(&stream));
    else
      impl_->take_as_stream(indices_slice, batch_size, reinterpret_cast<uint8_t*>(&stream));
    return stream;
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

ArrowArrayStream BlockingFragmentReader::ReadAllAsStream(uint32_t batch_size) {
  try {
    ArrowArrayStream stream;
    if (source_context_)
      ffi::source_fragment_stream(*impl_, 0, {}, 0, 0, batch_size, reinterpret_cast<uint8_t*>(&stream));
    else
      impl_->read_all_as_stream(batch_size, reinterpret_cast<uint8_t*>(&stream));
    return stream;
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

ArrowArrayStream BlockingFragmentReader::ReadRangesAsStream(uint32_t row_range_start,
                                                            uint32_t row_range_end,
                                                            uint32_t batch_size) {
  try {
    ArrowArrayStream stream;
    if (source_context_)
      ffi::source_fragment_stream(*impl_, 1, {}, row_range_start, row_range_end, batch_size,
                                  reinterpret_cast<uint8_t*>(&stream));
    else
      impl_->read_ranges_as_stream(row_range_start, row_range_end, batch_size, reinterpret_cast<uint8_t*>(&stream));
    return stream;
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

std::unique_ptr<BlockingScanner> BlockingDataset::Scan(ArrowSchema& schema, uint32_t batch_size) {
  try {
    auto impl = source_context_ ? ffi::source_create_scanner(*impl_, reinterpret_cast<uint8_t*>(&schema), batch_size)
                                : ffi::create_scanner(*impl_, reinterpret_cast<uint8_t*>(&schema), batch_size);
    return std::make_unique<BlockingScanner>(std::move(impl), source_context_);
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

#ifdef BUILD_GTEST
arrow::Status ExternalizeRowIdsForTesting(const std::string& uri) {
  try {
    ffi::externalize_row_ids_for_testing(rust::Str(uri.data(), uri.length()));
    return arrow::Status::OK();
  } catch (const rust::cxxbridge1::Error& e) {
    return arrow::Status::IOError("Failed to externalize Lance row IDs for testing: ", e.what());
  }
}

LanceIOStats BlockingDataset::IOStatsIncremental() {
  static_assert(noexcept(impl_->io_stats_incremental()), "Lance IO statistics must remain infallible");
  auto stats = impl_->io_stats_incremental();
  return {stats.read_iops, stats.read_bytes};
}
#endif  // BUILD_GTEST

ArrowArrayStream BlockingDataset::Take(const std::vector<int64_t>& indices, ArrowSchema& schema) {
  try {
    ArrowArrayStream stream;
    std::vector<uint64_t> uint64_indices(indices.begin(), indices.end());
    rust::Slice<const uint64_t> indices_slice(uint64_indices.data(), uint64_indices.size());
    if (source_context_)
      ffi::source_dataset_take(*impl_, indices_slice, reinterpret_cast<uint8_t*>(&schema),
                               reinterpret_cast<uint8_t*>(&stream));
    else
      ffi::dataset_take(*impl_, indices_slice, reinterpret_cast<uint8_t*>(&schema),
                        reinterpret_cast<uint8_t*>(&stream));
    return stream;
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

uint64_t BlockingScanner::CountRows() const {
  try {
    return source_context_ ? ffi::source_count_rows(*impl_) : impl_->count_rows();
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

ArrowArrayStream BlockingScanner::OpenStream() {
  try {
    ArrowArrayStream stream;
    if (source_context_)
      ffi::source_scanner_stream(*impl_, reinterpret_cast<uint8_t*>(&stream));
    else
      impl_->open_stream(reinterpret_cast<uint8_t*>(&stream));
    return stream;
  } catch (const rust::cxxbridge1::Error& e) {
    throw LanceException(source_context_ ? SourceLanceBridgeStatus(e.what()) : arrow::Status::IOError(e.what()));
  }
}

}  // namespace milvus_storage::lance
