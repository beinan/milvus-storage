#pragma once
#include <atomic>
#include <condition_variable>
#include <mutex>
#include "source_context_bridge.h"
namespace milvus_storage::source_context::test {
struct State {
  std::atomic<int64_t> now{1893456000000};
  std::atomic<uint32_t> status{0};
  std::atomic<uint64_t> calls{0}, list_calls{0}, destroyed{0};
  std::mutex mutex;
  std::condition_variable cv;
  bool blocked = false;
};
class Harness {
 public:
  Harness();
  std::shared_ptr<ffi::SourceContextBridge> context() const;
  void drop_owner() const { owner_.reset(); }
  void cancel() const { if(auto p = weak_.lock()) p->Cancel(); }
  void set_time(int64_t value) const { state_->now.store(value); }
  void set_status(uint32_t value) const { state_->status.store(value); }
  void block(bool value) const { std::lock_guard lock(state_->mutex); state_->blocked = value; state_->cv.notify_all(); }
  uint64_t calls() const { return state_->calls.load(); }
  uint64_t list_calls() const { return state_->list_calls.load(); }
  uint64_t destroyed() const { return state_->destroyed.load(); }
 private:
  std::shared_ptr<State> state_;
  mutable SourceReadContextPtr owner_;
  std::weak_ptr<SourceReadContext> weak_;
};
std::shared_ptr<Harness> make_harness();
}
