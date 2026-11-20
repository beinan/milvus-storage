#include "test_support.h"
#include "milvus-source-context-native-tests/src/lib.rs.h"
#include <ctime>
namespace milvus_storage::source_context::test {
class Provider final : public SourceCredentialProvider {
 public:
  explicit Provider(std::shared_ptr<State> state) : state_(std::move(state)) {}
  ~Provider() override { ++state_->destroyed; }
  SourceCredentialResponse Fetch(const SourceCredentialRequest& req) const override {
    const auto call = ++state_->calls;
    if (req.operation == LOON_SOURCE_LIST) ++state_->list_calls;
    { std::unique_lock lock(state_->mutex); state_->cv.wait(lock, [&]{return !state_->blocked;}); }
    SourceCredentialResponse result;
    result.status = static_cast<LoonSourceCredentialStatus>(state_->status.load());
    result.kind = LOON_SOURCE_CREDENTIAL_KIND_AZURE_SAS;
    result.account_name = "acct";
    result.scope_root_uri = req.scope_root_uri;
    result.expires_at_unix_ms = state_->now.load() + 120000;
    auto t = static_cast<time_t>(result.expires_at_unix_ms / 1000);
    tm calendar{}; gmtime_r(&t, &calendar); char expiry[64];
    strftime(expiry, sizeof(expiry), "%Y-%m-%dT%H:%M:%SZ", &calendar);
    result.sas_token = std::string("sr=d&sdd=2&sp=") + (req.operation == LOON_SOURCE_LIST ? "l" : "r") + "&se=" + expiry + "&sig=lease" + std::to_string(call);
    return result;
  }
 private:
  std::shared_ptr<State> state_;
};
Harness::Harness() : state_(std::make_shared<State>()) {
  auto state=state_;
  auto made=SourceReadContext::Create({"acct", {"https://acct.blob.core.windows.net/tables/team/table"}, 0, 3000}, std::make_shared<Provider>(state), [state]{return state->now.load();});
  owner_=made.ValueOrDie(); weak_=owner_;
}
std::shared_ptr<ffi::SourceContextBridge> Harness::context() const {return std::make_shared<ffi::SourceContextBridge>(owner_);}
std::shared_ptr<Harness> make_harness() {return std::make_shared<Harness>();}
}

