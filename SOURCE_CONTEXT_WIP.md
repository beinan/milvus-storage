# WIP: source-authorized readers on 3.0

This branch is an integration checkpoint, not a buildable release or dependency pin. It snapshots an in-progress adaptation of the main source-context work onto the actual PR #817 merge, `f6b5e15e89aa968d1cc1a2d4739211f35a726a22`. It is not an upstream backport submission.

No commits from #824, #825, or #829 are prerequisites or incorporated here. Legacy Azure SAS provider policy remains at the #817 base; held short-lease policy changes are excluded. The new, separately scoped source credential cache has its own renewal policy.

## Incomplete work

- `cpp/src/format/lance/lance_format.cpp` and `cpp/src/format/lance/lance_table_reader.cpp` intentionally retain merge-conflict markers. The imported main-side source methods still require adaptation to 3.0.
- The source-aware Lance Rust bridge, filesystem/object-store integration, cache/scheduler isolation, and typed error propagation are not implemented on this branch. Existing source Lance declarations must not be interpreted as working support.
- A full native build and end-to-end engine integration have not been completed. Do not select this head as a production dependency.
- Original-user authorization and full Milvus query tests remain outstanding.

## Scoped verification completed before this snapshot

- 51 C++ source context/filesystem/policy tests using current changed objects and a supporting library. This was not a full build of this checkout.
- 34 Rust/native tests passed using the actual current C++ bridge/context and production Rust modules. These cover credential ownership, scope, typed errors, expiry, cancellation, and per-HTTP-attempt renewal under injected retries.
- A negative control that omitted renewed SAS installation failed the expiry/retry test as expected.
- Current full Rust static library build and test compilation passed.
- Direct C++ to Rust Iceberg planner/FileIO tests passed for source status propagation, root/cancel rejection, and finite unknown-error detail. This is not a full FormatExplore or engine test.
- Live Azure validation of the new Rust path was still running when this snapshot was made. Earlier real Azure table and multi-hour reader runs exercise a separate main C++ implementation and do not qualify this entire branch.

The full changed-file list and unresolved files are recorded in `SOURCE_CONTEXT_WIP.json`.

## LIST HTTP prerequisite follow-up

The additive CXX `resolve_http` method reuses existing native request classification. The source HTTP adapter can opt into directory listing, with separate READ/LIST credential caches and fresh credentials for retry/pagination. Production Iceberg reads keep listing disabled. This is a prerequisite; the Lance provider and reader are still not wired.

The relocatable harness under `cpp/test/source_context_rust_harness` passed 37 tests with 1 intentionally ignored exploratory probe. The corresponding standalone development harness passed the same 37 tests; its live probe is separate. A negative control routing LIST through the READ cache failed as expected. Full Rust check and static library build passed after the CXX update. These tests use injected HTTP/clock/provider and actual native context/OpenDAL.

The first real Azure Rust retained-reader run passed the initial read, one actual expiration, and broker 503/429 recovery, then failed at the next expiry because the shared port-forward exited. The same infrastructure failure ended the separate main C++ soak after 42 successful reads/94 minutes. Neither run establishes completed multi-hour validation. The pod services remained running; supervised forwarding and a separate frozen-binary Rust rerun were started. No expired credential fallback was used. Full multi-hour validation and original-user engine tests remain outstanding.

## Real Azure retained Rust reader result

After restoring forwarding, the exact frozen binary from the first checkpoint completed a fresh run successfully: 3 reads through the same retained Iceberg FileIO reader, two actual 120-second SAS expirations, and broker statuses `[200, 503, 429, 200, 200]`; elapsed 298.43 seconds, exit 0. Returned bytes matched the fixture each time. This exercised the first checkpoint's production Rust HTTP/read modules and real native context through the existing isolated fixed-service Catalog. It did not exercise the newly added LIST opt-in, a complete Iceberg table, the engine, or original-user OBO. The failed first run remains separate evidence.

The current C++ bridge plus full Rust static library were also relinked and tested after the additive CXX change: all five source status categories, transient retries, context/root/cancellation rejection, and finite unknown-planner detail passed. Supporting legacy native dependencies came from the earlier prototype; a full native build of this WIP branch remains outstanding.
