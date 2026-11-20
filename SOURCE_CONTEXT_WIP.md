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
