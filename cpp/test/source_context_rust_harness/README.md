# Standalone source-context Rust/native tests

This WIP integration harness compiles the actual C++ source context, URI scope parser and CXX bridge, and includes the production Rust modules. It uses injected provider/HTTP/clock implementations; it does not stub the native boundary or contact Azure.

Prerequisites: Rust toolchain from the parent bridge, C++20 compiler, and Arrow 17 headers/shared library. Set `ARROW_HOME` to an Arrow installation or a PyArrow 17 package directory. If the library is not `$ARROW_HOME/libarrow.so.1700`, set `ARROW_LIBRARY` to its absolute path.

```sh
ARROW_HOME=/path/to/arrow cargo test --locked --manifest-path cpp/test/source_context_rust_harness/Cargo.toml --lib
```

Use a separate Cargo target directory from the production bridge: this standalone harness disables debug info. The deliberately ignored upstream URI probe is exploratory, not a passing correctness test. Live Azure probes and private engine tests are separate; these tests do not establish original-user authorization or full Lance support.

The Lance dataset test generates a real four-chunk dataset and reads it through
Lance 7 at the production pinned Git revision, the production scoped ObjectStore,
and the actual native context/issuer bridge. It covers pinned/latest discovery,
expired leases, storage 503/429 recovery, typed provider failure and recovery,
owner isolation and cancellation. The final HTTP transport and clock are
injected; it does not exercise the C++ format/table wrapper or live Azure.

The standalone Cargo.lock is independent of the full bridge lock (including
Arrow patch versions); full native and engine builds remain separate gates.
Lance's outer get retry currently repeats the four-attempt acquisition batch:
the single-file failure fixture observes 16 provider calls. Do not describe four
attempts as the budget for an entire high-level Lance read.
