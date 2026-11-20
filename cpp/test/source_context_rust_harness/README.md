# Standalone source-context Rust/native tests

This WIP integration harness compiles the actual C++ source context, URI scope parser and CXX bridge, and includes the production Rust modules. It uses injected provider/HTTP/clock implementations; it does not stub the native boundary or contact Azure.

Prerequisites: Rust toolchain from the parent bridge, C++20 compiler, and Arrow 17 headers/shared library. Set `ARROW_HOME` to an Arrow installation or a PyArrow 17 package directory. If the library is not `$ARROW_HOME/libarrow.so.1700`, set `ARROW_LIBRARY` to its absolute path.

```sh
ARROW_HOME=/path/to/arrow cargo test --locked --manifest-path cpp/test/source_context_rust_harness/Cargo.toml --lib
```

Use a separate Cargo target directory from the production bridge: this standalone harness disables debug info. The deliberately ignored upstream URI probe is exploratory, not a passing correctness test. Live Azure probes and private engine tests are separate; these tests do not establish original-user authorization or full Lance support.
