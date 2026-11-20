fn main() {
    println!("cargo:rustc-check-cfg=cfg(feature, values(\"source-test-transport\"))");
    let manifest = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let root = manifest.join("../..").canonicalize().unwrap();
    let root = root.to_str().unwrap();
    for file in ["src/source_read_context.cpp", "src/filesystem/source_read_scope.cpp", "src/ffi/source_context_bridge.cpp", "include/milvus-storage/source_read_context.h", "include/milvus-storage/filesystem/source_read_scope.h", "include/milvus-storage/ffi_source_read_context_c.h", "src/format/bridge/rust/include/source_context_bridge.h"] {
        println!("cargo:rerun-if-changed={root}/{file}");
    }
    for file in ["test_support.h", "test_support.cpp", "generated/rust-bridge/lib.h"] {
        println!("cargo:rerun-if-changed={file}");
    }
    println!("cargo:rerun-if-env-changed=ARROW_HOME");
    println!("cargo:rerun-if-env-changed=ARROW_LIBRARY");
    let arrow = std::env::var("ARROW_HOME").expect("Set ARROW_HOME to Arrow 17 headers and libraries (e.g. the pyarrow 17 package directory)");
    let library = std::env::var("ARROW_LIBRARY").unwrap_or_else(|_| format!("{arrow}/libarrow.so.1700"));
    cxx_build::bridges(["bridge.rs", "src/lib.rs"])
      .file(format!("{root}/src/ffi/source_context_bridge.cpp"))
      .file(format!("{root}/src/source_read_context.cpp"))
      .file(format!("{root}/src/filesystem/source_read_scope.cpp"))
      .file("test_support.cpp")
      .include(".").include("generated")
      .include(format!("{root}/include"))
      .include(format!("{root}/src/format/bridge/rust/include"))
      .include(format!("{arrow}/include"))
      .std("c++20").compile("source-context-native");
    println!("cargo:rustc-link-search=native={arrow}");
    println!("cargo:rustc-link-arg={library}");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{arrow}");
}
