cmake_policy(SET CMP0135 NEW)

include(FetchContent)
FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.5
)
FetchContent_MakeAvailable(Corrosion)

set(SOURCE_CXX_FILES lib.rs)
if (WITH_UT)
    list(APPEND CORROSION_FEATURES source-test-transport)
    list(APPEND SOURCE_CXX_FILES source_test_transport.rs)
endif()

corrosion_import_crate(
    MANIFEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml
    FEATURES ${CORROSION_FEATURES}
    CRATES rust-bridge
)

corrosion_set_env_vars(rust_bridge
    "RUSTFLAGS=-C force-frame-pointers=yes"
)

# corrosion_add_cxxbridge can only import a single static library; 
# otherwise, `rlib` will conflict.
corrosion_add_cxxbridge(rust-bridge
    CRATE rust_bridge
    FILES ${SOURCE_CXX_FILES})

target_include_directories(rust-bridge PUBLIC ${Arrow_INCLUDE_DIRS}
    ${PROJECT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/corrosion_generated/cxxbridge/rust-bridge/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

file(GLOB_RECURSE CPP_SOURCE_FILE CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
)
if (WITH_UT)
    # Keep callbacks in the same archive as their generated CXX callers so
    # consumers of rust-bridge do not depend on static archive link order.
    target_sources(rust-bridge PRIVATE ${PROJECT_SOURCE_DIR}/test/support/source_http_test_support.cpp)
    target_include_directories(rust-bridge PUBLIC ${PROJECT_SOURCE_DIR}/test/include)
    target_compile_definitions(rust-bridge PRIVATE BUILD_GTEST)
    target_link_libraries(rust-bridge PRIVATE azure-sdk-for-cpp::azure-sdk-for-cpp)
endif()

add_library(prsbridge STATIC ${CPP_SOURCE_FILE})

if (WITH_UT)
    target_compile_definitions(prsbridge PRIVATE BUILD_GTEST)
endif()

target_include_directories(prsbridge PUBLIC ${Arrow_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}/corrosion_generated/cxxbridge/rust-bridge/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/include
)
target_include_directories(prsbridge PRIVATE
    $<TARGET_PROPERTY:milvus-common::milvus-common,INTERFACE_INCLUDE_DIRECTORIES>
)

target_link_libraries(prsbridge PUBLIC rust-bridge)
target_link_libraries(prsbridge PRIVATE azure-sdk-for-cpp::azure-sdk-for-cpp)
