find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

# One binary. The suites (unit / contract / diff / fault / invariant) are
# distinguished by test-name prefix, not by executable, so a seed reproducing a
# differential failure runs the same way as any other test.
# The C smoke test is compiled as C99 on purpose: it is the only thing that
# proves elysiumkv.h is C-compatible (ARCHITECTURE.md "The ABI boundary").
set_source_files_properties(capi/elysiumkv_c_smoke.c PROPERTIES LANGUAGE C)

add_executable(elysiumkv_tests
    capi/elysiumkv_c_smoke.c
    capi/capi_test.cpp
    unit/wire_format_test.cpp
    unit/status_test.cpp
    unit/slice_test.cpp
    unit/hash_test.cpp
    unit/block_test.cpp
    unit/bloom_test.cpp
    unit/compression_test.cpp
    unit/footer_test.cpp
    unit/sst_test.cpp
    unit/sst_reader_cache_test.cpp
    unit/memtable_test.cpp
    unit/block_cache_test.cpp
    unit/memory_budget_test.cpp
    unit/read_path_test.cpp
    unit/picker_test.cpp
    unit/compaction_test.cpp
    unit/large_value_test.cpp
    unit/overlap_boundary_test.cpp
    unit/key_edges_test.cpp
    unit/reverse_iteration_test.cpp
    unit/truncate_test.cpp
    unit/version_test.cpp
    unit/version_set_test.cpp
    unit/entry_count_test.cpp
    unit/sweep_concurrency_test.cpp
    support/alloc_counter.cpp
    perf/allocation_test.cpp
    soak/growth_test.cpp
    diff/op_stream.cpp
    diff/replay.cpp
    diff/differential_test.cpp
    invariant/invariant_test.cpp
    invariant/negative_control_test.cpp
    contract/manifest_catalog_contract.cpp
    contract/file_manifest_catalog_test.cpp
    contract/blob_store_contract.cpp
    contract/local_file_blob_store_test.cpp
    contract/cache_blob_store_test.cpp
    fault/fault_injecting_blob_store.cpp
    fault/fault_injecting_blob_store_test.cpp
    fault/db_fault_test.cpp
    fault/cache_fault_test.cpp
    fault/kill_point_test.cpp
    fault/config_validation_test.cpp
    fault/durability_test.cpp
    fault/tier_migration_test.cpp
    fault/migration_test.cpp
    fault/watermark_test.cpp
    fault/maintenance_test.cpp
    fault/read_only_test.cpp
)
# ARCHITECTURE.md "Contract suites" — the claim that an implementation "earns its place by passing this unchanged"
# is only true if the remote ones are actually run through it. They skip without
# ELYSIUMKV_S3_ENDPOINT, and this is the only place they run under the sanitizers.
if(ELYSIUMKV_BUILD_AWS)
    target_sources(elysiumkv_tests PRIVATE contract/remote_store_test.cpp)
endif()

target_link_libraries(elysiumkv_tests PRIVATE
    elysiumkv elysiumkv_flags GTest::gtest GTest::gtest_main)
target_include_directories(elysiumkv_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR})

# A ctest TIMEOUT *property* takes precedence over `ctest --timeout`, so a job that needs
# longer cannot ask for it from the outside — it has to be set here, at configure time. The
# nightly full-profile run does exactly that; 600 s is right for everything else.
set(ELYSIUMKV_TEST_TIMEOUT 600 CACHE STRING "Per-test timeout in seconds")

gtest_discover_tests(elysiumkv_tests
    PROPERTIES TIMEOUT ${ELYSIUMKV_TEST_TIMEOUT}
    DISCOVERY_TIMEOUT 60)

# Links against the shared library alone, so a missing export fails here rather
# than in the first binding that tries to use it (ARCHITECTURE.md "Dependencies and artifacts").
set_source_files_properties(capi/shared_link_main.c PROPERTIES LANGUAGE C)
add_executable(elysiumkv_capi_smoke capi/shared_link_main.c capi/elysiumkv_c_smoke.c)
target_link_libraries(elysiumkv_capi_smoke PRIVATE elysiumkv_shared)
target_include_directories(elysiumkv_capi_smoke PRIVATE ${CMAKE_SOURCE_DIR}/include)
add_test(NAME CApi.SharedLibraryExportsEnoughToLinkAgainst COMMAND elysiumkv_capi_smoke)

# Linking proves the symbols a binding needs are *present*; this proves nothing
# else is. Neither check subsumes the other.
add_test(NAME CApi.SharedLibraryExportsNothingBesidesTheAbi
    COMMAND ${CMAKE_COMMAND}
        -DLIBRARY=$<TARGET_FILE:elysiumkv_shared>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/capi/check_exports.cmake)

# ARCHITECTURE.md "Negative controls" — the control on the gate above. The fixture exports one symbol that
# matches the expected prefix and one that does not; the gate must reject it and
# must say which.
add_library(elysiumkv_export_fixture SHARED capi/export_fixture.c)
set_target_properties(elysiumkv_export_fixture PROPERTIES C_VISIBILITY_PRESET hidden)

add_test(NAME CApi.TheExportGateRejectsAStraySymbol
    COMMAND ${CMAKE_COMMAND}
        -DGATE=${CMAKE_CURRENT_SOURCE_DIR}/capi/check_exports.cmake
        -DLIBRARY=$<TARGET_FILE:elysiumkv_export_fixture>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/capi/check_exports_control.cmake)

# The glue is dlopened into a JVM that may already have its own zstd, so the same
# rule applies to it. RegisterNatives means it needs no exported Java_* symbols at
# all — JNI_OnLoad hands the JVM function pointers — so the whole surface is two.
if(ELYSIUMKV_BUILD_JNI)
    add_test(NAME Jni.GlueExportsNothingBesidesItsEntryPoints
        COMMAND ${CMAKE_COMMAND}
            -DLIBRARY=$<TARGET_FILE:elysiumkv_jni>
            # $<SEMICOLON>, not a bare ';': add_test would otherwise split the
            # list into two arguments and silently check only the first prefix.
            "-DPREFIXES=Java_$<SEMICOLON>JNI_On"
            -DMINIMUM=2
            -P ${CMAKE_CURRENT_SOURCE_DIR}/capi/check_exports.cmake)
endif()
