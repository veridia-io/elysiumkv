cmake_minimum_required(VERSION 3.25)

# Manifest features must be selected before project() runs, because that is when
# the vcpkg toolchain installs dependencies.
option(ELYSIUMKV_BUILD_TESTS "Build the test suites" ON)
option(ELYSIUMKV_BUILD_BENCH "Build the benchmarks" OFF)
option(ELYSIUMKV_WERROR "Treat warnings as errors" ON)
option(ELYSIUMKV_PARANOID "Compile the ARCHITECTURE.md - Invariants and sanitizers continuous invariant checks" OFF)

if(ELYSIUMKV_BUILD_TESTS)
    list(APPEND VCPKG_MANIFEST_FEATURES "tests")
endif()
if(ELYSIUMKV_BUILD_BENCH)
    list(APPEND VCPKG_MANIFEST_FEATURES "bench")
endif()
# Off by default, and deliberately so: aws-sdk-cpp is by far the heaviest thing
# in this build, and an embedder with no remote tier should not pay for it — nor
# should the four-preset matrix that runs on every change.
option(ELYSIUMKV_BUILD_CLI "Build the elysiumkv command-line tool" ON)
if(ELYSIUMKV_BUILD_CLI)
    list(APPEND VCPKG_MANIFEST_FEATURES "cli")
endif()
option(ELYSIUMKV_BUILD_AWS "Build the S3 and DynamoDB implementations" OFF)
if(ELYSIUMKV_BUILD_AWS)
    list(APPEND VCPKG_MANIFEST_FEATURES "aws")
endif()

# --- version ------------------------------------------------------------------
#
# **The release tag is the version.** The publish workflow passes it in; this default is what a
# development build reports, and it is not something anyone has to remember to bump. Keeping a
# number in the source that has to be re-tagged into agreement just moves the release-day mistake
# from "the library reports the wrong version" to "the release fails at the tag".
set(ELYSIUMKV_VERSION "0.0.0-dev" CACHE STRING
    "Version reported by elysiumkv_version(); the publish workflow sets it from the release tag")

# `project(VERSION)` and SOVERSION accept digits and dots only, so a pre-release tag like
# 1.0.1-rc1 has to be trimmed for them — while elysiumkv_version() still reports it in full,
# because "1.0.1-rc1" is exactly what an operator needs to see.
string(REGEX MATCH "^[0-9]+(\\.[0-9]+)*" ELYSIUMKV_VERSION_NUMERIC "${ELYSIUMKV_VERSION}")
if(NOT ELYSIUMKV_VERSION_NUMERIC)
    message(FATAL_ERROR "ELYSIUMKV_VERSION (\"${ELYSIUMKV_VERSION}\") must start with a number")
endif()

project(elysiumkv VERSION ${ELYSIUMKV_VERSION_NUMERIC} LANGUAGES C CXX)

set(CMAKE_C_STANDARD 99)            # ARCHITECTURE.md "The ABI boundary" — the C ABI header must be C99-compatible
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 23)          # std::expected is C++23; ARCHITECTURE.md "Absence is an answer, not an error" puts it in the public API
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# --- shared compile flags -----------------------------------------------------
# Not linked PUBLIC: an embedder's warning policy is its own business.
add_library(elysiumkv_flags INTERFACE)
target_compile_options(elysiumkv_flags INTERFACE
    -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow
    # C++-only: gcc rejects -fno-rtti outright on a C source, and the C ABI's
    # smoke test is compiled as C precisely so it exercises a C compiler.
    $<$<COMPILE_LANGUAGE:CXX>:-fno-rtti>
    # -Wextra turns this on, and it is a false positive for every aggregate in
    # this codebase: `Tier`, `LevelOptions` and `Options` give every field a
    # default member initializer, so `Tier{.store = s, .durability = d}` is
    # complete by construction, not an oversight. gcc <14 warns anyway, and
    # warns even for designated initializers (gcc PR 96868). Silencing the
    # diagnostic is right here; the alternative is spelling out defaults at
    # every construction site, which is what the defaults exist to avoid.
    -Wno-missing-field-initializers)
if(ELYSIUMKV_WERROR)
    target_compile_options(elysiumkv_flags INTERFACE -Werror)
endif()
if(ELYSIUMKV_PARANOID)
    # Compiles the invariant checks in; Options::paranoid_checks still gates them
    # at runtime.
    target_compile_definitions(elysiumkv_flags INTERFACE ELYSIUMKV_PARANOID=1)
endif()

# --- generated version header -------------------------------------------------
# `elysiumkv_version()` (ARCHITECTURE.md "The ABI boundary") reports ELYSIUMKV_VERSION, rather than a literal in the C
# ABI that has to be remembered separately at release time.
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/version.hpp.in
    ${CMAKE_CURRENT_BINARY_DIR}/generated/elysiumkv/elysiumkv_version.hpp
    @ONLY)

# --- dependencies -------------------------------------------------------------
find_package(zstd CONFIG REQUIRED)
find_package(lz4 CONFIG REQUIRED)

# **Crypto is not hand-rolled here.** AES-256-GCM with hardware acceleration, a vetted
# implementation and an audit trail is what an encryption-at-rest claim rests on; writing one is
# not a reasonable thing to do inside a storage engine. Only libcrypto is needed — no TLS.
find_package(OpenSSL REQUIRED)

if(TARGET zstd::libzstd)
    set(ELYSIUMKV_ZSTD zstd::libzstd)
elseif(TARGET zstd::libzstd_static)
    set(ELYSIUMKV_ZSTD zstd::libzstd_static)
else()
    set(ELYSIUMKV_ZSTD zstd::libzstd_shared)
endif()

# --- the engine ---------------------------------------------------------------
add_library(elysiumkv STATIC
    src/capi/elysiumkv_c.cpp
    src/status.cpp
    src/crypt/encrypted_object.cpp
    src/crypt/encryption.cpp
    src/crypt/aes256_gcm_encryption_provider.cpp
    src/crypt/no_encryption_provider.cpp
    src/crypt/static_encryption_key_manager.cpp
    src/db/db_impl.cpp
    src/db/level_config.cpp
    src/blob/disk_blob_store.cpp
    src/blob/disk_cache_blob_store.cpp
    src/blob/memory_cache_blob_store.cpp
    src/blob/range_cache.cpp
    src/blob/placement.cpp
    src/cache/sharded_lru.cpp
    src/compact/merging_iterator.cpp
    src/compact/compactor.cpp
    src/compact/migrator.cpp
    src/compact/picker.cpp
    src/memtable/arena.cpp
    src/memtable/skiplist_memtable.cpp
    src/manifest/disk_manifest_catalog.cpp
    src/version/version.cpp
    src/version/manifest_payload.cpp
    src/version/version_edit.cpp
    src/version/version_set.cpp
    src/sst/block_builder.cpp
    src/sst/block_reader.cpp
    src/sst/bloom.cpp
    src/sst/compression.cpp
    src/sst/crc32c.cpp
    src/sst/footer.cpp
    src/sst/concat_iterator.cpp
    src/sst/sst_reader.cpp
    src/sst/sst_reader_cache.cpp
    src/sst/sst_writer.cpp
)
target_include_directories(elysiumkv
    PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    # PRIVATE: the generated header carries the version *value*, an implementation
    # detail. Consumers read the version through `elysiumkv_version()`, which is what
    # makes the answer come from the library they actually loaded.
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_BINARY_DIR}/generated)
target_link_libraries(elysiumkv PRIVATE elysiumkv_flags ${ELYSIUMKV_ZSTD} lz4::lz4
                                      OpenSSL::Crypto)
set_target_properties(elysiumkv PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)

# --- the shared library (ARCHITECTURE.md "The ABI boundary", ARCHITECTURE.md "Dependencies and artifacts") ---------------------------------------------
#
# Two artifacts from the same PIC objects, with different contracts:
#
#   libelysiumkv.a      — static, exports the C++ API (ARCHITECTURE.md "Absence is an answer, not an error"). Standard-library types
#                       cross that boundary, so it is only safe under the same
#                       toolchain, which static linking already implies.
#   libelysiumkv_c.so   — shared, exports the C ABI (ARCHITECTURE.md "The ABI boundary") and nothing else. This is
#                       what every binding targets, and what FFM can load.
#
# **Symbol visibility is a correctness requirement, not hygiene.** Default
# visibility re-exports statically linked dependencies, which can then interpose
# against a different copy already loaded in the host process — likely inside a
# JVM, where other native libraries bundle their own zstd. The failure presents
# as corruption or a crash depending on load order.
add_library(elysiumkv_shared SHARED src/capi/elysiumkv_c.cpp)
set_target_properties(elysiumkv_shared PROPERTIES
    OUTPUT_NAME elysiumkv
    POSITION_INDEPENDENT_CODE ON
    CXX_VISIBILITY_PRESET hidden
    C_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON
    VERSION ${PROJECT_VERSION}
    SOVERSION 0)
target_include_directories(elysiumkv_shared
    PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    # Compiles the same `elysiumkv_c.cpp` as the archive, so it needs the same
    # generated version header.
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_BINARY_DIR}/generated)
target_compile_definitions(elysiumkv_shared PRIVATE ELYSIUMKV_BUILDING_SHARED)
target_link_libraries(elysiumkv_shared PRIVATE elysiumkv elysiumkv_flags)

if(APPLE)
    # -exported_symbols_list is the mach-o equivalent of a version script.
    target_link_options(elysiumkv_shared PRIVATE
        -Wl,-exported_symbols_list,${CMAKE_CURRENT_SOURCE_DIR}/elysiumkv.exported_symbols)
elseif(UNIX)
    # --exclude-libs is the one commonly omitted: -fvisibility=hidden governs
    # our own translation units only, not symbols pulled from static archives.
    target_link_options(elysiumkv_shared PRIVATE
        -Wl,--exclude-libs,ALL
        -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/elysiumkv.map
        -static-libstdc++ -static-libgcc)
endif()

# --- remote storage (ARCHITECTURE.md "The ABI boundary", ARCHITECTURE.md "Ownership is one compare-and-set") --------------------------------------------
#
# A separate target so an embedder without S3 links none of it, which is the
# whole reason the storage seams are interfaces. With ELYSIUMKV_BUILD_AWS off the
# engine acquires no AWS dependency by being built at all; the C ABI still
# declares the remote constructors and reports ELYSIUMKV_CONFIG from them, because
# an ABI whose *shape* varies with a build flag cannot be checked by a binding
# with a set comparison.
#
# CMake permits a cycle between STATIC libraries and this one is real: the C ABI
# in `elysiumkv` calls into `elysiumkv_aws`, which uses `elysiumkv`'s headers.
if(ELYSIUMKV_BUILD_AWS)
    add_subdirectory(aws)
    foreach(target elysiumkv elysiumkv_shared)
        target_compile_definitions(${target} PRIVATE ELYSIUMKV_WITH_AWS=1)
        # The SDK's include directories arrive with it, marked SYSTEM there.
        target_link_libraries(${target} PRIVATE elysiumkv_aws)
    endforeach()
endif()

# --- the JNI glue (ARCHITECTURE.md "The ABI boundary", ARCHITECTURE.md "Dependencies and artifacts") -------------------------------------------------
#
# A *third* artifact, deliberately: the glue is its own shared object linking the
# **static** archive rather than sitting on top of libelysiumkv.so. That way the
# jar carries one native file instead of two with a load-order dependency, and
# JNI_OnLoad and the Java_* symbols stay out of the general-purpose library.
#
# The export discipline is the same one the C ABI gets, for the same reason: this
# thing is dlopened into a JVM that may already have its own zstd.
find_package(JNI QUIET COMPONENTS JVM)
option(ELYSIUMKV_BUILD_JNI "Build the JNI glue for the Java binding" ${JNI_FOUND})

if(ELYSIUMKV_BUILD_JNI)
    if(NOT JNI_FOUND)
        message(FATAL_ERROR "ELYSIUMKV_BUILD_JNI is on but no JDK was found (set JAVA_HOME)")
    endif()
    add_library(elysiumkv_jni SHARED bindings/java/src/main/native/elysiumkv_jni.cpp)
    set_target_properties(elysiumkv_jni PROPERTIES
        POSITION_INDEPENDENT_CODE ON
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON)
    # JNIEXPORT already marks the entry points default-visibility, so hidden
    # visibility here removes everything else rather than the ABI — the failure
    # mode that emptied the C ABI's export set once already.
    target_include_directories(elysiumkv_jni PRIVATE ${JNI_INCLUDE_DIRS})
    target_link_libraries(elysiumkv_jni PRIVATE elysiumkv elysiumkv_flags)

    # A .dylib, not a .bundle: System.load takes either on macOS, and keeping the
    # suffix uniform means one packaging rule for both platforms.
    if(APPLE)
        set_target_properties(elysiumkv_jni PROPERTIES SUFFIX ".dylib")
        target_link_options(elysiumkv_jni PRIVATE
            -Wl,-exported_symbols_list,${CMAKE_CURRENT_SOURCE_DIR}/elysiumkv_jni.exported_symbols)
    elseif(UNIX)
        target_link_options(elysiumkv_jni PRIVATE
            -Wl,--exclude-libs,ALL
            -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/elysiumkv_jni.map
            -static-libstdc++ -static-libgcc)
    endif()
endif()

# --- tests and benchmarks -----------------------------------------------------
if(ELYSIUMKV_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
if(ELYSIUMKV_BUILD_BENCH)
    add_subdirectory(bench)
endif()

# --- decoder fuzzing ----------------------------------------------------------
# Off by default and clang-only; see fuzz/CMakeLists.txt. Registered under `enable_testing` above
# so the short corpus run joins the ordinary suite when it is switched on.
option(ELYSIUMKV_FUZZ "Build libFuzzer targets for the decoders (clang only)" OFF)
if(ELYSIUMKV_FUZZ)
    enable_testing()
    add_subdirectory(fuzz)
endif()

# --- the operator CLI ---------------------------------------------------------
# A shipped binary rather than a build-tree tool, so it is not gated on tests or benchmarks: it is
# what you reach for when a store is misbehaving, which is when a release build is what you have.
if(ELYSIUMKV_BUILD_CLI)
    add_subdirectory(cli)
endif()
