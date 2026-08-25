# ARCHITECTURE.md "Dependencies and artifacts" — the remote implementations, confined to their own target.
# Before the SDK, because its exported targets name ZLIB::ZLIB and CURL::libcurl without importing
# them. vcpkg's toolchain resolves those on its own, so this is invisible on a vcpkg build and a hard
# failure on any other — a distribution package or an SDK built from source, which is how the musl
# artifact is produced.
find_package(ZLIB REQUIRED)
find_package(CURL QUIET)

find_package(AWSSDK CONFIG REQUIRED COMPONENTS s3 dynamodb kms)

add_library(elysiumkv_aws STATIC
    s3_blob_store.cpp
    s3_manifest_catalog.cpp
    dynamo_manifest_catalog.cpp
    aws_kms_encryption_key_manager.cpp
)
target_include_directories(elysiumkv_aws
    PUBLIC  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(elysiumkv_aws
    PUBLIC  elysiumkv
    PRIVATE elysiumkv_flags ${AWSSDK_LINK_LIBRARIES})
set_target_properties(elysiumkv_aws PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)

# The AWS SDK's own headers are not clean under this project's warning set, and
# they are not this project's to fix. PRIVATE elysiumkv_flags applies the flags to
# our sources; the SDK arrives as SYSTEM here so its headers are exempt.
#
# PUBLIC rather than PRIVATE so the C ABI — which includes the S3 and DynamoDB
# headers, and through them the SDK's — gets them the same way. The alternative
# was re-deriving AWSSDK_INCLUDE_DIRS in the parent, where it does not exist:
# add_subdirectory gives this file its own scope and find_package results do not
# escape it.
target_include_directories(elysiumkv_aws SYSTEM PUBLIC ${AWSSDK_INCLUDE_DIRS})

# A linkage-and-liveness smoke check, not the contract suite — that lives in the
# Java tests (Testcontainers drives LocalStack there). This exists because
# "it compiles" says nothing about an S3 client, and because it is the only place
# the remote code can be run under the sanitizers.
#
# Skipped unless ELYSIUMKV_S3_ENDPOINT is set, so it never fails a build that has
# no LocalStack.
if(ELYSIUMKV_BUILD_TESTS)
    # This directory is added before the top-level enable_testing(), so add_test
    # here would silently register nothing — which is how these two smokes were
    # invisible to ctest until someone ran ctest and got "No tests were found".
    enable_testing()

    add_executable(elysiumkv_s3_smoke s3_smoke_main.cpp)
    target_link_libraries(elysiumkv_s3_smoke PRIVATE elysiumkv_aws elysiumkv_flags)
    target_include_directories(elysiumkv_s3_smoke SYSTEM PRIVATE ${AWSSDK_INCLUDE_DIRS})
    add_test(NAME S3.Smoke COMMAND elysiumkv_s3_smoke)
    set_tests_properties(S3.Smoke PROPERTIES SKIP_RETURN_CODE 77)

    add_executable(elysiumkv_s3_catalog_smoke s3_catalog_smoke_main.cpp)
    target_link_libraries(elysiumkv_s3_catalog_smoke PRIVATE elysiumkv_aws elysiumkv_flags)
    target_include_directories(elysiumkv_s3_catalog_smoke SYSTEM PRIVATE ${AWSSDK_INCLUDE_DIRS})
    add_test(NAME S3.CatalogSmoke COMMAND elysiumkv_s3_catalog_smoke)
    set_tests_properties(S3.CatalogSmoke PROPERTIES SKIP_RETURN_CODE 77)

    add_executable(elysiumkv_kms_smoke kms_smoke_main.cpp)
    target_link_libraries(elysiumkv_kms_smoke PRIVATE elysiumkv_aws elysiumkv_flags)
    target_include_directories(elysiumkv_kms_smoke SYSTEM PRIVATE ${AWSSDK_INCLUDE_DIRS})
    add_test(NAME Kms.Smoke COMMAND elysiumkv_kms_smoke)
    set_tests_properties(Kms.Smoke PROPERTIES SKIP_RETURN_CODE 77)

    add_executable(elysiumkv_dynamo_catalog_smoke dynamo_catalog_smoke_main.cpp)
    target_link_libraries(elysiumkv_dynamo_catalog_smoke PRIVATE elysiumkv_aws elysiumkv_flags)
    target_include_directories(elysiumkv_dynamo_catalog_smoke SYSTEM PRIVATE ${AWSSDK_INCLUDE_DIRS})
    add_test(NAME Dynamo.CatalogSmoke COMMAND elysiumkv_dynamo_catalog_smoke)
    set_tests_properties(Dynamo.CatalogSmoke PROPERTIES SKIP_RETURN_CODE 77)
endif()
