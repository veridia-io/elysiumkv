# The `elysiumkv` binary: a deliverable, not a build-tree convenience.
#
# It links the static archive and reaches into `src/` deliberately — commands use the engine's own
# decoders and `Version::apply` rather than a second implementation of any format, so a format
# change breaks this build instead of quietly changing what it reports.
# Argument parsing, JSON and tables are all solved problems, and each one hand-rolled here would
# be a second implementation to keep correct — the JSON escaping already was one.
find_package(CLI11 CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(tabulate CONFIG REQUIRED)

# The target cannot be called `elysiumkv`: that name is the library. The binary an operator types
# is `elysiumkv`, which is what OUTPUT_NAME settles.
add_executable(elysiumkv_cli
    main.cpp
    catalog.cpp
    version_load.cpp
    manifest_command.cpp
    stats_command.cpp)
set_target_properties(elysiumkv_cli PROPERTIES OUTPUT_NAME elysiumkv)

target_link_libraries(elysiumkv_cli PRIVATE elysiumkv elysiumkv_flags
    CLI11::CLI11 nlohmann_json::nlohmann_json tabulate::tabulate)
target_include_directories(elysiumkv_cli PRIVATE ${CMAKE_SOURCE_DIR}/src)

if(ELYSIUMKV_BUILD_AWS)
    # The define as well as the link: without it the S3 and DynamoDB branches are preprocessed
    # away and the binary reports "no AWS support" while sitting on the SDK it just linked
    # against — which is exactly what happened the first time this was wired.
    target_compile_definitions(elysiumkv_cli PRIVATE ELYSIUMKV_WITH_AWS=1)
    target_link_libraries(elysiumkv_cli PRIVATE elysiumkv_aws)
    target_include_directories(elysiumkv_cli SYSTEM PRIVATE ${AWSSDK_INCLUDE_DIRS})
endif()

install(TARGETS elysiumkv_cli RUNTIME DESTINATION bin)
