find_package(benchmark CONFIG REQUIRED)

# ARCHITECTURE.md "Benchmarks" — reported, not gating. Populated as the engine lands: point lookup
# (hot / cold / bloom-rejected) and the prefix scan that must not scale with
# total key count.
add_executable(elysiumkv_bench
    bench_main.cpp
    # The latency-injecting store is the tests' one, not a second copy: a benchmark double that
    # drifts from the tested one measures itself.
    ${CMAKE_SOURCE_DIR}/tests/fault/fault_injecting_blob_store.cpp
)
target_link_libraries(elysiumkv_bench PRIVATE
    elysiumkv elysiumkv_flags benchmark::benchmark)
target_include_directories(elysiumkv_bench PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/tests)

enable_testing()
add_test(NAME Bench.TheCheckerGatesStructureNotTime
    COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/check_regression_test.py)
