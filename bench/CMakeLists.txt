find_package(benchmark CONFIG REQUIRED)

# ARCHITECTURE.md "Benchmarks" — reported, not gating. Populated as the engine lands: point lookup
# (hot / cold / bloom-rejected) and the prefix scan that must not scale with
# total key count.
add_executable(elysiumkv_bench
    bench_main.cpp
)
target_link_libraries(elysiumkv_bench PRIVATE
    elysiumkv elysiumkv_flags benchmark::benchmark)
target_include_directories(elysiumkv_bench PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/tests)

# ARCHITECTURE.md "Negative controls" — the control on the ratchet. Registered with the benchmarks because it
# needs the binary, and cheap enough (two 50ms runs) to sit in the ordinary suite.
enable_testing()
add_test(NAME Bench.TheRatchetTripsOnASlowdown
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/ratchet_control.sh
            $<TARGET_FILE:elysiumkv_bench>
            ${CMAKE_CURRENT_SOURCE_DIR}/check_regression.py)
