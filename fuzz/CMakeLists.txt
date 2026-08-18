# ARCHITECTURE.md "Invariants and sanitizers" — libFuzzer targets for the four decoders.
#
# **Clang only, and off unless asked for.** `-fsanitize=fuzzer` is a clang feature; gcc has no
# equivalent, and the matrix builds with whatever `/usr/bin/c++` is. So this is opt-in via
# `-DELYSIUMKV_FUZZ=ON` rather than a preset everyone pays for, and it refuses loudly on a compiler
# that cannot honour it instead of silently building nothing.
if(NOT ELYSIUMKV_FUZZ)
    return()
endif()

if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR
        "ELYSIUMKV_FUZZ needs clang: -fsanitize=fuzzer has no gcc equivalent. "
        "Configure with -DCMAKE_CXX_COMPILER=clang++.")
endif()

# **Configurable, because one toolchain cannot run the default.** `address,undefined` is what CI
# uses and what these are for. Homebrew LLVM on macOS/arm64 deadlocks at startup when libFuzzer and
# AddressSanitizer are combined — before `main`, with no output — so a developer there can set this
# to `undefined` and still get input exploration and UB checking rather than nothing at all.
set(ELYSIUMKV_FUZZ_SANITIZERS "address,undefined" CACHE STRING
    "Sanitizers to combine with libFuzzer")

# Seeds are generated, never committed — see seed_corpus.cpp for why. Built without the fuzzer or
# the sanitizers: it is an ordinary program that writes files.
add_executable(elysiumkv_fuzz_seed seed_corpus.cpp)
target_link_libraries(elysiumkv_fuzz_seed PRIVATE elysiumkv elysiumkv_flags)
target_include_directories(elysiumkv_fuzz_seed
    PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/include)

set(ELYSIUMKV_FUZZ_SEEDS ${CMAKE_CURRENT_BINARY_DIR}/seeds)
add_custom_command(
    OUTPUT ${ELYSIUMKV_FUZZ_SEEDS}/.stamp
    COMMAND elysiumkv_fuzz_seed ${ELYSIUMKV_FUZZ_SEEDS}
    COMMAND ${CMAKE_COMMAND} -E touch ${ELYSIUMKV_FUZZ_SEEDS}/.stamp
    DEPENDS elysiumkv_fuzz_seed
    COMMENT "Encoding fuzz seeds")
add_custom_target(elysiumkv_fuzz_seeds ALL DEPENDS ${ELYSIUMKV_FUZZ_SEEDS}/.stamp)

# The fuzzers link the engine's sources directly rather than the shared library: the decoders are
# internal, and a target that could only reach the C ABI would be fuzzing the wrong layer.
foreach(target edit snapshot block footer)
    if(target STREQUAL "edit")
        set(source fuzz_manifest_edit.cpp)
    elseif(target STREQUAL "snapshot")
        set(source fuzz_manifest_snapshot.cpp)
    else()
        set(source fuzz_${target}.cpp)
    endif()

    add_executable(elysiumkv_fuzz_${target} ${source})
    target_link_libraries(elysiumkv_fuzz_${target} PRIVATE elysiumkv elysiumkv_flags)
    target_include_directories(elysiumkv_fuzz_${target}
        PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/include)
    # The fuzzer supplies inputs; the sanitizers supply the teeth. Without them a decoder that
    # reads past its buffer simply returns a wrong answer and the run stays green.
    target_compile_options(elysiumkv_fuzz_${target}
        PRIVATE -fsanitize=fuzzer,${ELYSIUMKV_FUZZ_SANITIZERS})
    target_link_options(elysiumkv_fuzz_${target}
        PRIVATE -fsanitize=fuzzer,${ELYSIUMKV_FUZZ_SANITIZERS})

    add_dependencies(elysiumkv_fuzz_${target} elysiumkv_fuzz_seeds)
    # libFuzzer requires its output directory to exist rather than creating one.
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/found/${target})

    # A short run in the ordinary suite. The point on a commit is that the target still builds and
    # still survives its seeds and every past regression; finding *new* inputs is the nightly's job.
    #
    # **The first directory is where libFuzzer writes**, so it is one in the build tree. Pointing it
    # at `corpus/` — which is the obvious thing to write and what this did first — means running the
    # test suite mutates the source tree, and the accreted one-byte mutants get committed as though
    # somebody chose them.
    add_test(NAME Fuzz.${target}SurvivesItsCorpus
        COMMAND elysiumkv_fuzz_${target}
                ${CMAKE_CURRENT_BINARY_DIR}/found/${target}
                ${ELYSIUMKV_FUZZ_SEEDS}/${target}
                ${CMAKE_CURRENT_SOURCE_DIR}/corpus/${target}
                -ignore_crashes=0 -runs=2000 -max_total_time=20 -print_final_stats=0)
endforeach()
