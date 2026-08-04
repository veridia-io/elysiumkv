# ARCHITECTURE.md "Negative controls" — the negative control for the export gate.
#
# Asserts the gate **fails, naming the stray symbol**. Asserting only a non-zero
# exit would pass when the gate breaks for an unrelated reason — a wrong fixture
# path, a missing library — and would then be vacuous in exactly the way it
# exists to prevent.
#
# Invoked as: cmake -DGATE=<path> -DLIBRARY=<fixture> -P check_exports_control.cmake

cmake_minimum_required(VERSION 3.25)

if(NOT EXISTS "${LIBRARY}")
    message(FATAL_ERROR "no fixture library at ${LIBRARY}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} "-DLIBRARY=${LIBRARY}" "-DMINIMUM=1" -P "${GATE}"
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE errors)
set(reported "${output}${errors}")

if(result EQUAL 0)
    message(FATAL_ERROR
        "the export gate accepted a library exporting "
        "`definitely_not_a_elysiumkv_symbol`. It is not checking anything, and every "
        "green result it has ever produced means only that it ran.")
endif()

if(NOT reported MATCHES "definitely_not_a_elysiumkv_symbol")
    message(FATAL_ERROR
        "the export gate failed, but not for the reason under test — it never named "
        "`definitely_not_a_elysiumkv_symbol`. A control that accepts any failure "
        "passes when the gate is broken.\n--- what it said ---\n${reported}")
endif()

# And the legitimate symbol must not be what it objected to.
if(reported MATCHES "elysiumkv_fixture_expected")
    message(FATAL_ERROR
        "the gate rejected `elysiumkv_fixture_expected`, which matches the allowed "
        "prefix. It is rejecting too much, not too little.\n${reported}")
endif()

message(STATUS "export gate rejected the stray symbol and nothing else")
