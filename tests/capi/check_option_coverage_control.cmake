# The control on the gate next door. Two ways it could silently pass — an unreachable field it does
# not notice, and a parse that quietly matches fewer declarations than it reports — and this asserts
# it fails for each, naming what it found.
#
# Inputs: GATE (the gate script), HEADER, CAPI, ALLOWED, MINIMUM, SCRATCH (a writable directory).

# Script mode does not inherit the project's policy settings, so this has to state them itself —
# without it CMP0057 is unset and `IN_LIST` is a stray argument rather than an operator. It fails on
# the project's own floor and passes under CMake 4, which treats the policy as NEW, so a laptop on 4
# cannot see the break at all.
cmake_minimum_required(VERSION 3.25)


if(NOT DEFINED SCRATCH)
    # Required rather than defaulted: in script mode CMAKE_CURRENT_BINARY_DIR is the working
    # directory, so a default would write fixtures into whatever tree the caller happened to be in.
    message(FATAL_ERROR "control: SCRATCH is required and must be a writable directory")
endif()
file(MAKE_DIRECTORY "${SCRATCH}")

# 1. A field no binding can reach must be reported by name.
file(READ "${HEADER}" header_text)
string(REPLACE "std::optional<Duration> ttl;"
               "std::optional<Duration> ttl;\n    size_t unreachable_by_construction = 0;"
               fixture_text "${header_text}")
if(fixture_text STREQUAL header_text)
    message(FATAL_ERROR "control: could not plant a field in ${HEADER}; the fixture is stale")
endif()
set(fixture "${SCRATCH}/option_coverage_fixture.hpp")
file(WRITE "${fixture}" "${fixture_text}")

execute_process(
    COMMAND ${CMAKE_COMMAND} -DHEADER=${fixture} -DCAPI=${CAPI} -DALLOWED=${ALLOWED}
            -DMINIMUM=${MINIMUM} -P ${GATE}
    RESULT_VARIABLE planted_result OUTPUT_VARIABLE planted_out ERROR_VARIABLE planted_err)
if(planted_result EQUAL 0)
    message(FATAL_ERROR "control: the gate accepted a field the C ABI never assigns")
endif()
if(NOT "${planted_err}" MATCHES "unreachable_by_construction")
    message(FATAL_ERROR "control: the gate rejected the fixture but did not name the field:\n${planted_err}")
endif()

# 2. A parse that matches nothing must fail on the floor rather than report success.
file(WRITE "${SCRATCH}/option_coverage_empty.hpp"
     "namespace elysiumkv {\nstruct Options {\n};\n}  // namespace elysiumkv\n")
execute_process(
    COMMAND ${CMAKE_COMMAND} -DHEADER=${SCRATCH}/option_coverage_empty.hpp
            -DCAPI=${CAPI} -DALLOWED=${ALLOWED} -DMINIMUM=${MINIMUM} -P ${GATE}
    RESULT_VARIABLE empty_result ERROR_VARIABLE empty_err)
if(empty_result EQUAL 0)
    message(FATAL_ERROR "control: the gate reported success having parsed no fields at all")
endif()
if(NOT "${empty_err}" MATCHES "parsed only")
    message(FATAL_ERROR "control: the gate failed on an empty struct for the wrong reason:\n${empty_err}")
endif()

file(REMOVE "${SCRATCH}/option_coverage_fixture.hpp" "${SCRATCH}/option_coverage_empty.hpp")

message(STATUS "check_option_coverage_control: the gate rejects both an unreachable field and an empty parse")
