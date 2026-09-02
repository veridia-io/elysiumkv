# Every field of `Options` must be reachable through the C ABI.
#
# The export gates next door ask what the shared library exposes. This asks the opposite question:
# whether anything the C++ API can configure is unreachable from a binding. `Options::ttl` shipped
# that way — a documented feature no binding could set, because it never reached
# `elysiumkv_options_configure` and had no setter of its own. Nothing noticed, because the Java
# coverage test compares the ABI against its Java bindings and there was no ABI symbol to be missing.
#
# Deliberate exclusions are named in ALLOWED and each needs a reason. An exclusion is a decision; an
# omission is the bug this gate exists for, and the two are indistinguishable without the list.
#
# Inputs: HEADER (options.hpp), CAPI (the C ABI translation unit), ALLOWED (a ;-list of field names).

# Script mode does not inherit the project's policy settings, so this has to state them itself —
# without it CMP0057 is unset and `IN_LIST` is a stray argument rather than an operator. It fails on
# the project's own floor and passes under CMake 4, which treats the policy as NEW, so a laptop on 4
# cannot see the break at all.
cmake_minimum_required(VERSION 3.25)


if(NOT EXISTS "${HEADER}")
    message(FATAL_ERROR "check_option_coverage: no such header: ${HEADER}")
endif()
if(NOT EXISTS "${CAPI}")
    message(FATAL_ERROR "check_option_coverage: no such translation unit: ${CAPI}")
endif()

file(READ "${HEADER}" header_text)
file(READ "${CAPI}" capi_text)

# The reachability search below is a substring match, so a field named only in a comment — including
# one explaining why it is *not* wired up — would count as reached. Strip them first, for the same
# reason the header's are stripped.
string(REGEX REPLACE "//[^\n]*" "" capi_text "${capi_text}")
string(REGEX REPLACE "/\\*([^*]|\\*[^/])*\\*/" "" capi_text "${capi_text}")

# Bounded to `struct Options` so members of Tier, LevelOptions and the rest are not swept in: they
# are configured through their own entry points and would each need an allowlist entry otherwise.
string(FIND "${header_text}" "struct Options {" options_start)
if(options_start EQUAL -1)
    message(FATAL_ERROR "check_option_coverage: no `struct Options {` in ${HEADER}")
endif()
string(SUBSTRING "${header_text}" ${options_start} -1 options_body)
# The first close at column zero ends the struct; anything after it belongs to the namespace.
string(REGEX REPLACE "\n};.*" "" options_body "${options_body}")

# Strip comments before looking for members, so a field named in prose is not mistaken for one.
string(REGEX REPLACE "//[^\n]*" "" options_body "${options_body}")
string(REGEX REPLACE "/\\*([^*]|\\*[^/])*\\*/" "" options_body "${options_body}")

# A brace initialiser may contain parentheses — `Duration orphan_retention{hours(24)}` — so parens
# are admitted inside braces and refused outside them. Refusing them everywhere silently skips every
# brace-initialised field; admitting them everywhere picks up member functions as fields.
#
# Parentheses are admitted in the *type* for the same reason: `std::function<uint64_t()> clock` is a
# field, and a type pattern that cannot spell it skips the field silently rather than reporting it.
# Member functions are still refused by the tail, which admits no `(` between the name and the `;`.
string(REGEX MATCHALL "\n    [A-Za-z_][A-Za-z_0-9:<>(), ]*[ >]([a-z_][a-z_0-9]*)({[^;]*})?[^;(]*;"
       declarations "${options_body}")

set(unreachable "")
set(parsed "")
foreach(declaration IN LISTS declarations)
    # The name is the last identifier before the terminator, and it has to be reached by stripping
    # rather than by matching: a leftmost regex finds `td` inside `std::optional<...>` and a greedy
    # one finds the initialiser. Both parse cleanly and check the wrong string.
    string(REGEX REPLACE ";.*$" "" head "${declaration}")
    string(REGEX REPLACE "\\{.*$" "" head "${head}")
    string(REGEX REPLACE "=.*$" "" head "${head}")
    string(STRIP "${head}" head)
    string(REGEX REPLACE ".*[ >]" "" field "${head}")
    if(NOT field MATCHES "^[a-z_][a-z_0-9]*$")
        continue()
    endif()
    list(APPEND parsed "${field}")
    if(field IN_LIST ALLOWED)
        continue()
    endif()
    # Assigned by any route: the wide `configure`, a dedicated setter, or a nested member.
    string(FIND "${capi_text}" "options.${field}" hit)
    if(hit EQUAL -1)
        list(APPEND unreachable "${field}")
    endif()
endforeach()

# The parse is the part of this gate that can rot silently: a pattern that stops matching some
# declaration shape checks fewer fields and still reports success. MINIMUM is the floor, and it is
# meant to be raised when fields are added, not lowered when the regex breaks.
list(LENGTH parsed found)
if(found LESS MINIMUM)
    message(FATAL_ERROR
        "check_option_coverage parsed only ${found} of `struct Options`, expected at least "
        "${MINIMUM}. Either fields were removed — lower MINIMUM deliberately — or the declaration "
        "pattern stopped matching a shape, in which case this gate is quietly checking less than it "
        "reports.")
endif()

if(NOT unreachable STREQUAL "")
    string(REPLACE ";" ", " pretty "${unreachable}")
    message(FATAL_ERROR
        "these Options fields are unreachable through the C ABI: ${pretty}\n"
        "Add a setter (or a parameter), or name the field in ALLOWED with a reason. A field the C++ "
        "API can set and no binding can is a documented feature nobody can use.")
endif()

list(LENGTH parsed checked)
if(COVERAGE_VERBOSE)
    string(REPLACE ";" " " pretty_parsed "${parsed}")
    message(STATUS "check_option_coverage: parsed ${pretty_parsed}")
endif()
message(STATUS "check_option_coverage: ${checked} Options fields, all reachable")
