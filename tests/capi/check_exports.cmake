# ARCHITECTURE.md "The ABI boundary" and "Dependencies and artifacts" — the shared library's export set is a correctness property, not a
# packaging detail. Two failures this guards, both of which have happened:
#
#   * Hidden visibility hid the ABI too, and the library exported *nothing*.
#     Everything still built and every C++ test passed; only a binding trying to
#     dlopen it would have found out.
#   * A statically-linked zstd got re-exported, so loading ElysiumKV into a host
#     process that already has its own zstd interposes one on the other. The
#     symptom is a decompression failure in unrelated code, months later.
#
# Invoked as: cmake -DLIBRARY=<path> [-DPREFIXES=a;b] [-DMINIMUM=n] -P check_exports.cmake
#
# PREFIXES defaults to elysiumkv_ (the C ABI); the JNI glue passes Java_;JNI_On
# instead. Same rule, two artifacts: whatever the export list claims, that is
# exactly what the linker must have produced.
#
# Script mode does not inherit the project's policy settings, so this has to
# state them itself — without it, CMP0057 is unset and `IN_LIST` is parsed as a
# stray argument rather than an operator.
cmake_minimum_required(VERSION 3.25)

if(NOT EXISTS "${LIBRARY}")
    message(FATAL_ERROR "no shared library at ${LIBRARY}")
endif()

find_program(NM_EXECUTABLE NAMES nm llvm-nm)
if(NOT NM_EXECUTABLE)
    message(FATAL_ERROR "nm not found; cannot verify the export set")
endif()

if(APPLE)
    set(NM_ARGS -gU)  # global, defined
else()
    set(NM_ARGS -D --defined-only)
endif()

execute_process(
    COMMAND ${NM_EXECUTABLE} ${NM_ARGS} "${LIBRARY}"
    OUTPUT_VARIABLE nm_output
    RESULT_VARIABLE nm_result
    ERROR_VARIABLE nm_error)
if(NOT nm_result EQUAL 0)
    message(FATAL_ERROR "nm failed on ${LIBRARY}: ${nm_error}")
endif()

string(REPLACE "\n" ";" nm_lines "${nm_output}")

if(NOT DEFINED PREFIXES)
    set(PREFIXES "elysiumkv_")
endif()
if(NOT DEFINED MINIMUM)
    set(MINIMUM 25)
endif()

# Linker-generated symbols that ELF emits whatever the version script says.
set(allowed_extras _init _fini _edata _end __bss_start)

set(exported "")
set(unexpected "")
foreach(line IN LISTS nm_lines)
    if(NOT line MATCHES "^[0-9a-fA-F]+ +([A-Za-z]) +(.+)$")
        continue()
    endif()
    set(type "${CMAKE_MATCH_1}")
    set(symbol "${CMAKE_MATCH_2}")
    # Absolute symbols are not entry points. ELF version scripts define a node
    # for the version name itself — `ELYSIUMKV_1.0` — and nm reports it as `A`
    # alongside the real exports. It cannot be called or interposed on.
    if(type STREQUAL "A" OR type STREQUAL "a")
        continue()
    endif()
    string(REGEX REPLACE "^_" "" symbol "${symbol}")     # mach-o underscore prefix
    string(REGEX REPLACE "@@?.*$" "" symbol "${symbol}")  # elf version decoration
    if(symbol IN_LIST allowed_extras)
        continue()
    endif()
    list(APPEND exported "${symbol}")
    set(matched FALSE)
    foreach(prefix IN LISTS PREFIXES)
        if(symbol MATCHES "^${prefix}")
            set(matched TRUE)
        endif()
    endforeach()
    if(NOT matched)
        list(APPEND unexpected "${symbol}")
    endif()
endforeach()

list(LENGTH exported exported_count)

# The ABI in elysiumkv.h is ~43 entry points; anything near
# zero means visibility swallowed the export list rather than applying it.
if(exported_count LESS MINIMUM)
    message(FATAL_ERROR
        "${LIBRARY} exports only ${exported_count} symbols, fewer than the ${MINIMUM} expected "
        "— the export list was swallowed rather than applied. Check that the declarations carry "
        "the visibility attribute and that the version script matches them.")
endif()

if(unexpected)
    list(JOIN unexpected "\n  " leaked)
    message(FATAL_ERROR
        "${LIBRARY} exports symbols outside ${PREFIXES}, which will interpose on the host "
        "process:\n  ${leaked}")
endif()

message(STATUS "${exported_count} exported symbols, all matching ${PREFIXES}")
