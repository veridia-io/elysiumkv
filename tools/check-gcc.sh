#!/usr/bin/env bash
#
# Compile every source under **g++ with the project's exact warning set**, syntax-only.
#
# This exists because CI is Linux/gcc and development here is macOS/clang, and the two disagree in
# ways that only ever show up after a push:
#
#   * libstdc++ does not provide the transitive includes libc++ does, so a missing <set> compiles
#     locally and fails there;
#   * gcc's -Wdangling-else fires on an unbraced `if` before a gtest macro where clang's does not;
#   * -Wconversion and -Wsign-conversion differ in coverage between the two front ends.
#
# Every one of those has broken the build at least once. Syntax-only, so it needs headers but no
# libraries and takes seconds -- there is no reason not to run it before pushing.
#
# Usage:  docker run --rm -v "$PWD":/src ghcr.io/.../gcc:14 bash /src/tools/check-gcc.sh
#     or: tools/check-gcc.sh          (on a machine that already has g++ and the dependencies)
set -uo pipefail
cd "$(dirname "$0")/.."

# Honour CXX so CI can pin a compiler newer than the runner default.
CXX="${CXX:-g++}"

WARNINGS=(-Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow
          -Wno-missing-field-initializers -Werror)
INCLUDES=(-Iinclude -Isrc -Itests -Ibuild/debug/generated)

# vcpkg's headers, if a local build has them; harmless when absent.
for d in build/*/vcpkg_installed/*/include; do
  [ -d "$d" ] && INCLUDES+=(-isystem "$d")
done

fail=0
checked=0
for f in $(find src tests -name '*.cpp' | sort); do
  # The AWS sources need the SDK headers, which are not part of the default build.
  case "$f" in *aws*|*dynamo*|*s3_*) continue;; esac

  if ! out=$("$CXX" -std=c++23 -fsyntax-only -fno-rtti -DELYSIUMKV_PARANOID=1 \
                 "${WARNINGS[@]}" "${INCLUDES[@]}" "$f" 2>&1); then
    # A missing third-party header means this file is out of scope here, not broken.
    if grep -qE "fatal error: (zstd|lz4|aws|gtest|gmock)" <<<"$out"; then continue; fi
    echo "=== $f"
    echo "$out" | head -12
    fail=1
  fi
  checked=$((checked + 1))
done
echo "checked $checked translation units"
exit $fail
