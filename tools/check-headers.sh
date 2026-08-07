#!/usr/bin/env bash
#
# Compile every project header **on its own** under g++/libstdc++.
#
# Complementary to check-gcc.sh rather than subsumed by it: that one compiles translation units, and
# a header missing an include still passes there whenever some earlier include happens to supply it.
# Compiling each header alone is what catches the missing include itself -- which is how a `std::set`
# with no <set> reached CI, invisible on libc++, which provides it transitively.
set -uo pipefail
cd "$(dirname "$0")/.."

# Honour CXX so CI can pin a compiler newer than the runner default.
CXX="${CXX:-g++}"

INCLUDES=(-Iinclude -Isrc -Ibuild/debug/generated)
for d in build/*/vcpkg_installed/*/include; do
  [ -d "$d" ] && INCLUDES+=(-isystem "$d")
done

fail=0
checked=0
for h in $(find include src -name '*.hpp' | sort); do
  grep -qE '#include <(zstd|lz4|aws/)' "$h" && continue
  if ! out=$("$CXX" -std=c++23 -fsyntax-only -fno-rtti -DELYSIUMKV_PARANOID=1 \
                 "${INCLUDES[@]}" -x c++-header "$h" 2>&1); then
    grep -qE 'zstd|lz4|aws' <<<"$out" && continue
    echo "=== $h"; echo "$out" | head -8; fail=1
  fi
  checked=$((checked + 1))
done
echo "checked $checked headers"
exit $fail
