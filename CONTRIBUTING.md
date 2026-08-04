# Contributing

Thanks for taking the time to contribute. ElysiumKV is a C++23 engine with a stable C ABI and a Java
binding, so "the tests" means more than one suite — this document says which ones and why.

Read [ARCHITECTURE.md](ARCHITECTURE.md) first. It explains the decisions the code is built on, and
comments throughout the source cite its headings by name. If a change contradicts something there,
the document needs updating in the same PR — a stale rationale is worse than none.

## Before you start

- **Search existing issues** before opening a new one.
- For a bug, give reproduction steps, expected and actual behaviour, and the preset you built with.
  A failing seed from the differential suite is the single most useful thing you can attach.
- For anything that changes an on-disk or ABI format, read [FORMAT.md](FORMAT.md) and see
  *Formats are frozen* below before writing code.

## Building

Dependencies come from vcpkg, pinned by `builtin-baseline` in `vcpkg.json`. Set `VCPKG_ROOT` and use
the presets:

```sh
cmake --preset debug && cmake --build --preset debug && ctest --preset debug
```

Four presets exist and all four gate a change: `debug`, `release`, `asan-ubsan`, `tsan`. The
sanitizer presets are not optional extras — the bugs this design can have (a pin outliving its cache
entry, a reader racing a compaction) are mostly invisible without them.

The Java binding builds on top of the native library:

```sh
cmake --build --preset release          # produces the JNI library the jar packages
cd bindings/java && mvn test
```

Remote-storage tests need Docker for LocalStack, and the AWS implementations need
`-DELYSIUMKV_BUILD_AWS=ON`. Both are off by default so a normal contribution does not pay for them.

## Tests

The testing approach assumes the interesting bugs are not the ones a unit test finds. What that means
for a PR:

- **Add or update tests when behaviour changes.** A change with no test is a change nobody can keep.
- **Every gate needs a negative control.** If you add an invariant, a validation or a refusal, add a
  paired test proving it *fails for the intended reason* when the mechanism is removed. Then actually
  remove the mechanism, watch the test fail, and put it back. Several tests in this repository were
  found to be vacuous — passing while exercising nothing — and the controls are what caught them.
  A test you have never seen fail has not been tested.
- **Prefer the differential suite** for engine behaviour. It replays random operation streams against
  a `std::map` oracle and has found real bugs; a hand-written scenario usually has not.
- **Do not gate on wall-clock time.** Allocation counts and structural properties are asserted;
  throughput is recorded but never a pass/fail condition, because a shared runner fails for reasons
  unrelated to your change.

## Formats are frozen

The SST layout, the manifest records and the stats buffer are compatibility contracts, documented in
[FORMAT.md](FORMAT.md) and pinned by `tests/unit/wire_format_test.cpp` and the stats-layout test in
`tests/capi/capi_test.cpp`.

If one of those tests fails, **it is not a test to fix.** It means the bytes changed. A deliberate
format change is: bump the version constant, teach the reader to accept both shapes, update
FORMAT.md, and update the pinning test — in one PR, with the migration path explained in the
description.

The SST magic and the 12-byte footer trailer are checked before anything else in a file is trusted.
Changing either orphans every file ever written.

## Style

- Follow the surrounding code: it is dense in comments explaining *why*, and thin on comments
  restating *what*. Match that. If a decision was not obvious, write down the alternative you
  rejected.
- Errors are values (`Result<T>`), and **absence is not an error**. A missing key and an unreachable
  store must never collapse into the same answer.
- Keep the C ABI's shape independent of build configuration. Optional features report a
  configuration error; they never disappear from the export set, because a binding verifies the ABI
  by comparing that set.
- No exception may cross the C ABI. Every entry point is wrapped.

## Pull requests

- Branch from the default branch and keep the PR scoped to one change.
- Link related issues.
- State which presets you ran. If you could not run one (no Docker, no ARM machine), say so rather
  than leaving it implied.
- CI runs the four presets, the Java suite and the remote tests. A red CI on a PR is expected to be
  investigated, not re-run until it passes.
