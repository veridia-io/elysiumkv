# NOTICE

ElysiumKV is distributed under the MIT License (see [LICENSE](LICENSE)).

## Third-party code in distributed binaries

**These libraries are statically linked into the artifacts this project ships**, so their notices
travel with the binary, not just with the source. That includes `libelysiumkv.{so,dylib}` and the
Java jar, which embeds the native library as a resource.

### Zstandard (zstd)

- Upstream: https://github.com/facebook/zstd
- Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
- Dual licensed under BSD-3-Clause and GPLv2. **This project uses it under BSD-3-Clause.**

### LZ4

- Upstream: https://github.com/lz4/lz4
- LZ4 Library, Copyright (c) 2011-2020, Yann Collet. All rights reserved.
- BSD-2-Clause.

### AWS SDK for C++ — optional

Present only in builds configured with `-DELYSIUMKV_BUILD_AWS=ON`, which is off by default. A
default build links none of it.

- Upstream: https://github.com/aws/aws-sdk-cpp
- Apache License 2.0.

The full license texts as vendored for the build are installed by vcpkg under
`build/<preset>/vcpkg_installed/<triplet>/share/<port>/copyright`. If you redistribute a build of
this project, include the applicable texts — BSD-2-Clause and BSD-3-Clause both require the
copyright notice and disclaimer to accompany binary redistributions.

## Algorithms implemented from published specifications

No upstream source was copied for these. They are independent implementations, written against the
published algorithm descriptions, so no third-party license applies to them — but they are recorded
here because the constants are necessarily identical to the reference implementations and that
similarity should not be mistaken for derivation.

- **XXH64** (`src/sst/xxhash.hpp`) — inlined rather than taken as a dependency, at roughly 150
  lines. Used only to seed the bloom filter's double hashing.
- **CRC32C / Castagnoli** (`src/sst/crc32c.cpp`) — table-driven with x86 SSE4.2 and ARMv8 CRC
  instruction paths.

## Build- and test-only dependencies

Not redistributed: they are not linked into any shipped artifact and are absent from the released
jar. Listed for completeness.

| Dependency | Used for | License |
| --- | --- | --- |
| GoogleTest | C++ test suites | BSD-3-Clause |
| google/benchmark | C++ benchmarks (`bench` feature) | Apache-2.0 |
| JUnit 5 | Java binding tests | EPL-2.0 |
| Testcontainers | LocalStack-backed remote tests | MIT |
| SLF4J | test logging | MIT |
| JMH | Java crossing-cost benchmarks (`jmh` profile) | GPLv2 with Classpath Exception |

Everything else in this repository is original to it.
