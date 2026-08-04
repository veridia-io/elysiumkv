# ElysiumKV — On-disk and ABI formats

**Normative.** This document states what the bytes are. It carries no rationale: the reasoning lives
in [ARCHITECTURE.md](ARCHITECTURE.md), and duplicating it here is how the two drift apart.

Every layout below is pinned by `tests/unit/wire_format_test.cpp`, which asserts the encoders'
actual output rather than the constants they were written from. **If you change a format, that test
fails — that is the point.** Update format, test and this document together, or not at all.

Current versions:

| Format | Version | Constant |
| --- | --- | --- |
| SST file | 1 | `Footer::kFormatVersion1` |
| Manifest edit | 1 | `kEditFormatVersion` |
| Manifest snapshot | 1 | `kSnapshotFormatVersion` |
| Stats buffer | 1 | `kStatsFormatVersion` |

Conventions throughout: **little-endian** fixed-width integers; `varint32`/`varint64` are
LEB128-style, 7 bits per byte, low group first, high bit set to continue. `‖` means concatenation.

---

## 1. Object naming

An SST object's name is normative, because recovery parses it back to a file number:

```
{file_number:012d}.sst          e.g. 000000000042.sst
```

Exactly 12 decimal digits, zero-padded, then `.sst`. Anything else in a store is not ours: a name
that does not match this shape is ignored at open rather than guessed at.

File numbers come from one monotonic counter and are **never reused**, including across tier
migration — a migrated file is written under a fresh number.

Manifest object naming is *not* part of this contract. Manifest objects are addressed as
`(generation, sequence)` through the `ManifestCatalog` seam, and each catalog implementation chooses
its own keys.

## 2. Block framing

One framing is used for every framed byte string in the system: SST data blocks, the SST index
block, the SST filter block, manifest edits and manifest snapshots.

```
payload            variable   the block's content, possibly compressed
uncompressed_len   uint32     length of the content *before* compression
compression_type   uint8      0 = none, 1 = lz4, 2 = zstd
crc32c             uint32     CRC32C of everything above (payload ‖ len ‖ type)
                   ───────
                   9-byte trailer  (`kBlockTrailerLength`)
```

The CRC covers the payload, the uncompressed length and the codec byte — that is, the whole framed
record except the CRC itself.

`compression_type` is **per block, not per file**. A codec that fails to shrink a block causes the
block to be stored raw with `compression_type = 0`, so blocks within one file may differ.

A reader must reject an unknown `compression_type` as corruption rather than guess.

## 3. Block content

Data blocks and the index block share one content encoding:

```
entry*                        as below, keys ascending
restart_offset   uint32 × N   byte offset of each restart point within the entry region
restart_count    uint32       N
```

An entry:

```
shared_len    varint32   bytes shared with the previous key
unshared_len  varint32   bytes that follow
value_type    uint8      0x00 = delete, 0x01 = put
value_len     varint32   0 for a delete
key_suffix    bytes      unshared_len bytes
value         bytes      value_len bytes; absent for a delete
```

Note the order: **`value_type` precedes `value_len`.**

At a restart point `shared_len == 0`, so the full key is present and binary search over the restart
array is possible. The first restart offset is always 0. A new restart point is taken once
`restart_interval` entries have been written since the last one.

`value_type` is an open enum: `0x02..0xFF` are reserved and a reader must reject them as corruption.

### Index block

The index block uses the same content encoding, with one entry per data block:

- key = the **last key** in that data block
- value = `varint64 offset ‖ varint64 length`, where `length` is the *framed* length of the block
  (payload plus the 9-byte trailer), so one range read fetches everything needed to validate it.

## 4. Filter block

The filter is a blocked bloom filter. Its payload, before framing:

```
bitmap        bytes      num_blocks × 64 bytes
num_blocks    uint32
num_probes    uint8
              ─────────
              5-byte trailer
```

Each bloom block is 512 bits (64 bytes, one cache line). A key is hashed with xxhash64; the hash
selects a block, and a second derived value strides the probes within it.

The filter block is always framed with `compression_type = 0`.

A malformed filter — zero blocks, zero probes, or a bitmap whose length is not
`num_blocks × 64` — must be treated as "may contain" and fall through to the data block. It is a
performance structure, never an authority on absence.

## 5. SST file layout

```
data block*         framed (§2), keys ascending across blocks
filter block        framed, compression_type = 0
index block         framed
footer              44 bytes, exactly at end of file
```

The footer, `kFooterLengthV1 = 44` bytes, at file offset `file_len - 44`:

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0 | `filter_handle.offset` | uint64 | |
| 8 | `filter_handle.length` | uint32 | framed length |
| 12 | `index_handle.offset` | uint64 | |
| 20 | `index_handle.length` | uint32 | framed length |
| 24 | `num_entries` | uint64 | |
| 32 | `format_version` | uint32 | 1 |
| 36 | `magic` | uint64 | `0x454C595349554D31` |

**The last 12 bytes are the invariant trailer** (`kTrailerLength = 12`): `format_version` then
`magic`. Those 12 bytes must stay fixed across every future format version.

A reader seeks to `file_len - 12`, validates the magic, and *then* dispatches on `format_version` to
learn the full footer width. This order is required: a reader must not need to know the footer width
in order to discover the version.

The magic spells `ELYSIUM1` in ASCII. It is frozen — changing it makes every existing file fail the
check before any other field is examined.

## 6. Manifest records

Both record types are framed with §2's framing, then the content below.

A **string** is `varint64 length ‖ bytes`. A **file entry** is:

```
level              varint64
file_number        varint64
store_id           string
smallest_key       string
largest_key        string
file_bytes         varint64
num_entries        varint64
num_tombstones     varint64
compression        varint64   0 = none, 1 = lz4, 2 = zstd
min_write_time_ms  varint64
```

`store_id` is persisted rather than derived: tier and level are independent, so a file's store cannot
be computed from its level. `min_write_time_ms` is carried over unchanged by migration, so placement
stays monotone across a renumber.

**Compaction pointers** are `varint64 count ‖ (varint64 level ‖ string key)*`.

### Edit

Framed with `compression_type = 0`.

```
format_version     varint32   1
next_file_number   varint64
added_count        varint64
added              file entry × added_count
deleted_count      varint64
deleted            (varint64 level ‖ varint64 file_number) × deleted_count
compaction_pointers
```

### Snapshot

Framed with **zstd**, because a snapshot is always read whole.

```
format_version     varint32   1
next_file_number   varint64
file_count         varint64
files              file entry × file_count
compaction_pointers
```

## 7. Stats buffer (C ABI)

`elysiumkv_stats_snapshot` fills a caller-provided buffer. The header declares its own size and the
size of each record type, so a decoder built against an older layout locates the records correctly
and skips fields it does not know. **Locate records by the declared sizes, never by summing the
fields you happen to know.**

Header — `kStatsHeaderBytes = 192`:

| Offset | Field | Type |
| --- | --- | --- |
| 0 | `format_version` | uint32 (1) |
| 4 | `header_bytes` | uint32 (192) |
| 8 | `level_record_bytes` | uint32 (32) |
| 12 | `tier_record_bytes` | uint32 (32) |
| 16 | `level_count` | uint32 |
| 20 | `tier_count` | uint32 |
| 24 | `requires_recovery` | uint8 (0/1) |
| 25 | *padding* | 7 bytes, zero |
| 32 | 20 × uint64 scalars | see below |

The 20 scalars, in order from offset 32: `memtable_bytes`, `memtable_age_ms`, `compactions`,
`compaction_bytes_read`, `compaction_bytes_written`, `migrations`, `migration_bytes`,
`stalled_total_ms`, `stall_count`, `block_cache_hits`, `block_cache_misses`, `block_cache_bytes`,
`pins_outstanding`, `reader_cache_hits`, `reader_cache_misses`, `reader_cache_bytes`, `open_readers`,
`memory_budget_used`, `memory_budget_total`, `budget_sheds`.

Then `level_count` level records at offset `header_bytes`, then `tier_count` tier records.

Level record — 32 bytes:

| Offset | Field | Type |
| --- | --- | --- |
| 0 | `level` | int32 |
| 4 | `file_count` | int32 |
| 8 | `bytes` | uint64 |
| 16 | `oldest_file_age_ms` | uint64 |
| 24 | `files_stale_codec` | int32 |
| 28 | `age_triggered` | uint8 (0/1) |
| 29 | `stalling` | uint8 (0/1) |
| 30 | *padding* | 2 bytes, zero |

Tier record — 32 bytes:

| Offset | Field | Type |
| --- | --- | --- |
| 0 | `tier` | int32 |
| 4 | `file_count` | int32 |
| 8 | `bytes` | uint64 |
| 16 | `oldest_file_age_ms` | uint64 |
| 24 | `files_pending_migration` | int32 |
| 28 | `stalling` | uint8 (0/1) |
| 29 | *padding* | 3 bytes, zero |

## 8. Entry limits

Enforced by the writer, not only the reader — a limit the writer does not know about is a trap that
accepts data which can never be read back.

| Limit | Value | Constant |
| --- | --- | --- |
| Maximum value | 1 MiB | `kMaxValueBytes` |
| Maximum key | 64 KiB | `kMaxKeyBytes` |

A key counts against the same budget as its value, because they share a block.
