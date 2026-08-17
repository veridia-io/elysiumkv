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
| Manifest edit | 5 | `kEditFormatVersion` |
| Manifest snapshot | 5 | `kSnapshotFormatVersion` |
| Stats buffer | 1 | `kStatsFormatVersion` |

Conventions throughout: **little-endian** fixed-width integers; `varint32`/`varint64` are
LEB128-style, 7 bits per byte, low group first, high bit set to continue. `‖` means concatenation.

**Every manifest version is a clean break: there is no dual-read path.** A manifest written at an
earlier version is refused with `Status::Unsupported` — not `Status::Corrupt`, which would tell an operator their
bytes are damaged when they are merely older, and not read-with-defaults, which would mean carrying
two shapes forever. [CONTRIBUTING.md](CONTRIBUTING.md) asks that a format change teach the reader
both shapes; this is a deliberate exception for a `0.x` engine, stated here rather than left
implicit. **A store written by an earlier version has to be deleted and rebuilt from its log.**

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
range delete block  framed, present only in format_version 2
footer              44 bytes (v1) or 56 bytes (v2), exactly at end of file
```

The footer, at file offset `file_len - footer_length`:

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0 | `filter_handle.offset` | uint64 | |
| 8 | `filter_handle.length` | uint32 | framed length |
| 12 | `index_handle.offset` | uint64 | |
| 20 | `index_handle.length` | uint32 | framed length |
| 24 | `num_entries` | uint64 | |
| 32 | `range_del_handle.offset` | uint64 | **v2 only** |
| 40 | `range_del_handle.length` | uint32 | **v2 only**, framed length |
| 32 / 44 | `format_version` | uint32 | 1 or 2 |
| 36 / 48 | `magic` | uint64 | `0x454C595349554D31` |

`kFooterLengthV1 = 44`, `kFooterLengthV2 = 56`. The v2 field is inserted *before* the invariant
trailer, which is what keeps that trailer invariant.

### Range delete block

Present only when the file carries range tombstones, and its presence is exactly what makes the file
`format_version = 2`. **The version is per file, not per writer**: a file nobody range-deleted from
is still written as v1, so adding this feature reformats nothing, and a reader that predates range
tombstones keeps reading every such file while refusing exactly the files whose keys it would
otherwise report as present.

The block is an ordinary §3 block. Each entry is one half-open range `[lower, upper)`: the **key is
`lower`**, the **value is `upper`**, and `value_type` is `1` (`Put`) — not `Delete`, because the type
byte says whether an entry carries a value and here the value is load-bearing. What makes these
deletions is the block they are in.

Entries are sorted by `lower` and are disjoint, so the tombstone covering a key — if any — is the
last entry whose `lower` is at or before it, found with one seek rather than a scan.

A range tombstone shadows every entry strictly older than the file that carries it, in
`(level, file_number)` order, and **nothing in that file**. Within a single file there is no
ordering to appeal to, so none is defined.

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
num_range_tombstones varint64
smallest_range_key string
largest_range_key  string
compression        varint64   0 = none, 1 = lz4, 2 = zstd
min_write_time_ms  varint64
max_write_time_ms  varint64
watermark_flags    varint64   bit 0 = low present, bit 1 = high present; other values invalid
watermark_low      varint64   zero when bit 0 is clear
watermark_high     varint64   zero when bit 1 is clear
```

The **range tombstone span** is the interval this file's range deletes cover, and it is deliberately
not bounded by `smallest_key` and `largest_key`: a file can delete a range it holds no keys in, so a
reader that consulted only the data span would walk past the tombstone answering its query.
`num_range_tombstones` is zero exactly when the file's SST is `format_version = 1`.

`store_id` is persisted rather than derived: tier and level are independent, so a file's store cannot
be computed from its level. `min_write_time_ms` is carried over unchanged by migration, so placement
stays monotone across a renumber.

The two write times answer opposite questions and are both persisted for that reason.
`min_write_time_ms` is the oldest write in the file and drives placement, so a file moves to cold
storage as soon as any of it qualifies. `max_write_time_ms` is the newest — a flushed file takes its
memtable's seal time, a compaction output the max over its inputs — and drives age-based expiry,
which may only drop a file once *everything* in it has outlived the limit. Zero means unknown, and
unknown never expires.

The **watermark interval** is the embedder's durability frontier for this file's data: `low` is a
strict lower bound on the positions it contains, `high` an upper bound. Presence is encoded because
zero is a valid position. `low` present with `high` absent is invalid — a `low` is only ever a
previously established `high` — and so is `low > high`; both are rejected at decode. A flushed file
takes its memtable's interval, a compaction output takes `min` of the lows and `max` of the highs,
and a migration carries it unchanged.

**Compaction pointers** are `varint64 count ‖ (varint64 level ‖ string key)*`.

### Edit

Framed with `compression_type = 0`.

```
format_version     varint32   5
next_file_number   varint64
added_count        varint64
added              file entry × added_count
deleted_count      varint64
deleted            (varint64 level ‖ varint64 file_number) × deleted_count
compaction_pointers
truncation_point   string
```

### Snapshot

Framed with **zstd**, because a snapshot is always read whole.

```
format_version     varint32   5
next_file_number   varint64
file_count         varint64
files              file entry × file_count
compaction_pointers
truncation_point   string
```

The **truncation point** is the key below which everything has been dropped. Empty means no
truncation, which is also the correct reading of "truncate below the empty key" — nothing sorts
under it. It is **monotone**: applying an edit takes the max of the edit's value and the version's,
so replaying the manifest is idempotent and an edit replayed out of order cannot resurrect data.
A file whose largest key is below the point holds nothing readable and is unlinked whole; a file
straddling it is narrowed by the next compaction to cover it.

## 7. Stats buffer (C ABI)

`elysiumkv_stats_snapshot` fills a caller-provided buffer. The header declares its own size and the
size of each record type, so a decoder built against an older layout locates the records correctly
and skips fields it does not know. **Locate records by the declared sizes, never by summing the
fields you happen to know.**

Header — `kStatsHeaderBytes = 240`:

| Offset | Field | Type |
| --- | --- | --- |
| 0 | `format_version` | uint32 (1) |
| 4 | `header_bytes` | uint32 (240) |
| 8 | `level_record_bytes` | uint32 (48) |
| 12 | `tier_record_bytes` | uint32 (32) |
| 16 | `level_count` | uint32 |
| 20 | `tier_count` | uint32 |
| 24 | `requires_recovery` | uint8 (0/1) |
| 25 | *padding* | 7 bytes, zero |
| 32 | 22 × uint64 scalars | see below |
| 208 | `watermark_present` | uint8 (0/1) |
| 209 | *padding* | 7 bytes, zero |
| 216 | `memtable_entries` | uint64 |
| 224 | `memtable_tombstones` | uint64 |
| 232 | `background_failures` | uint64 |

The **22** scalars in the contiguous run at offset 32, in order: `memtable_bytes`,
`memtable_age_ms`, `compactions`, `compaction_bytes_read`, `compaction_bytes_written`,
`migrations`, `migration_bytes`, `stalled_total_ms`, `stall_count`, `block_cache_hits`,
`block_cache_misses`, `block_cache_bytes`, `pins_outstanding`, `reader_cache_hits`,
`reader_cache_misses`, `reader_cache_bytes`, `open_readers`, `memory_budget_used`,
`memory_budget_total`, `budget_sheds`, `flushes` (offset 192), `durable_watermark` (offset 200).
That run ends at 208, where `watermark_present` begins.

Three more uint64 scalars follow the flag and its padding, and are **not** part of that run:
`memtable_entries` (216), `memtable_tombstones` (224), `background_failures` (232). Twenty-five in
total; the run is twenty-two. Count from the table above, which is keyed by offset and is the
normative part — the prose here previously said "23" and then listed 25, which is the mistake a
decoder author would inherit.

`flushes`, `durable_watermark`, `memtable_entries`, `memtable_tombstones` and
`background_failures` were **appended without a version bump**, and so were the level record's `entries` and `tombstones` — which is what the
self-describing header is for: a decoder that locates records at `header_bytes` and steps by
`level_record_bytes` skips fields it does not know, and seven earlier scalars arrived the same way.
The level record growing is the first time the *record* size has moved rather than the header, and it
is safe for the same reason and under the same rule. `watermark_present` is 0 when no watermark
has been set — zero is a valid position, so an exporter omits the series rather than publishing zero.

Then `level_count` level records at offset `header_bytes`, then `tier_count` tier records.

Level record — 48 bytes:

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
| 32 | `entries` | uint64 |
| 40 | `tombstones` | uint64 |

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
