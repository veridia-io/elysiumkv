/* ARCHITECTURE.md "The ABI boundary" — the header is C99-compatible, and this file is the only thing that proves
 * it — it is compiled as C, so anything C++-only in elysiumkv.h fails the build
 * rather than being discovered by the first binding that tries.
 *
 * It also exercises the ABI the way a binding would: opaque handles, status
 * codes, the pin protocol, and no C++ in sight. Returns 0 on success. */

#include "elysiumkv/elysiumkv.h"

#include <stdio.h>
#include <string.h>

#define CHECK(condition, message)                                            \
    do {                                                                     \
        if (!(condition)) {                                                  \
            fprintf(stderr, "FAIL %s:%d: %s (last error: %s)\n", __FILE__,   \
                    __LINE__, (message), elysiumkv_last_error());              \
            return 1;                                                        \
        }                                                                    \
    } while (0)

static uint32_t read_u32(const uint8_t* p) {
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

int elysiumkv_c_smoke(const char* store_directory, const char* catalog_directory) {
    elysiumkv_options* options;
    elysiumkv_db* db = NULL;
    elysiumkv_batch* batch;
    elysiumkv_iter* iter;
    void* store;
    void* catalog;
    elysiumkv_status status;
    const uint8_t* value;
    size_t value_len;
    uint64_t pin;
    uint64_t leaked;
    size_t stats_bytes;
    uint8_t stats_buf[512];
    int seen;
    int i;
    char key[32];
    char expected[32];

    store = elysiumkv_local_blob_store_create(store_directory, "store-0");
    CHECK(store != NULL, "local blob store");
    catalog = elysiumkv_file_manifest_catalog_create(catalog_directory);
    CHECK(catalog != NULL, "file manifest catalog");

    options = elysiumkv_options_create();
    CHECK(options != NULL, "options");
    status = elysiumkv_options_configure(options, catalog, NULL, 64u * 1024u, 1024, 0, 0, 0, 0, 0, -1, -1, -1, 0, 0);
    CHECK(status == ELYSIUMKV_OK, "configure");

    /* One durable tier, three levels: the simplest correct configuration. */
    status = elysiumkv_options_add_tier(options, store, ELYSIUMKV_DURABLE, 0, 0, 0, 0);
    CHECK(status == ELYSIUMKV_OK, "add tier");
    status = elysiumkv_options_set_level(options, 0, ELYSIUMKV_COMPRESSION_NONE, 0, 4, 8, 12, 0);
    CHECK(status == ELYSIUMKV_OK, "level 0");
    status = elysiumkv_options_set_level(options, 1, ELYSIUMKV_COMPRESSION_ZSTD, 4u * 1024u * 1024u,
                                       0, 0, 0, 0);
    CHECK(status == ELYSIUMKV_OK, "level 1");
    status = elysiumkv_options_set_level(options, 2, ELYSIUMKV_COMPRESSION_ZSTD, 0, 0, 0, 0, 0);
    CHECK(status == ELYSIUMKV_OK, "level 2");

    status = elysiumkv_open(options, &db);
    CHECK(status == ELYSIUMKV_OK, "open");
    CHECK(db != NULL, "db handle");
    elysiumkv_options_destroy(options);

    for (i = 0; i < 200; ++i) {
        sprintf(key, "key:%06d", i);
        sprintf(expected, "value:%06d", i);
        status = elysiumkv_put(db, (const uint8_t*)key, strlen(key), (const uint8_t*)expected,
                             strlen(expected));
        CHECK(status == ELYSIUMKV_OK, "put");
    }

    /* Zero copy: the value points into a pinned block until we release it. */
    sprintf(key, "key:%06d", 42);
    sprintf(expected, "value:%06d", 42);
    status = elysiumkv_get(db, (const uint8_t*)key, strlen(key), &value, &value_len, &pin);
    CHECK(status == ELYSIUMKV_OK, "get");
    CHECK(value_len == strlen(expected), "value length");
    CHECK(memcmp(value, expected, value_len) == 0, "value bytes");
    CHECK(elysiumkv_pins_outstanding(db) == 1, "one pin outstanding");
    elysiumkv_unpin(db, pin);
    CHECK(elysiumkv_pins_outstanding(db) == 0, "pin released");

    /* A double unpin is a lookup miss, not a double free. */
    elysiumkv_unpin(db, pin);
    CHECK(elysiumkv_pins_outstanding(db) == 0, "double unpin is harmless");

    /* Absence is a distinct status, never an empty value. */
    status = elysiumkv_get(db, (const uint8_t*)"absent", 6, &value, &value_len, &pin);
    CHECK(status == ELYSIUMKV_NOT_FOUND, "absent key");

    status = elysiumkv_delete(db, (const uint8_t*)key, strlen(key));
    CHECK(status == ELYSIUMKV_OK, "delete");
    status = elysiumkv_get(db, (const uint8_t*)key, strlen(key), &value, &value_len, &pin);
    CHECK(status == ELYSIUMKV_NOT_FOUND, "deleted key");

    batch = elysiumkv_batch_create();
    CHECK(batch != NULL, "batch");
    elysiumkv_batch_put(batch, (const uint8_t*)"batch-a", 7, (const uint8_t*)"1", 1);
    elysiumkv_batch_put(batch, (const uint8_t*)"batch-b", 7, (const uint8_t*)"2", 1);
    elysiumkv_batch_delete(batch, (const uint8_t*)"batch-a", 7);
    CHECK(elysiumkv_batch_size(batch) == 3, "batch size");
    status = elysiumkv_write(db, batch);
    CHECK(status == ELYSIUMKV_OK, "write batch");
    elysiumkv_batch_destroy(batch);

    status = elysiumkv_get(db, (const uint8_t*)"batch-b", 7, &value, &value_len, &pin);
    CHECK(status == ELYSIUMKV_OK, "batch value");
    elysiumkv_unpin(db, pin);
    status = elysiumkv_get(db, (const uint8_t*)"batch-a", 7, &value, &value_len, &pin);
    CHECK(status == ELYSIUMKV_NOT_FOUND, "batch delete");

    status = elysiumkv_flush(db);
    CHECK(status == ELYSIUMKV_OK, "flush");

    /* Prefix iteration, which ARCHITECTURE.md "Absence is an answer, not an error" makes a first-class path. */
    status = elysiumkv_iter_prefix(db, (const uint8_t*)"key:", 4, &iter);
    CHECK(status == ELYSIUMKV_OK, "prefix iterator");
    seen = 0;
    while (elysiumkv_iter_next(iter)) {
        const uint8_t* iter_key;
        size_t iter_key_len;
        elysiumkv_iter_key(iter, &iter_key, &iter_key_len);
        CHECK(iter_key_len >= 4 && memcmp(iter_key, "key:", 4) == 0, "prefix respected");
        ++seen;
    }
    CHECK(elysiumkv_iter_status(iter) == ELYSIUMKV_OK, "iterator status");
    CHECK(seen == 199, "one key was deleted");
    elysiumkv_iter_destroy(iter);

    /* Bounded iteration. */
    status = elysiumkv_iter_create(db, (const uint8_t*)"key:000100", 10, (const uint8_t*)"key:000110",
                                 10, &iter);
    CHECK(status == ELYSIUMKV_OK, "bounded iterator");
    seen = 0;
    while (elysiumkv_iter_next(iter)) ++seen;
    CHECK(seen == 10, "bounded range");
    elysiumkv_iter_destroy(iter);

    /* One snapshot, decoded by the declared record sizes — the shape every
       binding repeats. Reading the counts is enough here; capi_test.cpp checks
       the fields. */
    status = elysiumkv_stats_snapshot(db, NULL, 0, &stats_bytes);
    CHECK(status == ELYSIUMKV_OK && stats_bytes >= 136, "stats size query");
    CHECK(stats_bytes <= sizeof(stats_buf), "stats fits the smoke buffer");
    status = elysiumkv_stats_snapshot(db, stats_buf, sizeof(stats_buf), &stats_bytes);
    CHECK(status == ELYSIUMKV_OK, "stats snapshot");
    CHECK(read_u32(stats_buf + 0) == 1, "format version");
    CHECK(read_u32(stats_buf + 16) == 3, "three levels");
    CHECK(read_u32(stats_buf + 20) == 1, "one tier");
    CHECK(stats_buf[24] == 0, "no recovery needed");

    leaked = elysiumkv_close(db);
    CHECK(leaked == 0, "clean close");

    elysiumkv_manifest_catalog_destroy(catalog);
    elysiumkv_blob_store_destroy(store);
    return 0;
}
