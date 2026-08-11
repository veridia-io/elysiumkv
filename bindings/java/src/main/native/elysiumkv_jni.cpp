/* ARCHITECTURE.md "The ABI boundary" — the JNI glue. It includes `elysiumkv.h` and nothing else from the engine:
 * one boundary, one place where a status code becomes a Java exception. A glue
 * layer reaching into the C++ headers would put `std::expected` and
 * `shared_ptr` on a path that must not throw.
 *
 * Two disciplines run through every function here:
 *
 *   * Nothing escapes. A C++ exception unwinding through a JNI frame is
 *     undefined behaviour, so every entry point is wrapped — the same catch-all
 *     the C ABI applies one layer down, because `std::bad_alloc` from the
 *     scratch buffers in this file would otherwise escape below it.
 *   * Absence is not failure. `ELYSIUMKV_NOT_FOUND` returns null or -1; every
 *     other non-OK status throws. ARCHITECTURE.md "The ABI boundary" puts this first for a reason: a binding
 *     that folds an unreachable store into "no such key" turns an outage into
 *     apparent data loss.
 */

#include "elysiumkv/elysiumkv.h"

#include <jni.h>

#include <new>
#include <string>
#include <vector>

namespace {

/* Cached in JNI_OnLoad. Looking these up per call is a real fraction of the
 * crossing cost, which is the number ARCHITECTURE.md "Benchmarks" asks us to keep small. */
jclass g_exception_class = nullptr;   // io/veridia/elysiumkv/ElysiumKVException
jmethodID g_exception_of = nullptr;   // static of(int, String) -> ElysiumKVException
jclass g_string_class = nullptr;      // java/lang/String
jclass g_runtime_exception = nullptr; // java/lang/RuntimeException, for glue-level failures

void* as_pointer(jlong handle) { return reinterpret_cast<void*>(static_cast<intptr_t>(handle)); }

jlong as_handle(const void* pointer) {
    return static_cast<jlong>(reinterpret_cast<intptr_t>(pointer));
}

elysiumkv_db* as_db(jlong handle) { return static_cast<elysiumkv_db*>(as_pointer(handle)); }
elysiumkv_iter* as_iter(jlong handle) { return static_cast<elysiumkv_iter*>(as_pointer(handle)); }
elysiumkv_batch* as_batch(jlong handle) { return static_cast<elysiumkv_batch*>(as_pointer(handle)); }
elysiumkv_options* as_options(jlong handle) {
    return static_cast<elysiumkv_options*>(as_pointer(handle));
}

/* Builds the exception in Java — one cached method id rather than six cached
 * classes, and the status-to-type mapping stays somewhere a Java reader can
 * find it. Never returns with the wrong exception pending: if constructing the
 * exception itself fails, whatever the JVM already raised is left alone. */
void throw_status(JNIEnv* env, elysiumkv_status status) {
    if (env->ExceptionCheck()) return;
    const char* detail = elysiumkv_last_error();
    jstring message = env->NewStringUTF(detail == nullptr ? "" : detail);
    if (message == nullptr) return;  // OOM already pending
    jobject exception = env->CallStaticObjectMethod(g_exception_class, g_exception_of,
                                                    static_cast<jint>(status), message);
    env->DeleteLocalRef(message);
    if (env->ExceptionCheck() || exception == nullptr) return;
    env->Throw(static_cast<jthrowable>(exception));
    env->DeleteLocalRef(exception);
}

void throw_glue(JNIEnv* env, const char* message) {
    if (env->ExceptionCheck()) return;
    env->ThrowNew(g_runtime_exception, message);
}

/* Throws unless the status is OK. Returns whether the call succeeded, so the
 * caller can return early rather than continue with an exception pending. */
bool check(JNIEnv* env, elysiumkv_status status) {
    if (status == ELYSIUMKV_OK) return true;
    throw_status(env, status);
    return false;
}

/* ARCHITECTURE.md "The ABI boundary" — GetByteArrayRegion, never GetPrimitiveArrayCritical. A critical region
 * must be short and must not block, and a `get` missing to a remote tier blocks
 * for tens of milliseconds while holding GC off. Keys are small, so the copy
 * costs nothing measurable — and this buffer keeps the common case off the heap
 * entirely. */
class ByteArrayCopy {
public:
    ByteArrayCopy(JNIEnv* env, jbyteArray array, jint length) {
        if (array == nullptr || length <= 0) return;
        const size_t needed = static_cast<size_t>(length);
        if (needed > sizeof(inline_storage_)) {
            heap_.resize(needed);
            data_ = heap_.data();
        } else {
            data_ = inline_storage_;
        }
        env->GetByteArrayRegion(array, 0, length, reinterpret_cast<jbyte*>(data_));
        size_ = needed;
    }

    const uint8_t* data() const { return data_; }
    size_t size() const { return size_; }

private:
    uint8_t inline_storage_[256];
    std::vector<uint8_t> heap_;
    uint8_t* data_ = nullptr;
    size_t size_ = 0;
};

/* The catch-all. `Fn` returns the JNI return value; on an escaping C++
 * exception we raise a Java one and return the fallback, because unwinding
 * through a JNI frame is undefined behaviour rather than an error. */
template <typename Fn>
auto guard(JNIEnv* env, Fn&& body, decltype(body()) fallback) -> decltype(body()) {
    try {
        return body();
    } catch (const std::bad_alloc&) {
        throw_glue(env, "elysiumkv JNI: out of memory");
        return fallback;
    } catch (const std::exception& error) {
        throw_glue(env, error.what());
        return fallback;
    } catch (...) {
        throw_glue(env, "elysiumkv JNI: unknown failure");
        return fallback;
    }
}

template <typename Fn>
void guard_void(JNIEnv* env, Fn&& body) {
    try {
        body();
    } catch (const std::bad_alloc&) {
        throw_glue(env, "elysiumkv JNI: out of memory");
    } catch (const std::exception& error) {
        throw_glue(env, error.what());
    } catch (...) {
        throw_glue(env, "elysiumkv JNI: unknown failure");
    }
}

// --- diagnostics -------------------------------------------------------------

jstring JNICALL last_error(JNIEnv* env, jclass) { return env->NewStringUTF(elysiumkv_last_error()); }

jstring JNICALL version(JNIEnv* env, jclass) { return env->NewStringUTF(elysiumkv_version()); }

jint JNICALL features(JNIEnv*, jclass) { return static_cast<jint>(elysiumkv_features()); }

// --- configuration -----------------------------------------------------------

jlong JNICALL options_create(JNIEnv* env, jclass) {
    return guard(env, [&] { return as_handle(elysiumkv_options_create()); }, jlong{0});
}

void JNICALL options_destroy(JNIEnv*, jclass, jlong options) {
    elysiumkv_options_destroy(as_options(options));
}

void JNICALL options_add_tier(JNIEnv* env, jclass, jlong options, jlong store, jint durability,
                              jlong max_age_ms, jlong max_bytes,
                              jlong stall_age_ms) {
    guard_void(env, [&] {
        check(env, elysiumkv_options_add_tier(as_options(options), as_pointer(store),
                                            static_cast<elysiumkv_durability>(durability), max_age_ms,
                                            max_bytes, stall_age_ms));
    });
}

void JNICALL options_set_level(JNIEnv* env, jclass, jlong options, jint level, jint compression,
                               jlong max_bytes, jint max_files, jint slowdown_at, jint stop_at,
                               jlong target_file_bytes) {
    guard_void(env, [&] {
        check(env, elysiumkv_options_set_level(as_options(options), level,
                                             static_cast<elysiumkv_compression>(compression),
                                             max_bytes, max_files, slowdown_at, stop_at,
                                             static_cast<size_t>(target_file_bytes)));
    });
}

void JNICALL options_configure(JNIEnv* env, jclass, jlong options, jlong catalog, jlong budget,
                               jlong memtable_bytes, jlong block_bytes, jlong block_cache_bytes,
                               jlong reader_cache_bytes, jint bloom_bits_per_key,
                               jlong max_compaction_bytes, jint manifest_edits_per_generation,
                               jint paranoid_checks, jint block_on_stall,
                               jlong flush_interval_ms, jlong maintenance_interval_ms,
                               jlong obsolete_retention_ms, jlong orphan_retention_ms,
                               jlong orphan_sweep_interval_ms) {
    guard_void(env, [&] {
        check(env, elysiumkv_options_configure(
                       as_options(options), as_pointer(catalog), as_pointer(budget),
                       static_cast<size_t>(memtable_bytes), static_cast<size_t>(block_bytes),
                       static_cast<size_t>(block_cache_bytes),
                       static_cast<size_t>(reader_cache_bytes), bloom_bits_per_key,
                       static_cast<size_t>(max_compaction_bytes), manifest_edits_per_generation,
                       paranoid_checks, block_on_stall,
                       static_cast<uint64_t>(flush_interval_ms),
                       static_cast<uint64_t>(maintenance_interval_ms),
                       static_cast<uint64_t>(obsolete_retention_ms),
                       static_cast<uint64_t>(orphan_retention_ms),
                       static_cast<uint64_t>(orphan_sweep_interval_ms)));
    });
}

// --- seams -------------------------------------------------------------------

/* A JNI string is not a C string until it is copied out, and the copy has to be
 * released on every path — including the one where the next JNI call fails. */
class Utf8 {
public:
    Utf8(JNIEnv* env, jstring value) : env_(env), value_(value) {
        chars_ = value == nullptr ? nullptr : env->GetStringUTFChars(value, nullptr);
    }
    ~Utf8() {
        if (chars_ != nullptr) env_->ReleaseStringUTFChars(value_, chars_);
    }
    Utf8(const Utf8&) = delete;
    Utf8& operator=(const Utf8&) = delete;

    const char* c_str() const { return chars_; }
    bool valid() const { return chars_ != nullptr; }

private:
    JNIEnv* env_;
    jstring value_;
    const char* chars_ = nullptr;
};

jlong JNICALL local_blob_store_create(JNIEnv* env, jclass, jstring root, jstring store_id) {
    return guard(env,
                 [&]() -> jlong {
                     Utf8 root_utf8(env, root);
                     Utf8 id_utf8(env, store_id);
                     if (!root_utf8.valid() || !id_utf8.valid()) return 0;
                     void* store = elysiumkv_local_blob_store_create(root_utf8.c_str(),
                                                                   id_utf8.c_str());
                     if (store == nullptr) throw_status(env, ELYSIUMKV_IO);
                     return as_handle(store);
                 },
                 jlong{0});
}

void JNICALL blob_store_destroy(JNIEnv*, jclass, jlong store) {
    elysiumkv_blob_store_destroy(as_pointer(store));
}

jlong JNICALL file_manifest_catalog_create(JNIEnv* env, jclass, jstring directory) {
    return guard(env,
                 [&]() -> jlong {
                     Utf8 directory_utf8(env, directory);
                     if (!directory_utf8.valid()) return 0;
                     void* catalog = elysiumkv_file_manifest_catalog_create(directory_utf8.c_str());
                     if (catalog == nullptr) throw_status(env, ELYSIUMKV_IO);
                     return as_handle(catalog);
                 },
                 jlong{0});
}

void JNICALL manifest_catalog_destroy(JNIEnv*, jclass, jlong catalog) {
    elysiumkv_manifest_catalog_destroy(as_pointer(catalog));
}

jlong JNICALL memory_budget_create(JNIEnv* env, jclass, jlong total_bytes) {
    return guard(env,
                 [&]() -> jlong {
                     void* budget = elysiumkv_memory_budget_create(static_cast<size_t>(total_bytes));
                     if (budget == nullptr) throw_status(env, ELYSIUMKV_CONFIG);
                     return as_handle(budget);
                 },
                 jlong{0});
}

void JNICALL memory_budget_destroy(JNIEnv*, jclass, jlong budget) {
    elysiumkv_memory_budget_destroy(as_pointer(budget));
}

jlong JNICALL memory_budget_used(JNIEnv*, jclass, jlong budget) {
    return static_cast<jlong>(elysiumkv_memory_budget_used(as_pointer(budget)));
}

jlong JNICALL disk_cache_blob_store_create(JNIEnv* env, jclass, jstring directory, jlong delegate,
                                           jlong max_cache_bytes, jboolean cache_on_write) {
    return guard(env,
                 [&]() -> jlong {
                     Utf8 directory_utf8(env, directory);
                     if (!directory_utf8.valid()) return 0;

                     void* cache = nullptr;
                     const elysiumkv_status status = elysiumkv_disk_cache_blob_store_create(
                         as_pointer(delegate), directory_utf8.c_str(),
                         static_cast<size_t>(max_cache_bytes),
                         cache_on_write == JNI_TRUE ? 1 : 0, &cache);
                     if (!check(env, status)) return 0;
                     return as_handle(cache);
                 },
                 jlong{0});
}

jlong JNICALL disk_cache_blob_store_create_chunked(JNIEnv* env, jclass, jstring directory,
                                                   jlong delegate, jlong max_cache_bytes,
                                                   jboolean cache_on_write,
                                                   jlong fetch_granularity) {
    return guard(env,
                 [&]() -> jlong {
                     Utf8 directory_utf8(env, directory);
                     if (!directory_utf8.valid()) return 0;

                     void* cache = nullptr;
                     const elysiumkv_status status = elysiumkv_disk_cache_blob_store_create_chunked(
                         as_pointer(delegate), directory_utf8.c_str(),
                         static_cast<size_t>(max_cache_bytes),
                         cache_on_write == JNI_TRUE ? 1 : 0,
                         static_cast<size_t>(fetch_granularity), &cache);
                     if (!check(env, status)) return 0;
                     return as_handle(cache);
                 },
                 jlong{0});
}

jlong JNICALL memory_cache_blob_store_create_chunked(JNIEnv* env, jclass, jlong delegate,
                                                     jlong budget, jlong max_cache_bytes,
                                                     jboolean cache_on_write,
                                                     jlong fetch_granularity) {
    return guard(env,
                 [&]() -> jlong {
                     void* cache = nullptr;
                     const elysiumkv_status status = elysiumkv_memory_cache_blob_store_create_chunked(
                         as_pointer(delegate), as_pointer(budget),
                         static_cast<size_t>(max_cache_bytes),
                         cache_on_write == JNI_TRUE ? 1 : 0,
                         static_cast<size_t>(fetch_granularity), &cache);
                     if (!check(env, status)) return 0;
                     return as_handle(cache);
                 },
                 jlong{0});
}

jlong JNICALL memory_cache_blob_store_create(JNIEnv* env, jclass, jlong delegate, jlong budget,
                                             jlong max_cache_bytes, jboolean cache_on_write) {
    return guard(env,
                 [&]() -> jlong {
                     void* cache = nullptr;
                     const elysiumkv_status status = elysiumkv_memory_cache_blob_store_create(
                         as_pointer(delegate), as_pointer(budget),
                         static_cast<size_t>(max_cache_bytes),
                         cache_on_write == JNI_TRUE ? 1 : 0, &cache);
                     if (!check(env, status)) return 0;
                     return as_handle(cache);
                 },
                 jlong{0});
}

/* --- remote seams -----------------------------------------------------------
 *
 * Most arguments here are genuinely optional — a null endpoint means the real
 * service, null credentials mean the SDK's own chain — so unlike the local
 * constructors these must tell "the caller passed nothing" apart from
 * "GetStringUTFChars failed". `Utf8::valid()` conflates them, which is right when
 * every argument is required and wrong here. */
bool utf8_failed(const Utf8& value, jstring original) {
    return original != nullptr && !value.valid();
}

jlong JNICALL s3_blob_store_create(JNIEnv* env, jclass, jstring bucket, jstring prefix,
                                   jstring region, jstring endpoint, jstring access_key,
                                   jstring secret_key, jlong point_timeout_ms,
                                   jlong bulk_timeout_ms, jstring store_id) {
    return guard(env,
                 [&]() -> jlong {
                     Utf8 bucket_utf8(env, bucket);
                     Utf8 prefix_utf8(env, prefix);
                     Utf8 region_utf8(env, region);
                     Utf8 endpoint_utf8(env, endpoint);
                     Utf8 key_utf8(env, access_key);
                     Utf8 secret_utf8(env, secret_key);
                     Utf8 id_utf8(env, store_id);
                     if (utf8_failed(bucket_utf8, bucket) || utf8_failed(prefix_utf8, prefix) ||
                         utf8_failed(region_utf8, region) || utf8_failed(endpoint_utf8, endpoint) ||
                         utf8_failed(key_utf8, access_key) ||
                         utf8_failed(secret_utf8, secret_key) || utf8_failed(id_utf8, store_id)) {
                         return 0;  // OOM already pending
                     }

                     void* store = nullptr;
                     const elysiumkv_status status = elysiumkv_s3_blob_store_create(
                         bucket_utf8.c_str(), prefix_utf8.c_str(), region_utf8.c_str(),
                         endpoint_utf8.c_str(), key_utf8.c_str(), secret_utf8.c_str(),
                         point_timeout_ms, bulk_timeout_ms, id_utf8.c_str(), &store);
                     if (!check(env, status)) return 0;
                     return as_handle(store);
                 },
                 jlong{0});
}

jlong JNICALL s3_manifest_catalog_create(JNIEnv* env, jclass, jstring bucket, jstring prefix,
                                         jstring region, jstring endpoint, jstring access_key,
                                         jstring secret_key, jlong point_timeout_ms,
                                         jlong bulk_timeout_ms) {
    return guard(env,
                 [&]() -> jlong {
                     Utf8 bucket_utf8(env, bucket);
                     Utf8 prefix_utf8(env, prefix);
                     Utf8 region_utf8(env, region);
                     Utf8 endpoint_utf8(env, endpoint);
                     Utf8 key_utf8(env, access_key);
                     Utf8 secret_utf8(env, secret_key);
                     if (utf8_failed(bucket_utf8, bucket) || utf8_failed(prefix_utf8, prefix) ||
                         utf8_failed(region_utf8, region) || utf8_failed(endpoint_utf8, endpoint) ||
                         utf8_failed(key_utf8, access_key) ||
                         utf8_failed(secret_utf8, secret_key)) {
                         return 0;
                     }

                     void* catalog = nullptr;
                     const elysiumkv_status status = elysiumkv_s3_manifest_catalog_create(
                         bucket_utf8.c_str(), prefix_utf8.c_str(), region_utf8.c_str(),
                         endpoint_utf8.c_str(), key_utf8.c_str(), secret_utf8.c_str(),
                         point_timeout_ms, bulk_timeout_ms, &catalog);
                     if (!check(env, status)) return 0;
                     return as_handle(catalog);
                 },
                 jlong{0});
}

jlong JNICALL dynamo_manifest_catalog_create(JNIEnv* env, jclass, jstring table, jstring store_id,
                                             jstring region, jstring endpoint, jstring access_key,
                                             jstring secret_key, jlong timeout_ms,
                                             jboolean create_table_if_missing) {
    return guard(env,
                 [&]() -> jlong {
                     Utf8 table_utf8(env, table);
                     Utf8 id_utf8(env, store_id);
                     Utf8 region_utf8(env, region);
                     Utf8 endpoint_utf8(env, endpoint);
                     Utf8 key_utf8(env, access_key);
                     Utf8 secret_utf8(env, secret_key);
                     if (utf8_failed(table_utf8, table) || utf8_failed(id_utf8, store_id) ||
                         utf8_failed(region_utf8, region) || utf8_failed(endpoint_utf8, endpoint) ||
                         utf8_failed(key_utf8, access_key) ||
                         utf8_failed(secret_utf8, secret_key)) {
                         return 0;
                     }

                     void* catalog = nullptr;
                     const elysiumkv_status status = elysiumkv_dynamo_manifest_catalog_create(
                         table_utf8.c_str(), id_utf8.c_str(), region_utf8.c_str(),
                         endpoint_utf8.c_str(), key_utf8.c_str(), secret_utf8.c_str(), timeout_ms,
                         create_table_if_missing == JNI_TRUE ? 1 : 0, &catalog);
                     if (!check(env, status)) return 0;
                     return as_handle(catalog);
                 },
                 jlong{0});
}

// --- open and close ----------------------------------------------------------

jlong JNICALL open_db(JNIEnv* env, jclass, jlong options) {
    return guard(env,
                 [&]() -> jlong {
                     elysiumkv_db* db = nullptr;
                     if (!check(env, elysiumkv_open(as_options(options), &db))) return 0;
                     return as_handle(db);
                 },
                 jlong{0});
}

jlong JNICALL open_read_only(JNIEnv* env, jclass, jlong options) {
    return guard(env,
                 [&]() -> jlong {
                     elysiumkv_db* db = nullptr;
                     if (!check(env, elysiumkv_open_read_only(as_options(options), &db))) return 0;
                     return as_handle(db);
                 },
                 jlong{0});
}

void JNICALL refresh(JNIEnv* env, jclass, jlong db) {
    guard_void(env, [&] { check(env, elysiumkv_refresh(as_db(db))); });
}

/* `n_stores` is in/out — capacity in, actual count out — and the call opens the
 * db, so it cannot be repeated to size the array. A configuration has a handful
 * of tiers; the capacity below is far past any of them, and the count is
 * reported honestly even in the impossible case where it is not. The returned
 * pointers belong to the db and die with it, so they become Java strings here
 * and now. */
jobjectArray JNICALL open_with_result(JNIEnv* env, jclass, jlong options, jlongArray db_out,
                                      jlongArray discarded_files_out,
                                      jbooleanArray recovery_out) {
    return guard(env,
                 [&]() -> jobjectArray {
                     constexpr size_t kMaxReportedStores = 64;
                     const char* names[kMaxReportedStores] = {nullptr};

                     elysiumkv_db* db = nullptr;
                     size_t store_count = kMaxReportedStores;
                     uint64_t discarded_files = 0;
                     bool requires_recovery = false;
                     if (!check(env, elysiumkv_open_with_result(as_options(options), &db, names,
                                                              &store_count, &discarded_files,
                                                              &requires_recovery))) {
                         return nullptr;
                     }

                     const jlong db_handle = as_handle(db);
                     env->SetLongArrayRegion(db_out, 0, 1, &db_handle);
                     const jlong files = static_cast<jlong>(discarded_files);
                     env->SetLongArrayRegion(discarded_files_out, 0, 1, &files);
                     const jboolean recovery = requires_recovery ? JNI_TRUE : JNI_FALSE;
                     env->SetBooleanArrayRegion(recovery_out, 0, 1, &recovery);

                     const size_t written =
                         store_count < kMaxReportedStores ? store_count : kMaxReportedStores;
                     jobjectArray stores = env->NewObjectArray(static_cast<jsize>(written),
                                                               g_string_class, nullptr);
                     if (stores == nullptr) return nullptr;
                     for (size_t i = 0; i < written; ++i) {
                         jstring name = env->NewStringUTF(names[i] == nullptr ? "" : names[i]);
                         if (name == nullptr) return nullptr;
                         env->SetObjectArrayElement(stores, static_cast<jsize>(i), name);
                         env->DeleteLocalRef(name);
                     }
                     return stores;
                 },
                 static_cast<jobjectArray>(nullptr));
}

jlong JNICALL close_db(JNIEnv*, jclass, jlong db) {
    return static_cast<jlong>(elysiumkv_close(as_db(db)));
}

// --- reads -------------------------------------------------------------------

/* Zero-copy: NewDirectByteBuffer wraps the pinned block itself, so the value
 * never lands on the Java heap. The pin handle goes back alongside it, and the
 * caller releases it — a leaked pin holds a block-cache entry indefinitely (ARCHITECTURE.md "The ABI boundary").
 */
jobject wrap_pinned(JNIEnv* env, const uint8_t* value, size_t value_len, uint64_t pin,
                    jlongArray pin_out) {
    jobject buffer = env->NewDirectByteBuffer(const_cast<uint8_t*>(value),
                                              static_cast<jlong>(value_len));
    if (buffer == nullptr) {
        throw_glue(env, "elysiumkv JNI: NewDirectByteBuffer failed");
        return nullptr;
    }
    if (pin_out != nullptr) {
        const jlong handle = static_cast<jlong>(pin);
        env->SetLongArrayRegion(pin_out, 0, 1, &handle);
    }
    return buffer;
}

jobject JNICALL get(JNIEnv* env, jclass, jlong db, jbyteArray key, jint key_length,
                    jlongArray pin_out) {
    return guard(env,
                 [&]() -> jobject {
                     ByteArrayCopy key_bytes(env, key, key_length);
                     if (env->ExceptionCheck()) return nullptr;

                     const uint8_t* value = nullptr;
                     size_t value_len = 0;
                     uint64_t pin = 0;
                     const elysiumkv_status status = elysiumkv_get(as_db(db), key_bytes.data(),
                                                               key_bytes.size(), &value, &value_len,
                                                               &pin);
                     if (status == ELYSIUMKV_NOT_FOUND) return nullptr;
                     if (!check(env, status)) return nullptr;
                     return wrap_pinned(env, value, value_len, pin, pin_out);
                 },
                 static_cast<jobject>(nullptr));
}

jobject JNICALL get_direct(JNIEnv* env, jclass, jlong db, jobject key, jint key_length,
                           jlongArray pin_out) {
    return guard(env,
                 [&]() -> jobject {
                     const uint8_t* key_bytes =
                         static_cast<const uint8_t*>(env->GetDirectBufferAddress(key));
                     if (key_bytes == nullptr) {
                         throw_glue(env, "elysiumkv JNI: key buffer is not direct");
                         return nullptr;
                     }
                     const uint8_t* value = nullptr;
                     size_t value_len = 0;
                     uint64_t pin = 0;
                     const elysiumkv_status status =
                         elysiumkv_get(as_db(db), key_bytes, static_cast<size_t>(key_length), &value,
                                     &value_len, &pin);
                     if (status == ELYSIUMKV_NOT_FOUND) return nullptr;
                     if (!check(env, status)) return nullptr;
                     return wrap_pinned(env, value, value_len, pin, pin_out);
                 },
                 static_cast<jobject>(nullptr));
}

void JNICALL unpin(JNIEnv*, jclass, jlong db, jlong pin) {
    elysiumkv_unpin(as_db(db), static_cast<uint64_t>(pin));
}

/* Copies once, from the pinned block straight into the Java array.
 *
 * The obvious implementation calls elysiumkv_get_copy with a scratch buffer and
 * then copies that into the array — three copies and a heap allocation per
 * lookup, on the path a caller reaches for precisely when they do *not* want to
 * think about pins. Pinning here and releasing immediately gives the same
 * contract for one copy and no allocation. */
jint JNICALL get_copy(JNIEnv* env, jclass, jlong db, jbyteArray key, jint key_length,
                      jbyteArray out) {
    return guard(env,
                 [&]() -> jint {
                     ByteArrayCopy key_bytes(env, key, key_length);
                     if (env->ExceptionCheck()) return -1;

                     const uint8_t* value = nullptr;
                     size_t value_len = 0;
                     uint64_t pin = 0;
                     const elysiumkv_status status = elysiumkv_get(as_db(db), key_bytes.data(),
                                                               key_bytes.size(), &value, &value_len,
                                                               &pin);
                     if (status == ELYSIUMKV_NOT_FOUND) return -1;
                     if (!check(env, status)) return -1;

                     const jsize capacity = out == nullptr ? 0 : env->GetArrayLength(out);
                     const jsize writable = static_cast<jsize>(value_len) < capacity
                                                ? static_cast<jsize>(value_len)
                                                : capacity;
                     if (writable > 0) {
                         env->SetByteArrayRegion(out, 0, writable,
                                                 reinterpret_cast<const jbyte*>(value));
                     }
                     elysiumkv_unpin(as_db(db), pin);
                     return static_cast<jint>(value_len);
                 },
                 jint{-1});
}

jlong JNICALL pins_outstanding(JNIEnv*, jclass, jlong db) {
    return static_cast<jlong>(elysiumkv_pins_outstanding(as_db(db)));
}

// --- writes ------------------------------------------------------------------

void JNICALL put(JNIEnv* env, jclass, jlong db, jbyteArray key, jint key_length, jbyteArray value,
                 jint value_length) {
    guard_void(env, [&] {
        ByteArrayCopy key_bytes(env, key, key_length);
        ByteArrayCopy value_bytes(env, value, value_length);
        if (env->ExceptionCheck()) return;
        check(env, elysiumkv_put(as_db(db), key_bytes.data(), key_bytes.size(), value_bytes.data(),
                               value_bytes.size()));
    });
}

void JNICALL remove_key(JNIEnv* env, jclass, jlong db, jbyteArray key, jint key_length) {
    guard_void(env, [&] {
        ByteArrayCopy key_bytes(env, key, key_length);
        if (env->ExceptionCheck()) return;
        check(env, elysiumkv_delete(as_db(db), key_bytes.data(), key_bytes.size()));
    });
}

jlong JNICALL batch_create(JNIEnv* env, jclass) {
    return guard(env,
                 [&]() -> jlong {
                     elysiumkv_batch* batch = elysiumkv_batch_create();
                     if (batch == nullptr) throw_status(env, ELYSIUMKV_UNUSABLE);
                     return as_handle(batch);
                 },
                 jlong{0});
}

void JNICALL batch_destroy(JNIEnv*, jclass, jlong batch) {
    elysiumkv_batch_destroy(as_batch(batch));
}

void JNICALL batch_put(JNIEnv* env, jclass, jlong batch, jbyteArray key, jint key_length,
                       jbyteArray value, jint value_length) {
    guard_void(env, [&] {
        ByteArrayCopy key_bytes(env, key, key_length);
        ByteArrayCopy value_bytes(env, value, value_length);
        if (env->ExceptionCheck()) return;
        elysiumkv_batch_put(as_batch(batch), key_bytes.data(), key_bytes.size(), value_bytes.data(),
                          value_bytes.size());
    });
}

void JNICALL batch_delete(JNIEnv* env, jclass, jlong batch, jbyteArray key, jint key_length) {
    guard_void(env, [&] {
        ByteArrayCopy key_bytes(env, key, key_length);
        if (env->ExceptionCheck()) return;
        elysiumkv_batch_delete(as_batch(batch), key_bytes.data(), key_bytes.size());
    });
}

jlong JNICALL batch_size(JNIEnv*, jclass, jlong batch) {
    return static_cast<jlong>(elysiumkv_batch_size(as_batch(batch)));
}

void JNICALL write_batch(JNIEnv* env, jclass, jlong db, jlong batch) {
    guard_void(env, [&] { check(env, elysiumkv_write(as_db(db), as_batch(batch))); });
}

void JNICALL flush(JNIEnv* env, jclass, jlong db) {
    guard_void(env, [&] { check(env, elysiumkv_flush(as_db(db))); });
}

void JNICALL compact_level(JNIEnv* env, jclass, jlong db, jint level) {
    guard_void(env, [&] { check(env, elysiumkv_compact_level(as_db(db), level)); });
}

// --- iteration ---------------------------------------------------------------

jlong JNICALL iter_create(JNIEnv* env, jclass, jlong db, jbyteArray lo, jint lo_length,
                          jbyteArray hi, jint hi_length) {
    return guard(env,
                 [&]() -> jlong {
                     ByteArrayCopy lo_bytes(env, lo, lo_length);
                     ByteArrayCopy hi_bytes(env, hi, hi_length);
                     if (env->ExceptionCheck()) return 0;
                     elysiumkv_iter* iter = nullptr;
                     if (!check(env, elysiumkv_iter_create(as_db(db), lo_bytes.data(),
                                                         lo_bytes.size(), hi_bytes.data(),
                                                         hi_bytes.size(), &iter))) {
                         return 0;
                     }
                     return as_handle(iter);
                 },
                 jlong{0});
}

jlong JNICALL iter_prefix(JNIEnv* env, jclass, jlong db, jbyteArray prefix, jint prefix_length) {
    return guard(env,
                 [&]() -> jlong {
                     ByteArrayCopy prefix_bytes(env, prefix, prefix_length);
                     if (env->ExceptionCheck()) return 0;
                     elysiumkv_iter* iter = nullptr;
                     if (!check(env, elysiumkv_iter_prefix(as_db(db), prefix_bytes.data(),
                                                         prefix_bytes.size(), &iter))) {
                         return 0;
                     }
                     return as_handle(iter);
                 },
                 jlong{0});
}

jlong JNICALL iter_create_reverse(JNIEnv* env, jclass, jlong db, jbyteArray lo, jint lo_length,
                                  jbyteArray hi, jint hi_length) {
    return guard(env,
                 [&]() -> jlong {
                     ByteArrayCopy lo_bytes(env, lo, lo_length);
                     ByteArrayCopy hi_bytes(env, hi, hi_length);
                     if (env->ExceptionCheck()) return 0;
                     elysiumkv_iter* iter = nullptr;
                     if (!check(env, elysiumkv_iter_create_reverse(as_db(db), lo_bytes.data(),
                                                                 lo_bytes.size(), hi_bytes.data(),
                                                                 hi_bytes.size(), &iter))) {
                         return 0;
                     }
                     return as_handle(iter);
                 },
                 jlong{0});
}

jlong JNICALL iter_prefix_reverse(JNIEnv* env, jclass, jlong db, jbyteArray prefix,
                                  jint prefix_length) {
    return guard(env,
                 [&]() -> jlong {
                     ByteArrayCopy prefix_bytes(env, prefix, prefix_length);
                     if (env->ExceptionCheck()) return 0;
                     elysiumkv_iter* iter = nullptr;
                     if (!check(env, elysiumkv_iter_prefix_reverse(as_db(db), prefix_bytes.data(),
                                                                 prefix_bytes.size(), &iter))) {
                         return 0;
                     }
                     return as_handle(iter);
                 },
                 jlong{0});
}

void JNICALL options_configure_compaction(JNIEnv* env, jclass, jlong options, jdouble trigger,
                                          jlong min_entries) {
    guard_void(env, [&] {
        check(env, elysiumkv_options_configure_compaction(as_options(options), trigger,
                                                      static_cast<uint64_t>(min_entries)));
    });
}

void JNICALL truncate_below(JNIEnv* env, jclass, jlong db, jbyteArray key, jint key_length) {
    guard_void(env, [&] {
        ByteArrayCopy key_bytes(env, key, key_length);
        if (env->ExceptionCheck()) return;
        check(env, elysiumkv_truncate_below(as_db(db), key_bytes.data(), key_bytes.size()));
    });
}

jboolean JNICALL iter_next(JNIEnv*, jclass, jlong iter) {
    return elysiumkv_iter_next(as_iter(iter)) ? JNI_TRUE : JNI_FALSE;
}

jobject JNICALL iter_key(JNIEnv* env, jclass, jlong iter) {
    const uint8_t* key = nullptr;
    size_t key_len = 0;
    elysiumkv_iter_key(as_iter(iter), &key, &key_len);
    if (key == nullptr) return nullptr;
    return env->NewDirectByteBuffer(const_cast<uint8_t*>(key), static_cast<jlong>(key_len));
}

jobject JNICALL iter_value(JNIEnv* env, jclass, jlong iter) {
    const uint8_t* value = nullptr;
    size_t value_len = 0;
    elysiumkv_iter_value(as_iter(iter), &value, &value_len);
    if (value == nullptr) return nullptr;
    return env->NewDirectByteBuffer(const_cast<uint8_t*>(value), static_cast<jlong>(value_len));
}

/* The allocating accessors above return a fresh direct buffer per call, which is
 * two Java objects per entry on a path ARCHITECTURE.md "Absence is an answer, not an error" calls the dominant read pattern. These
 * copy into a buffer the caller reuses instead: same single crossing, no
 * allocation. Returns the full length, which may exceed `dst`. */
jint copy_out(JNIEnv* env, const uint8_t* data, size_t len, jbyteArray dst) {
    if (data == nullptr) return -1;
    const jsize capacity = dst == nullptr ? 0 : env->GetArrayLength(dst);
    const jsize writable = static_cast<jsize>(len) < capacity ? static_cast<jsize>(len) : capacity;
    if (writable > 0) {
        env->SetByteArrayRegion(dst, 0, writable, reinterpret_cast<const jbyte*>(data));
    }
    return static_cast<jint>(len);
}

/* Fills the caller's *direct* buffer in place: the C ABI writes into it and the
 * Java side decodes from it, so nothing is staged in between.
 *
 * The first version took a byte[], which meant a scratch vector, a copy into it
 * and a copy out — reintroducing per-entry cost on the very path batching exists
 * to make cheap — and smuggled two numbers back through a packed long. Only the
 * entry count is needed, so it is the return value; the decoder walks the
 * records and does not care how many bytes they occupy. */
jint JNICALL iter_next_batch(JNIEnv* env, jclass, jlong iter, jobject dst, jint capacity) {
    return guard(env,
                 [&]() -> jint {
                     auto* buffer = static_cast<uint8_t*>(env->GetDirectBufferAddress(dst));
                     if (buffer == nullptr) {
                         throw_glue(env, "elysiumkv JNI: batch buffer is not direct");
                         return 0;
                     }
                     size_t count = 0;
                     size_t bytes = 0;
                     if (!check(env, elysiumkv_iter_next_batch(as_iter(iter), buffer,
                                                             static_cast<size_t>(capacity), &count,
                                                             &bytes))) {
                         return 0;
                     }
                     // Zero entries with a non-zero size is not exhaustion: it is
                     // one entry too large for this buffer. Reporting the size as
                     // a negative distinguishes the two without a second return.
                     if (count == 0) return bytes == 0 ? 0 : -static_cast<jint>(bytes);
                     return static_cast<jint>(count);
                 },
                 jint{0});
}

jint JNICALL iter_key_into(JNIEnv* env, jclass, jlong iter, jbyteArray dst) {
    const uint8_t* key = nullptr;
    size_t key_len = 0;
    elysiumkv_iter_key(as_iter(iter), &key, &key_len);
    return copy_out(env, key, key_len, dst);
}

jint JNICALL iter_value_into(JNIEnv* env, jclass, jlong iter, jbyteArray dst) {
    const uint8_t* value = nullptr;
    size_t value_len = 0;
    elysiumkv_iter_value(as_iter(iter), &value, &value_len);
    return copy_out(env, value, value_len, dst);
}

void JNICALL iter_status(JNIEnv* env, jclass, jlong iter) {
    guard_void(env, [&] { check(env, elysiumkv_iter_status(as_iter(iter))); });
}

void JNICALL iter_destroy(JNIEnv*, jclass, jlong iter) { elysiumkv_iter_destroy(as_iter(iter)); }

// --- statistics --------------------------------------------------------------

/* This one keeps a staging buffer, unlike get_copy and the batch: a snapshot is
 * a diagnostic call taken seconds apart, and the two-pass size query makes the
 * direct-buffer dance cost more in API than it saves in copies. */
jint JNICALL stats_snapshot(JNIEnv* env, jclass, jlong db, jbyteArray out) {
    return guard(env,
                 [&]() -> jint {
                     size_t needed = 0;
                     if (!check(env, elysiumkv_stats_snapshot(as_db(db), nullptr, 0, &needed))) {
                         return 0;
                     }
                     const jsize capacity = out == nullptr ? 0 : env->GetArrayLength(out);
                     if (out == nullptr || static_cast<size_t>(capacity) < needed) {
                         return static_cast<jint>(needed);
                     }
                     std::vector<uint8_t> scratch(needed);
                     size_t written = 0;
                     if (!check(env, elysiumkv_stats_snapshot(as_db(db), scratch.data(),
                                                            scratch.size(), &written))) {
                         return 0;
                     }
                     env->SetByteArrayRegion(out, 0, static_cast<jsize>(written),
                                             reinterpret_cast<const jbyte*>(scratch.data()));
                     return static_cast<jint>(written);
                 },
                 jint{0});
}

void JNICALL mark_recovery_complete(JNIEnv*, jclass, jlong db) {
    elysiumkv_mark_recovery_complete(as_db(db));
}

// --- watermark ----------------------------------------------------------------

void JNICALL set_watermark(JNIEnv* env, jclass, jlong db, jlong position) {
    guard_void(env, [&] {
        // Java has no unsigned long, so the position crosses as a signed 64-bit value and is
        // reinterpreted here. A changelog offset is never negative in practice; the reinterpret
        // is what keeps the ABI a plain `uint64_t` rather than inventing a wider representation
        // for a range nobody uses.
        check(env, elysiumkv_set_watermark(as_db(db),
                                          static_cast<uint64_t>(static_cast<int64_t>(position))));
    });
}

/* Returns the recovered watermark, or -1 when there is none.
 *
 * A sentinel rather than an out-parameter because absence has to survive the crossing and a boxed
 * `OptionalLong` would mean a class lookup in the glue. Zero could not serve — it is a valid
 * position, a store at the start of its log — whereas a negative value cannot come from any
 * position Java can represent: the crossing is a signed 64-bit value, so positions at or above 2^63
 * are already outside what this binding can carry, and the sentinel shares that limit rather than
 * adding one. A changelog offset is nowhere near it. The Java side turns -1 back into an empty
 * `OptionalLong`. */
jlong JNICALL watermark(JNIEnv* env, jclass, jlong db) {
    return guard(env,
                 [&]() -> jlong {
                     uint64_t value = 0;
                     bool present = false;
                     check(env, elysiumkv_watermark(as_db(db), &value, &present));
                     if (!present) return static_cast<jlong>(-1);
                     return static_cast<jlong>(value);
                 },
                 static_cast<jlong>(-1));
}

/* ARCHITECTURE.md "The ABI boundary" — registered explicitly, so a rename or a signature change fails at load
 * naming the method, rather than as an UnsatisfiedLinkError the first time a
 * rarely-used call runs. */
const JNINativeMethod kMethods[] = {
    {const_cast<char*>("lastError"), const_cast<char*>("()Ljava/lang/String;"),
     reinterpret_cast<void*>(last_error)},
    {const_cast<char*>("version"), const_cast<char*>("()Ljava/lang/String;"),
     reinterpret_cast<void*>(version)},
    {const_cast<char*>("features"), const_cast<char*>("()I"), reinterpret_cast<void*>(features)},

    {const_cast<char*>("optionsCreate"), const_cast<char*>("()J"),
     reinterpret_cast<void*>(options_create)},
    {const_cast<char*>("optionsDestroy"), const_cast<char*>("(J)V"),
     reinterpret_cast<void*>(options_destroy)},
    {const_cast<char*>("optionsAddTier"), const_cast<char*>("(JJIJJJ)V"),
     reinterpret_cast<void*>(options_add_tier)},
    {const_cast<char*>("optionsSetLevel"), const_cast<char*>("(JIIJIIIJ)V"),
     reinterpret_cast<void*>(options_set_level)},
    {const_cast<char*>("optionsConfigure"), const_cast<char*>("(JJJJJJJIJIIIJJJJJ)V"),
     reinterpret_cast<void*>(options_configure)},

    {const_cast<char*>("localBlobStoreCreate"),
     const_cast<char*>("(Ljava/lang/String;Ljava/lang/String;)J"),
     reinterpret_cast<void*>(local_blob_store_create)},
    {const_cast<char*>("blobStoreDestroy"), const_cast<char*>("(J)V"),
     reinterpret_cast<void*>(blob_store_destroy)},
    {const_cast<char*>("fileManifestCatalogCreate"), const_cast<char*>("(Ljava/lang/String;)J"),
     reinterpret_cast<void*>(file_manifest_catalog_create)},
    {const_cast<char*>("manifestCatalogDestroy"), const_cast<char*>("(J)V"),
     reinterpret_cast<void*>(manifest_catalog_destroy)},

    {const_cast<char*>("memoryBudgetCreate"), const_cast<char*>("(J)J"),
     reinterpret_cast<void*>(memory_budget_create)},
    {const_cast<char*>("memoryBudgetDestroy"), const_cast<char*>("(J)V"),
     reinterpret_cast<void*>(memory_budget_destroy)},
    {const_cast<char*>("memoryBudgetUsed"), const_cast<char*>("(J)J"),
     reinterpret_cast<void*>(memory_budget_used)},

    {const_cast<char*>("diskCacheBlobStoreCreate"),
     const_cast<char*>("(Ljava/lang/String;JJZ)J"),
     reinterpret_cast<void*>(disk_cache_blob_store_create)},
    {const_cast<char*>("diskCacheBlobStoreCreateChunked"),
     const_cast<char*>("(Ljava/lang/String;JJZJ)J"),
     reinterpret_cast<void*>(disk_cache_blob_store_create_chunked)},
    {const_cast<char*>("memoryCacheBlobStoreCreateChunked"), const_cast<char*>("(JJJZJ)J"),
     reinterpret_cast<void*>(memory_cache_blob_store_create_chunked)},
    {const_cast<char*>("memoryCacheBlobStoreCreate"), const_cast<char*>("(JJJZ)J"),
     reinterpret_cast<void*>(memory_cache_blob_store_create)},

    {const_cast<char*>("s3BlobStoreCreate"),
     const_cast<char*>("(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;"
                       "Ljava/lang/String;Ljava/lang/String;JJLjava/lang/String;)J"),
     reinterpret_cast<void*>(s3_blob_store_create)},
    {const_cast<char*>("s3ManifestCatalogCreate"),
     const_cast<char*>("(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;"
                       "Ljava/lang/String;Ljava/lang/String;JJ)J"),
     reinterpret_cast<void*>(s3_manifest_catalog_create)},
    {const_cast<char*>("dynamoManifestCatalogCreate"),
     const_cast<char*>("(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;"
                       "Ljava/lang/String;Ljava/lang/String;JZ)J"),
     reinterpret_cast<void*>(dynamo_manifest_catalog_create)},

    {const_cast<char*>("open"), const_cast<char*>("(J)J"), reinterpret_cast<void*>(open_db)},
    {const_cast<char*>("openReadOnly"), const_cast<char*>("(J)J"),
     reinterpret_cast<void*>(open_read_only)},
    {const_cast<char*>("refresh"), const_cast<char*>("(J)V"), reinterpret_cast<void*>(refresh)},
    {const_cast<char*>("openWithResult"), const_cast<char*>("(J[J[J[Z)[Ljava/lang/String;"),
     reinterpret_cast<void*>(open_with_result)},
    {const_cast<char*>("close"), const_cast<char*>("(J)J"), reinterpret_cast<void*>(close_db)},

    {const_cast<char*>("get"), const_cast<char*>("(J[BI[J)Ljava/nio/ByteBuffer;"),
     reinterpret_cast<void*>(get)},
    {const_cast<char*>("getDirect"),
     const_cast<char*>("(JLjava/nio/ByteBuffer;I[J)Ljava/nio/ByteBuffer;"),
     reinterpret_cast<void*>(get_direct)},
    {const_cast<char*>("unpin"), const_cast<char*>("(JJ)V"), reinterpret_cast<void*>(unpin)},
    {const_cast<char*>("getCopy"), const_cast<char*>("(J[BI[B)I"),
     reinterpret_cast<void*>(get_copy)},
    {const_cast<char*>("pinsOutstanding"), const_cast<char*>("(J)J"),
     reinterpret_cast<void*>(pins_outstanding)},

    {const_cast<char*>("put"), const_cast<char*>("(J[BI[BI)V"), reinterpret_cast<void*>(put)},
    {const_cast<char*>("delete"), const_cast<char*>("(J[BI)V"),
     reinterpret_cast<void*>(remove_key)},
    {const_cast<char*>("batchCreate"), const_cast<char*>("()J"),
     reinterpret_cast<void*>(batch_create)},
    {const_cast<char*>("batchDestroy"), const_cast<char*>("(J)V"),
     reinterpret_cast<void*>(batch_destroy)},
    {const_cast<char*>("batchPut"), const_cast<char*>("(J[BI[BI)V"),
     reinterpret_cast<void*>(batch_put)},
    {const_cast<char*>("batchDelete"), const_cast<char*>("(J[BI)V"),
     reinterpret_cast<void*>(batch_delete)},
    {const_cast<char*>("batchSize"), const_cast<char*>("(J)J"),
     reinterpret_cast<void*>(batch_size)},
    {const_cast<char*>("write"), const_cast<char*>("(JJ)V"),
     reinterpret_cast<void*>(write_batch)},
    {const_cast<char*>("flush"), const_cast<char*>("(J)V"), reinterpret_cast<void*>(flush)},
    {const_cast<char*>("compactLevel"), const_cast<char*>("(JI)V"),
     reinterpret_cast<void*>(compact_level)},

    {const_cast<char*>("iterCreate"), const_cast<char*>("(J[BI[BI)J"),
     reinterpret_cast<void*>(iter_create)},
    {const_cast<char*>("optionsConfigureCompaction"), const_cast<char*>("(JDJ)V"),
     reinterpret_cast<void*>(options_configure_compaction)},
    {const_cast<char*>("truncateBelow"), const_cast<char*>("(J[BI)V"),
     reinterpret_cast<void*>(truncate_below)},
    {const_cast<char*>("iterCreateReverse"), const_cast<char*>("(J[BI[BI)J"),
     reinterpret_cast<void*>(iter_create_reverse)},
    {const_cast<char*>("iterPrefixReverse"), const_cast<char*>("(J[BI)J"),
     reinterpret_cast<void*>(iter_prefix_reverse)},
    {const_cast<char*>("iterPrefix"), const_cast<char*>("(J[BI)J"),
     reinterpret_cast<void*>(iter_prefix)},
    {const_cast<char*>("iterNext"), const_cast<char*>("(J)Z"),
     reinterpret_cast<void*>(iter_next)},
    {const_cast<char*>("iterKey"), const_cast<char*>("(J)Ljava/nio/ByteBuffer;"),
     reinterpret_cast<void*>(iter_key)},
    {const_cast<char*>("iterValue"), const_cast<char*>("(J)Ljava/nio/ByteBuffer;"),
     reinterpret_cast<void*>(iter_value)},
    {const_cast<char*>("iterNextBatch"), const_cast<char*>("(JLjava/nio/ByteBuffer;I)I"),
     reinterpret_cast<void*>(iter_next_batch)},
    {const_cast<char*>("iterKeyInto"), const_cast<char*>("(J[B)I"),
     reinterpret_cast<void*>(iter_key_into)},
    {const_cast<char*>("iterValueInto"), const_cast<char*>("(J[B)I"),
     reinterpret_cast<void*>(iter_value_into)},
    {const_cast<char*>("iterStatus"), const_cast<char*>("(J)V"),
     reinterpret_cast<void*>(iter_status)},
    {const_cast<char*>("iterDestroy"), const_cast<char*>("(J)V"),
     reinterpret_cast<void*>(iter_destroy)},

    {const_cast<char*>("statsSnapshot"), const_cast<char*>("(J[B)I"),
     reinterpret_cast<void*>(stats_snapshot)},
    {const_cast<char*>("markRecoveryComplete"), const_cast<char*>("(J)V"),
     reinterpret_cast<void*>(mark_recovery_complete)},
    {const_cast<char*>("setWatermark"), const_cast<char*>("(JJ)V"),
     reinterpret_cast<void*>(set_watermark)},
    {const_cast<char*>("watermark"), const_cast<char*>("(J)J"),
     reinterpret_cast<void*>(watermark)},
};

jclass global_class(JNIEnv* env, const char* name) {
    jclass local = env->FindClass(name);
    if (local == nullptr) return nullptr;
    jclass global = static_cast<jclass>(env->NewGlobalRef(local));
    env->DeleteLocalRef(local);
    return global;
}

}  // namespace

extern "C" JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void*) {
    JNIEnv* env = nullptr;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        return JNI_ERR;
    }

    g_runtime_exception = global_class(env, "java/lang/RuntimeException");
    g_string_class = global_class(env, "java/lang/String");
    g_exception_class = global_class(env, "io/veridia/elysiumkv/ElysiumKVException");
    if (g_runtime_exception == nullptr || g_string_class == nullptr ||
        g_exception_class == nullptr) {
        return JNI_ERR;
    }
    g_exception_of = env->GetStaticMethodID(g_exception_class, "of",
                                            "(ILjava/lang/String;)Lio/veridia/elysiumkv/ElysiumKVException;");
    if (g_exception_of == nullptr) return JNI_ERR;

    jclass native_class = env->FindClass("io/veridia/elysiumkv/Native");
    if (native_class == nullptr) return JNI_ERR;
    const jint registered =
        env->RegisterNatives(native_class, kMethods,
                             static_cast<jint>(sizeof(kMethods) / sizeof(kMethods[0])));
    env->DeleteLocalRef(native_class);
    if (registered != JNI_OK) return JNI_ERR;

    return JNI_VERSION_1_8;
}

extern "C" JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void*) {
    JNIEnv* env = nullptr;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) return;
    if (g_exception_class != nullptr) env->DeleteGlobalRef(g_exception_class);
    if (g_string_class != nullptr) env->DeleteGlobalRef(g_string_class);
    if (g_runtime_exception != nullptr) env->DeleteGlobalRef(g_runtime_exception);
    g_exception_class = nullptr;
    g_string_class = nullptr;
    g_runtime_exception = nullptr;
    g_exception_of = nullptr;
}
