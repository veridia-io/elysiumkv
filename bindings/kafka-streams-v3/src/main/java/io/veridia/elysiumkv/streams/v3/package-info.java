/**
 * A Kafka Streams state store backed by ElysiumKV, built against the Kafka 3.x
 * {@code StateStore} interface.
 *
 * <p>The package name carries the Kafka major version because the 4.x adapter is a separate
 * artifact: {@code StateStore} gained {@code managesOffsets}, {@code commit} and {@code
 * committedOffset} in KIP-1035, which cannot be compiled against 3.x. Sharing a package across the
 * two jars would be a split package, rejected under JPMS.
 *
 * <p>Migrating to the 4.x adapter is an import change as well as a dependency change, and the
 * semantics differ: there the store owns its changelog offsets, which is what makes the hybrid tier
 * configuration — transient local over durable object storage — safe to run.
 *
 * <p>Fault tolerance is unchanged. The changelog is still the source of truth, restore still
 * replays it, and the checkpoint file still says where replay resumes; only where the bytes live
 * changes.
 *
 * @see io.veridia.elysiumkv.streams.v3.ElysiumKVKeyValueBytesStoreSupplier
 * @see io.veridia.elysiumkv.streams.v3.StorageMode
 */
package io.veridia.elysiumkv.streams.v3;
