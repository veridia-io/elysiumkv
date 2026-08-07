/**
 * A Kafka Streams state store backed by ElysiumKV, built against the <b>Kafka 3.x</b>
 * {@code StateStore} interface.
 *
 * <p><b>Why the package is versioned.</b> The 4.x adapter is a separate artifact rather than a later
 * release of this one: {@code StateStore} gained {@code managesOffsets}, {@code commit} and
 * {@code committedOffset} in KIP-1035, which do not exist on 3.x and cannot be compiled against it.
 * Sharing a package across two jars would be a split package — rejected outright under JPMS — and
 * would give two different stores the same fully-qualified names. An application has exactly one
 * Kafka on its classpath, so the two never need to coexist; naming them apart is what keeps that
 * true by construction rather than by luck.
 *
 * <p>Migrating to the 4.x adapter is therefore an import change as well as a dependency change, and
 * that is the honest signal: the store's <em>semantics</em> change there too. On 4.x the store owns
 * its changelog offsets, which is what makes the hybrid tier configuration — transient local over
 * durable object storage — safe to run at all.
 *
 * <h2>What this adapter does and does not change</h2>
 *
 * <p>Streams' fault tolerance is exactly what it was: the changelog is still the source of truth,
 * restore still replays it, and the checkpoint file still says where replay resumes. The only thing
 * that changes is where the bytes live — which is the point, because that is what lets state exceed
 * local disk.
 *
 * @see io.veridia.elysiumkv.streams.v3.ElysiumKVKeyValueBytesStoreSupplier
 * @see io.veridia.elysiumkv.streams.v3.StorageMode
 */
package io.veridia.elysiumkv.streams.v3;
