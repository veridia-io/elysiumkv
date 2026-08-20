package io.veridia.elysiumkv.partitioned.kafka;

import io.veridia.elysiumkv.partitioned.AbortableNotCommitted;
import io.veridia.elysiumkv.partitioned.OutcomeUnknown;
import io.veridia.elysiumkv.partitioned.PendingPosition;
import io.veridia.elysiumkv.partitioned.ProducerDead;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The only place in this design that touches Kafka's exception taxonomy — a worked example, not
 * shipped API.
 *
 * <p>In test sources on purpose: {@code io.veridia.elysiumkv.partitioned} takes no Kafka
 * dependency, reaching a log through {@code Changelog}, {@code Restore} and {@code CommitAction}.
 * Copy it. Living here is also what gets it compiled and exercised.
 *
 * <p>It reduces every Kafka failure to what the caller is then allowed to do, which is a different
 * question from what happened. Classifying a fencing error as "definitely not committed" would be
 * true and still wrong: the caller would call {@code abortTransaction()} on a producer for which
 * closing is the only remaining option, and the abort would throw the same error back.
 *
 * <pre>
 * AbortableNotCommitted   discard, then abortTransaction()
 * OutcomeUnknown          discard, then close(); aborting is not permitted
 * ProducerDead            discard, then close(); aborting would throw
 * </pre>
 *
 * <p>The send goes through here too, not just the commit. {@code Producer.send} can throw
 * synchronously — a serialization failure, a full buffer, an interrupt — and a send left in the
 * application's changelog callback throws something none of those three types cover, escaping every
 * catch in the processing loop and leaving the transaction open with batches staged.
 *
 * <p>Wiring it up is two lambdas, which is why there is no adapter class for them:
 *
 * <pre>{@code
 * KafkaTransaction tx = new KafkaTransaction(producer);
 * MutationCodec codec = new PrefixedMutationCodec();
 *
 * PartitionedStore<Bytes> store = PartitionedStore.<Bytes>builder()
 *         .options(config::optionsFor)
 *         .keyBytes(Bytes::get)
 *         .changelog((partition, key, mutation) ->
 *                 tx.send(STATE_TOPIC, partition, key.get(), codec.encode(mutation)))
 *         .restore(myReplay)
 *         .build();
 *
 * tx.begin();
 * // ... getCommittedBatch, fold, stage ...
 * store.commit(() -> tx.commit(polled.nextOffsets(), consumer.groupMetadata()));
 * }</pre>
 */
final class KafkaTransaction {

    private final Producer<byte[], byte[]> producer;

    KafkaTransaction(Producer<byte[], byte[]> producer) {
        this.producer = Objects.requireNonNull(producer, "producer");
    }

    void begin() {
        try {
            producer.beginTransaction();
        } catch (ProducerFencedException | InvalidProducerEpochException | OutOfOrderSequenceException
                | UnsupportedVersionException | AuthorizationException fatal) {
            throw new ProducerDead(fatal);
        } catch (KafkaException | IllegalStateException other) {
            throw new AbortableNotCommitted(other);
        }
    }

    /**
     * Enqueues a state record into the open transaction and remembers where it landed.
     *
     * <p>The offset is captured in the send callback rather than by blocking on the future. Kafka
     * guarantees every record callback in a transaction has run by the time {@code commitTransaction}
     * returns, so the position is a field read afterwards — no blocking, no interruption, and no
     * fifth "committed but the offset could not be resolved" outcome to classify.
     */
    PendingPosition send(String topic, int partition, byte[] key, byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "a changelog value may not be null; encode deletes as values (MutationCodec)");
        }
        // Atomic rather than a bare field: the callback runs on the producer's I/O thread and the
        // read happens on this one. -1 also distinguishes "never acknowledged" from offset 0.
        AtomicLong landed = new AtomicLong(-1L);
        try {
            producer.send(new ProducerRecord<>(topic, partition, key, value), (metadata, error) -> {
                if (error == null) {
                    landed.set(metadata.offset());
                }
            });
        } catch (ProducerFencedException | InvalidProducerEpochException | OutOfOrderSequenceException
                | UnsupportedVersionException | AuthorizationException fatal) {
            throw new ProducerDead(fatal);
        } catch (KafkaException | IllegalStateException other) {
            // Nothing is committed yet, so aborting is both legal and correct.
            throw new AbortableNotCommitted(other);
        }
        return () -> {
            long offset = landed.get();
            if (offset < 0) {
                throw new IllegalStateException(
                        "the changelog position was read before the commit acknowledged it");
            }
            return offset;
        };
    }

    /**
     * Everything Kafka must do to commit, as one call — which is why it is what
     * {@code PartitionedStore.commit} is handed.
     *
     * <p>The two phases are classified separately and that is not tidiness. A timeout from
     * {@code sendOffsetsToTransaction} happens before any commit was requested, so it is abortable;
     * the same exception from {@code commitTransaction} may mean the commit reached the broker, and
     * Kafka forbids switching to an abort while that is outstanding. Mapping both through one
     * taxonomy throws away a recoverable producer on every phase-one timeout.
     */
    void commit(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata group) {
        // Phase 1. These offsets only join the transaction here; nothing is committed yet.
        try {
            producer.sendOffsetsToTransaction(offsets, group);
        } catch (ProducerFencedException | InvalidProducerEpochException | OutOfOrderSequenceException
                | UnsupportedVersionException | AuthorizationException fatal) {
            throw new ProducerDead(fatal);
        } catch (KafkaException | IllegalStateException other) {
            // CommitFailedException, TimeoutException, a consumer kicked out of the group.
            throw new AbortableNotCommitted(other);
        }

        // Phase 2. The only call in this design whose outcome cannot be established.
        try {
            producer.commitTransaction();
        } catch (TimeoutException | InterruptException indeterminate) {
            throw new OutcomeUnknown(indeterminate);
        } catch (ProducerFencedException | InvalidProducerEpochException | OutOfOrderSequenceException
                | UnsupportedVersionException | AuthorizationException fatal) {
            throw new ProducerDead(fatal);
        } catch (KafkaException | IllegalStateException other) {
            throw new AbortableNotCommitted(other);
        }
    }

    /**
     * Aborts, for the {@link AbortableNotCommitted} path only.
     *
     * <p>{@code abortTransaction} throws the fatal taxonomy itself, so even here the result can be
     * that the producer must simply be closed.
     */
    void abort() {
        try {
            producer.abortTransaction();
        } catch (ProducerFencedException | InvalidProducerEpochException | OutOfOrderSequenceException
                | UnsupportedVersionException | AuthorizationException fatal) {
            throw new ProducerDead(fatal);
        } catch (TimeoutException | InterruptException indeterminate) {
            throw new OutcomeUnknown(indeterminate);
        }
    }
}
