package io.veridia.elysiumkv.partitioned.kafka;

import io.veridia.elysiumkv.partitioned.AbortableNotCommitted;
import io.veridia.elysiumkv.partitioned.Mutation;
import io.veridia.elysiumkv.partitioned.OutcomeUnknown;
import io.veridia.elysiumkv.partitioned.PendingPosition;
import io.veridia.elysiumkv.partitioned.ProducerDead;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The exception mapping, against the real client rather than against a reading of its javadoc.
 *
 * <p>That distinction is the reason this file exists. An earlier draft of this mapper lived only in
 * a design document, where it declared a functional interface whose obvious lambda could not have
 * implemented it — {@code Future.get()} throws a checked {@code ExecutionException}. It read
 * perfectly. Nothing compiled it.
 */
class KafkaTransactionTest {

    private static final String TOPIC = "state";
    private static final Map<TopicPartition, OffsetAndMetadata> OFFSETS =
            Collections.singletonMap(new TopicPartition("input", 0), new OffsetAndMetadata(7L));

    private MockProducer<byte[], byte[]> producer;
    private KafkaTransaction transaction;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        producer.initTransactions();
        transaction = new KafkaTransaction(producer);
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private ConsumerGroupMetadata group() {
        return new ConsumerGroupMetadata("processor");
    }

    // --- the position contract -----------------------------------------------

    @Test
    @DisplayName("a successful commit makes every pending position available without blocking")
    void everyPositionIsResolvedByTheTimeCommitReturns() {
        transaction.begin();
        PendingPosition first = transaction.send(TOPIC, 0, bytes("a"), bytes("1"));
        PendingPosition second = transaction.send(TOPIC, 0, bytes("b"), bytes("2"));
        PendingPosition other = transaction.send(TOPIC, 1, bytes("c"), bytes("3"));

        transaction.commit(OFFSETS, group());

        // No get(), no timeout, no InterruptedException: the contract is a field read. What is being
        // asserted is that each position resolved to the offset its own send was acknowledged at —
        // not what those numbers are. MockProducer counts from one global sequence rather than per
        // partition, so pinning the values would assert the mock's behaviour and not this class's.
        assertEquals(0L, first.position());
        assertEquals(1L, second.position());
        assertEquals(2L, other.position());
        assertTrue(producer.transactionCommitted());
    }

    @Test
    @DisplayName("a position read before the commit is a programming error, not a wait")
    void readingAPositionEarlyThrows() {
        MockProducer<byte[], byte[]> manual =
                new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        manual.initTransactions();
        KafkaTransaction pending = new KafkaTransaction(manual);
        pending.begin();
        PendingPosition position = pending.send(TOPIC, 0, bytes("a"), bytes("1"));

        IllegalStateException failure = assertThrows(IllegalStateException.class, position::position);
        assertTrue(failure.getMessage().contains("before the commit"));
        manual.close();
    }

    @Test
    @DisplayName("a null changelog value is refused: a tombstone cannot be restored incrementally")
    void aNullValueIsRefused() {
        transaction.begin();
        assertThrows(IllegalArgumentException.class,
                () -> transaction.send(TOPIC, 0, bytes("a"), null));
    }

    // --- phase one: sendOffsetsToTransaction ---------------------------------

    @Test
    @DisplayName("a phase-one timeout is abortable, because nothing was committed yet")
    void aTimeoutSendingOffsetsIsAbortable() {
        transaction.begin();
        producer.sendOffsetsToTransactionException = new TimeoutException("injected");

        AbortableNotCommitted failure = assertThrows(AbortableNotCommitted.class,
                () -> transaction.commit(OFFSETS, group()));
        assertTrue(failure.getCause() instanceof TimeoutException);
        assertFalse(producer.transactionCommitted());

        // And the abort it licenses is actually legal.
        transaction.abort();
        assertTrue(producer.transactionAborted());
    }

    @Test
    @DisplayName("a fatal error sending offsets means close, not abort")
    void aFatalErrorSendingOffsetsIsProducerDead() {
        transaction.begin();
        producer.sendOffsetsToTransactionException = new InvalidProducerEpochException("injected");
        assertThrows(ProducerDead.class, () -> transaction.commit(OFFSETS, group()));
    }

    // --- phase two: commitTransaction ----------------------------------------

    @Test
    @DisplayName("the same timeout from phase two is NOT abortable")
    void aTimeoutCommittingIsUnknown() {
        transaction.begin();
        producer.commitTransactionException = new TimeoutException("injected");

        OutcomeUnknown failure = assertThrows(OutcomeUnknown.class,
                () -> transaction.commit(OFFSETS, group()));
        assertTrue(failure.getCause() instanceof TimeoutException);
    }

    @Test
    @DisplayName("an interrupt while committing is unknown too")
    void anInterruptCommittingIsUnknown() {
        transaction.begin();
        producer.commitTransactionException = new InterruptException("injected");
        assertThrows(OutcomeUnknown.class, () -> transaction.commit(OFFSETS, group()));
    }

    /**
     * The pair that makes the phase split worth having. Same exception type, same call site in the
     * caller's loop, opposite legal actions — a single mapper that ignored the phase would classify
     * one of these wrongly, and no test that looked at only one phase would notice.
     */
    @Test
    @DisplayName("one exception type, two phases, two different outcomes")
    void thePhaseDecidesAndNotTheType() {
        transaction.begin();
        producer.sendOffsetsToTransactionException = new TimeoutException("phase one");
        assertThrows(AbortableNotCommitted.class, () -> transaction.commit(OFFSETS, group()));

        setUp();
        transaction.begin();
        producer.commitTransactionException = new TimeoutException("phase two");
        assertThrows(OutcomeUnknown.class, () -> transaction.commit(OFFSETS, group()));
    }

    @Test
    @DisplayName("every fatal producer error maps to close-only, in both phases")
    void theFatalTaxonomyIsProducerDead() {
        RuntimeException[] fatal = {
            new ProducerFencedException("injected"),
            new InvalidProducerEpochException("injected"),
            new OutOfOrderSequenceException("injected"),
            new UnsupportedVersionException("injected"),
            new AuthorizationException("injected"),
        };
        for (RuntimeException error : fatal) {
            setUp();
            transaction.begin();
            producer.commitTransactionException = error;
            assertThrows(ProducerDead.class, () -> transaction.commit(OFFSETS, group()),
                    error.getClass().getSimpleName() + " must not license an abort");
        }
    }

    /**
     * {@code InvalidProducerEpochException} is the one that shows the rule earning its keep: it is
     * not obviously fatal, and {@code abortTransaction} can itself throw it — so a caller told
     * "abortable" would take an action that fails.
     */
    @Test
    @DisplayName("an abort that throws a fatal error reports close-only rather than propagating Kafka's")
    void anAbortThatThrowsIsReclassified() {
        transaction.begin();
        producer.abortTransactionException = new InvalidProducerEpochException("injected");
        assertThrows(ProducerDead.class, () -> transaction.abort());
    }

    // --- the send ------------------------------------------------------------

    @Test
    @DisplayName("a synchronous send failure is classified rather than escaping the loop")
    void aSendFailureIsClassified() {
        transaction.begin();
        producer.sendException = new TimeoutException("injected");
        AbortableNotCommitted failure = assertThrows(AbortableNotCommitted.class,
                () -> transaction.send(TOPIC, 0, bytes("a"), bytes("1")));
        assertNotNull(failure.getCause());
    }

    @Test
    @DisplayName("a fatal send failure means close, not abort")
    void aFatalSendFailureIsProducerDead() {
        transaction.begin();
        producer.sendException = new ProducerFencedException("injected");
        assertThrows(ProducerDead.class, () -> transaction.send(TOPIC, 0, bytes("a"), bytes("1")));
    }

    // --- the codec -----------------------------------------------------------

    @Test
    @DisplayName("a delete encodes to a non-null value and decodes back to a delete")
    void deletesTravelAsValues() {
        PrefixedMutationCodec codec = new PrefixedMutationCodec();

        byte[] encoded = codec.encode(Mutation.delete());
        assertNotNull(encoded, "a tombstone would be dropped by compaction before a lagging replica saw it");
        assertTrue(codec.decode(encoded).isDelete());

        byte[] value = bytes("payload");
        byte[] put = codec.encode(Mutation.put(value));
        assertNotNull(put);
        assertFalse(codec.decode(put).isDelete());
        assertArrayEquals(value, codec.decode(put).value());

        byte[] empty = codec.encode(Mutation.put(new byte[0]));
        assertArrayEquals(new byte[0], codec.decode(empty).value(), "an empty value is a put, not a delete");
    }

    @Test
    @DisplayName("decoding a real tombstone fails fast")
    void aTombstoneFailsToDecode() {
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new PrefixedMutationCodec().decode(null));
        assertTrue(failure.getMessage().contains("compaction"),
                "the message should say why an incremental restore cannot be trusted here");
    }
}
