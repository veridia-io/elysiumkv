package io.veridia.elysiumkv.partitioned.kafka;

import static io.veridia.elysiumkv.partitioned.kafka.KafkaStoreFixture.POLL;
import static io.veridia.elysiumkv.partitioned.kafka.KafkaStoreFixture.bytes;
import static io.veridia.elysiumkv.partitioned.kafka.KafkaStoreFixture.string;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.veridia.elysiumkv.partitioned.Mutation;
import io.veridia.elysiumkv.partitioned.OutcomeUnknown;
import io.veridia.elysiumkv.partitioned.PartitionNotAssignedException;
import io.veridia.elysiumkv.partitioned.PartitionedStore;
import io.veridia.elysiumkv.partitioned.ProducerDead;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The lifecycle around a real exactly-once loop: input offsets inside the transaction, a crash
 * between the commit and the apply, a partition changing hands, and a commit whose outcome cannot
 * be established.
 *
 * <p>These are the cases {@code InMemoryLog} cannot pose. The protocol logic is already covered
 * there — {@code anUnknownOutcomeMarksThePartitionsBehind} and friends — but a fake log cannot fence
 * a producer, cannot make a commit indeterminate, and has no consumer group whose offsets are part
 * of the same transaction as the changelog records.
 */
class PartitionedStoreKafkaLifecycleTest {

    private static final int PARTITION = 0;

    private KafkaStoreFixture fixture;

    @TempDir Path root;
    @TempDir Path newOwnerRoot;

    @BeforeEach
    void setUp() {
        fixture = new KafkaStoreFixture(1);
    }

    @AfterEach
    void tearDown() {
        // setUp() aborts on an assumption when there is no Docker, or no AWS in the native. JUnit
        // still runs this, so without the guard a clean skip is reported as an error.
        if (fixture != null) {
            fixture.close();
        }
    }

    // --- the loop -------------------------------------------------------------------------

    /**
     * One turn of a real exactly-once processing loop: read input, fold it into state, and commit
     * the changelog records <em>and</em> the input offsets as one transaction.
     *
     * <p>This is the shape the design is for, and the reason it is written out rather than reduced
     * to a helper call: everything the transaction must do lives inside the {@code commit} action,
     * so a failure anywhere in it is classified rather than escaping unmapped.
     */
    private int processOneBatch(PartitionedStore<String> store, KafkaTransaction tx,
                                Consumer<byte[], byte[]> input) {
        TopicPartition topicPartition = new TopicPartition(fixture.inputTopic, PARTITION);
        ConsumerRecords<byte[], byte[]> polled = input.poll(POLL);
        if (polled.isEmpty()) {
            return 0;
        }

        tx.begin();
        Map<String, Mutation> mutations = new LinkedHashMap<>();
        long lastOffset = -1;
        for (ConsumerRecord<byte[], byte[]> record : polled.records(topicPartition)) {
            String key = string(record.key());
            mutations.put(key, Mutation.put(bytes("state-" + key)));
            lastOffset = record.offset();
        }
        store.put(PARTITION, mutations);

        Map<TopicPartition, OffsetAndMetadata> offsets =
                Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset + 1));
        store.commit(() -> tx.commit(offsets, input.groupMetadata()));
        return mutations.size();
    }

    private Consumer<byte[], byte[]> assignedInput(String group) {
        Consumer<byte[], byte[]> consumer = fixture.own(fixture.consumer(group));
        consumer.assign(Collections.singletonList(new TopicPartition(fixture.inputTopic, PARTITION)));
        consumer.seekToBeginning(
                Collections.singletonList(new TopicPartition(fixture.inputTopic, PARTITION)));
        return consumer;
    }

    private static byte[] read(PartitionedStore<String> store, String key) {
        return store.get(PARTITION, key);
    }

    // --- 3. the input offsets are part of the transaction ---------------------------------

    /**
     * The half an empty offset map never reaches: after a committed batch, a <em>new</em> consumer
     * in the same group must resume after the processed records rather than reprocessing them.
     * That is only true if the offsets were committed by the transaction.
     */
    @Test
    void aCommittedBatchAdvancesTheInputGroupOffsetTransactionally() {
        fixture.publishInput(PARTITION, "a", "b", "c");
        KafkaTransaction tx = new KafkaTransaction(fixture.producer("tx-" + UUID.randomUUID()));
        PartitionedStore<String> store =
                fixture.storeOver(root, tx, fixture.restoreFromTheChangelog());
        store.assign(Collections.singletonList(PARTITION));

        assertEquals(3, processOneBatch(store, tx, assignedInput(fixture.groupId)));
        assertArrayEquals(bytes("state-a"), read(store, "a"));

        // A fresh consumer in the same group, starting from the committed position.
        try (Consumer<byte[], byte[]> resumed = fixture.consumer(fixture.groupId)) {
            TopicPartition tp = new TopicPartition(fixture.inputTopic, PARTITION);
            resumed.assign(Collections.singletonList(tp));
            OffsetAndMetadata position = resumed.committed(Collections.singleton(tp)).get(tp);
            assertNotNull(position, "the transaction did not commit any input offset");
            assertEquals(3, position.offset(),
                    "the group offset must cover exactly the records the batch consumed");
        }
    }

    @Test
    void anAbortedBatchLeavesTheInputOffsetWhereItWas() {
        fixture.publishInput(PARTITION, "a", "b");
        Producer<byte[], byte[]> producer = fixture.producer("tx-" + UUID.randomUUID());
        KafkaTransaction tx = new KafkaTransaction(producer);
        PartitionedStore<String> store =
                fixture.storeOver(root, tx, fixture.restoreFromTheChangelog());
        store.assign(Collections.singletonList(PARTITION));

        Consumer<byte[], byte[]> input = assignedInput(fixture.groupId);
        tx.begin();
        store.put(PARTITION, Collections.singletonMap("a", Mutation.put(bytes("state-a"))));
        producer.abortTransaction();
        store.discard();

        try (Consumer<byte[], byte[]> resumed = fixture.consumer(fixture.groupId)) {
            TopicPartition tp = new TopicPartition(fixture.inputTopic, PARTITION);
            resumed.assign(Collections.singletonList(tp));
            assertNull(resumed.committed(Collections.singleton(tp)).get(tp),
                    "an aborted transaction must not advance the input offset");
        }
        assertNull(read(store, "a"));
        assertTrue(input.assignment().contains(new TopicPartition(fixture.inputTopic, PARTITION)));
    }

    // --- 1. a crash between the commit and the apply ---------------------------------------

    /**
     * The dangerous window. The log committed and the process died before the store applied
     * it, so the store is behind the log and nothing in the store records that. On restart the
     * replay must close the gap — get this wrong and the store serves state that is missing a
     * committed update, silently and permanently.
     *
     * <p>Simulated by committing the transaction directly rather than through
     * {@code store.commit}, which is exactly the state a kill between those two calls leaves.
     */
    @Test
    void aCommitTheStoreNeverAppliedIsRecoveredByTheReplay() {
        Producer<byte[], byte[]> producer = fixture.producer("tx-" + UUID.randomUUID());
        KafkaTransaction tx = new KafkaTransaction(producer);
        PartitionedStore<String> store =
                fixture.storeOver(root, tx, fixture.restoreFromTheChangelog());
        store.assign(Collections.singletonList(PARTITION));

        // One batch that completes normally, so there is a watermark to resume from.
        tx.begin();
        store.put(PARTITION, Collections.singletonMap("first", Mutation.put(bytes("one"))));
        try (Consumer<byte[], byte[]> group = fixture.consumer(fixture.groupId)) {
            store.commit(() -> tx.commit(Collections.emptyMap(), group.groupMetadata()));
        }

        // The second batch reaches the log and the process dies before applyCommitted().
        tx.begin();
        store.put(PARTITION, Collections.singletonMap("second", Mutation.put(bytes("two"))));
        producer.commitTransaction();
        // The premise, and stronger than reading the key: begin() is what notices an unresolved
        // staged set, so a partition it marks behind proves the apply never ran.
        store.begin();
        assertEquals(Collections.singleton(PARTITION), store.behind(),
                "the apply never ran, by construction");
        store.close();

        KafkaTransaction after = new KafkaTransaction(fixture.producer("tx-" + UUID.randomUUID()));
        PartitionedStore<String> restarted =
                fixture.storeOver(root, after, fixture.restoreFromTheChangelog());
        restarted.assign(Collections.singletonList(PARTITION));

        assertArrayEquals(bytes("two"), read(restarted, "second"),
                "a commit the store never applied must be recovered by the replay");
        assertArrayEquals(bytes("one"), read(restarted, "first"));
    }

    // --- 2. a partition changing hands -----------------------------------------------------

    /**
     * The rebalance: one instance gives a partition up, another takes it. The transactional id is
     * per partition — as Kafka Streams does it — so the new owner claiming it fences the old one,
     * and the old instance's next commit must arrive as {@link ProducerDead} rather than as a
     * write that lands behind the new owner's back.
     */
    @Test
    void aPartitionHandedOverIsRebuiltByTheNewOwnerAndFencesTheOld() {
        String sharedId = "tx-partition-" + PARTITION + "-" + UUID.randomUUID();
        Producer<byte[], byte[]> oldProducer = fixture.producer(sharedId);
        KafkaTransaction first = new KafkaTransaction(oldProducer);
        PartitionedStore<String> outgoing =
                fixture.storeOver(root, first, fixture.restoreFromTheChangelog());
        outgoing.assign(Collections.singletonList(PARTITION));

        for (int i = 0; i < 3; i++) {
            first.begin();
            outgoing.put(PARTITION,
                    Collections.singletonMap("key" + i, Mutation.put(bytes("value" + i))));
            try (Consumer<byte[], byte[]> group = fixture.consumer(fixture.groupId)) {
                outgoing.commit(() -> first.commit(Collections.emptyMap(), group.groupMetadata()));
            }
        }

        // Handover: flush and give it up.
        outgoing.revoke(Collections.singletonList(PARTITION));
        assertTrue(outgoing.assignment().isEmpty());
        assertThrows(PartitionNotAssignedException.class, () -> outgoing.put(PARTITION,
                Collections.singletonMap("late", Mutation.put(bytes("nope")))));

        // The new owner starts cold and rebuilds from the changelog alone.
        KafkaTransaction incomingTx = new KafkaTransaction(fixture.producer(sharedId));
        PartitionedStore<String> incoming =
                fixture.storeOver(newOwnerRoot, incomingTx, fixture.restoreFromTheChangelog());
        incoming.assign(Collections.singletonList(PARTITION));

        for (int i = 0; i < 3; i++) {
            assertArrayEquals(bytes("value" + i), read(incoming, "key" + i),
                    "the new owner did not rebuild key" + i);
        }

        // The old producer is now fenced — but only where it talks to the broker. beginTransaction
        // is local bookkeeping and cannot see the bumped epoch, so the fence surfaces at the
        // commit, which is precisely why the mapper classifies per call rather than per producer.
        first.begin();
        first.send(fixture.changelogTopic, PARTITION, KafkaStoreFixture.bytes("late"),
                fixture.codec.encode(Mutation.put(KafkaStoreFixture.bytes("nope"))));
        try (Consumer<byte[], byte[]> group = fixture.consumer(fixture.groupId)) {
            assertThrows(ProducerDead.class,
                    () -> first.commit(Collections.emptyMap(), group.groupMetadata()));
        }
    }

    // --- 4. a commit whose outcome cannot be established ------------------------------------

    /**
     * The only path to {@code behind()}, and the one a fake log can only simulate: the broker stops
     * answering while {@code commitTransaction()} is outstanding, so the commit may or may not have
     * reached it. The partition must be marked behind rather than served, and a repair must return
     * it to service.
     */
    @Test
    void aCommitThatTimesOutMarksThePartitionBehindUntilItIsRepaired() {
        Properties impatient = new Properties();
        impatient.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "4000");
        impatient.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000");
        impatient.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "4000");
        impatient.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "10000");

        KafkaTransaction tx =
                new KafkaTransaction(fixture.producer("tx-" + UUID.randomUUID(), impatient));
        PartitionedStore<String> store =
                fixture.storeOver(root, tx, fixture.restoreFromTheChangelog());
        store.assign(Collections.singletonList(PARTITION));

        // A committed batch first, so the replay after the repair has a watermark to resume from.
        tx.begin();
        store.put(PARTITION, Collections.singletonMap("before", Mutation.put(bytes("kept"))));
        try (Consumer<byte[], byte[]> group = fixture.consumer(fixture.groupId)) {
            store.commit(() -> tx.commit(Collections.emptyMap(), group.groupMetadata()));
        }

        tx.begin();
        store.put(PARTITION, Collections.singletonMap("during", Mutation.put(bytes("unknown"))));

        // The metadata is captured while the broker still answers, and it is a local read
        // afterwards. Passing null here instead would throw an unclassified exception, which
        // commit() also routes through discardUnknown() — so the test would pass without the
        // timeout ever happening. It did, once.
        try (Consumer<byte[], byte[]> group = fixture.consumer(fixture.groupId)) {
            ConsumerGroupMetadata metadata = group.groupMetadata();
            KafkaEnvironment.pauseBroker();
            try {
                assertThrows(OutcomeUnknown.class,
                        () -> store.commit(() -> tx.commit(Collections.emptyMap(), metadata)));
            } finally {
                KafkaEnvironment.resumeBroker();
            }
        }

        assertEquals(Collections.singleton(PARTITION), store.behind(),
                "an indeterminate commit must leave the partition behind");
        assertThrows(IllegalStateException.class, () -> read(store, "before"));

        store.repair(Collections.singletonList(PARTITION));
        assertTrue(store.behind().isEmpty(), "a repaired partition returns to service");
        assertArrayEquals(bytes("kept"), read(store, "before"));
    }
}
