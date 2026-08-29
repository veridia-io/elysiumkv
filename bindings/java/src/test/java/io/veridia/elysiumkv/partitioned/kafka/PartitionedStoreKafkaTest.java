package io.veridia.elysiumkv.partitioned.kafka;

import io.veridia.elysiumkv.Compression;
import io.veridia.elysiumkv.DiskBlobStore;
import io.veridia.elysiumkv.DiskManifestCatalog;
import io.veridia.elysiumkv.Durability;
import io.veridia.elysiumkv.ElysiumKVOptions;
import io.veridia.elysiumkv.partitioned.Changelog;
import io.veridia.elysiumkv.partitioned.Mutation;
import io.veridia.elysiumkv.partitioned.PartitionedStore;
import io.veridia.elysiumkv.partitioned.PendingPosition;
import io.veridia.elysiumkv.partitioned.ProducerDead;
import io.veridia.elysiumkv.partitioned.Restore;
import io.veridia.elysiumkv.partitioned.WriteSink;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link PartitionedStore} against a real broker.
 *
 * <p>The differential suite already drives the protocol far harder than this does, against a fake
 * log with seeded streams. What it cannot reach is the half that belongs to Kafka: aborted records
 * are physically present but must stay invisible, the last stable offset is not the log end,
 * offsets come from the broker, and a producer is fenced by another one claiming its
 * {@code transactional.id}. Each test here exists because that difference is where the design
 * either holds or does not.
 *
 * <p>Everything is per-test isolated by a fresh topic and a fresh {@code transactional.id}, so the
 * suite shares one broker without sharing any state.
 */
class PartitionedStoreKafkaTest {

    private static final int PARTITIONS = 2;
    private static final Duration POLL = Duration.ofMillis(500);

    private final List<AutoCloseable> owned = new ArrayList<>();
    private final MutationCodec codec = new PrefixedMutationCodec();

    private String bootstrap;
    private String topic;
    private String groupId;

    @TempDir Path root;

    @BeforeEach
    void startBroker() {
        bootstrap = KafkaEnvironment.requireBootstrap();
        topic = "states-" + UUID.randomUUID();
        groupId = "group-" + UUID.randomUUID();
        KafkaEnvironment.createTopic(topic, PARTITIONS);
    }

    @AfterEach
    void closeOwned() throws Exception {
        // Reverse: a store closes before the blob stores and catalogs it was opened over.
        Collections.reverse(owned);
        for (AutoCloseable closeable : owned) {
            closeable.close();
        }
        owned.clear();
    }

    private <T extends AutoCloseable> T own(T closeable) {
        owned.add(closeable);
        return closeable;
    }

    // --- the store ------------------------------------------------------------------------

    /** A local mirror of PartitionFixture, which is package-private one package up. */
    private ElysiumKVOptions optionsFor(Path base, int partition) {
        try {
            Path dir = base.resolve("partition-" + partition);
            Path store = dir.resolve("store");
            Files.createDirectories(store);
            DiskBlobStore blobs = own(new DiskBlobStore(store.toString(), "store-" + partition));
            DiskManifestCatalog catalog = own(new DiskManifestCatalog(dir.toString()));
            return own(new ElysiumKVOptions()
                    .manifestCatalog(catalog)
                    .memtableBytes(64 * 1024)
                    .blockBytes(1024)
                    .paranoidChecks(true)
                    .addTier(blobs, Durability.DURABLE, 0, 0, 0)
                    .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                    .level(1, Compression.NONE, 0, 0, 0, 0, 0));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private PartitionedStore<String> storeOver(Path base, KafkaTransaction tx, Restore<String> restore) {
        PartitionedStore<String> store = PartitionedStore.<String>builder()
                .options(partition -> optionsFor(base, partition))
                .keyBytes(key -> key.getBytes(StandardCharsets.UTF_8))
                .changelog(new Changelog<String>() {
                    @Override
                    public PendingPosition send(int partition, String key, Mutation mutation) {
                        return tx.send(topic, partition,
                                ChangelogRecords.pointKey(key.getBytes(StandardCharsets.UTF_8)),
                                codec.encode(mutation));
                    }

                    @Override
                    public PendingPosition sendDeleteRange(int partition, byte[] lo, byte[] hi) {
                        return tx.send(topic, partition, ChangelogRecords.rangeKey(lo, hi),
                                ChangelogRecords.RANGE_VALUE);
                    }
                })
                .restore(restore)
                .build();
        return own(store);
    }

    private Restore<String> restoreFromTheLog() {
        return new ExampleStateRestore<>(partition -> consumer(), topic,
                bytes -> new String(bytes, StandardCharsets.UTF_8), codec, POLL, 100);
    }

    // --- Kafka ----------------------------------------------------------------------------

    private Producer<byte[], byte[]> producer(String transactionalId) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        Producer<byte[], byte[]> producer = new KafkaProducer<>(config);
        producer.initTransactions();
        owned.add(producer::close);
        return producer;
    }

    private Consumer<byte[], byte[]> consumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // The whole reason a real broker is worth the seconds it costs.
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new KafkaConsumer<>(config);
    }

    /** Every committed record in a partition, in offset order. */
    private List<String> committedKeys(int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        List<String> keys = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = consumer()) {
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToBeginning(Collections.singletonList(tp));
            long end = consumer.endOffsets(Collections.singletonList(tp)).get(tp);
            while (consumer.position(tp) < end) {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(POLL);
                if (polled.isEmpty()) break;
                for (ConsumerRecord<byte[], byte[]> record : polled.records(tp)) {
                    keys.add(new String(ChangelogRecords.entityKey(record.key()),
                            StandardCharsets.UTF_8));
                }
            }
        }
        return keys;
    }

    private void commitThrough(PartitionedStore<String> store, KafkaTransaction tx) {
        try (Consumer<byte[], byte[]> group = consumer()) {
            store.commit(() -> tx.commit(Collections.emptyMap(), group.groupMetadata()));
        }
    }

    /** Appends to partition 0 without going through the store, so the store falls behind the log. */
    private void appendDirectly(String key, String value) {
        Producer<byte[], byte[]> producer = producer("tx-direct-" + UUID.randomUUID());
        producer.beginTransaction();
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                topic, 0, ChangelogRecords.pointKey(bytes(key)),
                codec.encode(Mutation.put(bytes(value)))));
        producer.commitTransaction();
    }

    private static Map<String, Mutation> put(String key, String value) {
        Map<String, Mutation> batch = new LinkedHashMap<>();
        batch.put(key, Mutation.put(value.getBytes(StandardCharsets.UTF_8)));
        return batch;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    // --- tests ----------------------------------------------------------------------------

    @Test
    void aCommittedTransactionMaterialisesAndTheLogHoldsIt() {
        KafkaTransaction tx = new KafkaTransaction(producer("tx-" + UUID.randomUUID()));
        PartitionedStore<String> store = storeOver(root, tx, restoreFromTheLog());
        store.assign(Arrays.asList(0, 1));

        tx.begin();
        store.put(0, put("alpha", "one"));
        store.put(1, put("beta", "two"));
        commitThrough(store, tx);

        assertArrayEquals(bytes("one"),
                store.get(0, "alpha"));
        assertArrayEquals(bytes("two"),
                store.get(1, "beta"));
        assertEquals(Collections.singletonList("alpha"), committedKeys(0));
        assertEquals(Collections.singletonList("beta"), committedKeys(1));
        assertTrue(store.behind().isEmpty(), "a committed transaction leaves nothing behind");
    }

    /**
     * The records are <em>in</em> the partition — an abort does not erase them — so this is the
     * case a fake log cannot pose: the store must be empty and a read_committed consumer must see
     * nothing, while the bytes are physically there.
     */
    @Test
    void anAbortedTransactionIsInvisibleToTheStoreAndToAReadCommittedConsumer() {
        Producer<byte[], byte[]> producer = producer("tx-" + UUID.randomUUID());
        KafkaTransaction tx = new KafkaTransaction(producer);
        PartitionedStore<String> store = storeOver(root, tx, restoreFromTheLog());
        store.assign(Collections.singletonList(0));

        tx.begin();
        store.put(0, put("alpha", "one"));
        producer.abortTransaction();
        store.discard();

        assertNull(store.get(0, "alpha"),
                "an aborted transaction must not materialise");
        assertEquals(Collections.emptyList(), committedKeys(0),
                "read_committed must not surface an aborted record");
        assertTrue(store.behind().isEmpty(), "an abort is definite, so nothing is behind");
    }

    /**
     * The range record's whole reason for existing, end to end: it has to survive a real topic and be
     * replayed into a store that has never seen the band. A record keyed so that a later write
     * supersedes it would pass every local test and fail exactly here.
     */
    @Test
    void aRangeDeleteTravelsTheTopicAndReplaysIntoAFreshStore(@TempDir Path second) {
        Producer<byte[], byte[]> producer = producer("tx-" + UUID.randomUUID());
        KafkaTransaction tx = new KafkaTransaction(producer);
        PartitionedStore<String> store = storeOver(root, tx, restoreFromTheLog());
        store.assign(Collections.singletonList(0));

        tx.begin();
        store.put(0, put("tenant-a:1", "keep"));
        store.put(0, put("tenant-b:1", "evict"));
        store.put(0, put("tenant-b:2", "evict"));
        store.put(0, put("tenant-c:1", "keep"));
        commitThrough(store, tx);

        tx.begin();
        store.deleteRange(0, bytes("tenant-b:"), bytes("tenant-c:"));
        commitThrough(store, tx);

        assertNull(store.get(0, "tenant-b:1"));
        assertNull(store.get(0, "tenant-b:2"));
        assertArrayEquals(bytes("keep"), store.get(0, "tenant-a:1"));
        assertArrayEquals(bytes("keep"), store.get(0, "tenant-c:1"),
                "the upper bound is exclusive, so tenant-c survives");

        // A write into the band after the delete lives, which is what separates a range delete from a
        // truncation point and is the reason this store has no truncation point.
        tx.begin();
        store.put(0, put("tenant-b:3", "re-seeded"));
        commitThrough(store, tx);

        KafkaTransaction other = new KafkaTransaction(producer("tx-" + UUID.randomUUID()));
        PartitionedStore<String> rebuilt = storeOver(second, other, restoreFromTheLog());
        rebuilt.assign(Collections.singletonList(0));

        assertNull(rebuilt.get(0, "tenant-b:1"), "the replayed range delete must cover the band again");
        assertNull(rebuilt.get(0, "tenant-b:2"));
        assertArrayEquals(bytes("re-seeded"), rebuilt.get(0, "tenant-b:3"),
                "a record after the range delete is not covered by it");
        assertArrayEquals(bytes("keep"), rebuilt.get(0, "tenant-a:1"));
        assertArrayEquals(bytes("keep"), rebuilt.get(0, "tenant-c:1"));
    }

    @Test
    void aFreshStoreRebuildsFromTheChangelog(@TempDir Path second) {
        KafkaTransaction tx = new KafkaTransaction(producer("tx-" + UUID.randomUUID()));
        PartitionedStore<String> store = storeOver(root, tx, restoreFromTheLog());
        store.assign(Collections.singletonList(0));

        for (int i = 0; i < 3; i++) {
            tx.begin();
            store.put(0, put("key" + i, "value" + i));
            commitThrough(store, tx);
        }
        store.close();

        // A different directory entirely: nothing carries over but the log.
        KafkaTransaction other = new KafkaTransaction(producer("tx-" + UUID.randomUUID()));
        PartitionedStore<String> rebuilt = storeOver(second, other, restoreFromTheLog());
        rebuilt.assign(Collections.singletonList(0));

        for (int i = 0; i < 3; i++) {
            assertArrayEquals(bytes("value" + i), rebuilt.get(0, "key" + i),
                    "key" + i + " did not survive the rebuild");
        }
    }

    /**
     * A store that already materialised through some offset must resume after it, not replay the
     * partition from the beginning — which is the entire point of the watermark, and the thing
     * that makes a restore proportional to the gap rather than to the topic.
     */
    @Test
    void restoreResumesAfterTheWatermarkRatherThanFromTheBeginning() {
        KafkaTransaction tx = new KafkaTransaction(producer("tx-" + UUID.randomUUID()));
        PartitionedStore<String> store = storeOver(root, tx, restoreFromTheLog());
        store.assign(Collections.singletonList(0));

        for (int i = 0; i < 4; i++) {
            tx.begin();
            store.put(0, put("key" + i, "value" + i));
            commitThrough(store, tx);
        }

        // Closing flushes, which is what makes the watermark durable — there is no WAL, so an
        // unflushed watermark is legitimately lost and the replay would correctly start over.
        store.close();

        // Two records the store knows nothing about, appended behind its back. They are the gap
        // the watermark exists to bound.
        appendDirectly("key4", "value4");
        appendDirectly("key5", "value5");

        AtomicInteger delivered = new AtomicInteger();
        Restore<String> counting = (partition, materializedThrough, sink) -> {
            WriteSink<String> counted = new WriteSink<String>() {
                @Override
                public void putBatch(long through, Map<String, Mutation> mutations) {
                    delivered.addAndGet(mutations.size());
                    sink.putBatch(through, mutations);
                }

                @Override
                public void deleteRange(long through, byte[] lo, byte[] hi) {
                    sink.deleteRange(through, lo, hi);
                }
            };
            restoreFromTheLog().restore(partition, materializedThrough, counted);
        };

        KafkaTransaction other = new KafkaTransaction(producer("tx-" + UUID.randomUUID()));
        PartitionedStore<String> reopened = storeOver(root, other, counting);
        reopened.assign(Collections.singletonList(0));
        reopened.repair(Collections.singletonList(0));

        assertEquals(2, delivered.get(),
                "the replay should cover the gap after the watermark, not the whole partition");
        assertArrayEquals(bytes("value5"),
                reopened.get(0, "key5"));
        assertArrayEquals(bytes("value0"),
                reopened.get(0, "key0"),
                "the records the watermark covered must still be there");
    }

    /**
     * A real fence, by a second producer claiming the same {@code transactional.id} — not a mocked
     * exception. It must arrive as {@link ProducerDead} and, because a fenced transaction
     * definitely did not commit, must leave the partition readable rather than behind.
     */
    @Test
    void aFencedProducerIsProducerDeadAndLeavesThePartitionReadable() {
        String transactionalId = "tx-" + UUID.randomUUID();
        KafkaTransaction first = new KafkaTransaction(producer(transactionalId));
        PartitionedStore<String> store = storeOver(root, first, restoreFromTheLog());
        store.assign(Collections.singletonList(0));

        first.begin();
        store.put(0, put("alpha", "one"));

        // Claiming the id fences the first producer's epoch.
        producer(transactionalId);

        assertThrows(ProducerDead.class, () -> commitThrough(store, first));
        assertTrue(store.behind().isEmpty(),
                "a fenced producer definitely did not commit, so nothing may be marked behind");
        assertNull(store.get(0, "alpha"),
                "the fenced transaction must not have materialised");
        assertFalse(committedKeys(0).contains("alpha"),
                "read_committed must not surface a fenced producer's record");
    }
}
