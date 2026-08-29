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
import io.veridia.elysiumkv.partitioned.Restore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * The plumbing every Kafka test needs: two topics, producers, {@code read_committed} consumers, and
 * a {@link PartitionedStore} over disk.
 *
 * <p>Two topics because exactly-once has two halves. {@link #inputTopic} carries the events being
 * consumed and {@link #changelogTopic} carries the state the store materialises — and it is the
 * transaction spanning both, with the input offsets inside it, that makes the pair atomic. A test
 * that commits an empty offset map exercises only the output half.
 */
final class KafkaStoreFixture implements AutoCloseable {

    static final Duration POLL = Duration.ofMillis(500);

    final String bootstrap;
    final String inputTopic;
    final String changelogTopic;
    final String groupId;
    final MutationCodec codec = new PrefixedMutationCodec();

    private final List<AutoCloseable> owned = new ArrayList<>();

    KafkaStoreFixture(int partitions) {
        bootstrap = KafkaEnvironment.requireBootstrap();
        String suffix = UUID.randomUUID().toString();
        inputTopic = "events-" + suffix;
        changelogTopic = "states-" + suffix;
        groupId = "group-" + suffix;
        KafkaEnvironment.createTopic(inputTopic, partitions);
        KafkaEnvironment.createTopic(changelogTopic, partitions);
    }

    <T extends AutoCloseable> T own(T closeable) {
        owned.add(closeable);
        return closeable;
    }

    /** Declared without a checked exception so try-with-resources does not force one on callers. */
    @Override
    public void close() {
        // Reverse: a store closes before the blob stores and catalogs it was opened over.
        Collections.reverse(owned);
        RuntimeException first = null;
        for (AutoCloseable closeable : owned) {
            try {
                closeable.close();
            } catch (Exception e) {
                if (first == null) {
                    first = e instanceof RuntimeException ? (RuntimeException) e
                                                          : new IllegalStateException(e);
                }
            }
        }
        owned.clear();
        if (first != null) throw first;
    }

    // --- the store ------------------------------------------------------------------------

    ElysiumKVOptions optionsFor(Path base, int partition) {
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

    PartitionedStore<String> storeOver(Path base, KafkaTransaction tx, Restore<String> restore) {
        return own(PartitionedStore.<String>builder()
                .options(partition -> optionsFor(base, partition))
                .keyBytes(KafkaStoreFixture::bytes)
                .changelog(new Changelog<String>() {
                    @Override
                    public PendingPosition send(int partition, String key, Mutation mutation) {
                        return tx.send(changelogTopic, partition, ChangelogRecords.pointKey(bytes(key)),
                                codec.encode(mutation));
                    }

                    @Override
                    public PendingPosition sendDeleteRange(int partition, byte[] lo, byte[] hi) {
                        return tx.send(changelogTopic, partition,
                                ChangelogRecords.rangeKey(lo, hi), ChangelogRecords.RANGE_VALUE);
                    }
                })
                .restore(restore)
                .build());
    }

    Restore<String> restoreFromTheChangelog() {
        return new ExampleStateRestore<>(partition -> consumer(groupId + "-restore"),
                changelogTopic, KafkaStoreFixture::string, codec, POLL, 100);
    }

    // --- Kafka ----------------------------------------------------------------------------

    /**
     * Registered for close so a fenced producer does not leak, but not closed by the test itself:
     * a producer whose transaction is in an indeterminate state must be closed rather than reused,
     * and letting the fixture do it keeps that out of every test body.
     */
    Producer<byte[], byte[]> producer(String transactionalId) {
        return producer(transactionalId, new Properties());
    }

    Producer<byte[], byte[]> producer(String transactionalId, Properties overrides) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.putAll(overrides);
        Producer<byte[], byte[]> producer = new KafkaProducer<>(config);
        producer.initTransactions();
        owned.add(producer::close);
        return producer;
    }

    Consumer<byte[], byte[]> consumer(String group) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // The whole reason a real broker is worth the seconds it costs.
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new KafkaConsumer<>(config);
    }

    /** Events for the processing loop to consume. Not transactional: this is upstream input. */
    void publishInput(int partition, String... keys) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            for (String key : keys) {
                producer.send(new ProducerRecord<>(inputTopic, partition, bytes(key), bytes(key)));
            }
            producer.flush();
        }
    }

    /** Appends state directly, bypassing the store, so the store falls behind its changelog. */
    void appendToChangelog(int partition, String key, String value) {
        Producer<byte[], byte[]> producer = producer("direct-" + UUID.randomUUID());
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(changelogTopic, partition,
                ChangelogRecords.pointKey(bytes(key)),
                codec.encode(Mutation.put(bytes(value)))));
        producer.commitTransaction();
    }

    static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static String string(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
