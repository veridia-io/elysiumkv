package io.veridia.elysiumkv.partitioned.kafka;

import static io.veridia.elysiumkv.partitioned.kafka.KafkaStoreFixture.bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.veridia.elysiumkv.Compression;
import io.veridia.elysiumkv.DiskBlobStore;
import io.veridia.elysiumkv.DynamoManifestCatalog;
import io.veridia.elysiumkv.Durability;
import io.veridia.elysiumkv.ElysiumKVOptions;
import io.veridia.elysiumkv.RemoteEnvironment;
import io.veridia.elysiumkv.S3BlobStore;
import io.veridia.elysiumkv.partitioned.Mutation;
import io.veridia.elysiumkv.partitioned.PartitionedStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The configuration production will actually run, end to end: an <b>ephemeral local tier over S3</b>
 * with the manifest in DynamoDB, driven through {@link PartitionedStore} against a <b>real Kafka
 * transaction</b>.
 *
 * <p>Every piece of this is already tested somewhere. The durability suite wipes a transient tier
 * but drives it from {@code InMemoryLog}; the Kafka suite uses a real broker but a single durable
 * tier on disk. Neither poses the case where <b>two independent recovery mechanisms meet</b>:
 *
 * <ol>
 *   <li>the <em>engine</em> discards files whose transient store did not come back, and lowers the
 *       recovered watermark to account for what it dropped;</li>
 *   <li>the <em>store</em> replays the changelog from that watermark.</li>
 * </ol>
 *
 * <p>The failure this is aimed at is a lowered watermark that is not low enough: the replay would
 * then resume past records the discard threw away, and the partition would come back missing
 * committed writes while reporting itself ready. That is the one failure class that does not
 * self-heal, because nothing afterwards goes looking for it.
 */
class PartitionedStoreRemoteKafkaTest {

    private static final int PARTITION = 0;
    /** Enough batches that the transient tier overflows and some files migrate to S3. */
    private static final int BATCHES = 24;

    private KafkaStoreFixture fixture;
    private String namespace;

    @TempDir Path root;

    @BeforeEach
    void setUp() {
        // Order matters only for the report: a machine without Docker should say so once.
        RemoteEnvironment.requireEndpoint();
        fixture = new KafkaStoreFixture(1);
        namespace = "partitioned-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    /** The local tier, which a restart is entitled to lose. */
    private Path hotDir(int partition) {
        return root.resolve("hot-" + partition);
    }

    /**
     * Transient local over durable S3, manifest in DynamoDB.
     *
     * <p>The transient tier is bounded by <em>capacity</em> rather than age, for the reason the
     * durability fixture gives: capacity makes migration a function of how much was written rather
     * than of how long the test slept.
     */
    private ElysiumKVOptions optionsFor(int partition) {
        try {
            Path hot = hotDir(partition);
            Files.createDirectories(hot);
            DiskBlobStore local = fixture.own(new DiskBlobStore(hot.toString(), "hot"));
            S3BlobStore cold = fixture.own(S3BlobStore.builder(RemoteEnvironment.BUCKET)
                    .prefix(namespace + "/" + partition)
                    .endpoint(RemoteEnvironment.requireEndpoint())
                    .credentials(RemoteEnvironment.ACCESS_KEY, RemoteEnvironment.SECRET_KEY)
                    .storeId("cold")
                    .open());
            DynamoManifestCatalog catalog = fixture.own(
                    DynamoManifestCatalog.builder(RemoteEnvironment.TABLE,
                                    namespace + "-" + partition)
                            .endpoint(RemoteEnvironment.requireEndpoint())
                            .credentials(RemoteEnvironment.ACCESS_KEY, RemoteEnvironment.SECRET_KEY)
                            .createTableIfMissing(true)
                            .open());
            return fixture.own(new ElysiumKVOptions()
                    .manifestCatalog(catalog)
                    .memtableBytes(8 * 1024)
                    .blockBytes(1024)
                    .paranoidChecks(true)
                    .addTier(local, Durability.TRANSIENT, 3_600_000L, 6 * 1024, 7_200_000L)
                    .addTier(cold, Durability.DURABLE, 0, 0, 0)
                    .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                    .level(1, Compression.NONE, 0, 0, 0, 0, 0));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private PartitionedStore<String> open() {
        KafkaTransaction tx = new KafkaTransaction(fixture.producer("tx-" + UUID.randomUUID()));
        PartitionedStore<String> store = fixture.own(PartitionedStore.<String>builder()
                .options(this::optionsFor)
                .keyBytes(KafkaStoreFixture::bytes)
                .changelog((partition, key, mutation) -> tx.send(
                        fixture.changelogTopic, partition, bytes(key), fixture.codec.encode(mutation)))
                .restore(fixture.restoreFromTheChangelog())
                .build());
        transactions.add(tx);
        return store;
    }

    private final List<KafkaTransaction> transactions = new ArrayList<>();

    /** Big enough that the 8 KiB memtable fills and real SSTs land on the transient tier. */
    private static byte[] payload(int batch) {
        StringBuilder value = new StringBuilder("value" + batch + ":");
        while (value.length() < 900) {
            value.append('x');
        }
        return bytes(value.toString());
    }

    private void commitBatch(PartitionedStore<String> store, KafkaTransaction tx, int batch) {
        tx.begin();
        store.stage(PARTITION, Collections.singletonMap("key" + batch,
                Mutation.put(payload(batch))));
        try (Consumer<byte[], byte[]> group = fixture.consumer(fixture.groupId)) {
            store.commit(() -> tx.commit(Collections.emptyMap(), group.groupMetadata()));
        }
    }

    /** Everything under the local tier, gone — a pod rescheduled onto a fresh volume. */
    private static void wipe(Path dir) {
        if (!Files.exists(dir)) return;
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toArray(Path[]::new)) {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static int fileCount(Path dir) {
        if (!Files.exists(dir)) return 0;
        try (Stream<Path> walk = Files.walk(dir)) {
            return (int) walk.filter(Files::isRegularFile).count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void aWipedTransientTierIsRebuiltFromTheChangelogWithNothingHiddenBehindTheWatermark() {
        PartitionedStore<String> store = open();
        KafkaTransaction tx = transactions.get(transactions.size() - 1);
        store.assign(Collections.singletonList(PARTITION));

        for (int batch = 0; batch < BATCHES; batch++) {
            commitBatch(store, tx, batch);
        }

        // After the close, because closing is what flushes the memtable into SSTs. Checked at all
        // because a wipe that destroys nothing would let this test pass without a replay: some of
        // the data must be on the local tier and nowhere else.
        store.close();
        assertTrue(fileCount(hotDir(PARTITION)) > 0,
                "the transient tier holds nothing, so wiping it proves nothing");
        wipe(hotDir(PARTITION));
        assertEquals(0, fileCount(hotDir(PARTITION)), "the local tier was not actually wiped");

        // Reopening: the engine discards what the transient store no longer has and lowers the
        // recovered watermark; assign() replays the changelog from there before serving.
        PartitionedStore<String> restarted = open();
        restarted.assign(Collections.singletonList(PARTITION));

        assertTrue(restarted.behind().isEmpty(),
                "a partition that completed its replay is not behind");

        List<String> keys = new ArrayList<>();
        for (int batch = 0; batch < BATCHES; batch++) {
            keys.add("key" + batch);
        }
        Map<String, byte[]> found = restarted.getCommittedBatch(PARTITION, keys);
        for (int batch = 0; batch < BATCHES; batch++) {
            assertArrayEquals(payload(batch), found.get("key" + batch),
                    "key" + batch + " did not survive the wipe — the replay resumed past it");
        }
    }

    /**
     * The same wipe, but with writes continuing afterwards: the rebuilt store must accept new
     * transactions and keep both halves. A replay that left the watermark inconsistent with the
     * data would surface here rather than in the read above.
     */
    @Test
    void aRebuiltPartitionKeepsServingAndAcceptsNewTransactions() {
        PartitionedStore<String> store = open();
        KafkaTransaction tx = transactions.get(transactions.size() - 1);
        store.assign(Collections.singletonList(PARTITION));
        for (int batch = 0; batch < BATCHES; batch++) {
            commitBatch(store, tx, batch);
        }
        store.close();
        wipe(hotDir(PARTITION));

        PartitionedStore<String> restarted = open();
        KafkaTransaction after = transactions.get(transactions.size() - 1);
        restarted.assign(Collections.singletonList(PARTITION));

        for (int batch = BATCHES; batch < BATCHES + 4; batch++) {
            commitBatch(restarted, after, batch);
        }

        List<String> keys = new ArrayList<>();
        for (int batch = 0; batch < BATCHES + 4; batch++) {
            keys.add("key" + batch);
        }
        Map<String, byte[]> found = restarted.getCommittedBatch(PARTITION, keys);
        for (int batch = 0; batch < BATCHES + 4; batch++) {
            assertArrayEquals(payload(batch), found.get("key" + batch),
                    "key" + batch + " is missing after a wipe followed by more writes");
        }

        // And it survives a second round trip, which is what proves the watermark the replay left
        // behind is usable rather than merely not fatal.
        restarted.close();
        PartitionedStore<String> again = open();
        again.assign(Collections.singletonList(PARTITION));
        assertArrayEquals(payload(BATCHES + 3),
                again.getCommittedBatch(PARTITION,
                        Collections.singletonList("key" + (BATCHES + 3))).get("key" + (BATCHES + 3)));
    }
}
