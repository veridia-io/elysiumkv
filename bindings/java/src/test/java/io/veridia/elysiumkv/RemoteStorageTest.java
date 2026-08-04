package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The remote implementations, driven <b>through the engine</b> against LocalStack.
 *
 * <p><b>This is not where the per-operation contract lives.</b> The plan had the C++
 * contract suites ported to Java; they are instead <em>instantiated</em> for S3 and
 * DynamoDB in {@code tests/contract/remote_store_test.cpp}, unchanged, which is both
 * less code and strictly better — it runs the same cases the local stores run, and it
 * runs them under ASan, UBSan and TSan. Porting them here would have meant adding a
 * dozen ABI functions that exist only for tests: the C ABI exposes the storage seams
 * for construction only, because a binding hands the engine a store and never calls
 * {@code get}/{@code put}/{@code list} itself.
 *
 * <p>What this file adds is the thing no contract case can: a <em>real database</em>
 * on remote storage. Every combination of store and catalog is opened, written,
 * flushed, compacted, closed and reopened, and every key read back. That exercises
 * ranged reads, write-once puts, listing, deletion and the pointer swap in the order
 * and interleaving the engine actually produces — which is where a store that passes
 * its contract in isolation still breaks. It also covers the two things only a live
 * database reaches: a file migrating across a network boundary, and a second writer
 * being fenced.
 *
 * <p>Skips without Docker or without an AWS-enabled native build — see {@link
 * RemoteEnvironment}, which turns those skips into failures under
 * {@code -Delysiumkv.remote.required=true}.
 */
class RemoteStorageTest {
    /** LocalStack is fresh per run, so a counter is enough to keep runs isolated. */
    private static final AtomicInteger NAMESPACE = new AtomicInteger();

    private enum StoreKind { LOCAL_FILE, S3 }

    private enum CatalogKind { FILE, S3, DYNAMO }

    /** Owns everything a case opened, closed in reverse. */
    private static final class Fixture implements AutoCloseable {
        private final List<AutoCloseable> owned = new ArrayList<>();
        private final Path dir;
        private final String namespace;

        Fixture(Path dir) {
            this.dir = dir;
            this.namespace = "run-" + NAMESPACE.incrementAndGet();
        }

        <T extends AutoCloseable> T own(T closeable) {
            owned.add(closeable);
            return closeable;
        }

        BlobStore store(StoreKind kind, String name) throws IOException {
            if (kind == StoreKind.LOCAL_FILE) {
                Path root = dir.resolve(name);
                Files.createDirectories(root);
                return own(new LocalFileBlobStore(root.toString(), namespace + "-" + name));
            }
            return own(S3BlobStore.builder(RemoteEnvironment.BUCKET)
                               .prefix(namespace + "/" + name)
                               .endpoint(RemoteEnvironment.requireEndpoint())
                               .credentials(RemoteEnvironment.ACCESS_KEY,
                                            RemoteEnvironment.SECRET_KEY)
                               .open());
        }

        ManifestCatalog catalog(CatalogKind kind) throws IOException {
            switch (kind) {
                case FILE:
                    Path manifest = dir.resolve("manifest");
                    Files.createDirectories(manifest);
                    return own(new FileManifestCatalog(manifest.toString()));
                case S3:
                    // A prefix of its own: sharing one with a blob store would put
                    // manifest objects and SSTs in the same namespace.
                    return own(S3ManifestCatalog.builder(RemoteEnvironment.BUCKET)
                                       .prefix(namespace + "/manifest")
                                       .endpoint(RemoteEnvironment.requireEndpoint())
                                       .credentials(RemoteEnvironment.ACCESS_KEY,
                                                    RemoteEnvironment.SECRET_KEY)
                                       .open());
                case DYNAMO:
                    return own(DynamoManifestCatalog.builder(RemoteEnvironment.TABLE, namespace)
                                       .endpoint(RemoteEnvironment.requireEndpoint())
                                       .credentials(RemoteEnvironment.ACCESS_KEY,
                                                    RemoteEnvironment.SECRET_KEY)
                                       .createTableIfMissing(true)
                                       .open());
                default:
                    throw new AssertionError(kind);
            }
        }

        @Override
        public void close() {
            for (int i = owned.size() - 1; i >= 0; --i) {
                try {
                    owned.get(i).close();
                } catch (Exception ignored) {
                    // A test that already failed should report that, not this.
                }
            }
        }
    }

    private static ElysiumKVOptions options(Fixture fixture, ManifestCatalog catalog, BlobStore store) {
        return fixture.own(new ElysiumKVOptions()
                                   .manifestCatalog(catalog)
                                   // Small enough that a few hundred keys produce
                                   // several files, so compaction and the manifest
                                   // pointer both move more than once.
                                   .memtableBytes(32 * 1024)
                                   .blockBytes(1024)
                                   .paranoidChecks(true)
                                   .addTier(store, Durability.DURABLE, 0, 0, 0, 0)
                                   .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                                   .level(1, Compression.ZSTD, 0, 0, 0, 0, 0));
    }

    private static final int KEYS = 600;

    private static byte[] value(int i) {
        // Long enough that a block holds only a few entries, so reads have to cross
        // block boundaries — which on S3 means real ranged GETs.
        StringBuilder text = new StringBuilder("value-").append(i).append('-');
        while (text.length() < 200) text.append('x');
        return TestSupport.bytes(text.toString());
    }

    @ParameterizedTest(name = "{0} store with a {1} catalog")
    @CsvSource({
        "LOCAL_FILE, FILE",     // the control: proves the case describes the engine,
        "LOCAL_FILE, S3",       // not one implementation's behaviour
        "LOCAL_FILE, DYNAMO",
        "S3, FILE",
        "S3, S3",
        "S3, DYNAMO",
    })
    void aDatabaseSurvivesEveryStoreAndCatalogCombination(StoreKind storeKind,
                                                          CatalogKind catalogKind,
                                                          @TempDir Path dir) throws IOException {
        RemoteEnvironment.requireEndpoint();

        try (Fixture fixture = new Fixture(dir)) {
            BlobStore store = fixture.store(storeKind, "store");
            ManifestCatalog catalog = fixture.catalog(catalogKind);

            try (ElysiumKV db = PinLeakExtension.watch(
                         ElysiumKV.open(options(fixture, catalog, store)))) {
                for (int i = 0; i < KEYS; ++i) db.put(TestSupport.key(i), value(i));
                db.flush();
                // A second round after the flush, so the reopen has to replay
                // manifest edits on top of a snapshot rather than only read one.
                for (int i = 0; i < KEYS; i += 3) db.put(TestSupport.key(i), value(i + 1_000_000));
                db.flush();
                db.delete(TestSupport.key(7));
                db.flush();
                db.compactLevel(0);

                ElysiumKVStats stats = db.stats();
                assertTrue(stats.levelBytesTotal() > 0, "the engine wrote files");
                assertEquals(stats.levelBytesTotal(), stats.tierBytesTotal(),
                             "every file sits in exactly one level and one tier");
            }

            // The reopen is the point: it reads the pointer, loads the generation and
            // replays the edits, all through the catalog under test.
            try (ElysiumKV db = PinLeakExtension.watch(
                         ElysiumKV.open(options(fixture, catalog, store)))) {
                for (int i = 0; i < KEYS; ++i) {
                    byte[] got = db.getCopy(TestSupport.key(i));
                    if (i == 7) {
                        assertNull(
                                got, "the deleted key stays deleted across a reopen");
                        continue;
                    }
                    byte[] want = i % 3 == 0 ? value(i + 1_000_000) : value(i);
                    assertArrayEquals(want, got, "key " + i);
                }

                int scanned = 0;
                try (ElysiumKVIterator it = db.prefixIterator(TestSupport.bytes("key:"))) {
                    while (it.next()) ++scanned;
                }
                assertEquals(KEYS - 1, scanned, "the scan agrees with the point reads");
            }
        }
    }

    /**
     * ARCHITECTURE.md "A tier is not a level" end to end: a hot local tier over a cold S3 one, with files migrating
     * down. Migration is a byte-for-byte copy between stores, so this is the only
     * test anywhere that moves an SST across a network boundary and then reads it.
     */
    @Test
    void filesMigrateFromALocalHotTierToS3(@TempDir Path dir) throws IOException {
        RemoteEnvironment.requireEndpoint();

        try (Fixture fixture = new Fixture(dir)) {
            BlobStore hot = fixture.store(StoreKind.LOCAL_FILE, "hot");
            BlobStore cold = fixture.store(StoreKind.S3, "cold");
            ManifestCatalog catalog = fixture.catalog(CatalogKind.DYNAMO);

            // maxAgeMs of 1: every file is immediately old enough for the cold tier,
            // so placement moves it on the next evaluation. Placement is a pure
            // function of age and size, which is what makes that predictable.
            ElysiumKVOptions options = fixture.own(
                    new ElysiumKVOptions()
                            .manifestCatalog(catalog)
                            .memtableBytes(32 * 1024)
                            .blockBytes(1024)
                            .paranoidChecks(true)
                            .addTier(hot, Durability.DURABLE, 1, 0, 0, 0)
                            .addTier(cold, Durability.DURABLE, 0, 0, 0, 0)
                            .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                            .level(1, Compression.NONE, 0, 0, 0, 0, 0));

            long coldBytes;
            try (ElysiumKV db = PinLeakExtension.watch(ElysiumKV.open(options))) {
                for (int i = 0; i < KEYS; ++i) db.put(TestSupport.key(i), value(i));
                db.flush();
                db.compactLevel(0);
                db.flush();

                ElysiumKVStats stats = db.stats();
                assertEquals(2, stats.tiers().size());
                coldBytes = stats.tiers().get(1).bytes();
                assertTrue(coldBytes > 0,
                           () -> "nothing reached the cold tier; tier bytes were "
                                   + stats.tiers().get(0).bytes() + " hot and "
                                   + stats.tiers().get(1).bytes() + " cold");
            }

            // Reads after the reopen must come from wherever the files ended up.
            try (ElysiumKV db = PinLeakExtension.watch(ElysiumKV.open(options))) {
                for (int i = 0; i < KEYS; ++i) {
                    assertArrayEquals(value(i), db.getCopy(TestSupport.key(i)), "key " + i);
                }
                assertEquals(coldBytes, db.stats().tiers().get(1).bytes(),
                             "the cold tier holds the same bytes after a reopen");
            }
        }
    }

    /**
     * <b>The fence, contended for real.</b> A remote catalog is the first place two
     * processes can genuinely race the manifest pointer — a single-writer filesystem
     * validates the token but can never lose to anyone, which is the whole reason
     * ARCHITECTURE.md "Ownership is one compare-and-set" introduced it.
     *
     * <p>Two databases open the same store and catalog. The second one's first
     * install moves the pointer, so the first one's next install finds its
     * expectation stale. That is <b>not</b> retryable: its own view of the LSM is
     * from before the other writer's compaction, so retrying would install a
     * manifest that has forgotten files. {@link FencedException} says reopen.
     */
    @ParameterizedTest(name = "{0} catalog")
    @EnumSource(value = CatalogKind.class, names = {"S3", "DYNAMO"})
    void theSecondWriterFencesTheFirst(CatalogKind catalogKind, @TempDir Path dir)
            throws IOException {
        RemoteEnvironment.requireEndpoint();

        try (Fixture fixture = new Fixture(dir)) {
            BlobStore storeA = fixture.store(StoreKind.S3, "writer-a");
            BlobStore storeB = fixture.store(StoreKind.S3, "writer-b");

            // **Both writers configure both stores, each with its own hot tier.**
            // Two things force this. A writer that does not configure a store its
            // manifest references cannot open at all — a missing store is a
            // configuration error, not something to work around. And two writers
            // sharing a hot tier collide on an SST *name* before either reaches the
            // manifest, because file numbers advance from the same replayed state in
            // both; that is also a safe failure, but it is not the one under test and
            // it would hide it. Separate hot tiers isolate the manifest.
            //
            // Placement only ever moves a file colder, so neither writer drags the
            // other's files out of the tier they were written to.
            ManifestCatalog mine = fixture.catalog(catalogKind);
            ElysiumKV a = PinLeakExtension.watch(
                    ElysiumKV.open(twoTierOptions(fixture, mine, storeA, storeB)));
            try {
                for (int i = 0; i < 200; ++i) a.put(TestSupport.key(i), value(i));
                a.flush();

                // A second instance on the same manifest. Nothing prevents opening
                // one: the engine is single-writer by protocol, and the manifest is
                // what enforces the protocol.
                ManifestCatalog theirs = fixture.catalog(catalogKind);
                try (ElysiumKV b = PinLeakExtension.watch(
                             ElysiumKV.open(twoTierOptions(fixture, theirs, storeB, storeA)))) {
                    for (int i = 200; i < 400; ++i) b.put(TestSupport.key(i), value(i));
                    b.flush();
                }

                for (int i = 400; i < 600; ++i) a.put(TestSupport.key(i), value(i));
                FencedException fenced = assertThrows(FencedException.class, a::flush,
                                                      "the fenced writer must not install");
                assertNotNull(fenced.getMessage());

                // Terminal, not retryable: the same call must not suddenly succeed.
                assertThrows(FencedException.class, a::flush,
                             "a fenced instance stays fenced until it is reopened");
            } finally {
                // Not try-with-resources: closing a fenced instance must be reached
                // without its outcome masking the assertion that got us here.
                a.close();
            }

            // The reopen the exception asks for reads the winner's manifest, so it
            // sees the second writer's keys and not the fenced writer's last batch.
            ManifestCatalog reopened = fixture.catalog(catalogKind);
            try (ElysiumKV c = PinLeakExtension.watch(
                         ElysiumKV.open(twoTierOptions(fixture, reopened, storeB, storeA)))) {
                assertArrayEquals(value(250), c.getCopy(TestSupport.key(250)),
                                  "the winner's writes are the ones that survived");
                assertNull(c.getCopy(TestSupport.key(500)),
                           "and the fenced writer's un-installed flush is not there");
            }
        }
    }

    /**
     * A hot tier for this writer's new files and a cold one holding the other's. One
     * day of age is never reached in a test, so every new file lands hot.
     */
    private static ElysiumKVOptions twoTierOptions(Fixture fixture, ManifestCatalog catalog,
                                                BlobStore hot, BlobStore cold) {
        return fixture.own(new ElysiumKVOptions()
                                   .manifestCatalog(catalog)
                                   .memtableBytes(32 * 1024)
                                   .blockBytes(1024)
                                   .paranoidChecks(true)
                                   .addTier(hot, Durability.DURABLE, 86_400_000L, 0, 0, 0)
                                   .addTier(cold, Durability.DURABLE, 0, 0, 0, 0)
                                   .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                                   .level(1, Compression.NONE, 0, 0, 0, 0, 0));
    }
}
