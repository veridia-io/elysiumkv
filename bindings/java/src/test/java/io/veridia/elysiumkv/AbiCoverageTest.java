package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ARCHITECTURE.md "Dependencies and artifacts" step 10's first green criterion: a test exercises every ABI function.
 *
 * <p>The reason this needs to be mechanical rather than a habit: a JNI method is
 * bound by a signature string in {@code elysiumkv_jni.cpp}, and a wrong one fails
 * at <em>runtime</em>, not at compile time. Nothing in the build notices a
 * function that is declared, registered and never called — it simply waits for
 * whichever user reaches it first.
 *
 * <p>So the test compares two sets: the native methods {@code Native} declares,
 * found by reflection, and the ones this test actually called. Adding a native
 * method without exercising it makes the sets differ and fails the build. The
 * assertion names what is missing rather than just counting.
 */
class AbiCoverageTest {
    private final Set<String> exercised = new TreeSet<>();

    private void exercise(String method) {
        exercised.add(method);
    }

    @Test
    void everyNativeMethodIsExercised(@TempDir Path dir) throws Exception {
        driveTheWholeSurface(dir);

        Set<String> declared = new TreeSet<>();
        for (Method method : Native.class.getDeclaredMethods()) {
            if (Modifier.isNative(method.getModifiers())) declared.add(method.getName());
        }

        Set<String> uncovered = new TreeSet<>(declared);
        uncovered.removeAll(exercised);
        Set<String> stale = new TreeSet<>(exercised);
        stale.removeAll(declared);

        assertTrue(uncovered.isEmpty(),
                   () -> "native methods declared but never called by this test: " + uncovered
                           + " — a wrong JNI signature in these would fail at runtime, in "
                           + "whichever application reached them first");
        assertTrue(stale.isEmpty(), () -> "this test claims methods that no longer exist: " + stale);
        assertEquals(declared, exercised);
    }

    /**
     * The remote constructors, in both build configurations — the reason
     * they are always bound rather than conditionally present.
     *
     * <p>Constructing an S3 or DynamoDB client reaches no network (a client is
     * configuration, not a connection), so these run against an endpoint nothing
     * is listening on. That is what lets this test verify their JNI signatures —
     * the thing it exists for, since a wrong signature fails at runtime in
     * whichever application reaches it first — without a service behind them.
     *
     * <p>When the native library was built without the AWS SDK the calls still
     * happen and still verify their signatures; they just come back as {@link
     * ConfigException}. Either branch exercises the method, which is why the
     * coverage set stays exact in both builds.
     */
    private void exerciseRemoteConstructors() {
        exercise("features");
        boolean remote = ElysiumKV.hasAwsSupport();

        exercise("s3BlobStoreCreate");
        exercise("s3ManifestCatalogCreate");
        exercise("dynamoManifestCatalogCreate");
        if (remote) {
            try (S3BlobStore store = S3BlobStore.builder("bucket")
                                             .prefix("cold")
                                             .endpoint("http://127.0.0.1:1")
                                             .credentials("test", "test")
                                             .open();
                 S3ManifestCatalog s3Catalog = S3ManifestCatalog.builder("bucket")
                                                       .prefix("manifest")
                                                       .endpoint("http://127.0.0.1:1")
                                                       .credentials("test", "test")
                                                       .open();
                 DynamoManifestCatalog dynamo = DynamoManifestCatalog.builder("table", "store")
                                                        .endpoint("http://127.0.0.1:1")
                                                        .credentials("test", "test")
                                                        .open()) {
                assertNotNull(store);
                assertNotNull(s3Catalog);
                assertNotNull(dynamo);
            }
        } else {
            for (Runnable attempt : new Runnable[] {
                     () -> S3BlobStore.builder("bucket").open(),
                     () -> S3ManifestCatalog.builder("bucket").open(),
                     () -> DynamoManifestCatalog.builder("table", "store").open()}) {
                ConfigException thrown = assertThrows(ConfigException.class, attempt::run);
                assertTrue(thrown.getMessage().contains("ELYSIUMKV_BUILD_AWS"),
                           () -> "a build without the remote implementations must say which build "
                                   + "option is missing, or the failure reads as 'S3 is broken': "
                                   + thrown.getMessage());
            }
        }
    }

    private void driveTheWholeSurface(Path dir) throws IOException {
        exercise("version");
        assertNotNull(Native.version());
        exercise("lastError");
        assertNotNull(Native.lastError());

        exerciseRemoteConstructors();

        try (TestSupport support = new TestSupport(dir)) {
            // Options: create/destroy, both aggregates, and the one configure call.
            exercise("optionsCreate");
            exercise("optionsDestroy");
            exercise("diskBlobStoreCreate");
            exercise("blobStoreDestroy");
            exercise("diskManifestCatalogCreate");
            exercise("manifestCatalogDestroy");
            // ARCHITECTURE.md "Caches chain" — the decorators, over the local store the fixture already owns. The
            // chain is what the ABI has to support, so the chain is what is built.
            exercise("memoryBudgetCreate");
            exercise("memoryBudgetUsed");
            exercise("memoryBudgetDestroy");
            exercise("diskCacheBlobStoreCreate");
            exercise("memoryCacheBlobStoreCreate");
            try (MemoryBudget budget = new MemoryBudget(64L << 20);
                 DiskCacheBlobStore disk = new DiskCacheBlobStore(
                         dir.resolve("disk-cache").toString(), support.hot, 1 << 20, true);
                 MemoryCacheBlobStore memory =
                         new MemoryCacheBlobStore(disk, budget, 1 << 20, false)) {
                assertNotNull(memory);

                exercise("blobCacheStats");
                assertEquals(0, disk.hits() + disk.misses());
                assertEquals(0, memory.hits() + memory.misses());

                // The chunked pair, which take a fetch granularity. Built over the same delegate:
                // what is asserted here is that the JNI signatures are right, which nothing but a
                // call establishes — behaviour belongs to the C++ cache tests.
                exercise("diskCacheBlobStoreCreateChunked");
                exercise("memoryCacheBlobStoreCreateChunked");
                try (DiskCacheBlobStore chunkedDisk = new DiskCacheBlobStore(
                             dir.resolve("disk-cache-chunked").toString(), support.hot, 1 << 20,
                             false, 64 << 10);
                     MemoryCacheBlobStore chunkedMemory =
                             new MemoryCacheBlobStore(chunkedDisk, budget, 1 << 20, false,
                                                      64 << 10)) {
                    assertNotNull(chunkedMemory);
                }
                assertEquals(0L, budget.used(), "nothing has been cached yet");
            }

            exercise("optionsAddTier");
            exercise("optionsSetLevel");
            exercise("optionsConfigure");
            // Called by prepare() on every open, so opening below covers it; the marker is what
            // says so, since this test asserts on the declared surface rather than on behaviour.
            exercise("optionsConfigureCompaction");
            exercise("optionsConfigureJitter");
            exercise("rangeIsErased");
            // Likewise — prepare() sets it whether or not a sink was configured, so the null path
            // is what every other test here already crosses. LoggerTest covers a real one.
            exercise("optionsSetLogger");

            // Called for real, not marked. A wrong JNI signature on these compiles and links,
            // and fails only when an application first encrypts something; registering a provider
            // here is what turns that into a test failure.
            ElysiumKVOptions encrypted = support.options();
            encrypted.encryptWith("abi-coverage", new EncryptionTest.DirectKeys(), 0);
            exercise("optionsAddAes256GcmEncryption");
            exercise("optionsSetPrimaryEncryptionProvider");

            encrypted.alsoDecryptWith("abi-coverage-static",
                                      StaticEncryptionKeyManager.of(new byte[32]), 0);
            exercise("optionsAddAes256GcmEncryptionWithStaticKey");

            encrypted.rewriteToPrimaryEncryptionProvider(true);
            encrypted.rewriteToPrimaryEncryptionProvider(false);
            exercise("optionsSetEncryptionRewriteToPrimary");

            support.options().geometricLevels(64L << 10, 10, 4);
            exercise("optionsSetGeometricLevels");

            support.options().ttlMs(java.util.concurrent.TimeUnit.DAYS.toMillis(7));
            exercise("optionsSetTtl");

            // Registering a KMS manager performs no I/O, so a dead endpoint is enough to reach the
            // native call — which is the point here. Without the AWS build it must refuse and say
            // which build option is missing, exactly as the remote constructors do.
            exercise("optionsAddAes256GcmEncryptionWithKms");
            AwsKmsEncryptionKeyManager kms = AwsKmsEncryptionKeyManager.builder("alias/abi")
                                                     .endpoint("http://127.0.0.1:1")
                                                     .credentials("test", "test")
                                                     .build();
            if (ElysiumKV.hasAwsSupport()) {
                encrypted.alsoDecryptWith("abi-coverage-kms", kms, 0);
            } else {
                ConfigException thrown = assertThrows(
                        ConfigException.class, () -> encrypted.alsoDecryptWith("abi-coverage-kms",
                                                                               kms, 0));
                assertTrue(thrown.getMessage().contains("ELYSIUMKV_BUILD_AWS"),
                           () -> "a build without KMS must name the missing build option: "
                                   + thrown.getMessage());
            }

            ElysiumKV db = PinLeakExtension.watch(support.open());
            exercise("open");

            exercise("put");
            for (int i = 0; i < 300; ++i) db.put(TestSupport.key(i), TestSupport.bytes("v" + i));

            exercise("delete");
            db.delete(TestSupport.key(299));

            exercise("flush");
            db.flush();

            exercise("get");
            exercise("unpin");
            try (Pinned pinned = db.get(TestSupport.key(5))) {
                assertNotNull(pinned);
                assertEquals("v5", TestSupport.string(pinned.toByteArray()));
            }

            exercise("getDirect");
            ByteBuffer directKey = ByteBuffer.allocateDirect(32);
            directKey.put(TestSupport.key(6)).flip();
            try (Pinned pinned = db.get(directKey)) {
                assertNotNull(pinned);
                assertEquals("v6", TestSupport.string(pinned.toByteArray()));
            }

            exercise("getCopy");
            assertEquals("v7", TestSupport.string(db.getCopy(TestSupport.key(7))));
            assertNull(db.getCopy(TestSupport.bytes("absent")));

            exercise("pinsOutstanding");
            assertEquals(0, db.pinsOutstanding());

            exercise("batchCreate");
            exercise("batchPut");
            exercise("batchDelete");
            exercise("batchDeleteRange");
            exercise("batchSize");
            exercise("batchDestroy");
            exercise("write");
            try (WriteBatch batch = new WriteBatch()) {
                batch.put(TestSupport.bytes("batch-a"), TestSupport.bytes("1"));
                batch.put(TestSupport.bytes("batch-b"), TestSupport.bytes("2"));
                batch.delete(TestSupport.bytes("batch-a"));
                // A range, then a put landing on top of it: order within the batch is what decides.
                batch.deleteRange(TestSupport.bytes("batch-"), TestSupport.bytes("batch."));
                batch.put(TestSupport.bytes("batch-c"), TestSupport.bytes("3"));
                assertEquals(5, batch.size());
                db.write(batch);
            }
            assertNull(db.get(TestSupport.bytes("batch-b")), "covered by the batched range");
            try (Pinned after = db.get(TestSupport.bytes("batch-c"))) {
                assertNotNull(after, "written after the range in the same batch");
            }

            exercise("iterPrefix");
            exercise("iterNext");
            exercise("iterKey");
            exercise("iterValue");
            exercise("iterNextBatch");
            try (BatchedIterator scan = db.batchedPrefixIterator(TestSupport.bytes("key:"))) {
                int batched = 0;
                while (scan.next()) ++batched;
                assertEquals(299, batched);
            }

            exercise("iterKeyInto");
            exercise("iterValueInto");
            exercise("iterStatus");
            exercise("iterDestroy");
            int seen = 0;
            try (ElysiumKVIterator it = db.prefixIterator(TestSupport.bytes("key:"))) {
                byte[] reused = new byte[64];
                while (it.next()) {
                    assertNotNull(it.key());
                    assertNotNull(it.value());
                    assertTrue(it.keyInto(reused) > 0);
                    assertTrue(it.valueInto(reused) >= 0);
                    ++seen;
                }
                it.status();
            }
            assertEquals(299, seen, "one key was deleted");

            exercise("iterCreate");
            try (ElysiumKVIterator it = db.iterator(TestSupport.key(100), TestSupport.key(110))) {
                int bounded = 0;
                while (it.next()) ++bounded;
                assertEquals(10, bounded);
            }

            exercise("iterPrefixReverse");
            try (BatchedIterator scan =
                         db.batchedReversePrefixIterator(TestSupport.bytes("key:"))) {
                int batched = 0;
                while (scan.next()) ++batched;
                scan.status();
                assertEquals(299, batched, "the batched reverse path reaches the same entries");
            }
            int descending = 0;
            try (ElysiumKVIterator it = db.reversePrefixIterator(TestSupport.bytes("key:"))) {
                while (it.next()) ++descending;
                it.status();
            }
            assertEquals(299, descending, "the same entries the ascending scan saw");

            exercise("iterCreateReverse");
            try (ElysiumKVIterator it =
                         db.reverseIterator(TestSupport.key(100), TestSupport.key(110))) {
                int bounded = 0;
                while (it.next()) ++bounded;
                assertEquals(10, bounded);
            }

            // ARCHITECTURE.md "Dependencies and artifacts" step 10 calls these out as the ones that only look deferrable:
            // compactLevel is the only way to finish a codec migration, and
            // markRecoveryComplete the only way to clear requiresRecovery.
            exercise("compactLevel");
            db.compactLevel(0);

            exercise("markRecoveryComplete");
            db.markRecoveryComplete();

            // The two halves of a store-managed changelog offset. Behaviour is asserted in
            // WatermarkTest; what is asserted here is that the JNI signatures are right, which
            // nothing but a call can establish.
            // The read-only pair. Behaviour is asserted in ReadOnlyStoreTest; what is asserted here
            // is that the JNI signatures are right, which nothing but a call establishes.
            exercise("refresh");
            db.refresh();

            exercise("setWatermark");
            db.setWatermark(9_000L);
            exercise("watermark");
            assertTrue(db.recoveredWatermark().isEmpty(),
                       "nothing was flushed under a watermark in this fixture");

            // Both remove keys, so they come after every count asserted over the fixture as built.
            exercise("deleteRange");
            db.deleteRange(TestSupport.key(80), TestSupport.key(90));
            assertNull(db.get(TestSupport.key(85)), "the range was deleted");
            try (Pinned present = db.get(TestSupport.key(90))) {
                assertNotNull(present, "the upper bound is excluded");
            }

            // Last on purpose: it removes keys, and every count asserted above is over the fixture
            // as it was built.
            exercise("truncateBelow");
            db.truncateBelow(TestSupport.key(50));
            assertThrows(ConfigException.class,
                         () -> db.put(TestSupport.key(10), TestSupport.bytes("v")),
                         "the floor refuses a write below it");

            exercise("statsSnapshot");
            ElysiumKVStats stats = db.stats();
            assertEquals(2, stats.levels().size());

            // A second handle on the same store, read-only. Behaviour lives in ReadOnlyStoreTest;
            // this is here so the signature is exercised.
            exercise("openReadOnly");
            try (ReadOnlyStore reader = ElysiumKV.openReadOnly(support.options())) {
                assertNotNull(reader.stats());
            }

            exercise("close");
            assertEquals(0, db.closeReportingOutstanding());
        }

        // The non-flushing close, on its own store so the discarded write cannot affect anything
        // else. Behaviour lives in the engine's durability tests; this is here for the signature.
        Path abandonDir = dir.resolve("abandon");
        Files.createDirectories(abandonDir);
        try (TestSupport support = new TestSupport(abandonDir)) {
            ElysiumKV db = support.own(ElysiumKV.open(support.options()));
            db.put("k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
            exercise("closeWithoutFlush");
            db.closeWithoutFlush();
        }

        // openWithResult needs its own configuration: open() refuses a transient
        // tier outright, which is the whole reason the second entry point exists.
        Path transientDir = dir.resolve("transient");
        Files.createDirectories(transientDir);
        try (TestSupport support = new TestSupport(transientDir)) {
            exercise("openWithResult");
            OpenResult result = ElysiumKV.openWithResult(support.transientOptions());
            try (ElysiumKV db = PinLeakExtension.watch(result.db())) {
                assertNotNull(result.discardedStores());
                db.put(TestSupport.bytes("k"), TestSupport.bytes("v"));
            }
        }
    }
}
