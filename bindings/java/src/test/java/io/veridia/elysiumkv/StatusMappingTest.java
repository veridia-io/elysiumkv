package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * ARCHITECTURE.md "The ABI boundary" — the first rule, from the Java side: <b>the ABI must never invite a binding to
 * reinterpret a failure as absence.</b> Absence is {@code null}; everything else
 * throws, and the type says whether retrying is meaningful.
 */
@ExtendWith(PinLeakExtension.class)
class StatusMappingTest {
    @Test
    void absenceIsNullAndOnlyAbsenceIs(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            db.put(TestSupport.bytes("present"), TestSupport.bytes("v"));

            try (Pinned pinned = db.get(TestSupport.bytes("present"))) {
                assertNotNull(pinned);
            }
            assertNull(db.get(TestSupport.bytes("absent")));
            assertNull(db.getCopy(TestSupport.bytes("absent")));

            db.delete(TestSupport.bytes("present"));
            assertNull(db.get(TestSupport.bytes("present")), "a tombstone reads as absent");
            db.close();
        }
    }

    @Test
    void aTransientTierIsRefusedByOpen(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVOptions options = support.transientOptions();
            // ARCHITECTURE.md "A tier is not a level" — a check, not a documented precondition, so that adding a
            // transient tier later cannot leave existing call sites silently
            // serving stale values after a discard.
            ConfigException thrown = assertThrows(ConfigException.class,
                                                  () -> ElysiumKV.open(options));
            assertEquals(Status.CONFIG, thrown.status());
            assertTrue(thrown.getMessage().length() > 0, "the detail must survive the crossing");
        }
    }

    @Test
    void anUnreachableStoreThrowsRatherThanReadingAsEmpty(@TempDir Path dir) throws Exception {
        // A file where the store's directory has to be: writes cannot land, and
        // the failure must arrive as a failure. The retryable type is what
        // distinguishes "try again" from "stop" (ARCHITECTURE.md "Immutable named objects").
        Path blocked = dir.resolve("blocked");
        java.nio.file.Files.write(blocked, new byte[] {1});

        ElysiumKVException thrown = assertThrows(ElysiumKVException.class, () -> {
            try (LocalFileBlobStore store = new LocalFileBlobStore(blocked.toString(), "broken");
                 FileManifestCatalog catalog = new FileManifestCatalog(dir.toString());
                 ElysiumKVOptions options = new ElysiumKVOptions()
                         .manifestCatalog(catalog)
                         .addTier(store, Durability.DURABLE, 0, 0, 0, 0)
                         .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                         .level(1, Compression.NONE, 0, 0, 0, 0, 0);
                 ElysiumKV db = ElysiumKV.open(options)) {
                for (int i = 0; i < 2000; ++i) {
                    db.put(TestSupport.key(i), TestSupport.bytes("value:" + i));
                }
                db.flush();
            }
        });
        assertNotNull(thrown.status());
        assertTrue(thrown.status() != Status.NOT_FOUND,
                   "an IO failure must never arrive as absence");
    }

    @Test
    void unknownStatusCodesDegradeToTheSafeDirection() {
        // A library newer than this binding could return a code we do not know.
        // Reporting it as UNUSABLE is the safe reading: not retryable, and above
        // all not absence.
        assertEquals(Status.UNUSABLE, Status.fromCode(9999));
        assertEquals(Status.UNUSABLE, Status.fromCode(-1));
        assertEquals(Status.NOT_FOUND, Status.fromCode(1));
    }
}
