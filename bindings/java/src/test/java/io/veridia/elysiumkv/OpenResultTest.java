package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * ARCHITECTURE.md "A tier is not a level" — the two open paths, and why there are two. {@link ElysiumKV#open} refuses
 * a transient tier outright so that adding one later cannot leave existing call
 * sites silently serving stale values after a discard; {@link
 * ElysiumKV#openWithResult} is the way to say "I know, and here is what I do about
 * it".
 */
@ExtendWith(PinLeakExtension.class)
class OpenResultTest {
    @Test
    void openRefusesWhatOpenWithResultAccepts(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            assertThrows(ConfigException.class, () -> ElysiumKV.open(support.transientOptions()));
        }
    }

    @Test
    void aCleanOpenReportsNothingDiscarded(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            OpenResult result = ElysiumKV.openWithResult(support.transientOptions());
            try (ElysiumKV db = PinLeakExtension.watch(result.db())) {
                assertNotNull(db);
                assertEquals(0, result.discardedStores().size());
                assertEquals(0, result.discardedFiles());
                assertFalse(result.requiresRecovery());
                assertFalse(db.stats().requiresRecovery());
            }
        }
    }

    @Test
    void theDiscardedStoreListIsImmutable(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            OpenResult result = ElysiumKV.openWithResult(support.transientOptions());
            try (ElysiumKV db = PinLeakExtension.watch(result.db())) {
                assertNotNull(db);
                // The ids are copied out of native memory that dies with the db,
                // so handing back a mutable view would invite a dangling read.
                assertThrows(UnsupportedOperationException.class,
                             () -> result.discardedStores().add("nope"));
            }
        }
    }

    // ARCHITECTURE.md "A tier is not a level" — requiresRecovery stays true until the embedder says otherwise, and
    // markRecoveryComplete is the only thing that clears it.
    @Test
    void recoveryIsAcknowledgedExplicitly(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            assertFalse(db.stats().requiresRecovery());
            db.markRecoveryComplete();
            assertFalse(db.stats().requiresRecovery(), "clearing an unset flag is harmless");
            db.close();
        }
    }
}
