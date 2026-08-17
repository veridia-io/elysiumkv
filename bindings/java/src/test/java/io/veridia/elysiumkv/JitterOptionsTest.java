package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two jitters as a binding sees them. What they <em>do</em> is settled by the C++ suite; what
 * is settled here is that the fractions cross the boundary and that a bad one is refused rather
 * than clamped — a store that quietly ran unjittered would look configured and still migrate its
 * whole contents as one burst.
 */
class JitterOptionsTest {

    @Test
    void aStoreOpensWithBothJittersSet(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            // openWithResult, not open: a transient tier is refused outright by open(), and a
            // tier that bounds age is the only shape ageJitter has anything to act on.
            ElysiumKV db = support.own(ElysiumKV.openWithResult(support.transientOptions()
                    .flushIntervalMs(30_000)
                    .flushIntervalJitter(0.2)
                    .ageJitter(0.25)).db());
            assertNotNull(db);
            db.put(TestSupport.bytes("k"), TestSupport.bytes("v"));
            try (Pinned found = db.get(TestSupport.bytes("k"))) {
                assertEquals("v", TestSupport.string(found.toByteArray()));
            }
        }
    }

    /** Zero is the default and a real setting: the trigger stays exact. */
    @Test
    void zeroIsAcceptedAndIsTheDefault(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            assertNotNull(support.own(ElysiumKV.open(
                    support.options().ageJitter(0.0).flushIntervalJitter(0.0))));
        }
    }

    @Test
    void aFractionAboveOneIsRefused(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVOptions options = support.options().ageJitter(1.5);
            assertThrows(RuntimeException.class, () -> ElysiumKV.open(options));
        }
    }

    @Test
    void aNegativeFractionIsRefused(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVOptions options = support.options().flushIntervalJitter(-0.1);
            assertThrows(RuntimeException.class, () -> ElysiumKV.open(options));
        }
    }

    /** NaN passes every range check it is asked, so it has to be refused by shape. */
    @Test
    void aNaNFractionIsRefused(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVOptions options = support.options().ageJitter(Double.NaN);
            assertThrows(RuntimeException.class, () -> ElysiumKV.open(options));
        }
    }
}
