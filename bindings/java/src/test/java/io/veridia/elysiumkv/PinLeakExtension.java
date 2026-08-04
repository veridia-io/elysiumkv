package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * ARCHITECTURE.md "Dependencies and artifacts" step 10's second green criterion: {@code pins_outstanding} is zero after
 * every test.
 *
 * <p>This is not tidiness. A pin holds a block-cache entry that can never be
 * evicted, so a leak in a binding shows up much later as a cache that stops
 * caching. Checking once, in one test, would only prove that one path is clean;
 * checking after every test makes it a property of the suite.
 */
final class PinLeakExtension implements AfterEachCallback {
    private static final List<ElysiumKV> watched = new ArrayList<>();

    static <T extends ElysiumKV> T watch(T db) {
        watched.add(db);
        return db;
    }

    @Override
    public void afterEach(ExtensionContext context) {
        try {
            for (ElysiumKV db : watched) {
                if (db.isOpen()) {
                    assertEquals(0, db.pinsOutstanding(),
                                 () -> "pins left outstanding by " + context.getDisplayName()
                                         + "; a leaked pin holds a block-cache entry forever");
                }
            }
        } finally {
            watched.clear();
        }
    }
}
