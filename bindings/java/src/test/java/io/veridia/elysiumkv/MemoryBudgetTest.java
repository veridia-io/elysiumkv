package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ARCHITECTURE.md "A process-wide memory budget" — the budget is per process, and the case it exists for is several
 * instances in one JVM. That is the case this checks: two databases, one budget, and the
 * charge visible from Java.
 *
 * <p>It could not be written before — the C ABI had no way to express a shared budget, so
 * a Java embedder running one instance per shard had no ceiling at all beyond
 * per-instance settings multiplied by the shard count.
 */
class MemoryBudgetTest {
    private ElysiumKVOptions optionsFor(Path dir, MemoryBudget budget, TestSupport support) {
        return support.own(new ElysiumKVOptions()
                                   .manifestCatalog(support.own(new DiskManifestCatalog(
                                           dir.toString())))
                                   .memoryBudget(budget)
                                   .memtableBytes(1 << 20)
                                   .blockBytes(1024)
                                   .addTier(support.hot, Durability.DURABLE, 0, 0, 0)
                                   .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                                   .level(1, Compression.NONE, 0, 0, 0, 0, 0));
    }

    @Test
    void twoInstancesInOneProcessShareOneBudget(@TempDir Path dir) throws IOException {
        Path firstDir = dir.resolve("first");
        Path secondDir = dir.resolve("second");
        Files.createDirectories(firstDir);
        Files.createDirectories(secondDir);

        try (MemoryBudget budget = new MemoryBudget(64L << 20);
             TestSupport first = new TestSupport(firstDir);
             TestSupport second = new TestSupport(secondDir)) {
            assertEquals(0L, budget.used(), "nothing is charged before anything opens");

            try (ElysiumKV a = PinLeakExtension.watch(
                         ElysiumKV.open(optionsFor(firstDir, budget, first)));
                 ElysiumKV b = PinLeakExtension.watch(
                         ElysiumKV.open(optionsFor(secondDir, budget, second)))) {
                for (int i = 0; i < 500; ++i) {
                    a.put(TestSupport.key(i), TestSupport.bytes("a" + i));
                }
                long afterFirst = budget.used();
                assertTrue(afterFirst > 0, "a memtable must charge the budget");

                for (int i = 0; i < 500; ++i) {
                    b.put(TestSupport.key(i), TestSupport.bytes("b" + i));
                }
                assertTrue(budget.used() > afterFirst,
                           "the second instance charges the *same* budget — the whole point of "
                                   + "it being per process");

                // Both instances see the shared figures, not their own share of them.
                ElysiumKVStats statsA = a.stats();
                ElysiumKVStats statsB = b.stats();
                assertEquals(64L << 20, statsA.memoryBudgetTotal());
                assertEquals(statsA.memoryBudgetTotal(), statsB.memoryBudgetTotal());
                assertEquals(statsA.memoryBudgetUsed(), statsB.memoryBudgetUsed());
                assertEquals(0L, statsA.budgetSheds(), "this budget is generous; nothing shed");
            }
        }
    }

    /** A budget far too small must slow writes down, never fail them. */
    @Test
    void anExhaustedBudgetDoesNotFailWrites(@TempDir Path dir) throws IOException {
        try (MemoryBudget budget = new MemoryBudget(8192);
             TestSupport support = new TestSupport(dir)) {
            try (ElysiumKV db = PinLeakExtension.watch(
                         ElysiumKV.open(optionsFor(dir, budget, support)))) {
                for (int i = 0; i < 300; ++i) {
                    db.put(TestSupport.key(i), TestSupport.bytes("value:" + i));
                }
                assertTrue(db.stats().budgetSheds() > 0,
                           "the budget was exceeded, so shedding must have run");
                for (int i = 0; i < 300; ++i) {
                    assertTrue(db.getCopy(TestSupport.key(i)) != null, "key " + i);
                }
            }
        }
    }
}
