package io.veridia.elysiumkv.partitioned;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.veridia.elysiumkv.MemoryBudget;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What happens to a database when the flush on its way out fails.
 *
 * <p>{@code revoke} removes a partition from the map before flushing it, so a flush that raises used
 * to leave a database nothing could reach and nothing could close — still running, still holding the
 * blob stores and manifest catalog the caller is entitled to release the moment {@code revoke}
 * returns. The only way to reclaim it was to end the process.
 */
class PartitionedStoreCloseTest {

    @TempDir Path root;

    /**
     * The flush fails, the close still happens, and the failure is what the caller sees.
     *
     * <p>Losing the memtable is the right trade here and is specific to this class: a partitioned
     * store is fed from a log and the durable watermark covers only what was flushed, so whoever
     * takes the partition next replays what was lost. A longer replay is cheaper than a database
     * that cannot be closed.
     */
    @Test
    void aFailedFlushStillClosesTheDatabase() throws IOException {
        // The observable. A live database charges the budget for its memtable and readers, and
        // releases the charge when it closes — so this distinguishes "closed without flushing" from
        // "left open and unreachable", which nothing else visible from Java does.
        try (MemoryBudget budget = new MemoryBudget(64L << 20);
             PartitionFixture fixture = new PartitionFixture(root)) {
            PartitionedStore<String> store = PartitionedStore.<String>builder()
                    .options(partition -> fixture.optionsFor(partition).memoryBudget(budget))
                    .keyBytes(PartitionFixture.KEY_BYTES)
                    .changelog(new Changelog<String>() {
                        @Override
                        public PendingPosition send(int p, String key, Mutation mutation) {
                            return () -> 0L;
                        }

                        @Override
                        public PendingPosition sendDeleteRange(int p, byte[] lo, byte[] hi) {
                            return () -> 0L;
                        }
                    })
                    // Nothing to replay: this test is about the way out, not the way in.
                    .restore((partition, materializedThrough, sink) -> { })
                    .build();
            store.assign(Set.of(0));
            store.begin();
            store.put(0, Map.of("k", Mutation.put("v".getBytes())));
            store.applyCommitted();

            // The flush writes an object; a store directory it cannot write to is the simplest way
            // to make that fail for real rather than by injection.
            assertTrue(budget.used() > 0, "an open database charges the budget");

            Path storeDir = root.resolve("partition-0").resolve("store");
            assumeTrue(readOnly(storeDir), "needs a filesystem where a directory can be made "
                    + "unwritable, which running as root defeats");

            assertThrows(RuntimeException.class, () -> store.revoke(Set.of(0)));

            // Closed anyway, and nothing else could have closed it: revoke removed the partition
            // before flushing, so the store no longer holds a reference to hand back. Left open, its
            // memtable and readers would still be charged here.
            assertEquals(0L, budget.used(),
                    "the database was left open and unreachable, still holding its memory");
            assertTrue(store.assignment().isEmpty(), "and it is gone from the assignment");

            writable(storeDir);
        }
    }

    private static boolean readOnly(Path directory) throws IOException {
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(directory);
        permissions.remove(PosixFilePermission.OWNER_WRITE);
        Files.setPosixFilePermissions(directory, permissions);
        Path probe = directory.resolve("writability-probe");
        try {
            Files.createFile(probe);
            Files.deleteIfExists(probe);
            return false;
        } catch (IOException expected) {
            return true;
        }
    }

    private static void writable(Path directory) throws IOException {
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(directory);
        permissions.add(PosixFilePermission.OWNER_WRITE);
        Files.setPosixFilePermissions(directory, permissions);
        assertEquals(true, Files.isWritable(directory));
    }
}
