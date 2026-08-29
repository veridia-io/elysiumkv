package io.veridia.elysiumkv.partitioned;

import io.veridia.elysiumkv.ElysiumKVOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static io.veridia.elysiumkv.partitioned.InMemoryLog.bytes;
import static io.veridia.elysiumkv.partitioned.InMemoryLog.string;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What survives a crash, and what the store is willing to claim afterwards.
 *
 * <p>Every assertion here is also satisfied by a watermark that is always absent, and that failure
 * is silent — a full replay every time looks exactly like the feature working. So each case asserts
 * both directions: that nothing is claimed which is not held, <em>and</em> that something is claimed
 * at all.
 */
class PartitionedStoreDurabilityTest {

    private static final int KEYS_PER_BATCH = 40;
    private static final int VALUE_BYTES = 64;

    @TempDir
    Path root;

    private PartitionFixture fixture;
    private InMemoryLog log;
    private List<OptionalLong> resumePoints;
    private PartitionedStore<String> store;

    @BeforeEach
    void setUp() {
        fixture = new PartitionFixture(root);
        log = new InMemoryLog();
        resumePoints = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
        fixture.close();
    }

    private PartitionedStore<String> open(IntFunction<ElysiumKVOptions> options, boolean replay) {
        return PartitionedStore.<String>builder()
                .options(options)
                .keyBytes(PartitionFixture.KEY_BYTES)
                .changelog(new Changelog<String>() {
                    @Override
                    public PendingPosition send(int partition, String key, Mutation mutation) {
                        return log.send(partition, key, mutation);
                    }

                    @Override
                    public PendingPosition sendDeleteRange(int partition, byte[] lo, byte[] hi) {
                        return log.sendDeleteRange(partition, lo, hi);
                    }
                })
                .restore((partition, materializedThrough, sink) -> {
                    resumePoints.add(materializedThrough);
                    if (replay) {
                        log.restoreIn(64).restore(partition, materializedThrough, sink);
                    }
                })
                .build();
    }

    /** One commit of distinct keys, so no record is ever superseded by a later one. */
    private void commitBatch(int batch, Map<String, String> expected) {
        Map<String, Mutation> updates = new LinkedHashMap<>();
        for (int i = 0; i < KEYS_PER_BATCH; ++i) {
            String key = "b" + batch + ":k" + i;
            StringBuilder value = new StringBuilder(VALUE_BYTES);
            while (value.length() < VALUE_BYTES) {
                value.append(key).append('.');
            }
            String truncated = value.substring(0, VALUE_BYTES);
            updates.put(key, Mutation.put(bytes(truncated)));
            if (expected != null) {
                expected.put(key, truncated);
            }
        }
        store.put(0, updates);
        store.commit(log::commitTransaction);
    }

    private String read(String key) {
        return string(store.get(0, key));
    }

    // -------------------------------------------------------------------------

    /**
     * A range delete is a tombstone that every read in the band consults until compaction resolves
     * it, and on a tiered store the files it covers are migrating while that happens. This is the one
     * interaction the single-tier tests cannot reach: the band spans a durable tier holding migrated
     * files and a transient tier holding the newest ones.
     */
    @Test
    @DisplayName("a range delete covering both tiers stays deleted, and stays deleted through a rebuild")
    void aRangeDeleteSpanningBothTiersSurvivesARebuild() {
        store = open(fixture::tieredOptionsFor, true);
        store.assign(Collections.singletonList(0));

        Map<String, String> expected = new LinkedHashMap<>();
        Path hot = fixture.hotDir(0);
        Path cold = fixture.coldDir(0);

        // Driven by volume rather than by a sleep: migration is a consequence of capacity here.
        int batch = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (PartitionFixture.sstCount(cold) == 0 && System.nanoTime() < deadline) {
            commitBatch(batch++, expected);
        }
        assertTrue(PartitionFixture.sstCount(cold) > 0,
                "nothing migrated, so the band would span a single tier and this asserts nothing");

        // Two more, which stay on the transient tier as the newest files.
        commitBatch(batch++, expected);
        commitBatch(batch++, expected);
        assertTrue(PartitionFixture.sstCount(hot) > 0,
                "nothing on the transient tier for the band to cover");

        // Every key written so far sorts inside this band, on both tiers.
        store.deleteRange(0, bytes("b"), bytes("c"));
        store.commit(log::commitTransaction);

        for (String key : expected.keySet()) {
            assertNull(read(key), key + " is inside the deleted band");
        }

        // And the band does not come back when the whole log is replayed into a fresh store.
        store.close();
        store = null;
        PartitionFixture replica = new PartitionFixture(root.resolve("replica"));
        try {
            resumePoints.clear();
            store = open(replica::tieredOptionsFor, true);
            store.assign(Collections.singletonList(0));
            assertEquals(OptionalLong.empty(), resumePoints.get(0), "a cold store asks for everything");
            for (String key : expected.keySet()) {
                assertNull(read(key), key + " must not be resurrected by the replay");
            }
        } finally {
            replica.close();
        }
    }

    @Test
    @DisplayName("a lost transient file is not hidden behind a newer file's bigger watermark")
    void aLostTransientFileIsNotHiddenBehindABiggerWatermark() {
        store = open(fixture::tieredOptionsFor, true);
        store.assign(Collections.singletonList(0));

        Map<String, String> expected = new LinkedHashMap<>();
        Path hot = fixture.hotDir(0);
        Path cold = fixture.coldDir(0);

        // Write until the oldest files have aged off the transient tier into the durable one. Driven
        // by volume rather than by a sleep: migration is a consequence of capacity here.
        int batch = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (PartitionFixture.sstCount(cold) == 0 && System.nanoTime() < deadline) {
            commitBatch(batch++, expected);
        }
        assertTrue(PartitionFixture.sstCount(cold) > 0,
                "nothing migrated to the durable tier, so there is no surviving file to over-report from");

        // Two more, which are the newest and so stay on the transient tier — carrying the *bigger*
        // watermarks that could mask the loss of everything under them.
        commitBatch(batch++, expected);
        commitBatch(batch++, expected);

        store.revoke(Collections.singletonList(0));    // flushes, so the tail is on disk too
        assertTrue(PartitionFixture.sstCount(hot) > 0, "nothing on the transient tier to lose");

        int lost = PartitionFixture.wipe(hot);
        assertTrue(lost > 0, "the wipe removed nothing, so the crash was not simulated");

        long highestCommitted = log.size(0) - 1;
        resumePoints.clear();
        store.assign(Collections.singletonList(0));

        // The rollback itself. Reporting the lost files' upper bound would be the silent failure:
        // it resumes past writes that went with them and never replays those records again.
        OptionalLong resume = resumePoints.get(0);
        assertTrue(!resume.isPresent() || resume.getAsLong() < highestCommitted,
                "the reported position must roll back below the lost writes, not stay at "
                        + highestCommitted);

        // And the consequence that actually matters: nothing was skipped.
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), read(entry.getKey()),
                    "key " + entry.getKey() + " was lost with the transient tier and never replayed");
        }
    }

    @Test
    @DisplayName("a kill between the apply and the flush never leaves the watermark ahead of the state")
    void theWatermarkNeverLeadsTheState() {
        // No replay on assign, so what the store holds is exactly what survived the kill.
        store = open(fixture::optionsFor, false);
        store.assign(Collections.singletonList(0));

        Map<String, String> expected = new LinkedHashMap<>();
        for (int batch = 0; batch < 40; ++batch) {
            commitBatch(batch, expected);
        }

        // closeWithoutFlush is what a crash leaves: the memtable, and the watermark in it, are gone.
        store.lost(Collections.singletonList(0));
        resumePoints.clear();
        store.assign(Collections.singletonList(0));

        OptionalLong resume = resumePoints.get(0);
        assertTrue(resume.isPresent(),
                "nothing was certified at all, so this proves only that absence is safe");

        // Every record the store claims to hold, it must actually hold. Keys are distinct per
        // record, so offset N's key is present if and only if that write survived.
        List<InMemoryLog.Record> records = log.committed(0);
        for (int offset = 0; offset <= resume.getAsLong(); ++offset) {
            String key = records.get(offset).key;
            assertNotNull(read(key),
                    "the watermark claims position " + resume.getAsLong() + ", but the write at "
                            + offset + " (" + key + ") is not in the store");
        }
    }

    @Test
    @DisplayName("a restore larger than one memtable keeps its position and its writes together")
    void aRestoreLargerThanOneMemtableIsConsistent() {
        store = open(fixture::tieredOptionsFor, true);
        store.assign(Collections.singletonList(0));
        Map<String, String> expected = new LinkedHashMap<>();
        // 60 batches of 40 keys at 64 bytes is well past the 4 KB memtable, so the replay below
        // crosses real flush boundaries rather than living in one memtable.
        for (int batch = 0; batch < 60; ++batch) {
            commitBatch(batch, expected);
        }
        store.close();
        store = null;

        PartitionFixture replica = new PartitionFixture(root.resolve("replica"));
        try {
            resumePoints.clear();
            store = open(replica::tieredOptionsFor, true);
            store.assign(Collections.singletonList(0));
            assertEquals(OptionalLong.empty(), resumePoints.get(0), "a cold store asks for everything");

            for (Map.Entry<String, String> entry : expected.entrySet()) {
                assertEquals(entry.getValue(), read(entry.getKey()));
            }

            store.revoke(Collections.singletonList(0));
            store.assign(Collections.singletonList(0));

            // Within one restore batch of the end, not exactly at it. A batch whose write seals the
            // memtable leaves the stamp that follows it on a fresh, empty one, and flushing an empty
            // memtable produces no file for it to ride — so the last batch's position can be dropped.
            // That is the safe direction (the tail is simply replayed again, idempotently) and the
            // reason the assertion is a bound rather than an equality. What it still rules out is a
            // restore whose position never advanced at all, which is what the sink exists to prevent.
            long end = log.size(0) - 1L;
            OptionalLong resumed = resumePoints.get(1);
            assertTrue(resumed.isPresent(), "the restore certified nothing");
            assertTrue(resumed.getAsLong() > end - 64,
                    "the restore must carry its position across the flushes it causes: expected within "
                            + "one batch of " + end + ", got " + resumed.getAsLong());
            for (Map.Entry<String, String> entry : expected.entrySet()) {
                assertEquals(entry.getValue(), read(entry.getKey()), "lost across the reopen");
            }
            store.close();
            store = null;
        } finally {
            replica.close();
        }
    }
}
