package io.veridia.elysiumkv.partitioned.kafka;

import io.veridia.elysiumkv.partitioned.Mutation;
import io.veridia.elysiumkv.partitioned.WriteSink;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The replay side: where it resumes from, where it stops, and what it reports along the way.
 *
 * <p>The class under test is a worked example rather than shipped API, and these cases are the reason
 * it is kept at all: the two off-by-ones below are the ones this design has repeatedly got wrong, and
 * a caller writing their own replay should be able to check theirs against them.
 */
class ExampleStateRestoreTest {

    private static final String TOPIC = "state";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final PrefixedMutationCodec CODEC = new PrefixedMutationCodec();

    /** One applied batch, as the sink saw it. */
    private static final class Applied {
        final long through;
        final Map<String, Mutation> mutations;

        Applied(long through, Map<String, Mutation> mutations) {
            this.through = through;
            this.mutations = mutations;
        }
    }

    private MockConsumer<byte[], byte[]> consumerWith(int records, long endOffset) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        // MockConsumer refuses records for an unassigned partition, so the fixture assigns first.
        // The restore assigns again, which is what a caller's consumer would do anyway.
        consumer.assign(Collections.singletonList(PARTITION));
        consumer.updateBeginningOffsets(Collections.singletonMap(PARTITION, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(PARTITION, endOffset));
        for (int offset = 0; offset < records; ++offset) {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset,
                    ChangelogRecords.pointKey(("k" + offset).getBytes(StandardCharsets.UTF_8)),
                    CODEC.encode(Mutation.put(("v" + offset).getBytes(StandardCharsets.UTF_8)))));
        }
        return consumer;
    }

    private List<Applied> replay(MockConsumer<byte[], byte[]> consumer, OptionalLong from,
                                 int batchSize) {
        List<Applied> applied = new ArrayList<>();
        ExampleStateRestore<String> restore = new ExampleStateRestore<>(
                partition -> consumer, TOPIC,
                key -> new String(key, StandardCharsets.UTF_8), CODEC,
                Duration.ofMillis(10), batchSize);
        restore.restore(0, from, new WriteSink<String>() {
            @Override
            public void putBatch(long through, Map<String, Mutation> mutations) {
                applied.add(new Applied(through, new LinkedHashMap<>(mutations)));
            }

            @Override
            public void deleteRange(long through, byte[] lower, byte[] upper) {
                throw new UnsupportedOperationException("this replay carries no range records");
            }
        });
        return applied;
    }

    @Test
    @DisplayName("a cold restore replays from the beginning and reports each batch's position")
    void aColdRestoreReplaysEverything() {
        List<Applied> applied = replay(consumerWith(5, 5L), OptionalLong.empty(), 2);

        assertEquals(3, applied.size(), "5 records in batches of 2");
        assertEquals(1L, applied.get(0).through);
        assertEquals(3L, applied.get(1).through);
        assertEquals(4L, applied.get(2).through, "the trailing partial batch carries its own position");
        assertTrue(applied.get(0).mutations.containsKey("k0"));
        assertEquals(5, applied.stream().mapToInt(a -> a.mutations.size()).sum());
    }

    @Test
    @DisplayName("an incremental restore resumes AFTER the position it was given, not at it")
    void theResumePointIsExclusiveOfWhatIsHeld() {
        // materializedThrough = 2 means offsets 0..2 are already in the store.
        List<Applied> applied = replay(consumerWith(5, 5L), OptionalLong.of(2L), 8);

        assertEquals(1, applied.size());
        assertEquals(4L, applied.get(0).through);
        Map<String, Mutation> mutations = applied.get(0).mutations;
        assertEquals(2, mutations.size(), "only offsets 3 and 4 remained to replay");
        assertTrue(mutations.containsKey("k3") && mutations.containsKey("k4"));
        assertTrue(!mutations.containsKey("k2"),
                "resuming AT the held position replays a record the store already has; resuming one "
                        + "past it in the other direction would skip one it does not");
    }

    @Test
    @DisplayName("a store already at the end replays nothing")
    void nothingToDo() {
        assertTrue(replay(consumerWith(5, 5L), OptionalLong.of(4L), 8).isEmpty());
    }

    @Test
    @DisplayName("the replay stops at the last stable offset, not at whatever is buffered")
    void theEndOffsetBoundsTheReplay() {
        // Six records exist but the last stable offset is 4: the tail belongs to an open or aborted
        // transaction. Under read_uncommitted those would be restored as though they were state.
        List<Applied> applied = replay(consumerWith(6, 4L), OptionalLong.empty(), 8);

        assertEquals(1, applied.size());
        assertEquals(3L, applied.get(0).through);
        assertEquals(4, applied.get(0).mutations.size());
        assertTrue(!applied.get(0).mutations.containsKey("k4"),
                "records past the last stable offset are not state");
    }

    @Test
    @DisplayName("a tombstone in the changelog fails the restore rather than being skipped")
    void aTombstoneFailsTheReplay() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        consumer.assign(Collections.singletonList(PARTITION));
        consumer.updateBeginningOffsets(Collections.singletonMap(PARTITION, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(PARTITION, 1L));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(StandardCharsets.UTF_8),
                null));

        assertThrows(IllegalStateException.class,
                () -> replay(consumer, OptionalLong.empty(), 8));
    }
}
