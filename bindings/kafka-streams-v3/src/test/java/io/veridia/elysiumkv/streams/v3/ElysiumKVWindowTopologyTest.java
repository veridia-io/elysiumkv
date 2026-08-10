package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The window store under a real topology — the two operators that could not use this store at all
 * before it existed: a windowed aggregation and a stream-stream join.
 *
 * <p>These are the cases that catch what a store-level test cannot. Streams wraps the store in
 * metering, caching and change-logging layers, decides its lifecycle, and materializes it through a
 * builder that inspects its interfaces. Every defect the key-value store had — a throwing {@code
 * getPosition}, an adapter silently discarding record timestamps — lived in exactly that gap.
 */
class ElysiumKVWindowTopologyTest {
    private static final String INPUT = "input";
    private static final String OTHER = "other";
    private static final String OUTPUT = "output";
    private static final String STORE = "counts";

    private static Properties config(Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "elysiumkv-window-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    /** A five-second tumbling count, materialized into the store under test. */
    private static Topology countingTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.Long()))
               .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
               .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
               .count(Materialized.as(ElysiumKVWindowBytesStoreSupplier.of(
                       STORE, ElysiumKVStoreConfig.local(), Duration.ofMinutes(10),
                       Duration.ofSeconds(5), false)))
               .toStream()
               .map((windowed, count) -> KeyValue.pair(
                       windowed.key() + "@" + windowed.window().start(), count))
               .to(OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    /**
     * <b>A windowed aggregation, end to end.</b> Counts are per window, so a store that lost an
     * entry, returned one from the wrong window, or could not be materialized at all produces wrong
     * numbers rather than an error.
     */
    @Test
    void aWindowedCountProducesTheRightTotalsPerWindow(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(countingTopology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            TestOutputTopic<String, Long> output =
                    driver.createOutputTopic(OUTPUT, Serdes.String().deserializer(),
                                             Serdes.Long().deserializer());

            // Two records in the first five-second window, one in the third.
            input.pipeInput("a", 1L, Instant.ofEpochMilli(1_000L));
            input.pipeInput("a", 1L, Instant.ofEpochMilli(4_000L));
            input.pipeInput("a", 1L, Instant.ofEpochMilli(12_000L));

            List<KeyValue<String, Long>> results = output.readKeyValuesToList();
            assertFalse(results.isEmpty(), "the aggregation produced nothing at all");
            // The last update for each window is what it settled on.
            long firstWindow = -1;
            long thirdWindow = -1;
            for (KeyValue<String, Long> entry : results) {
                if (entry.key.equals("a@0")) firstWindow = entry.value;
                if (entry.key.equals("a@10000")) thirdWindow = entry.value;
            }
            assertEquals(2L, firstWindow, "two records fell in [0, 5000)");
            assertEquals(1L, thirdWindow, "one record fell in [10000, 15000)");
        }
    }

    /** The materialized store is readable afterwards, and holds what the aggregation put there. */
    @Test
    void theMaterializedStoreHoldsThePerWindowCounts(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(countingTopology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 1L, Instant.ofEpochMilli(1_000L));
            input.pipeInput("a", 1L, Instant.ofEpochMilli(4_000L));
            input.pipeInput("b", 1L, Instant.ofEpochMilli(2_000L));

            WindowStore<String, ValueAndTimestamp<Long>> store =
                    driver.getTimestampedWindowStore(STORE);

            List<String> seen = new ArrayList<>();
            try (KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>> all = store.all()) {
                while (all.hasNext()) {
                    KeyValue<Windowed<String>, ValueAndTimestamp<Long>> entry = all.next();
                    seen.add(entry.key.key() + "@" + entry.key.window().start() + "="
                             + entry.value.value());
                }
            }
            assertEquals(List.of("a@0=2", "b@0=1"), seen);
        }
    }

    /**
     * Record timestamps survive a round trip through the materialized store.
     *
     * <p>Stated narrowly on purpose: this does <em>not</em> pin the {@code TimestampedBytesStore}
     * marker. Removing the marker demonstrably changes what reaches the store — a counted window
     * arrives as eight bytes instead of sixteen, the adapter having stripped the timestamp — and
     * this assertion still passes, so something further up reconstructs it on this path. What the
     * marker does guard is covered by the mismatch test in {@link ElysiumKVWindowVariantTest}.
     */
    @Test
    void recordTimestampsSurviveTheRoundTrip(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(countingTopology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 1L, Instant.ofEpochMilli(4_321L));

            WindowStore<String, ValueAndTimestamp<Long>> store =
                    driver.getTimestampedWindowStore(STORE);
            try (KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>> all = store.all()) {
                assertTrue(all.hasNext());
                ValueAndTimestamp<Long> stored = all.next().value;
                assertEquals(1L, stored.value());
                assertEquals(4_321L, stored.timestamp(),
                             "the record's own timestamp, not a fabricated -1");
            }
        }
    }

    /**
     * <b>A stream-stream join</b>, which is the operator that needs {@code retainDuplicates} and the
     * one this store existed least for. Streams builds two window stores here, both ours.
     */
    @Test
    void aStreamStreamJoinMatchesRecordsAcrossTheWindow(@TempDir Path dir) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.Long()))
               .join(builder.stream(OTHER, Consumed.with(Serdes.String(), Serdes.Long())),
                     (left, right) -> left + right,
                     JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
                     StreamJoined.with(Serdes.String(), Serdes.Long(), Serdes.Long())
                             .withStoreName("join")
                             // Streams checks these against the JoinWindows and refuses a
                             // mismatch: for a join the window is the full time difference either
                             // side, and retention is that plus the grace period.
                             .withThisStoreSupplier(ElysiumKVWindowBytesStoreSupplier.plain(
                                     "join-this", ElysiumKVStoreConfig.local(),
                                     Duration.ofSeconds(20), Duration.ofSeconds(20), true))
                             .withOtherStoreSupplier(ElysiumKVWindowBytesStoreSupplier.plain(
                                     "join-other", ElysiumKVStoreConfig.local(),
                                     Duration.ofSeconds(20), Duration.ofSeconds(20), true)))
               .to(OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(dir))) {
            TestInputTopic<String, Long> left =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            TestInputTopic<String, Long> right =
                    driver.createInputTopic(OTHER, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            TestOutputTopic<String, Long> output =
                    driver.createOutputTopic(OUTPUT, Serdes.String().deserializer(),
                                             Serdes.Long().deserializer());

            left.pipeInput("k", 10L, Instant.ofEpochMilli(1_000L));
            right.pipeInput("k", 5L, Instant.ofEpochMilli(3_000L));    // inside the window
            right.pipeInput("k", 7L, Instant.ofEpochMilli(60_000L));   // far outside it

            List<KeyValue<String, Long>> joined = output.readKeyValuesToList();
            assertEquals(1, joined.size(), "exactly the pair inside the join window");
            assertEquals(15L, joined.get(0).value);
        }
    }

    // --- IQv2 ----------------------------------------------------------------

    private static <R> QueryResult<R> run(TopologyTestDriver driver, Query<R> query) {
        StateStore store = (StateStore) driver.getTimestampedWindowStore(STORE);
        return store.query(query, PositionBound.unbounded(), new QueryConfig(false));
    }

    /**
     * <b>IQv2 against a window store.</b> Without {@code query}, {@code KafkaStreams.query(...)}
     * reports every window query as unsupported and an outside caller cannot read this store at all.
     */
    @Test
    void aWindowKeyQueryReturnsTheWindowsForOneKey(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(countingTopology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 1L, Instant.ofEpochMilli(1_000L));
            input.pipeInput("a", 1L, Instant.ofEpochMilli(12_000L));
            input.pipeInput("b", 1L, Instant.ofEpochMilli(1_000L));

            QueryResult<WindowStoreIterator<ValueAndTimestamp<Long>>> result =
                    run(driver, WindowKeyQuery.withKeyAndWindowStartRange(
                            "a", Instant.ofEpochMilli(0L), Instant.ofEpochMilli(60_000L)));

            assertTrue(result.isSuccess(), () -> "query failed: " + result.getFailureMessage());
            List<String> seen = new ArrayList<>();
            try (WindowStoreIterator<ValueAndTimestamp<Long>> it = result.getResult()) {
                while (it.hasNext()) {
                    KeyValue<Long, ValueAndTimestamp<Long>> entry = it.next();
                    seen.add(entry.key + "=" + entry.value.value());
                }
            }
            assertEquals(List.of("0=1", "10000=1"), seen, "only key a's windows, in time order");
        }
    }

    @Test
    void aWindowRangeQueryReturnsEveryKeysWindowsInTheSpan(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(countingTopology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 1L, Instant.ofEpochMilli(1_000L));
            input.pipeInput("b", 1L, Instant.ofEpochMilli(2_000L));
            input.pipeInput("c", 1L, Instant.ofEpochMilli(30_000L));

            QueryResult<KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>>> result =
                    run(driver, WindowRangeQuery.withWindowStartRange(
                            Instant.ofEpochMilli(0L), Instant.ofEpochMilli(10_000L)));

            assertTrue(result.isSuccess(), () -> "query failed: " + result.getFailureMessage());
            List<String> seen = new ArrayList<>();
            try (KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>> it = result.getResult()) {
                while (it.hasNext()) {
                    KeyValue<Windowed<String>, ValueAndTimestamp<Long>> entry = it.next();
                    seen.add(entry.key.key() + "@" + entry.key.window().start());
                }
            }
            assertEquals(List.of("a@0", "b@0"), seen, "c's window starts outside the span");
        }
    }

    /**
     * A key-only window range query is a <em>session</em>-store query. Declining it as an unknown
     * type is the honest answer; guessing at a meaning would be worse than refusing.
     */
    @Test
    void aKeyOnlyWindowRangeQueryIsDeclined(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(countingTopology(), config(dir))) {
            driver.createInputTopic(INPUT, Serdes.String().serializer(), Serdes.Long().serializer())
                  .pipeInput("a", 1L, Instant.ofEpochMilli(1_000L));

            QueryResult<KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>>> result =
                    run(driver, WindowRangeQuery.withKey("a"));

            assertTrue(result.isFailure());
            assertEquals(FailureReason.UNKNOWN_QUERY_TYPE, result.getFailureReason());
        }
    }
}
