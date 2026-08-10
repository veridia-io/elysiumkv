package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The session store under a real topology — where session <em>merging</em> happens.
 *
 * <p>This is what a store-level test cannot reach. A record arriving between two existing sessions
 * makes Streams remove both and put back one spanning the pair, so the store is asked for a precise
 * sequence of removes and puts. Getting the key layout subtly wrong shows up here as a session that
 * failed to merge, not as an error.
 */
class ElysiumKVSessionTopologyTest {
    private static final String INPUT = "input";
    private static final String STORE = "sessions";

    private static Properties config(Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "elysiumkv-session-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    /** A session count with a 30-second inactivity gap. */
    private static Topology countingTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.Long()))
               .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
               .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30)))
               .count(Materialized.as(ElysiumKVSessionBytesStoreSupplier.of(
                       STORE, ElysiumKVStoreConfig.local(), Duration.ofMinutes(10))));
        return builder.build();
    }

    private static List<String> sessions(TopologyTestDriver driver) {
        SessionStore<String, Long> store = driver.getSessionStore(STORE);
        List<String> out = new ArrayList<>();
        try (KeyValueIterator<Windowed<String>, Long> it = store.fetch("a")) {
            while (it.hasNext()) {
                KeyValue<Windowed<String>, Long> entry = it.next();
                out.add(entry.key.window().start() + "-" + entry.key.window().end() + "="
                        + entry.value);
            }
        }
        return out;
    }

    /** Two records inside the gap belong to one session. */
    @Test
    void recordsWithinTheGapFormOneSession(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(countingTopology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 1L, Instant.ofEpochMilli(1_000L));
            input.pipeInput("a", 1L, Instant.ofEpochMilli(10_000L));

            assertEquals(List.of("1000-10000=2"), sessions(driver));
        }
    }

    /** A gap longer than the inactivity window starts a second session. */
    @Test
    void aGapLongerThanTheInactivityWindowSplitsTheSession(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(countingTopology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 1L, Instant.ofEpochMilli(1_000L));
            input.pipeInput("a", 1L, Instant.ofEpochMilli(100_000L));

            assertEquals(List.of("1000-1000=1", "100000-100000=1"), sessions(driver));
        }
    }

    /**
     * <b>The merge.</b> A record arriving between two sessions closes the gap, and Streams asks the
     * store to remove both and write one spanning them. A store whose removes miss leaves the
     * originals behind — visible here as three sessions instead of one.
     */
    @Test
    void aBridgingRecordMergesTwoSessionsIntoOne(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(countingTopology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 1L, Instant.ofEpochMilli(0L));
            input.pipeInput("a", 1L, Instant.ofEpochMilli(60_000L));
            assertEquals(2, sessions(driver).size(), "60s apart, so two sessions to begin with");

            // Lands within 30s of both, so the two become one.
            input.pipeInput("a", 1L, Instant.ofEpochMilli(30_000L));

            assertEquals(List.of("0-60000=3"), sessions(driver),
                         "both originals removed and one session written in their place");
        }
    }

    /** IQv2: the key form of a window range query is the session query. */
    @Test
    void aWindowRangeQueryWithAKeyReturnsThatKeysSessions(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(countingTopology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 1L, Instant.ofEpochMilli(1_000L));
            input.pipeInput("b", 1L, Instant.ofEpochMilli(1_000L));

            StateStore store = (StateStore) driver.<String, Long>getSessionStore(STORE);
            QueryResult<KeyValueIterator<Windowed<String>, Long>> result =
                    store.query(WindowRangeQuery.withKey("a"), PositionBound.unbounded(),
                                new QueryConfig(false));

            assertTrue(result.isSuccess(), () -> "query failed: " + result.getFailureMessage());
            List<String> seen = new ArrayList<>();
            try (KeyValueIterator<Windowed<String>, Long> it = result.getResult()) {
                while (it.hasNext()) seen.add(it.next().key.key());
            }
            assertEquals(List.of("a"), seen, "only key a's sessions");
        }
    }
}
