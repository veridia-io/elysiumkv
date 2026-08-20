package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * IQv2 — {@code StateStore.query} — against the store.
 *
 * <p>Distinct from the topology tests next door because it exercises the read path an <em>outside
 * caller</em> uses: {@code KafkaStreams.query(...)} reaches the store through {@code query} rather
 * than through the {@code KeyValueStore} methods, and until this was implemented the default
 * reported every query as unsupported.
 *
 * <p>Caching is disabled throughout. {@code CachingKeyValueStore} answers {@code KeyQuery}
 * out of its cache without consulting the store beneath it, so with caching on these tests would
 * pass no matter what this store did — they would be testing Kafka.
 */
class ElysiumKVInteractiveQueryTest {
    private static final String INPUT = "input";
    private static final String STORE = "totals";

    private static Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.Long()))
               .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
               .reduce(Long::sum,
                       Materialized.<String, Long>as(new ElysiumKVKeyValueBytesStoreSupplier(
                                       STORE, ElysiumKVStoreConfig.local()))
                               .withKeySerde(Serdes.String())
                               .withValueSerde(Serdes.Long())
                               .withCachingDisabled());
        return builder.build();
    }

    private static Properties config(Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "elysiumkv-iq-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    /** Feeds the fixture used by every test: a=1+2=3, b=10, c=100. */
    private static StateStore storeWith(TopologyTestDriver driver) {
        TestInputTopic<String, Long> input =
                driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                        Serdes.Long().serializer());
        input.pipeInput("a", 1L);
        input.pipeInput("b", 10L);
        input.pipeInput("a", 2L);
        input.pipeInput("c", 100L);
        // Not getKeyValueStore(): that hands back a facade of the test driver's own, which declines
        // every query without consulting the store. Reaching the real chain is the whole point here.
        return (StateStore) driver.<String, Long>getTimestampedKeyValueStore(STORE);
    }

    private static <R> QueryResult<R> run(StateStore store, Query<R> query) {
        return run(store, query, PositionBound.unbounded());
    }

    private static <R> QueryResult<R> run(StateStore store, Query<R> query, PositionBound bound) {
        return store.query(query, bound, new QueryConfig(false));
    }

    @Test
    void aKeyQueryReturnsTheCurrentValue(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);

            QueryResult<Long> result = run(store, KeyQuery.<String, Long>withKey("a"));

            assertTrue(result.isSuccess(), () -> "query failed: " + result.getFailureMessage());
            assertEquals(3L, result.getResult(), "the aggregate, read through IQv2");
        }
    }

    /**
     * A key that is not there is a successful query with a null result, not a failure. The
     * distinction is the caller's to act on: absent and broken deserve different handling, and
     * collapsing them into a failure would make an ordinary miss look like an outage.
     */
    @Test
    void aKeyQueryForAnAbsentKeySucceedsWithNoValue(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);

            QueryResult<Long> result = run(store, KeyQuery.<String, Long>withKey("absent"));

            assertTrue(result.isSuccess(), "a miss is not a failure");
            assertNull(result.getResult());
        }
    }

    @Test
    void aRangeQueryReturnsExactlyTheKeysInTheRange(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);

            QueryResult<KeyValueIterator<String, Long>> result =
                    run(store, RangeQuery.<String, Long>withRange("a", "b"));

            assertTrue(result.isSuccess(), () -> "query failed: " + result.getFailureMessage());
            // Inclusive of the upper bound, which is the Streams range contract rather than the
            // engine's — the store bridges the two.
            assertEquals(List.of(KeyValue.pair("a", 3L), KeyValue.pair("b", 10L)),
                         drain(result.getResult()));
        }
    }

    @Test
    void anUnboundedRangeQueryReturnsEverything(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);

            QueryResult<KeyValueIterator<String, Long>> result =
                    run(store, RangeQuery.withNoBounds());

            assertTrue(result.isSuccess(), () -> "query failed: " + result.getFailureMessage());
            assertEquals(List.of(KeyValue.pair("a", 3L), KeyValue.pair("b", 10L),
                                 KeyValue.pair("c", 100L)),
                         drain(result.getResult()));
        }
    }

    @Test
    void aHalfOpenRangeQueryIsBoundedOnOneSideOnly(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);

            assertEquals(List.of(KeyValue.pair("b", 10L), KeyValue.pair("c", 100L)),
                         drain(run(store, RangeQuery.<String, Long>withLowerBound("b"))
                                       .getResult()));
            assertEquals(List.of(KeyValue.pair("a", 3L), KeyValue.pair("b", 10L)),
                         drain(run(store, RangeQuery.<String, Long>withUpperBound("b"))
                                       .getResult()));
        }
    }

    /**
     * A descending range is served, streamed. The engine iterates backwards natively, so the
     * result is produced one entry at a time in both directions — a range larger than memory is
     * answerable descending, which is the reason not to fake this by buffering and reversing.
     */
    @Test
    void aDescendingRangeQueryReturnsTheRangeInReverse(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);

            QueryResult<KeyValueIterator<String, Long>> result =
                    run(store, RangeQuery.<String, Long>withNoBounds().withDescendingKeys());

            assertTrue(result.isSuccess(), () -> "query failed: " + result.getFailureMessage());
            assertEquals(List.of(KeyValue.pair("c", 100L), KeyValue.pair("b", 10L),
                                 KeyValue.pair("a", 3L)),
                         drain(result.getResult()));
        }
    }

    /** Descending honours the bounds too, and they keep their forward inclusivity. */
    @Test
    void aBoundedDescendingRangeQueryStaysInsideItsBounds(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);

            QueryResult<KeyValueIterator<String, Long>> result =
                    run(store, RangeQuery.<String, Long>withRange("a", "b").withDescendingKeys());

            assertTrue(result.isSuccess(), () -> "query failed: " + result.getFailureMessage());
            assertEquals(List.of(KeyValue.pair("b", 10L), KeyValue.pair("a", 3L)),
                         drain(result.getResult()));
        }
    }

    /**
     * A position bound is a freshness demand. Naming an offset the store has not reached must be
     * refused — serving the older state would answer a question the caller did not ask.
     */
    @Test
    void aPositionBoundAheadOfTheStoreIsRefused(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);
            PositionBound unreached =
                    PositionBound.at(Position.emptyPosition().withComponent(INPUT, 0, 9_999L));

            QueryResult<Long> result = run(store, KeyQuery.<String, Long>withKey("a"), unreached);

            assertTrue(result.isFailure(), "the store has not consumed that far");
            assertEquals(FailureReason.NOT_UP_TO_BOUND, result.getFailureReason());
        }
    }

    @Test
    void aPositionBoundTheStoreHasReachedIsServed(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);
            // Four records went in at offsets 0..3, so offset 3 is exactly reached: the comparison
            // is inclusive, and an off-by-one here would refuse a query it should serve.
            PositionBound reached =
                    PositionBound.at(Position.emptyPosition().withComponent(INPUT, 0, 3L));

            QueryResult<Long> result = run(store, KeyQuery.<String, Long>withKey("a"), reached);

            assertTrue(result.isSuccess(), () -> "should have been served: " + result);
            assertEquals(3L, result.getResult());
        }
    }

    /** A bound about someone else's partition says nothing about this store, so it is ignored. */
    @Test
    void aPositionBoundOnAnotherPartitionDoesNotBlockTheQuery(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);
            PositionBound elsewhere =
                    PositionBound.at(Position.emptyPosition().withComponent(INPUT, 7, 9_999L));

            QueryResult<Long> result = run(store, KeyQuery.<String, Long>withKey("a"), elsewhere);

            assertTrue(result.isSuccess(), () -> "should have been served: " + result);
            assertEquals(3L, result.getResult());
        }
    }

    /** The answer carries where it was read from, so a caller can bound its next query on it. */
    @Test
    void anAnsweredQueryReportsThePositionItWasServedFrom(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);

            QueryResult<Long> result = run(store, KeyQuery.<String, Long>withKey("a"));

            assertEquals(3L, result.getPosition().getPartitionPositions(INPUT).get(0),
                         "four records at offsets 0..3");
        }
    }

    /** An unrecognised query is declined as such, rather than answered wrongly or thrown from. */
    @Test
    void anUnknownQueryTypeIsDeclined(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            StateStore store = storeWith(driver);

            QueryResult<String> result = run(store, new Query<String>() { });

            assertTrue(result.isFailure());
            assertEquals(FailureReason.UNKNOWN_QUERY_TYPE, result.getFailureReason());
            assertFalse(result.getFailureMessage().isEmpty());
        }
    }

    /**
     * The same queries against the default, caching-enabled configuration — which is what
     * {@code Materialized} builds unless told otherwise, and so what most callers will actually be
     * querying. The paths differ: {@code CachingKeyValueStore} answers a {@code KeyQuery} from its
     * cache and only a range query reaches this store. Both must work, and only one of them is
     * covered by the tests above.
     */
    @Test
    void queriesWorkAgainstTheDefaultCachingConfiguration(@TempDir Path dir) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.Long()))
               .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
               .reduce(Long::sum,
                       Materialized.<String, Long>as(new ElysiumKVKeyValueBytesStoreSupplier(
                                       STORE, ElysiumKVStoreConfig.local()))
                               .withKeySerde(Serdes.String())
                               .withValueSerde(Serdes.Long()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(dir))) {
            StateStore store = storeWith(driver);

            QueryResult<Long> key = run(store, KeyQuery.<String, Long>withKey("a"));
            assertTrue(key.isSuccess(), () -> "key query failed: " + key.getFailureMessage());
            assertEquals(3L, key.getResult());

            QueryResult<KeyValueIterator<String, Long>> range =
                    run(store, RangeQuery.<String, Long>withRange("a", "b"));
            assertTrue(range.isSuccess(), () -> "range query failed: " + range.getFailureMessage());
            assertEquals(List.of(KeyValue.pair("a", 3L), KeyValue.pair("b", 10L)),
                         drain(range.getResult()));
        }
    }

    /**
     * Record timestamps survive. Not an IQv2 concern on its face, but the same cause: a store
     * that does not declare {@link org.apache.kafka.streams.state.TimestampedBytesStore} gets an
     * adapter spliced in front of it that drops the timestamp on write and returns {@code -1} on
     * read. Every {@code ValueAndTimestamp} this store produced was therefore stamped {@code -1} —
     * silently, since the value itself was right.
     */
    @Test
    void recordTimestampsSurviveTheRoundTrip(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            Instant when = Instant.ofEpochMilli(1_700_000_000_000L);
            input.pipeInput("a", 42L, when);

            ValueAndTimestamp<Long> stored =
                    driver.<String, Long>getTimestampedKeyValueStore(STORE).get("a");

            assertEquals(42L, stored.value());
            assertEquals(when.toEpochMilli(), stored.timestamp(),
                         "the record's own timestamp, not a fabricated -1");
        }
    }

    private static List<KeyValue<String, Long>> drain(KeyValueIterator<String, Long> iterator) {
        List<KeyValue<String, Long>> out = new ArrayList<>();
        try (KeyValueIterator<String, Long> it = iterator) {
            while (it.hasNext()) {
                out.add(it.next());
            }
        }
        return out;
    }

}
