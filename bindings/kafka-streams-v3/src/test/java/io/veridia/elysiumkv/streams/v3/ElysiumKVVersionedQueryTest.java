package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * IQv2 against the versioned store.
 *
 * <p>Unlike the timestamped queries, which Streams' metered layer answers from the plain surface,
 * these two reach the store itself: the metered layer serializes the key, hands the query down, and
 * deserializes what comes back with the <em>plain</em> value serde. A store that does not implement
 * them fails the query outright with {@code UNKNOWN_QUERY_TYPE}, which is what this store did before
 * these tests existed.
 */
class ElysiumKVVersionedQueryTest {
    private static final String TOPIC = "prices";
    private static final String STORE = "price-history";

    private static Properties config(Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "elysiumkv-versioned-query-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    private static Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(TOPIC, Consumed.with(Serdes.String(), Serdes.String()),
                      Materialized.as(ElysiumKVVersionedBytesStoreSupplier.of(
                              STORE, ElysiumKVStoreConfig.local(), Duration.ofHours(1))));
        return builder.build();
    }

    /** Three versions of one key, and a second key that must never appear in the answers. */
    private static TopologyTestDriver driverWithHistory(Path dir) {
        TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir));
        TestInputTopic<String, String> in = driver.createInputTopic(
                TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        in.pipeInput("widget", "10", Instant.ofEpochMilli(1_000L));
        in.pipeInput("widget", "20", Instant.ofEpochMilli(5_000L));
        in.pipeInput("widget", "30", Instant.ofEpochMilli(9_000L));
        in.pipeInput("gadget", "99", Instant.ofEpochMilli(5_000L));
        return driver;
    }

    private static StateStore store(TopologyTestDriver driver) {
        return (StateStore) driver.getVersionedKeyValueStore(STORE);
    }

    private static <R> R answer(QueryResult<R> result) {
        assertTrue(result.isSuccess(),
                   () -> "query failed: " + result.getFailureReason() + " / "
                         + result.getFailureMessage());
        return result.getResult();
    }

    // --- VersionedKeyQuery ----------------------------------------------------

    @Test
    void aVersionedKeyQueryWithoutATimeReturnsTheCurrentValue(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            VersionedRecord<String> record = answer(store(driver).query(
                    VersionedKeyQuery.<String, String>withKey("widget"),
                    PositionBound.unbounded(), new QueryConfig(false)));

            assertEquals("30", record.value());
            assertEquals(9_000L, record.timestamp());
        }
    }

    @Test
    void aVersionedKeyQueryAsOfATimeReturnsTheVersionThatApplied(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            VersionedRecord<String> record = answer(store(driver).query(
                    VersionedKeyQuery.<String, String>withKey("widget")
                            .asOf(Instant.ofEpochMilli(6_000L)),
                    PositionBound.unbounded(), new QueryConfig(false)));

            assertEquals("20", record.value(), "the version in force at 6000");
            assertEquals(5_000L, record.timestamp(), "reported as of when it was set");
        }
    }

    /** A key with no value at that time is an empty answer, not a failed query. */
    @Test
    void aVersionedKeyQueryBeforeTheKeyExistedSucceedsWithNothing(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            assertNull(answer(store(driver).query(
                    VersionedKeyQuery.<String, String>withKey("widget")
                            .asOf(Instant.ofEpochMilli(500L)),
                    PositionBound.unbounded(), new QueryConfig(false))));

            assertNull(answer(store(driver).query(
                    VersionedKeyQuery.<String, String>withKey("absent"),
                    PositionBound.unbounded(), new QueryConfig(false))));
        }
    }

    // --- MultiVersionedKeyQuery -----------------------------------------------

    private static List<String> drain(VersionedRecordIterator<String> it) {
        List<String> out = new ArrayList<>();
        try (VersionedRecordIterator<String> open = it) {
            while (open.hasNext()) {
                VersionedRecord<String> record = open.next();
                out.add(record.timestamp() + ":" + record.value() + "->"
                        + record.validTo().map(String::valueOf).orElse("now"));
            }
        }
        return out;
    }

    @Test
    void aMultiVersionedKeyQueryReturnsEveryVersionOldestFirst(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            assertEquals(List.of("1000:10->5000", "5000:20->9000", "9000:30->now"),
                         drain(answer(store(driver).query(
                                 MultiVersionedKeyQuery.<String, String>withKey("widget"),
                                 PositionBound.unbounded(), new QueryConfig(false)))),
                         "each version carries when it stopped applying");
        }
    }

    @Test
    void descendingOrderReversesTheVersions(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            assertEquals(List.of("9000:30->now", "5000:20->9000", "1000:10->5000"),
                         drain(answer(store(driver).query(
                                 MultiVersionedKeyQuery.<String, String>withKey("widget")
                                         .withDescendingTimestamps(),
                                 PositionBound.unbounded(), new QueryConfig(false)))));
        }
    }

    /**
     * <b>The interval selects versions that were <em>in force</em>, not versions written then.</b>
     * The version set at 1000 is still the answer at 2000, so a query from 2000 must include it even
     * though nothing was written in that window.
     */
    @Test
    void aTimeRangeIncludesTheVersionAlreadyInForceWhenItBegan(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            assertEquals(List.of("1000:10->5000", "5000:20->9000"),
                         drain(answer(store(driver).query(
                                 MultiVersionedKeyQuery.<String, String>withKey("widget")
                                         .fromTime(Instant.ofEpochMilli(2_000L))
                                         .toTime(Instant.ofEpochMilli(6_000L)),
                                 PositionBound.unbounded(), new QueryConfig(false)))),
                         "the version in force at 2000 was set at 1000");
        }
    }

    /** A version superseded exactly when the interval opens is no longer in force. */
    @Test
    void aVersionSupersededAtTheLowerBoundIsExcluded(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            assertEquals(List.of("5000:20->9000", "9000:30->now"),
                         drain(answer(store(driver).query(
                                 MultiVersionedKeyQuery.<String, String>withKey("widget")
                                         .fromTime(Instant.ofEpochMilli(5_000L)),
                                 PositionBound.unbounded(), new QueryConfig(false)))),
                         "the version set at 1000 ended exactly at 5000");
        }
    }

    @Test
    void aMultiVersionedKeyQueryReturnsOnlyTheKeyAskedFor(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            assertEquals(List.of("5000:99->now"),
                         drain(answer(store(driver).query(
                                 MultiVersionedKeyQuery.<String, String>withKey("gadget"),
                                 PositionBound.unbounded(), new QueryConfig(false)))));
        }
    }

    @Test
    void aMultiVersionedKeyQueryForAnAbsentKeyIsEmpty(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            assertEquals(List.of(),
                         drain(answer(store(driver).query(
                                 MultiVersionedKeyQuery.<String, String>withKey("absent"),
                                 PositionBound.unbounded(), new QueryConfig(false)))));
        }
    }

    /** A deletion is not a value: it bounds the version before it and returns nothing itself. */
    @Test
    void aDeletedVersionIsNotReturnedButStillEndsThePreviousOne(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            TestInputTopic<String, String> in = driver.createInputTopic(
                    TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
            in.pipeInput("widget", "10", Instant.ofEpochMilli(1_000L));
            in.pipeInput("widget", null, Instant.ofEpochMilli(5_000L));

            assertEquals(List.of("1000:10->5000"),
                         drain(answer(store(driver).query(
                                 MultiVersionedKeyQuery.<String, String>withKey("widget"),
                                 PositionBound.unbounded(), new QueryConfig(false)))),
                         "the tombstone is absent, but it is why the value ended at 5000");
        }
    }

    /** A bound this store has not reached is refused rather than answered with older state. */
    @Test
    void aPositionBoundBeyondWhatTheStoreHasSeenIsRefused(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            QueryResult<VersionedRecord<String>> result = store(driver).query(
                    VersionedKeyQuery.<String, String>withKey("widget"),
                    PositionBound.at(org.apache.kafka.streams.query.Position.emptyPosition()
                                             .withComponent(TOPIC, 0, Long.MAX_VALUE)),
                    new QueryConfig(false));

            assertTrue(result.isFailure());
            assertEquals(org.apache.kafka.streams.query.FailureReason.NOT_UP_TO_BOUND,
                         result.getFailureReason());
        }
    }

    @Test
    void anUnknownQueryTypeIsReportedAsSuch(@TempDir Path dir) {
        try (TopologyTestDriver driver = driverWithHistory(dir)) {
            QueryResult<Object> result = store(driver).query(
                    new org.apache.kafka.streams.query.Query<Object>() {},
                    PositionBound.unbounded(), new QueryConfig(false));

            assertTrue(result.isFailure());
            assertEquals(org.apache.kafka.streams.query.FailureReason.UNKNOWN_QUERY_TYPE,
                         result.getFailureReason());
        }
    }
}
