package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The versioned store under a real topology, which is the only place its point is visible.
 *
 * <p>A versioned table changes what a stream-table join <em>means</em>: the lookup is as of the
 * stream record's timestamp rather than of whatever the table happens to hold now. So a late stream
 * record joins against the value that was current when it happened. That behaviour is the store's
 * {@code get(key, asOf)} seen from the outside, and it is also the check that Streams accepts this
 * supplier as a versioned one at all — an ordinary store here would silently give present-tense
 * answers instead of failing.
 */
class ElysiumKVVersionedTopologyTest {
    private static final String TABLE_TOPIC = "prices";
    private static final String STREAM_TOPIC = "orders";
    private static final String OUTPUT = "priced-orders";
    private static final String STORE = "price-history";

    private static Properties config(Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "elysiumkv-versioned-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    /** Orders joined against the price that applied when the order was placed. */
    private static Topology joinTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> prices = builder.table(
                TABLE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as(ElysiumKVVersionedBytesStoreSupplier.of(
                        STORE, ElysiumKVStoreConfig.local(), Duration.ofHours(1))));

        KStream<String, String> orders =
                builder.stream(STREAM_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        // No grace period: the stream side is not buffered, so each order joins as it arrives. The
        // as-of lookup is the table being versioned, not the grace, so this keeps the two apart.
        orders.join(prices, (order, price) -> order + "@" + price,
                    Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "join"))
              .to(OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    /**
     * A late order is priced at the time it happened. Both prices are in the table before the
     * order is processed, so a non-versioned table would answer with the newer one.
     */
    @Test
    void aLateRecordJoinsAgainstThePriceThatAppliedThen(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(joinTopology(), config(dir))) {
            TestInputTopic<String, String> prices = driver.createInputTopic(
                    TABLE_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> orders = driver.createInputTopic(
                    STREAM_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    OUTPUT, Serdes.String().deserializer(), Serdes.String().deserializer());

            prices.pipeInput("widget", "10", Instant.ofEpochMilli(1_000L));
            prices.pipeInput("widget", "20", Instant.ofEpochMilli(5_000L));

            // Placed between the two price changes, but processed after both.
            orders.pipeInput("widget", "order-1", Instant.ofEpochMilli(3_000L));

            assertEquals(List.of("order-1@10"), output.readValuesToList(),
                         "the price at 3000 was 10, not the current 20");
        }
    }

    /** And an order after the change gets the new price, so the lookup is not simply stuck. */
    @Test
    void anOrderAfterThePriceChangeGetsTheNewPrice(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(joinTopology(), config(dir))) {
            TestInputTopic<String, String> prices = driver.createInputTopic(
                    TABLE_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> orders = driver.createInputTopic(
                    STREAM_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    OUTPUT, Serdes.String().deserializer(), Serdes.String().deserializer());

            prices.pipeInput("widget", "10", Instant.ofEpochMilli(1_000L));
            prices.pipeInput("widget", "20", Instant.ofEpochMilli(5_000L));
            orders.pipeInput("widget", "order-2", Instant.ofEpochMilli(9_000L));

            assertEquals(List.of("order-2@20"), output.readValuesToList());
        }
    }

    /** An order before the key existed joins with nothing. */
    @Test
    void anOrderBeforeThePriceExistedDoesNotJoin(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(joinTopology(), config(dir))) {
            TestInputTopic<String, String> prices = driver.createInputTopic(
                    TABLE_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> orders = driver.createInputTopic(
                    STREAM_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    OUTPUT, Serdes.String().deserializer(), Serdes.String().deserializer());

            prices.pipeInput("widget", "10", Instant.ofEpochMilli(5_000L));
            orders.pipeInput("widget", "order-3", Instant.ofEpochMilli(1_000L));

            assertEquals(List.of(), output.readValuesToList());
        }
    }

    /**
     * Streams hands the store back as a {@link VersionedKeyValueStore}, which is what a versioned
     * KTable's materialization is supposed to be. Reading it directly also proves the table wrote
     * versions rather than overwriting.
     */
    @Test
    void theMaterializedStoreIsAVersionedOne(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(joinTopology(), config(dir))) {
            TestInputTopic<String, String> prices = driver.createInputTopic(
                    TABLE_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
            prices.pipeInput("widget", "10", Instant.ofEpochMilli(1_000L));
            prices.pipeInput("widget", "20", Instant.ofEpochMilli(5_000L));

            VersionedKeyValueStore<String, String> store =
                    driver.getVersionedKeyValueStore(STORE);

            assertEquals("20", store.get("widget").value(), "the current price");
            VersionedRecord<String> asOf = store.get("widget", 3_000L);
            assertEquals("10", asOf.value(), "the price at 3000");
            assertEquals(1_000L, asOf.timestamp(), "carrying the time it was set");
            assertNull(store.get("widget", 999L), "before the key existed");
        }
    }

    /** A tombstone in the table is a version: the key disappears, its past does not. */
    @Test
    void aDeletedKeyKeepsItsPast(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(joinTopology(), config(dir))) {
            TestInputTopic<String, String> prices = driver.createInputTopic(
                    TABLE_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
            prices.pipeInput("widget", "10", Instant.ofEpochMilli(1_000L));
            prices.pipeInput("widget", null, Instant.ofEpochMilli(5_000L));

            VersionedKeyValueStore<String, String> store =
                    driver.getVersionedKeyValueStore(STORE);

            assertNull(store.get("widget"), "gone as of now");
            assertEquals("10", store.get("widget", 3_000L).value(), "but not as of before");
        }
    }
}
