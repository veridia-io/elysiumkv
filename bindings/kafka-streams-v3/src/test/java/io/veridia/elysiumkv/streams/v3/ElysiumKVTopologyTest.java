package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The store driven by a <em>real topology</em>, rather than called directly.
 *
 * <p>The contract tests next door prove the store behaves like a {@code KeyValueStore}. They do not
 * prove Streams can drive it, and that is a different question: Streams owns the lifecycle — it
 * decides when {@code init} happens and with what context, when {@code flush} happens relative to a
 * commit, when the store is closed and reopened — and it reaches the store through the DSL and
 * interactive queries rather than through the interface directly.
 *
 * <p>{@link TopologyTestDriver} runs that for real, minus the broker: a genuine {@code
 * StateStoreContext}, genuine commit points, a genuine changelog. What it cannot exercise is a
 * rebalance, restore-from-changelog after data loss, or standby tasks — those need a cluster and
 * belong to an integration suite.
 */
class ElysiumKVTopologyTest {
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String STORE = "totals";
    private static final String APPLICATION_ID = "elysiumkv-topology-test";

    /** A running total per key, materialized into the store under test. */
    private static Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.Long()))
               .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
               .reduce(Long::sum,
                       Materialized.<String, Long>as(new ElysiumKVKeyValueBytesStoreSupplier(
                                       STORE, ElysiumKVStoreConfig.local()))
                               .withKeySerde(Serdes.String())
                               .withValueSerde(Serdes.Long()))
               .toStream()
               .to(OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    /**
     * The same topology with the caching layer removed. {@code CachingKeyValueStore} keeps a
     * position of its own and answers {@code getPosition()} from it, so with caching on, the value
     * a caller sees never comes from the store underneath. Disabling it is what puts this store on
     * the answering end — and is also the configuration where reporting an empty position would be
     * a real defect rather than a shadowed one.
     */
    private static Topology uncachedTopology() {
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    /**
     * The whole point: an aggregation whose correctness depends on the store returning what was put
     * into it. A reduce reads the previous value for every record after the first, so a store that
     * silently lost or reordered anything produces wrong totals rather than an error.
     */
    @Test
    void anAggregationOverTheStoreProducesCorrectTotals(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            TestOutputTopic<String, Long> output =
                    driver.createOutputTopic(OUTPUT, Serdes.String().deserializer(),
                                             Serdes.Long().deserializer());

            input.pipeInput("a", 1L);
            input.pipeInput("b", 10L);
            input.pipeInput("a", 2L);
            input.pipeInput("a", 3L);
            input.pipeInput("b", 20L);

            List<KeyValue<String, Long>> emitted = output.readKeyValuesToList();
            assertEquals(List.of(KeyValue.pair("a", 1L), KeyValue.pair("b", 10L),
                                 KeyValue.pair("a", 3L), KeyValue.pair("a", 6L),
                                 KeyValue.pair("b", 30L)),
                         emitted,
                         "each record's output is the running total, so every one of these came "
                         + "from a read of the previous value out of the store");

            KeyValueStore<String, Long> store = driver.getKeyValueStore(STORE);
            assertEquals(6L, store.get("a"));
            assertEquals(30L, store.get("b"));
            assertNull(store.get("absent"));
        }
    }

    /**
     * Phase 1's central claim is that Streams' fault tolerance is unchanged: the changelog is
     * still written, so the store is still restorable by the ordinary mechanism. If the adapter had
     * quietly marked itself non-persistent or non-logged, everything else here would still pass and
     * a pod loss would lose the state.
     */
    @Test
    void theChangelogIsStillWritten(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 1L);
            input.pipeInput("a", 2L);

            TestOutputTopic<String, Long> changelog =
                    driver.createOutputTopic(APPLICATION_ID + "-" + STORE + "-changelog",
                                             Serdes.String().deserializer(),
                                             Serdes.Long().deserializer());
            assertEquals(List.of(KeyValue.pair("a", 1L), KeyValue.pair("a", 3L)),
                         changelog.readKeyValuesToList(),
                         "every update to the store reached the changelog");
        }
    }

    /** Interactive queries go through the same store: range, all, and the entry estimate. */
    @Test
    void interactiveQueriesReachTheStore(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            for (String key : List.of("k1", "k2", "k3", "k4", "k5")) {
                input.pipeInput(key, 1L);
            }

            KeyValueStore<String, Long> store = driver.getKeyValueStore(STORE);

            List<String> ranged = new ArrayList<>();
            try (KeyValueIterator<String, Long> it = store.range("k2", "k4")) {
                while (it.hasNext()) ranged.add(it.next().key);
            }
            assertEquals(List.of("k2", "k3", "k4"), ranged, "Streams' range is inclusive both ends");

            List<String> all = new ArrayList<>();
            try (KeyValueIterator<String, Long> it = store.all()) {
                while (it.hasNext()) all.add(it.next().key);
            }
            assertEquals(List.of("k1", "k2", "k3", "k4", "k5"), all);

            // Records, so an upper bound on live keys rather than a count of them.
            assertTrue(store.approximateNumEntries() >= 5,
                       "an upper bound on the five distinct keys");
        }
    }

    /**
     * A tombstone through the DSL. Streams writes a null value to delete, and the aggregation's
     * store has to treat that as an absence rather than as a value — otherwise a deleted key comes
     * back on the next read.
     */
    @Test
    void deletesThroughTheStoreRemoveTheKey(@TempDir Path dir) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 5L);

            KeyValueStore<String, Long> store = driver.getKeyValueStore(STORE);
            assertEquals(5L, store.get("a"));

            store.delete("a");
            assertNull(store.get("a"), "a deleted key must not reappear");

            // And the store keeps working afterwards, rather than the tombstone poisoning reads.
            input.pipeInput("a", 7L);
            assertEquals(7L, store.get("a"), "the aggregation restarted from nothing, as it should");
        }
    }

    /**
     * The store reports how far it has advanced through its input topic. {@code
     * getPosition()} is not optional decoration: its default implementation throws, and Streams'
     * caching layer calls it on every commit — so a store that leaves it alone cannot be
     * materialized at all. It fails at the first commit rather than at construction, which is why
     * calling the store directly never revealed it.
     *
     * <p>Asserting the value, not merely that it does not throw: a position that stayed empty would
     * satisfy the topology tests above while being useless to anything that reads it.
     */
    @Test
    void theStoreReportsHowFarItHasAdvancedThroughTheInputTopic(@TempDir Path dir) {
        try (TopologyTestDriver driver =
                     new TopologyTestDriver(uncachedTopology(), config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            KeyValueStore<String, Long> store = driver.getKeyValueStore(STORE);
            assertTrue(store.getPosition().isEmpty(), "nothing consumed, nothing to report");

            input.pipeInput("a", 1L);
            input.pipeInput("a", 2L);
            input.pipeInput("b", 3L);

            Position position = store.getPosition();
            assertEquals(Set.of(INPUT), position.getTopics(), "the position names the input topic");
            assertEquals(2L, position.getPartitionPositions(INPUT).get(0),
                         "three records at offsets 0..2 leave the position at the last of them");
        }
    }

    /**
     * State survives the task being torn down and rebuilt on the same directory, which is
     * what a persistent store is for. The second driver reads what the first wrote from disk rather
     * than from memory — nothing else in this suite proves the on-disk format can be reopened.
     *
     * <p>The copy is not decoration. {@link TopologyTestDriver#close()} ends with {@code
     * StateDirectory.clean()}, which deletes the task directories — so a second driver pointed at
     * the same path would find nothing, and the test would fail whether or not the store persists.
     * Taking the state aside after a flush and pointing the second driver at the copy is what makes
     * the assertion about the store instead of about the harness.
     */
    @Test
    void stateSurvivesCloseAndReopenOnTheSameDirectory(@TempDir Path dir) throws Exception {
        Path live = dir.resolve("live");
        Path preserved = dir.resolve("preserved");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology(), config(live))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 4L);
            input.pipeInput("a", 6L);

            KeyValueStore<String, Long> store = driver.getKeyValueStore(STORE);
            assertEquals(10L, store.get("a"));

            // Durable before the copy, so what lands in `preserved` is a state the engine can open.
            store.flush();
            copyTree(live, preserved);
        }

        try (TopologyTestDriver reopened =
                     new TopologyTestDriver(topology(), config(preserved))) {
            KeyValueStore<String, Long> store = reopened.getKeyValueStore(STORE);
            assertNotNull(store, "the store reopened on the existing directory");
            assertEquals(10L, store.get("a"),
                         "the total written before the restart was read back off disk");

            // And the reopened store is writable, not merely readable: the aggregation continues
            // from the recovered total rather than restarting from nothing.
            reopened.createInputTopic(INPUT, Serdes.String().serializer(),
                                      Serdes.Long().serializer())
                    .pipeInput("a", 5L);
            assertEquals(15L, store.get("a"), "the reduce resumed from the recovered value");
        }
    }

    private static void copyTree(Path from, Path to) throws Exception {
        try (Stream<Path> tree = Files.walk(from)) {
            for (Path source : tree.collect(Collectors.toList())) {
                Path target = to.resolve(from.relativize(source).toString());
                if (Files.isDirectory(source)) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }
}
