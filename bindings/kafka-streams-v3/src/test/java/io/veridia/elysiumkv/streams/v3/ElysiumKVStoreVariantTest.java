package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two store variants, and what happens when the wrong one is used.
 *
 * <p>The supplier hands out a timestamped store by default, because that is what a KTable needs.
 * A Processor-API store built with {@code Stores.keyValueStoreBuilder} needs the plain one — and the
 * distinction is not cosmetic: Streams decides whether restored changelog values need a timestamp
 * prepended by looking at the store's marker interface, while the live write path is decided by the
 * builder. Mismatch them and a store writes one format live and a different one on restore.
 */
class ElysiumKVStoreVariantTest {
    private static final String INPUT = "input";
    private static final String STORE = "plain-state";

    /** A Processor-API topology that writes what it reads straight into the store. */
    private static Topology processorTopology(KeyValueBytesStoreSupplier supplier) {
        Topology topology = new Topology();
        topology.addSource("src", Serdes.String().deserializer(), Serdes.Long().deserializer(),
                           INPUT)
                .addProcessor("proc", () -> new Processor<String, Long, Void, Void>() {
                    private KeyValueStore<String, Long> store;

                    @Override
                    public void init(ProcessorContext<Void, Void> context) {
                        store = context.getStateStore(STORE);
                    }

                    @Override
                    public void process(Record<String, Long> record) {
                        store.put(record.key(), record.value());
                    }
                }, "src")
                .addStateStore(Stores.keyValueStoreBuilder(supplier, Serdes.String(),
                                                           Serdes.Long()), "proc");
        return topology;
    }

    private static Properties config(Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "elysiumkv-variant-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    /** The plain variant in the position it is for: a Processor-API store. */
    @Test
    void thePlainVariantBacksAProcessorApiStore(@TempDir Path dir) {
        Topology topology = processorTopology(
                ElysiumKVKeyValueBytesStoreSupplier.plain(STORE, ElysiumKVStoreConfig.local()));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(dir))) {
            TestInputTopic<String, Long> input =
                    driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                            Serdes.Long().serializer());
            input.pipeInput("a", 7L);

            assertEquals(7L, driver.<String, Long>getKeyValueStore(STORE).get("a"));
        }
    }

    /**
     * <b>The mismatch is refused at startup.</b> Left to run, this store would write bare values on
     * the live path and timestamp-prefixed values on restore — a divergence that shows up only after
     * a rebalance, as corrupt reads far from the code that caused them. An exception naming the
     * remedy is a better outcome than that, so the store refuses to open rather than half-work.
     */
    @Test
    void theTimestampedVariantRefusesToBeAPlainStore(@TempDir Path dir) {
        Topology topology = processorTopology(
                new ElysiumKVKeyValueBytesStoreSupplier(STORE, ElysiumKVStoreConfig.local()));

        Exception failure = assertThrows(Exception.class,
                                         () -> new TopologyTestDriver(topology, config(dir)));

        String message = rootCause(failure).getMessage();
        assertTrue(message.contains("plain"),
                   () -> "the failure should name the fix, but said: " + message);
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
